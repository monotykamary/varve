use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, Ident, ObjectNamePart, Query, Select, SelectFlavor, SetExpr, Statement,
    TableFactor, UnaryOperator, Value, Visit, Visitor,
};
use sqlparser::dialect::DuckDbDialect;
use sqlparser::parser::Parser;

use crate::query::QueryCatalog;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanPlan {
    pub table: String,
    pub rollup: bool,
    pub rollup_width_us: Option<i64>,
    pub tenant: Option<String>,
    pub series: Option<String>,
    pub start_us: Option<i64>,
    pub end_us: Option<i64>,
    pub empty: bool,
}

#[allow(dead_code)]
pub fn plan(sql: &str, table_names: &[String]) -> Option<ScanPlan> {
    plan_with_catalog(sql, table_names, &QueryCatalog::default())
}

pub fn plan_with_catalog(
    sql: &str,
    table_names: &[String],
    catalog: &QueryCatalog,
) -> Option<ScanPlan> {
    let dialect = DuckDbDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql).ok()?;
    if statements.len() != 1 {
        return None;
    }

    let Statement::Query(query) = statements.pop()? else {
        return None;
    };

    let mut query_counter = QueryCounter::default();
    let _ = query.visit(&mut query_counter);
    if !is_supported_query(&query) {
        return None;
    }

    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if !is_supported_select(select) {
        return None;
    }
    if select.from.is_empty() || metadata_only_select(select, catalog) {
        return (query_counter.count == 1).then(storage_free_plan);
    }

    let source = base_source(&query)?;
    if source.query_count != query_counter.count {
        // A derived-table chain is the only accepted location for nested queries.
        return None;
    }
    if source.query_count > 1 && query.visit(&mut NestedChainValidator).is_break() {
        return None;
    }

    let (table, rollup, rollup_width_us) = resolve_table(source.relation, table_names, catalog)?;
    let qualifier = source.alias.unwrap_or(source.relation);
    let bounds = if rollup {
        // Raw timestamps and aggregate bucket timestamps have different semantics.
        Bounds::default()
    } else {
        source
            .selection
            .map(|selection| extract_bounds(selection, qualifier))
            .unwrap_or_default()
    };
    let series = source
        .selection
        .map(|selection| extract_series(selection, qualifier))
        .unwrap_or_default();

    Some(ScanPlan {
        table,
        rollup,
        rollup_width_us,
        tenant: series.tenant,
        series: series.series,
        start_us: bounds.start,
        end_us: bounds.end,
        empty: bounds.empty || series.empty,
    })
}

impl ScanPlan {
    pub(crate) fn proves_single_storage_source(&self) -> bool {
        !self.table.is_empty()
    }

    pub fn matches_series(&self, tenant: &str, series: &str) -> bool {
        !self.empty
            && self
                .tenant
                .as_deref()
                .is_none_or(|expected| tenant == expected)
            && self
                .series
                .as_deref()
                .is_none_or(|expected| series == expected)
    }
}

#[derive(Default)]
struct SeriesFilter {
    tenant: Option<String>,
    series: Option<String>,
    empty: bool,
}

impl SeriesFilter {
    fn intersection(self, other: Self) -> Self {
        let conflict = self
            .tenant
            .as_ref()
            .zip(other.tenant.as_ref())
            .is_some_and(|(left, right)| left != right)
            || self
                .series
                .as_ref()
                .zip(other.series.as_ref())
                .is_some_and(|(left, right)| left != right);
        Self {
            tenant: self.tenant.or(other.tenant),
            series: self.series.or(other.series),
            empty: self.empty || other.empty || conflict,
        }
    }
}

fn extract_series(expr: &Expr, qualifier: &Ident) -> SeriesFilter {
    match expr {
        Expr::Nested(inner) => extract_series(inner, qualifier),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => extract_series(left, qualifier).intersection(extract_series(right, qualifier)),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            for (column, value) in [
                (left.as_ref(), right.as_ref()),
                (right.as_ref(), left.as_ref()),
            ] {
                let Some(value) = string_constant(value) else {
                    continue;
                };
                if is_column(column, qualifier, "tenant") {
                    return SeriesFilter {
                        tenant: Some(value.into()),
                        ..Default::default()
                    };
                }
                if is_column(column, qualifier, "series") {
                    return SeriesFilter {
                        series: Some(value.into()),
                        ..Default::default()
                    };
                }
            }
            SeriesFilter::default()
        }
        // No inference through OR, NOT, casts, functions, parameters or collations.
        _ => SeriesFilter::default(),
    }
}

fn string_constant(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Nested(inner) => string_constant(inner),
        Expr::Value(value) => match &value.value {
            Value::SingleQuotedString(value) => Some(value),
            _ => None,
        },
        _ => None,
    }
}

fn storage_free_plan() -> ScanPlan {
    ScanPlan {
        // Valid Varve table names are non-empty, so this cannot select a storage table.
        table: String::new(),
        rollup: false,
        rollup_width_us: None,
        tenant: None,
        series: None,
        start_us: None,
        end_us: None,
        empty: true,
    }
}

#[derive(Default)]
struct QueryCounter {
    count: usize,
}

impl Visitor for QueryCounter {
    type Break = ();

    fn pre_visit_query(&mut self, _query: &Query) -> ControlFlow<Self::Break> {
        self.count += 1;
        ControlFlow::Continue(())
    }
}

struct NestedChainValidator;

impl Visitor for NestedChainValidator {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        if matches!(expr, Expr::Collate { .. }) {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    }
}

struct BaseSource<'a> {
    relation: &'a Ident,
    alias: Option<&'a Ident>,
    selection: Option<&'a Expr>,
    query_count: usize,
}

fn base_source(query: &Query) -> Option<BaseSource<'_>> {
    if !is_supported_query(query) {
        return None;
    }
    let SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    if !is_supported_select(select) || select.from.len() != 1 {
        return None;
    }
    let from = &select.from[0];
    if !from.joins.is_empty() {
        return None;
    }

    if let Some((relation, alias)) = plain_table(&from.relation) {
        return Some(BaseSource {
            relation,
            alias,
            selection: select.selection.as_ref(),
            query_count: 1,
        });
    }

    let TableFactor::Derived {
        lateral,
        subquery,
        alias,
        sample,
    } = &from.relation
    else {
        return None;
    };
    if *lateral
        || sample.is_some()
        || alias
            .as_ref()
            .is_some_and(|alias| !alias.columns.is_empty())
    {
        return None;
    }
    let mut source = base_source(subquery)?;
    source.query_count += 1;
    Some(source)
}

fn is_supported_query(query: &Query) -> bool {
    query.with.is_none()
        && query.locks.is_empty()
        && query.for_clause.is_none()
        && query.settings.is_none()
        && query.format_clause.is_none()
        && query.pipe_operators.is_empty()
}

fn is_supported_select(select: &Select) -> bool {
    select.optimizer_hint.is_none()
        && select.select_modifiers.is_none()
        && select.top.is_none()
        && !select.top_before_distinct
        && select.exclude.is_none()
        && select.into.is_none()
        && select.lateral_views.is_empty()
        && select.prewhere.is_none()
        && select.connect_by.is_empty()
        && select.cluster_by.is_empty()
        && select.distribute_by.is_empty()
        && select.sort_by.is_empty()
        && select.value_table_mode.is_none()
        && select.flavor == SelectFlavor::Standard
}

fn metadata_only_select(select: &Select, catalog: &QueryCatalog) -> bool {
    !select.from.is_empty()
        && select.from.iter().all(|from| {
            known_catalog_macro(&from.relation, catalog)
                && from
                    .joins
                    .iter()
                    .all(|join| known_catalog_macro(&join.relation, catalog))
        })
}

fn known_catalog_macro(relation: &TableFactor, catalog: &QueryCatalog) -> bool {
    let TableFactor::Table {
        name,
        alias,
        args: Some(args),
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
    } = relation
    else {
        return false;
    };
    if !args.args.is_empty()
        || args.settings.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
        || alias
            .as_ref()
            .is_some_and(|alias| !alias.columns.is_empty())
    {
        return false;
    }
    let [ObjectNamePart::Identifier(name)] = name.0.as_slice() else {
        return false;
    };
    catalog
        .relations
        .iter()
        .any(|relation| identifier_matches(name, &relation.name))
}

fn plain_table(relation: &TableFactor) -> Option<(&Ident, Option<&Ident>)> {
    let TableFactor::Table {
        name,
        alias,
        args,
        with_hints,
        version,
        with_ordinality,
        partitions,
        json_path,
        sample,
        index_hints,
    } = relation
    else {
        return None;
    };

    if args.is_some()
        || !with_hints.is_empty()
        || version.is_some()
        || *with_ordinality
        || !partitions.is_empty()
        || json_path.is_some()
        || sample.is_some()
        || !index_hints.is_empty()
        || alias
            .as_ref()
            .is_some_and(|alias| !alias.columns.is_empty())
    {
        return None;
    }

    let [ObjectNamePart::Identifier(name)] = name.0.as_slice() else {
        return None;
    };
    Some((name, alias.as_ref().map(|alias| &alias.name)))
}

fn resolve_table(
    relation: &Ident,
    table_names: &[String],
    catalog: &QueryCatalog,
) -> Option<(String, bool, Option<i64>)> {
    let mut candidates = Vec::new();

    for table in table_names {
        if identifier_matches(relation, table) {
            candidates.push((table.clone(), false, None));
        }

        if let Some(base) = strip_rollup_suffix(&relation.value)
            && base.eq_ignore_ascii_case(table)
        {
            candidates.push((table.clone(), true, None));
        }
    }

    for alias in &catalog.aggregates {
        if identifier_matches(relation, &alias.name) {
            for table in table_names {
                if table.eq_ignore_ascii_case(&alias.source) {
                    candidates.push((table.clone(), true, Some(alias.width_us)));
                }
            }
        }
    }

    if candidates.len() == 1 {
        candidates.pop()
    } else {
        None
    }
}

fn strip_rollup_suffix(name: &str) -> Option<&str> {
    const SUFFIX: &str = "__rollup";
    let suffix_start = name.len().checked_sub(SUFFIX.len())?;
    let suffix = name.get(suffix_start..)?;
    if suffix.eq_ignore_ascii_case(SUFFIX) {
        name.get(..suffix_start)
    } else {
        None
    }
}

fn identifier_matches(identifier: &Ident, expected: &str) -> bool {
    identifier.value.eq_ignore_ascii_case(expected)
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Bounds {
    start: Option<i64>,
    end: Option<i64>,
    empty: bool,
}

impl Bounds {
    fn empty() -> Self {
        Self {
            start: None,
            end: None,
            empty: true,
        }
    }

    fn starting_at(start: i64) -> Self {
        Self {
            start: Some(start),
            ..Self::default()
        }
    }

    fn ending_before(end: i64) -> Self {
        if end == i64::MIN {
            Self::empty()
        } else {
            Self {
                end: Some(end),
                ..Self::default()
            }
        }
    }

    fn intersection(self, other: Self) -> Self {
        if self.empty || other.empty {
            return Self::empty();
        }

        let start = match (self.start, other.start) {
            (Some(left), Some(right)) => Some(left.max(right)),
            (left, right) => left.or(right),
        };
        let end = match (self.end, other.end) {
            (Some(left), Some(right)) => Some(left.min(right)),
            (left, right) => left.or(right),
        };

        if matches!((start, end), (Some(start), Some(end)) if start >= end) {
            Self::empty()
        } else {
            Self {
                start,
                end,
                empty: false,
            }
        }
    }
}

fn extract_bounds(expr: &Expr, qualifier: &Ident) -> Bounds {
    match expr {
        Expr::Nested(inner) => extract_bounds(inner, qualifier),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => extract_bounds(left, qualifier).intersection(extract_bounds(right, qualifier)),
        Expr::BinaryOp { left, op, right } => comparison_bounds(left, op, right, qualifier),
        Expr::Between {
            expr,
            negated: false,
            low,
            high,
        } if is_timestamp(expr, qualifier) => {
            let (Some(low), Some(high)) = (integer_constant(low), integer_constant(high)) else {
                return Bounds::default();
            };
            comparison_for_value(&BinaryOperator::GtEq, low)
                .intersection(comparison_for_value(&BinaryOperator::LtEq, high))
        }
        _ => Bounds::default(),
    }
}

fn comparison_bounds(
    left: &Expr,
    operator: &BinaryOperator,
    right: &Expr,
    qualifier: &Ident,
) -> Bounds {
    if is_timestamp(left, qualifier)
        && let Some(value) = integer_constant(right)
    {
        return comparison_for_value(operator, value);
    }

    if is_timestamp(right, qualifier)
        && let Some(value) = integer_constant(left)
    {
        return reversed_comparison_for_value(operator, value);
    }

    Bounds::default()
}

fn comparison_for_value(operator: &BinaryOperator, value: i64) -> Bounds {
    match operator {
        BinaryOperator::Gt => value
            .checked_add(1)
            .map(Bounds::starting_at)
            .unwrap_or_else(Bounds::empty),
        BinaryOperator::GtEq => Bounds::starting_at(value),
        BinaryOperator::Lt => Bounds::ending_before(value),
        BinaryOperator::LtEq => value
            .checked_add(1)
            .map(Bounds::ending_before)
            .unwrap_or_default(),
        BinaryOperator::Eq => {
            let lower = Bounds::starting_at(value);
            value
                .checked_add(1)
                .map(Bounds::ending_before)
                .map(|upper| lower.intersection(upper))
                .unwrap_or(lower)
        }
        _ => Bounds::default(),
    }
}

fn reversed_comparison_for_value(operator: &BinaryOperator, value: i64) -> Bounds {
    let reversed = match operator {
        BinaryOperator::Gt => BinaryOperator::Lt,
        BinaryOperator::GtEq => BinaryOperator::LtEq,
        BinaryOperator::Lt => BinaryOperator::Gt,
        BinaryOperator::LtEq => BinaryOperator::GtEq,
        BinaryOperator::Eq => BinaryOperator::Eq,
        _ => return Bounds::default(),
    };
    comparison_for_value(&reversed, value)
}

fn is_timestamp(expr: &Expr, qualifier: &Ident) -> bool {
    is_column(expr, qualifier, "timestamp_us")
}

fn is_column(expr: &Expr, qualifier: &Ident, name: &str) -> bool {
    match expr {
        Expr::Nested(inner) => is_column(inner, qualifier, name),
        Expr::Identifier(column) => identifier_matches(column, name),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            identifier_matches(&parts[0], &qualifier.value) && identifier_matches(&parts[1], name)
        }
        _ => false,
    }
}

fn integer_constant(expr: &Expr) -> Option<i64> {
    match expr {
        Expr::Nested(inner) => integer_constant(inner),
        Expr::Value(value) => positive_integer(&value.value),
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => integer_constant(expr),
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => {
            let magnitude = unsigned_integer(expr)?;
            if magnitude == (i64::MAX as u64) + 1 {
                Some(i64::MIN)
            } else {
                i64::try_from(magnitude).ok().map(|value| -value)
            }
        }
        _ => None,
    }
}

fn positive_integer(value: &Value) -> Option<i64> {
    let Value::Number(text, long) = value else {
        return None;
    };
    if *long || text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

fn unsigned_integer(expr: &Expr) -> Option<u64> {
    match expr {
        Expr::Nested(inner) => unsigned_integer(inner),
        Expr::Value(value) => {
            let Value::Number(text, long) = &value.value else {
                return None;
            };
            if *long || text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
                return None;
            }
            text.parse().ok()
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tables() -> Vec<String> {
        vec!["metrics".to_string(), "events".to_string()]
    }

    fn planned(sql: &str) -> ScanPlan {
        plan(sql, &tables()).unwrap_or_else(|| panic!("expected a plan for {sql}"))
    }

    #[test]
    fn plans_plain_and_quoted_tables_from_the_ast() {
        assert_eq!(
            planned(" SELECT * FROM metrics ; "),
            ScanPlan {
                table: "metrics".to_string(),
                rollup: false,
                rollup_width_us: None,
                tenant: None,
                series: None,
                start_us: None,
                end_us: None,
                empty: false,
            }
        );
        assert_eq!(
            planned("SELECT 'FROM events' FROM \"metrics\""),
            ScanPlan {
                table: "metrics".to_string(),
                rollup: false,
                rollup_width_us: None,
                tenant: None,
                series: None,
                start_us: None,
                end_us: None,
                empty: false,
            }
        );
    }

    #[test]
    fn extracts_comparisons_and_reversed_comparisons() {
        let plan = planned(
            "SELECT * FROM metrics m \
             WHERE m.timestamp_us >= -10 AND m.timestamp_us < 20 AND value > 0",
        );
        assert_eq!(
            (plan.start_us, plan.end_us, plan.empty),
            (Some(-10), Some(20), false)
        );

        let plan = planned("SELECT * FROM metrics WHERE -5 < timestamp_us AND 5 >= timestamp_us");
        assert_eq!(
            (plan.start_us, plan.end_us, plan.empty),
            (Some(-4), Some(6), false)
        );

        let plan = planned("SELECT * FROM metrics WHERE (timestamp_us) = (+7)");
        assert_eq!(
            (plan.start_us, plan.end_us, plan.empty),
            (Some(7), Some(8), false)
        );
    }

    #[test]
    fn extracts_non_negated_between_with_quoted_alias() {
        let plan = planned(
            "SELECT * FROM \"metrics\" AS \"m\" \
             WHERE (\"m\".\"timestamp_us\" BETWEEN -5 AND 5)",
        );
        assert_eq!(
            (plan.start_us, plan.end_us, plan.empty),
            (Some(-5), Some(6), false)
        );
    }

    #[test]
    fn and_keeps_known_bounds_but_or_and_not_do_not_prune() {
        let plan = planned(
            "SELECT * FROM metrics WHERE timestamp_us >= 10 AND tenant = 'a' \
             AND (timestamp_us < 20 OR timestamp_us > 30)",
        );
        assert_eq!((plan.start_us, plan.end_us), (Some(10), None));

        let plan = planned("SELECT * FROM metrics WHERE timestamp_us > 1 OR timestamp_us < 9");
        assert_eq!((plan.start_us, plan.end_us), (None, None));

        let plan = planned("SELECT * FROM metrics WHERE NOT (timestamp_us >= 10)");
        assert_eq!((plan.start_us, plan.end_us), (None, None));

        let plan = planned("SELECT * FROM metrics WHERE timestamp_us NOT BETWEEN 1 AND 9");
        assert_eq!((plan.start_us, plan.end_us), (None, None));
    }

    #[test]
    fn handles_extrema_contradictions_and_exclusive_end_without_overflow() {
        let plan = planned("SELECT * FROM metrics WHERE timestamp_us > 9223372036854775807");
        assert!(plan.empty);

        let plan = planned("SELECT * FROM metrics WHERE timestamp_us < -9223372036854775808");
        assert!(plan.empty);

        let plan = planned("SELECT * FROM metrics WHERE timestamp_us = 9223372036854775807");
        assert_eq!(
            (plan.start_us, plan.end_us, plan.empty),
            (Some(i64::MAX), None, false)
        );

        let plan = planned("SELECT * FROM metrics WHERE timestamp_us <= 9223372036854775807");
        assert_eq!(
            (plan.start_us, plan.end_us, plan.empty),
            (None, None, false)
        );

        let plan = planned(
            "SELECT * FROM metrics WHERE timestamp_us BETWEEN \
             -9223372036854775808 AND 9223372036854775807",
        );
        assert_eq!(
            (plan.start_us, plan.end_us, plan.empty),
            (Some(i64::MIN), None, false)
        );

        let plan = planned("SELECT * FROM metrics WHERE timestamp_us >= 10 AND timestamp_us < 10");
        assert!(plan.empty);
    }

    #[test]
    fn only_uses_the_actual_raw_timestamp_column() {
        let plan =
            planned("SELECT CASE WHEN value > 0 THEN 1 ELSE 0 END AS timestamp_us FROM metrics");
        assert_eq!((plan.start_us, plan.end_us), (None, None));

        let plan = planned("SELECT value AS timestamp_us FROM metrics HAVING timestamp_us > 100");
        assert_eq!((plan.start_us, plan.end_us), (None, None));

        let plan = planned("SELECT * FROM metrics m WHERE metrics.timestamp_us > 10");
        assert_eq!((plan.start_us, plan.end_us), (None, None));
    }

    #[test]
    fn rollup_plans_never_apply_raw_time_pruning() {
        assert_eq!(
            planned("SELECT * FROM \"metrics__rollup\" r WHERE timestamp_us > 100"),
            ScanPlan {
                table: "metrics".to_string(),
                rollup: true,
                rollup_width_us: None,
                tenant: None,
                series: None,
                start_us: None,
                end_us: None,
                empty: false,
            }
        );
    }

    #[test]
    fn rejects_non_simple_or_unresolved_sources() {
        for sql in [
            "SELECT * FROM missing",
            "SELECT * FROM main.metrics",
            "SELECT * FROM metrics, events",
            "SELECT * FROM metrics JOIN events USING (timestamp_us)",
            "SELECT * FROM read_parquet('part.parquet')",
            "SELECT * FROM query_table('metrics')",
            "SELECT * FROM metrics AS m(ts, tenant, series, value, tags, sequence, ordinal)",
            "WITH m AS (SELECT * FROM metrics) SELECT * FROM m",
            "SELECT * FROM metrics UNION ALL SELECT * FROM events",
            "SELECT * FROM metrics; SELECT * FROM events",
            "DELETE FROM metrics",
        ] {
            assert!(plan(sql, &tables()).is_none(), "unexpected plan for {sql}");
        }
    }

    #[test]
    fn nested_single_source_chains_use_only_base_select_predicates() {
        let plan = planned(
            r#"SELECT timestamp_us, value
                FROM (
                    SELECT timestamp_us, value,
                        row_number() OVER (
                            ORDER BY timestamp_us DESC, value DESC
                        ) AS rn
                    FROM metrics m
                    WHERE m.tenant='tenant_0'
                        AND m.series='series_0000'
                        AND m.timestamp_us >= 10
                ) ranked
                WHERE rn <= 5"#,
        );
        assert_eq!(plan.tenant.as_deref(), Some("tenant_0"));
        assert_eq!(plan.series.as_deref(), Some("series_0000"));
        assert_eq!((plan.start_us, plan.end_us), (Some(10), None));

        let plan = planned(
            r#"SELECT *
                FROM (
                    SELECT *
                    FROM (
                        SELECT * FROM metrics
                        WHERE tenant='a' AND timestamp_us < 20
                        LIMIT 100
                    ) first
                    LIMIT 10
                ) second
                WHERE timestamp_us >= 15 AND series='outer'"#,
        );
        assert_eq!(plan.tenant.as_deref(), Some("a"));
        assert_eq!(plan.series, None);
        assert_eq!((plan.start_us, plan.end_us), (None, Some(20)));

        let plan = planned(
            r#"SELECT *
                FROM (
                    SELECT tenant, sum(value) AS total
                    FROM metrics
                    WHERE series='cpu'
                    GROUP BY tenant
                ) grouped
                WHERE tenant='a'"#,
        );
        assert_eq!(plan.tenant, None);
        assert_eq!(plan.series.as_deref(), Some("cpu"));
    }

    #[test]
    fn outer_predicates_never_cross_a_derived_boundary() {
        let plan = planned(
            r#"SELECT *
                FROM (
                    SELECT *, row_number() OVER (ORDER BY timestamp_us) AS rn
                    FROM metrics
                    LIMIT 10
                ) ranked
                WHERE tenant='a'
                    AND series='cpu'
                    AND timestamp_us >= 10
                    AND rn <= 5"#,
        );
        assert_eq!(plan.tenant, None);
        assert_eq!(plan.series, None);
        assert_eq!((plan.start_us, plan.end_us), (None, None));
    }

    #[test]
    fn rejects_side_queries_ambiguous_sources_and_unsupported_nested_syntax() {
        for sql in [
            "SELECT (SELECT max(value) FROM events) FROM metrics",
            "SELECT * FROM metrics WHERE value IN (SELECT value FROM events)",
            "SELECT * FROM metrics ORDER BY (SELECT max(value) FROM events)",
            "SELECT * FROM (SELECT timestamp_us, (SELECT max(value) FROM events) FROM metrics WHERE tenant='a') nested",
            "SELECT * FROM (SELECT * FROM metrics WHERE value IN (SELECT value FROM events)) nested",
            "SELECT * FROM (SELECT * FROM metrics JOIN events ON true) nested",
            "SELECT * FROM (SELECT * FROM metrics, events) nested",
            "SELECT * FROM (SELECT * FROM metrics UNION ALL SELECT * FROM events) nested",
            "SELECT * FROM (WITH source AS (SELECT * FROM metrics) SELECT * FROM source) nested",
            "SELECT * FROM (SELECT * FROM metrics WHERE tenant COLLATE NOCASE = 'a') nested",
            "SELECT * FROM LATERAL (SELECT * FROM metrics) nested",
        ] {
            assert!(plan(sql, &tables()).is_none(), "unexpected plan for {sql}");
        }
    }

    #[test]
    fn scalar_queries_need_no_storage_tables() {
        assert_eq!(planned("SELECT 1"), storage_free_plan());
        assert_eq!(
            planned("SELECT current_setting('threads')"),
            storage_free_plan()
        );
    }

    #[test]
    fn known_metadata_macros_need_no_storage_tables() {
        let catalog = QueryCatalog {
            relations: vec![
                crate::query::CatalogRelation {
                    name: "varve_tables".to_string(),
                    columns: vec![("name".to_string(), "VARCHAR".to_string())],
                    rows: Vec::new(),
                },
                crate::query::CatalogRelation {
                    name: "varve_status".to_string(),
                    columns: vec![("state".to_string(), "VARCHAR".to_string())],
                    rows: Vec::new(),
                },
            ],
            aggregates: Vec::new(),
        };
        assert_eq!(
            plan_with_catalog("SELECT * FROM varve_tables()", &tables(), &catalog),
            Some(storage_free_plan())
        );
        assert_eq!(
            plan_with_catalog(
                "SELECT * FROM varve_tables() t JOIN varve_status() s ON true",
                &tables(),
                &catalog,
            ),
            Some(storage_free_plan())
        );
        assert!(plan_with_catalog("SELECT * FROM unknown_macro()", &tables(), &catalog).is_none());
        assert!(
            plan_with_catalog(
                "SELECT (SELECT count(*) FROM metrics) FROM varve_tables()",
                &tables(),
                &catalog,
            )
            .is_none()
        );
    }

    #[test]
    fn storage_source_proof_depends_on_shape_and_aliases_not_catalog_rows() {
        let mut catalog = QueryCatalog {
            relations: vec![crate::query::CatalogRelation {
                name: "varve_status".to_string(),
                columns: vec![("sequence".to_string(), "UBIGINT".to_string())],
                rows: vec![serde_json::json!([7])],
            }],
            aggregates: vec![crate::query::AggregateAlias {
                name: "cpu_hourly".to_string(),
                source: "metrics".to_string(),
                width_us: 3_600_000_000,
            }],
        };
        for sql in [
            "SELECT * FROM metrics",
            "SELECT * FROM (SELECT * FROM metrics WHERE tenant='a') nested",
            "SELECT * FROM cpu_hourly",
        ] {
            let full = plan_with_catalog(sql, &tables(), &catalog);
            catalog.relations[0].rows.clear();
            let schema_only = plan_with_catalog(sql, &tables(), &catalog);
            assert_eq!(full, schema_only, "{sql}");
            assert!(
                schema_only
                    .as_ref()
                    .is_some_and(ScanPlan::proves_single_storage_source),
                "{sql}"
            );
            catalog.relations[0].rows = vec![serde_json::json!([9])];
        }

        for sql in [
            "SELECT 1",
            "SELECT * FROM varve_status()",
            "SELECT (SELECT sequence FROM varve_status()) FROM metrics",
            "SELECT * FROM metrics JOIN varve_status() ON true",
            "WITH source AS (SELECT * FROM metrics) SELECT * FROM source",
            "SELECT * FROM query_table('metrics')",
            "SELECT * FROM __varve_input",
        ] {
            assert!(
                !plan_with_catalog(sql, &tables(), &catalog)
                    .as_ref()
                    .is_some_and(ScanPlan::proves_single_storage_source),
                "unexpected storage-only proof for {sql}"
            );
        }
    }

    #[test]
    fn aggregate_aliases_select_only_source_rollups() {
        let catalog = QueryCatalog {
            relations: Vec::new(),
            aggregates: vec![crate::query::AggregateAlias {
                name: "cpu_hourly".to_string(),
                source: "metrics".to_string(),
                width_us: 3_600_000_000,
            }],
        };
        assert_eq!(
            plan_with_catalog("SELECT * FROM cpu_hourly", &tables(), &catalog),
            Some(ScanPlan {
                table: "metrics".to_string(),
                rollup: true,
                rollup_width_us: Some(3_600_000_000),
                tenant: None,
                series: None,
                start_us: None,
                end_us: None,
                empty: false,
            })
        );
    }

    #[test]
    fn series_equalities_are_binary_and_conjunctive() {
        let plan = planned(
            "SELECT * FROM metrics m WHERE 'O''Brien' = m.tenant AND (m.series) = ('東京') AND timestamp_us >= -1",
        );
        assert_eq!(plan.tenant.as_deref(), Some("O'Brien"));
        assert_eq!(plan.series.as_deref(), Some("東京"));
        assert_eq!(plan.start_us, Some(-1));
        assert!(plan.matches_series("O'Brien", "東京"));
        assert!(!plan.matches_series("o'brien", "東京"));
        assert!(planned("SELECT * FROM metrics WHERE tenant='a' AND tenant='A'").empty);
        assert!(planned("SELECT * FROM metrics WHERE series='a' AND series='b'").empty);
        assert!(!planned("SELECT * FROM metrics WHERE tenant='a' AND tenant='a'").empty);
    }

    #[test]
    fn contradictory_count_query_needs_no_storage() {
        let sql = "SELECT count(*)::BIGINT AS n FROM metrics WHERE tenant='a' AND tenant='b'";
        let projection = plan(sql, &tables());
        assert!(
            projection.as_ref().is_some_and(|plan| plan.empty),
            "{projection:?}"
        );
    }

    #[test]
    fn uncertain_string_semantics_never_prune_series() {
        for predicate in [
            "tenant='a' OR series='b'",
            "NOT (tenant='a')",
            "tenant IN ('a','b')",
            "tenant COLLATE NOCASE = 'A'",
            "tenant = ('A' COLLATE NOCASE)",
            "lower(tenant)='a'",
            "tenant::VARCHAR='a'",
            "tenant LIKE 'a%'",
            "metrics.tenant='a'",
            "tenant=$1",
            "tenant=1",
        ] {
            let plan = planned(&format!("SELECT * FROM metrics m WHERE {predicate}"));
            assert_eq!((plan.tenant, plan.series), (None, None), "{predicate}");
            assert!(!plan.empty, "{predicate}");
        }
        let plan = planned("SELECT * FROM metrics WHERE tenant='a' AND (series='b' OR series='c')");
        assert_eq!(plan.tenant.as_deref(), Some("a"));
        assert_eq!(plan.series, None);
    }

    #[test]
    fn unsupported_escaped_string_syntax_falls_back_without_pruning() {
        assert!(plan("SELECT * FROM metrics WHERE tenant=E'a'", &tables()).is_none());
    }

    #[test]
    fn ambiguous_supplied_names_fall_back() {
        let names = vec!["metrics".to_string(), "METRICS".to_string()];
        assert!(plan("SELECT * FROM metrics", &names).is_none());

        let names = vec!["metrics".to_string(), "metrics__rollup".to_string()];
        assert!(plan("SELECT * FROM metrics__rollup", &names).is_none());
    }
}
