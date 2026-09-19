use super::*;
use crate::raw_memory::WeakRawRows;
use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
#[path = "resident_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "resident_descriptor_tests.rs"]
mod descriptor_delta_tests;

type BatchKey = (String, String);

const RETAINED_IDENTITY_RESERVE_BYTES: usize = 256;
const LINEAGE_ID_RESERVE_BYTES: usize = 64;
// A decimal position label stored in the disposable DuckDB relation.
const ROLLUP_SLOT_BYTES: usize = 64;

#[derive(Clone, Copy, Debug, Default)]
struct DescriptorWork {
    #[cfg(test)]
    scope_comparisons: usize,
    #[cfg(test)]
    scope_builds: usize,
    #[cfg(test)]
    setup_builds: usize,
    #[cfg(test)]
    relation_rows_compared: usize,
    #[cfg(test)]
    relation_builds: usize,
    #[cfg(test)]
    payload_clones: usize,
}

impl DescriptorWork {
    fn scope_comparison(&mut self) {
        #[cfg(test)]
        {
            self.scope_comparisons += 1;
        }
    }
    fn scope_build(&mut self) {
        #[cfg(test)]
        {
            self.scope_builds += 1;
        }
    }
    fn setup_build(&mut self) {
        #[cfg(test)]
        {
            self.setup_builds += 1;
        }
    }
    fn relation_row_compared(&mut self) {
        #[cfg(test)]
        {
            self.relation_rows_compared += 1;
        }
    }
    fn relation_build(&mut self) {
        #[cfg(test)]
        {
            self.relation_builds += 1;
        }
    }
    fn payload_clone(&mut self) {
        #[cfg(test)]
        {
            self.payload_clones += 1;
        }
    }
}

#[derive(Debug)]
pub(super) struct ResidentCapacityError {
    limit: usize,
}

impl std::fmt::Display for ResidentCapacityError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "DuckDB resident logical input and batch metadata exceeds {} bytes",
            self.limit
        )
    }
}

impl std::error::Error for ResidentCapacityError {}

fn capacity_error(limit: usize) -> anyhow::Error {
    ResidentCapacityError { limit }.into()
}

fn checked_size(current: usize, additional: usize) -> Result<usize> {
    current
        .checked_add(additional)
        .ok_or_else(|| capacity_error(usize::MAX))
}

fn ensure_capacity(total: usize, limit: usize) -> Result<()> {
    if total > limit {
        return Err(capacity_error(limit));
    }
    Ok(())
}

fn retained_batch_identity_bytes(table_name: &str, id: &str, path: Option<&Path>) -> Result<usize> {
    let key_bytes = checked_size(table_name.len(), id.len())?;
    // A complete loaded identity has one key in the batch map and another in
    // CompleteCoverage. The fixed reserve covers both container values and
    // LoadedBatch; dynamic key bytes are charged for both copies explicitly.
    let mut bytes = checked_size(key_bytes, key_bytes)?;
    if let Some(path) = path {
        bytes = checked_size(bytes, path.as_os_str().as_encoded_bytes().len())?;
    }
    checked_size(bytes, RETAINED_IDENTITY_RESERVE_BYTES)
}

#[derive(Clone, Copy, Debug)]
struct Budget {
    used: usize,
    limit: usize,
}

impl Budget {
    fn new(limit: usize) -> Self {
        Self { used: 0, limit }
    }

    fn debit(&mut self, additional: usize) -> Result<()> {
        let total = checked_size(self.used, additional)?;
        ensure_capacity(total, self.limit)?;
        self.used = total;
        Ok(())
    }
}

struct BuildContext<'a> {
    budget: Budget,
    deadline: Instant,
    cancelled: &'a AtomicBool,
    work: DescriptorWork,
}

impl<'a> BuildContext<'a> {
    fn new(limit: usize, deadline: Instant, cancelled: &'a AtomicBool) -> Self {
        Self {
            budget: Budget::new(limit),
            deadline,
            cancelled,
            work: DescriptorWork::default(),
        }
    }

    fn check(&self) -> Result<()> {
        check_deadline(self.deadline, self.cancelled)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct TableScopeDescriptor {
    name: String,
    cutoff_us: Option<i64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CatalogScopeDescriptor {
    name: String,
    columns: Vec<(String, String)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AliasScopeDescriptor {
    name: String,
    source: String,
    width_us: i64,
}

#[derive(Clone, Debug)]
struct QueryScopeDescriptor {
    tables: Vec<TableScopeDescriptor>,
    catalog: Vec<CatalogScopeDescriptor>,
    aliases: Vec<AliasScopeDescriptor>,
    setup: String,
    objects: String,
    logical_bytes: usize,
}

impl QueryScopeDescriptor {
    fn matches_source(
        &self,
        tables: &[QueryTable],
        catalog: &QueryCatalog,
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> Result<bool> {
        if self.tables.len() != tables.len()
            || self.catalog.len() != catalog.relations.len()
            || self.aliases.len() != catalog.aggregates.len()
        {
            return Ok(false);
        }
        for (prepared, source) in self.tables.iter().zip(tables) {
            check_deadline(deadline, cancelled)?;
            if prepared.name != source.name || prepared.cutoff_us != source.cutoff_us {
                return Ok(false);
            }
        }
        for (prepared, source) in self.catalog.iter().zip(&catalog.relations) {
            check_deadline(deadline, cancelled)?;
            if prepared.name != source.name || prepared.columns != source.columns {
                return Ok(false);
            }
        }
        for (prepared, source) in self.aliases.iter().zip(&catalog.aggregates) {
            check_deadline(deadline, cancelled)?;
            if prepared.name != source.name
                || prepared.source != source.source
                || prepared.width_us != source.width_us
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn same_typed_scope(&self, other: &Self) -> bool {
        self.tables == other.tables
            && self.catalog == other.catalog
            && self.aliases == other.aliases
    }

    fn source_logical_bytes(
        tables: &[QueryTable],
        catalog: &QueryCatalog,
        context: &BuildContext<'_>,
    ) -> Result<usize> {
        context.check()?;
        let mut bytes = std::mem::size_of::<Self>();
        for table in tables {
            context.check()?;
            bytes = checked_size(bytes, std::mem::size_of::<TableScopeDescriptor>())?;
            bytes = checked_size(bytes, table.name.len())?;
        }
        for relation in &catalog.relations {
            context.check()?;
            bytes = checked_size(bytes, std::mem::size_of::<CatalogScopeDescriptor>())?;
            bytes = checked_size(bytes, relation.name.len())?;
            for (name, kind) in &relation.columns {
                bytes = checked_size(bytes, std::mem::size_of::<(String, String)>())?;
                bytes = checked_size(bytes, name.len())?;
                bytes = checked_size(bytes, kind.len())?;
            }
        }
        for alias in &catalog.aggregates {
            context.check()?;
            bytes = checked_size(bytes, std::mem::size_of::<AliasScopeDescriptor>())?;
            bytes = checked_size(bytes, alias.name.len())?;
            bytes = checked_size(bytes, alias.source.len())?;
        }
        Ok(bytes)
    }

    fn new(
        tables: &[QueryTable],
        options: &QueryOptions,
        catalog: &QueryCatalog,
        native_files: bool,
        source_bytes: usize,
        context: &mut BuildContext<'_>,
    ) -> Result<Arc<Self>> {
        context.check()?;
        let empty_tables: Vec<_> = tables
            .iter()
            .map(|table| QueryTable {
                name: table.name.clone(),
                hot: vec![],
                files: if native_files {
                    table.files.clone()
                } else {
                    vec![]
                },
                rollups: vec![],
                cutoff_us: table.cutoff_us,
            })
            .collect();
        let empty_catalog = QueryCatalog {
            relations: catalog
                .relations
                .iter()
                .map(|relation| CatalogRelation {
                    name: relation.name.clone(),
                    columns: relation.columns.clone(),
                    rows: vec![],
                })
                .collect(),
            aggregates: catalog.aggregates.clone(),
        };
        let (setup, input) = build_query_mode(
            &empty_tables,
            "",
            options,
            &empty_catalog,
            Path::new("."),
            false,
        )?;
        debug_assert!(input.is_empty());
        let (_, setup) = setup
            .split_once("CREATE TEMP TABLE __varve_version_gate")
            .context("missing resident version gate")?;
        let setup = format!("CREATE TEMP TABLE __varve_version_gate{setup}");
        let mut objects = String::new();
        for table in &empty_tables {
            append_table_views(&mut objects, table)?;
        }
        for relation in &empty_catalog.relations {
            append_catalog_macro(&mut objects, relation)?;
        }
        for alias in &empty_catalog.aggregates {
            append_aggregate_alias(&mut objects, alias, &empty_tables)?;
        }
        ensure!(
            setup.ends_with(&objects),
            "resident schema object suffix mismatch"
        );
        let base = setup[..setup.len() - objects.len()].to_owned();
        ensure!(
            base.len().saturating_add(objects.len()) <= MAX_QUERY_SCRIPT_BYTES,
            "resident schema exceeds SQL script limit"
        );
        context
            .budget
            .debit(base.len().saturating_add(objects.len()))?;
        let logical_bytes = checked_size(source_bytes, base.len().saturating_add(objects.len()))?;
        let table_descriptors = tables
            .iter()
            .map(|table| TableScopeDescriptor {
                name: table.name.clone(),
                cutoff_us: table.cutoff_us,
            })
            .collect::<Vec<_>>();
        let catalog_descriptors = catalog
            .relations
            .iter()
            .map(|relation| CatalogScopeDescriptor {
                name: relation.name.clone(),
                columns: relation.columns.clone(),
            })
            .collect::<Vec<_>>();
        let alias_descriptors = catalog
            .aggregates
            .iter()
            .map(|alias| AliasScopeDescriptor {
                name: alias.name.clone(),
                source: alias.source.clone(),
                width_us: alias.width_us,
            })
            .collect::<Vec<_>>();
        context.work.scope_build();
        context.work.setup_build();
        Ok(Arc::new(Self {
            tables: table_descriptors,
            catalog: catalog_descriptors,
            aliases: alias_descriptors,
            setup: base,
            objects,
            logical_bytes,
        }))
    }

    fn append_drop_objects(&self, sql: &mut String) {
        for alias in self.aliases.iter().rev() {
            sql.push_str("DROP VIEW IF EXISTS ");
            sql.push_str(&quote_identifier(&alias.name));
            sql.push_str("; ");
        }
        for relation in self.catalog.iter().rev() {
            sql.push_str("DROP MACRO IF EXISTS ");
            sql.push_str(&quote_identifier(&relation.name));
            sql.push_str("; ");
        }
        for table in self.tables.iter().rev() {
            sql.push_str("DROP VIEW IF EXISTS ");
            sql.push_str(&quote_identifier(&format!("{}__rollup", table.name)));
            sql.push_str("; DROP VIEW IF EXISTS ");
            sql.push_str(&quote_identifier(&table.name));
            sql.push_str("; ");
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ExactF64(u64);

impl ExactF64 {
    fn new(value: f64, context: &str) -> Result<Self> {
        ensure!(value.is_finite(), "{context} must be finite");
        Ok(Self(value.to_bits()))
    }

    fn get(self) -> f64 {
        f64::from_bits(self.0)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PreparedRollupRow {
    width_us: i64,
    bucket_us: i64,
    tenant: String,
    series: String,
    tags: BTreeMap<String, String>,
    count: u64,
    sum: ExactF64,
    min: ExactF64,
    max: ExactF64,
    first: ExactF64,
    last: ExactF64,
    first_timestamp_us: i64,
    last_timestamp_us: i64,
    first_sequence: u64,
    first_ordinal: u32,
    last_sequence: u64,
    last_ordinal: u32,
}

impl PreparedRollupRow {
    fn new(row: &RollupRow, work: &mut DescriptorWork) -> Result<Self> {
        let sum = ExactF64::new(row.sum, "rollup sum")?;
        let min = ExactF64::new(row.min, "rollup min")?;
        let max = ExactF64::new(row.max, "rollup max")?;
        let first = ExactF64::new(row.first, "rollup first")?;
        let last = ExactF64::new(row.last, "rollup last")?;
        work.payload_clone();
        Ok(Self {
            width_us: row.width_us,
            bucket_us: row.bucket_us,
            tenant: row.tenant.clone(),
            series: row.series.clone(),
            tags: row.tags.clone(),
            count: row.count,
            sum,
            min,
            max,
            first,
            last,
            first_timestamp_us: row.first_timestamp_us,
            last_timestamp_us: row.last_timestamp_us,
            first_sequence: row.first_sequence,
            first_ordinal: row.first_ordinal,
            last_sequence: row.last_sequence,
            last_ordinal: row.last_ordinal,
        })
    }

    fn matches(&self, row: &RollupRow) -> bool {
        self.width_us == row.width_us
            && self.bucket_us == row.bucket_us
            && self.tenant == row.tenant
            && self.series == row.series
            && self.tags == row.tags
            && self.count == row.count
            && self.sum.0 == row.sum.to_bits()
            && self.min.0 == row.min.to_bits()
            && self.max.0 == row.max.to_bits()
            && self.first.0 == row.first.to_bits()
            && self.last.0 == row.last.to_bits()
            && self.first_timestamp_us == row.first_timestamp_us
            && self.last_timestamp_us == row.last_timestamp_us
            && self.first_sequence == row.first_sequence
            && self.first_ordinal == row.first_ordinal
            && self.last_sequence == row.last_sequence
            && self.last_ordinal == row.last_ordinal
    }

    fn materialized_bytes(&self) -> Result<usize> {
        let mut bytes = checked_size(std::mem::size_of::<Self>(), ROLLUP_SLOT_BYTES)?;
        bytes = checked_size(bytes, self.tenant.len())?;
        bytes = checked_size(bytes, self.series.len())?;
        for (key, value) in &self.tags {
            bytes = checked_size(bytes, key.len())?;
            bytes = checked_size(bytes, value.len())?;
            bytes = checked_size(bytes, 64)?;
        }
        Ok(bytes)
    }

    fn source_logical_bytes(row: &RollupRow) -> Result<usize> {
        ExactF64::new(row.sum, "rollup sum")?;
        ExactF64::new(row.min, "rollup min")?;
        ExactF64::new(row.max, "rollup max")?;
        ExactF64::new(row.first, "rollup first")?;
        ExactF64::new(row.last, "rollup last")?;
        let mut bytes = checked_size(std::mem::size_of::<Self>(), ROLLUP_SLOT_BYTES)?;
        bytes = checked_size(bytes, row.tenant.len())?;
        bytes = checked_size(bytes, row.series.len())?;
        for (key, value) in &row.tags {
            bytes = checked_size(bytes, key.len())?;
            bytes = checked_size(bytes, value.len())?;
            bytes = checked_size(bytes, 64)?;
        }
        Ok(bytes)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PreparedCatalogValue {
    Null,
    Text(String),
    Signed(i64),
    Unsigned(u64),
    Double(ExactF64),
    Boolean(bool),
}

impl PreparedCatalogValue {
    fn new(value: &Value, column_type: &str, work: &mut DescriptorWork) -> Result<Self> {
        if value.is_null() {
            return Ok(Self::Null);
        }
        match column_type {
            "VARCHAR" => {
                let value = value.as_str().context("catalog VARCHAR value")?;
                work.payload_clone();
                Ok(Self::Text(value.to_owned()))
            }
            "BIGINT" => Ok(Self::Signed(
                value.as_i64().context("catalog BIGINT value")?,
            )),
            "UBIGINT" => Ok(Self::Unsigned(
                value.as_u64().context("catalog UBIGINT value")?,
            )),
            "DOUBLE" => Ok(Self::Double(ExactF64::new(
                value.as_f64().context("catalog DOUBLE value")?,
                "catalog DOUBLE value",
            )?)),
            "BOOLEAN" => Ok(Self::Boolean(
                value.as_bool().context("catalog BOOLEAN value")?,
            )),
            _ => bail!("unsupported catalog column type {column_type:?}"),
        }
    }

    fn matches(&self, value: &Value, column_type: &str) -> bool {
        match (self, column_type) {
            (Self::Null, _) => value.is_null(),
            (Self::Text(prepared), "VARCHAR") => value.as_str() == Some(prepared),
            (Self::Signed(prepared), "BIGINT") => value.as_i64() == Some(*prepared),
            (Self::Unsigned(prepared), "UBIGINT") => value.as_u64() == Some(*prepared),
            (Self::Double(prepared), "DOUBLE") => value
                .as_f64()
                .is_some_and(|value| value.to_bits() == prepared.0),
            (Self::Boolean(prepared), "BOOLEAN") => value.as_bool() == Some(*prepared),
            _ => false,
        }
    }

    fn to_json(&self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Text(value) => Value::String(value.clone()),
            Self::Signed(value) => Value::Number((*value).into()),
            Self::Unsigned(value) => Value::Number((*value).into()),
            Self::Double(value) => Value::Number(
                serde_json::Number::from_f64(value.get()).expect("validated finite float"),
            ),
            Self::Boolean(value) => Value::Bool(*value),
        }
    }

    fn source_logical_bytes(value: &Value, column_type: &str) -> Result<usize> {
        catalog_type(column_type)?;
        if value.is_null() {
            return Ok(std::mem::size_of::<Self>());
        }
        let dynamic = match column_type {
            "VARCHAR" => value.as_str().context("catalog VARCHAR value")?.len(),
            "BIGINT" => {
                value.as_i64().context("catalog BIGINT value")?;
                0
            }
            "UBIGINT" => {
                value.as_u64().context("catalog UBIGINT value")?;
                0
            }
            "DOUBLE" => {
                ExactF64::new(
                    value.as_f64().context("catalog DOUBLE value")?,
                    "catalog DOUBLE value",
                )?;
                0
            }
            "BOOLEAN" => {
                value.as_bool().context("catalog BOOLEAN value")?;
                0
            }
            _ => unreachable!("catalog_type validated the type"),
        };
        checked_size(std::mem::size_of::<Self>(), dynamic)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum RelationKey {
    Rollup(String),
    Catalog(String),
}

impl RelationKey {
    fn name(&self) -> &str {
        match self {
            Self::Rollup(name) | Self::Catalog(name) => name,
        }
    }

    fn is_rollup(&self, name: &str) -> bool {
        matches!(self, Self::Rollup(current) if current == name)
    }

    fn is_catalog(&self, name: &str) -> bool {
        matches!(self, Self::Catalog(current) if current == name)
    }

    fn append_delete(&self, script: &mut String) {
        let kind = match self {
            Self::Rollup(_) => "rollup",
            Self::Catalog(_) => "catalog",
        };
        script.push_str("DELETE FROM __varve_input WHERE kind = ");
        script.push_str(&quote_literal(kind));
        script.push_str(" AND table_name = ");
        script.push_str(&quote_literal(self.name()));
        script.push_str(";\n");
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PreparedRelationRows {
    Rollup(Vec<PreparedRollupRow>),
    Catalog(Vec<Vec<PreparedCatalogValue>>),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RelationDescriptor {
    key: RelationKey,
    rows: PreparedRelationRows,
    logical_bytes: usize,
}

const RELATION_MAP_ENTRY_BYTES: usize =
    std::mem::size_of::<Arc<RelationDescriptor>>() + 4 * std::mem::size_of::<usize>();

fn relation_map_key_bytes(name: &str) -> Result<usize> {
    checked_size(std::mem::size_of::<RelationKey>(), name.len())
}

impl RelationDescriptor {
    fn rollup_source_logical_bytes(
        table: &QueryTable,
        context: &BuildContext<'_>,
    ) -> Result<usize> {
        let mut bytes = checked_size(std::mem::size_of::<Self>(), table.name.len())?;
        for row in &table.rollups {
            context.check()?;
            bytes = checked_size(bytes, PreparedRollupRow::source_logical_bytes(row)?)?;
        }
        Ok(bytes)
    }

    fn catalog_source_logical_bytes(
        relation: &CatalogRelation,
        context: &BuildContext<'_>,
    ) -> Result<usize> {
        let outer_rows = relation
            .rows
            .len()
            .checked_mul(std::mem::size_of::<Vec<PreparedCatalogValue>>())
            .ok_or_else(|| capacity_error(context.budget.limit))?;
        let mut bytes = checked_size(std::mem::size_of::<Self>(), relation.name.len())?;
        bytes = checked_size(bytes, outer_rows)?;
        for row in &relation.rows {
            context.check()?;
            match row {
                Value::Array(values) => {
                    ensure!(
                        values.len() == relation.columns.len(),
                        "catalog relation {} row has {} values for {} columns",
                        relation.name,
                        values.len(),
                        relation.columns.len()
                    );
                    for (value, (_, column_type)) in values.iter().zip(&relation.columns) {
                        bytes = checked_size(
                            bytes,
                            PreparedCatalogValue::source_logical_bytes(value, column_type)?,
                        )?;
                    }
                }
                Value::Object(values) => {
                    ensure!(
                        values.len() == relation.columns.len(),
                        "catalog relation {} row has {} fields for {} columns",
                        relation.name,
                        values.len(),
                        relation.columns.len()
                    );
                    for (name, column_type) in &relation.columns {
                        let value = values.get(name).with_context(|| {
                            format!("catalog relation {} row is missing {name}", relation.name)
                        })?;
                        bytes = checked_size(
                            bytes,
                            PreparedCatalogValue::source_logical_bytes(value, column_type)?,
                        )?;
                    }
                }
                _ => bail!(
                    "catalog relation {} row must be an array or object",
                    relation.name
                ),
            }
        }
        Ok(bytes)
    }

    fn from_rollups(
        table: &QueryTable,
        logical_bytes: usize,
        context: &mut BuildContext<'_>,
    ) -> Result<Arc<Self>> {
        let mut rows = Vec::with_capacity(table.rollups.len());
        for row in &table.rollups {
            context.check()?;
            rows.push(PreparedRollupRow::new(row, &mut context.work)?);
        }
        context.work.relation_build();
        Ok(Arc::new(Self {
            key: RelationKey::Rollup(table.name.clone()),
            rows: PreparedRelationRows::Rollup(rows),
            logical_bytes,
        }))
    }

    fn from_catalog(
        relation: &CatalogRelation,
        logical_bytes: usize,
        context: &mut BuildContext<'_>,
    ) -> Result<Arc<Self>> {
        let mut rows = Vec::with_capacity(relation.rows.len());
        for row in &relation.rows {
            context.check()?;
            let values = catalog_row_values(relation, row)?;
            let mut prepared = Vec::with_capacity(values.len());
            for (value, (_, column_type)) in values.into_iter().zip(&relation.columns) {
                prepared.push(PreparedCatalogValue::new(
                    value,
                    column_type,
                    &mut context.work,
                )?);
            }
            rows.push(prepared);
        }
        context.work.relation_build();
        Ok(Arc::new(Self {
            key: RelationKey::Catalog(relation.name.clone()),
            rows: PreparedRelationRows::Catalog(rows),
            logical_bytes,
        }))
    }

    fn matches_rollups(
        &self,
        table: &QueryTable,
        deadline: Instant,
        cancelled: &AtomicBool,
        work: &mut DescriptorWork,
    ) -> Result<bool> {
        let PreparedRelationRows::Rollup(prepared) = &self.rows else {
            return Ok(false);
        };
        if !self.key.is_rollup(&table.name) || prepared.len() != table.rollups.len() {
            return Ok(false);
        }
        for (prepared, source) in prepared.iter().zip(&table.rollups) {
            check_deadline(deadline, cancelled)?;
            work.relation_row_compared();
            if !prepared.matches(source) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn matches_catalog(
        &self,
        relation: &CatalogRelation,
        deadline: Instant,
        cancelled: &AtomicBool,
        work: &mut DescriptorWork,
    ) -> Result<bool> {
        let PreparedRelationRows::Catalog(prepared) = &self.rows else {
            return Ok(false);
        };
        if !self.key.is_catalog(&relation.name) || prepared.len() != relation.rows.len() {
            return Ok(false);
        }
        for (prepared, source) in prepared.iter().zip(&relation.rows) {
            check_deadline(deadline, cancelled)?;
            work.relation_row_compared();
            let source = catalog_row_values(relation, source)?;
            if prepared.len() != source.len()
                || !prepared.iter().zip(source).zip(&relation.columns).all(
                    |((prepared, source), (_, column_type))| prepared.matches(source, column_type),
                )
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn is_empty(&self) -> bool {
        match &self.rows {
            PreparedRelationRows::Rollup(rows) => rows.is_empty(),
            PreparedRelationRows::Catalog(rows) => rows.is_empty(),
        }
    }
}

enum RelationPlan<'a> {
    Rollup {
        table: &'a QueryTable,
        existing: Option<Arc<RelationDescriptor>>,
        logical_bytes: usize,
    },
    Catalog {
        relation: &'a CatalogRelation,
        existing: Option<Arc<RelationDescriptor>>,
        logical_bytes: usize,
    },
}

fn validate_resident_structure(
    tables: &[QueryTable],
    snapshot: &ResidentSnapshot,
    deadline: Instant,
    cancelled: &AtomicBool,
) -> Result<()> {
    ensure!(
        tables.iter().all(|table| table.hot.is_empty()),
        "resident query requires empty QueryTable.hot"
    );
    ensure!(
        tables.len() == snapshot.tables.len(),
        "resident table scope mismatch"
    );
    check_deadline(deadline, cancelled)?;

    let mut lineage = BTreeMap::new();
    for table in &snapshot.lineage {
        check_deadline(deadline, cancelled)?;
        ensure!(
            lineage.insert(table.name.as_str(), table).is_none(),
            "duplicate resident lineage table"
        );
        let mut ids = BTreeSet::new();
        for id in &table.ids {
            check_deadline(deadline, cancelled)?;
            ensure!(
                ids.insert(id.as_str()),
                "duplicate resident lineage identity"
            );
        }
    }

    let selected = tables
        .iter()
        .map(|table| table.name.as_str())
        .collect::<BTreeSet<_>>();
    ensure!(
        selected.len() == tables.len(),
        "resident table scope mismatch"
    );
    let mut names = BTreeSet::new();
    for table in &snapshot.tables {
        check_deadline(deadline, cancelled)?;
        ensure!(
            selected.contains(table.name.as_str()) && names.insert(table.name.as_str()),
            "resident table scope mismatch"
        );
        let query_table = tables
            .iter()
            .find(|source| source.name == table.name)
            .expect("validated resident table");
        let selected_paths = table
            .files
            .iter()
            .map(|file| &file.path)
            .collect::<BTreeSet<_>>();
        let query_paths = query_table.files.iter().collect::<BTreeSet<_>>();
        ensure!(
            selected_paths == query_paths && selected_paths.len() == table.files.len(),
            "resident selected file scope mismatch"
        );
        let source = lineage
            .get(table.name.as_str())
            .context("resident selected table is missing lineage")?;
        let mut live = BTreeSet::new();
        for id in &source.ids {
            check_deadline(deadline, cancelled)?;
            ensure!(
                live.insert(id.as_str()),
                "duplicate resident lineage identity"
            );
        }
        let mut ids = BTreeSet::new();
        for batch in &table.batches {
            check_deadline(deadline, cancelled)?;
            ensure!(
                ids.insert(batch.id.as_str()),
                "duplicate resident batch identity"
            );
            ensure!(
                live.contains(batch.id.as_str()),
                "invalid resident batch identity"
            );
        }
        for file in &table.files {
            check_deadline(deadline, cancelled)?;
            ensure!(
                file.path.is_absolute(),
                "resident file path must be absolute"
            );
            ensure!(file.rows > 0, "resident file rows must be positive");
            ensure!(
                file.min_timestamp_us <= file.max_timestamp_us,
                "resident file timestamp bounds are invalid"
            );
            ensure!(
                ids.insert(file.id.as_str()),
                "duplicate resident batch identity"
            );
            ensure!(
                live.contains(file.id.as_str()),
                "invalid resident file identity"
            );
        }
    }
    Ok(())
}

enum RequestedBatch<'a> {
    Memory(&'a ResidentBatch),
    File(&'a ResidentFile),
}

impl RequestedBatch<'_> {
    fn rows(&self) -> usize {
        match self {
            Self::Memory(batch) => batch.rows.len(),
            Self::File(file) => file.rows,
        }
    }

    fn charged_bytes(&self) -> usize {
        match self {
            Self::Memory(batch) => batch.charged_bytes,
            Self::File(file) => file.charged_bytes,
        }
    }

    fn retained_identity_bytes(&self, key: &BatchKey) -> Result<usize> {
        let path = match self {
            Self::Memory(_) => None,
            Self::File(file) => Some(file.path.as_path()),
        };
        retained_batch_identity_bytes(&key.0, &key.1, path)
    }
}

struct RequestedLineage<'a> {
    raw_stamp: u64,
    ids: BTreeSet<&'a str>,
}

pub(super) struct ResidentRequest<'a> {
    snapshot: &'a ResidentSnapshot,
    scope: Arc<QueryScopeDescriptor>,
    relations: BTreeMap<RelationKey, Arc<RelationDescriptor>>,
    batches: BTreeMap<BatchKey, RequestedBatch<'a>>,
    lineage: BTreeMap<String, RequestedLineage<'a>>,
    retained_bytes: usize,
    limit: usize,
    native_files: bool,
    #[cfg(test)]
    work: DescriptorWork,
}

impl<'a> ResidentRequest<'a> {
    #[cfg(test)]
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        tables: &'a [QueryTable],
        snapshot: &'a ResidentSnapshot,
        options: &QueryOptions,
        catalog: &'a QueryCatalog,
        candidates: &[&ResidentState],
        native_files: bool,
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> Result<Self> {
        Self::new_with_limit(
            tables,
            snapshot,
            options,
            catalog,
            candidates,
            native_files,
            MAX_QUERY_INPUT_BYTES,
            deadline,
            cancelled,
        )
    }

    // Immutable request context and the independently enforced admission ceiling.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new_with_limit(
        tables: &'a [QueryTable],
        snapshot: &'a ResidentSnapshot,
        options: &QueryOptions,
        catalog: &'a QueryCatalog,
        candidates: &[&ResidentState],
        native_files: bool,
        limit: usize,
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> Result<Self> {
        let mut context = BuildContext::new(limit, deadline, cancelled);
        Self::new_with_context(
            tables,
            snapshot,
            options,
            catalog,
            candidates,
            native_files,
            &mut context,
        )
    }

    pub(super) fn native_files(&self) -> bool {
        self.native_files
    }

    pub(super) fn installable_on(&self, state: &ResidentState) -> Result<bool> {
        match ResidentInstallPlan::new(Some(state), self) {
            Ok(_) => Ok(true),
            Err(error) if error.is::<ResidentCapacityError>() => Ok(false),
            Err(error) => Err(error),
        }
    }

    fn new_with_context(
        tables: &'a [QueryTable],
        snapshot: &'a ResidentSnapshot,
        options: &QueryOptions,
        catalog: &'a QueryCatalog,
        candidates: &[&ResidentState],
        native_files: bool,
        context: &mut BuildContext<'_>,
    ) -> Result<Self> {
        validate_resident_structure(tables, snapshot, context.deadline, context.cancelled)?;

        let mut matching_scope = None;
        for candidate in candidates {
            context.work.scope_comparison();
            if candidate.scope.matches_source(
                tables,
                catalog,
                context.deadline,
                context.cancelled,
            )? && !native_files
            {
                matching_scope = Some(Arc::clone(&candidate.scope));
                break;
            }
        }
        let scope_source_bytes = if let Some(scope) = &matching_scope {
            context.budget.debit(scope.logical_bytes)?;
            None
        } else {
            let bytes = QueryScopeDescriptor::source_logical_bytes(tables, catalog, context)?;
            context.budget.debit(bytes)?;
            Some(bytes)
        };

        context.budget.debit(snapshot.namespace.len())?;
        let mut lineage = BTreeMap::new();
        if native_files {
            for table in &snapshot.tables {
                context.check()?;
                let source = snapshot
                    .lineage
                    .iter()
                    .find(|lineage| lineage.name == table.name)
                    .expect("validated resident lineage");
                context.budget.debit(checked_size(
                    table.name.len(),
                    std::mem::size_of::<ResidentLineage>(),
                )?)?;
                let mut ids = BTreeSet::new();
                for batch in &table.batches {
                    context.check()?;
                    context
                        .budget
                        .debit(checked_size(batch.id.len(), LINEAGE_ID_RESERVE_BYTES)?)?;
                    ids.insert(batch.id.as_str());
                }
                lineage.insert(
                    table.name.clone(),
                    RequestedLineage {
                        raw_stamp: source.raw_stamp,
                        ids,
                    },
                );
            }
        } else {
            for table in &snapshot.lineage {
                context.check()?;
                context.budget.debit(checked_size(
                    table.name.len(),
                    std::mem::size_of::<ResidentLineage>(),
                )?)?;
                let mut ids = BTreeSet::new();
                for id in &table.ids {
                    context.check()?;
                    context
                        .budget
                        .debit(checked_size(id.len(), LINEAGE_ID_RESERVE_BYTES)?)?;
                    ensure!(
                        ids.insert(id.as_str()),
                        "duplicate resident lineage identity"
                    );
                }
                ensure!(
                    lineage
                        .insert(
                            table.name.clone(),
                            RequestedLineage {
                                raw_stamp: table.raw_stamp,
                                ids,
                            },
                        )
                        .is_none(),
                    "duplicate resident lineage table"
                );
            }
        }

        // Scope and identity validation completed before any capacity decision.
        // This pass charges only metadata retained by the selected execution mode.
        for table in &snapshot.tables {
            context.check()?;
            context.budget.debit(checked_size(
                table.name.len(),
                RETAINED_IDENTITY_RESERVE_BYTES,
            )?)?;
            for batch in &table.batches {
                context.check()?;
                context.budget.debit(retained_batch_identity_bytes(
                    &table.name,
                    &batch.id,
                    None,
                )?)?;
            }
            for file in &table.files {
                context.check()?;
                if !native_files && file.charged_bytes == 0 {
                    return Err(capacity_error(context.budget.limit));
                }
                context.budget.debit(retained_batch_identity_bytes(
                    &table.name,
                    &file.id,
                    Some(&file.path),
                )?)?;
            }
        }

        let mut plans = Vec::new();
        for table in tables {
            context.check()?;
            context.budget.debit(RELATION_MAP_ENTRY_BYTES)?;
            context.budget.debit(relation_map_key_bytes(&table.name)?)?;
            let key = RelationKey::Rollup(table.name.clone());
            let mut existing = None;
            for candidate in candidates {
                if let Some(descriptor) = candidate.relations.get(&key)
                    && descriptor.matches_rollups(
                        table,
                        context.deadline,
                        context.cancelled,
                        &mut context.work,
                    )?
                {
                    existing = Some(Arc::clone(descriptor));
                    break;
                }
            }
            let logical_bytes = match &existing {
                Some(descriptor) => descriptor.logical_bytes,
                None => RelationDescriptor::rollup_source_logical_bytes(table, context)?,
            };
            context.budget.debit(logical_bytes)?;
            plans.push((
                key,
                RelationPlan::Rollup {
                    table,
                    existing,
                    logical_bytes,
                },
            ));
        }
        for relation in &catalog.relations {
            context.check()?;
            context.budget.debit(RELATION_MAP_ENTRY_BYTES)?;
            context
                .budget
                .debit(relation_map_key_bytes(&relation.name)?)?;
            let key = RelationKey::Catalog(relation.name.clone());
            let mut existing = None;
            for candidate in candidates {
                if let Some(descriptor) = candidate.relations.get(&key)
                    && descriptor.matches_catalog(
                        relation,
                        context.deadline,
                        context.cancelled,
                        &mut context.work,
                    )?
                {
                    existing = Some(Arc::clone(descriptor));
                    break;
                }
            }
            let logical_bytes = match &existing {
                Some(descriptor) => descriptor.logical_bytes,
                None => RelationDescriptor::catalog_source_logical_bytes(relation, context)?,
            };
            context.budget.debit(logical_bytes)?;
            plans.push((
                key,
                RelationPlan::Catalog {
                    relation,
                    existing,
                    logical_bytes,
                },
            ));
        }

        let scope = match matching_scope {
            Some(scope) => scope,
            None => QueryScopeDescriptor::new(
                tables,
                options,
                catalog,
                native_files,
                scope_source_bytes.expect("new scope has a source charge"),
                context,
            )?,
        };
        let retained_bytes = context.budget.used;

        let mut relations = BTreeMap::new();
        for (key, plan) in plans {
            let descriptor = match plan {
                RelationPlan::Rollup {
                    table,
                    existing,
                    logical_bytes,
                } => match existing {
                    Some(descriptor) => descriptor,
                    None => RelationDescriptor::from_rollups(table, logical_bytes, context)?,
                },
                RelationPlan::Catalog {
                    relation,
                    existing,
                    logical_bytes,
                } => match existing {
                    Some(descriptor) => descriptor,
                    None => RelationDescriptor::from_catalog(relation, logical_bytes, context)?,
                },
            };
            ensure!(
                relations.insert(key, descriptor).is_none(),
                "duplicate resident relation"
            );
        }

        let mut batches = BTreeMap::new();
        for table in &snapshot.tables {
            for batch in &table.batches {
                ensure!(
                    batches
                        .insert(
                            (table.name.clone(), batch.id.clone()),
                            RequestedBatch::Memory(batch),
                        )
                        .is_none(),
                    "duplicate resident batch identity"
                );
            }
            for file in &table.files {
                if native_files {
                    continue;
                }
                ensure!(
                    batches
                        .insert(
                            (table.name.clone(), file.id.clone()),
                            RequestedBatch::File(file),
                        )
                        .is_none(),
                    "duplicate resident batch identity"
                );
            }
        }
        if !native_files {
            let mut minimum_bytes = retained_bytes;
            for batch in batches.values() {
                minimum_bytes = checked_size(minimum_bytes, batch.charged_bytes())?;
            }
            ensure_capacity(minimum_bytes, context.budget.limit)?;
        }
        Ok(Self {
            snapshot,
            scope,
            relations,
            batches,
            lineage,
            retained_bytes,
            limit: context.budget.limit,
            native_files,
            #[cfg(test)]
            work: context.work,
        })
    }
}

enum LoadedSource {
    Memory(WeakRawRows),
    VerifiedSegment,
}

impl LoadedSource {
    fn capture(batch: &RequestedBatch<'_>) -> Self {
        match batch {
            RequestedBatch::Memory(batch) if !batch.verified_segment => {
                Self::Memory(batch.rows.downgrade())
            }
            RequestedBatch::Memory(_) | RequestedBatch::File(_) => Self::VerifiedSegment,
        }
    }
}

struct LoadedBatch {
    source: LoadedSource,
    rows: usize,
    bytes: usize,
    charged_bytes: usize,
    // Covers the loaded-map identity and its optional complete-coverage key.
    // Selected requests already carry this reserve, so it is added only while
    // the loaded identity is outside the current selection.
    identity_bytes: usize,
    // Installed selected-ID membership; covered by the fixed identity reserve.
    selected: bool,
}

impl LoadedBatch {
    fn matches(&self, requested: &RequestedBatch<'_>) -> bool {
        // The caller has already matched the immutable ID via the batch map key.
        self.charged_bytes == requested.charged_bytes()
            && self.rows == requested.rows()
            && match (&self.source, requested) {
                (LoadedSource::Memory(identity), RequestedBatch::Memory(batch)) => {
                    identity.ptr_eq(&batch.rows.downgrade())
                }
                (LoadedSource::VerifiedSegment, RequestedBatch::File(_)) => true,
                (LoadedSource::VerifiedSegment, RequestedBatch::Memory(batch)) => {
                    batch.verified_segment
                }
                _ => false,
            }
    }
}

struct CompleteCoverage {
    raw_stamp: u64,
    lineage_ids: BTreeSet<String>,
    ids: BTreeSet<BatchKey>,
}

pub(super) struct ResidentState {
    namespace: String,
    sequence: u64,
    scope: Arc<QueryScopeDescriptor>,
    batches: BTreeMap<BatchKey, LoadedBatch>,
    complete: BTreeMap<String, CompleteCoverage>,
    relations: BTreeMap<RelationKey, Arc<RelationDescriptor>>,
    pub(super) rows: usize,
    pub(super) bytes: usize,
    pub(super) materialized_bytes: usize,
}

impl ResidentState {
    pub(super) fn compatible(&self, request: &ResidentRequest<'_>) -> bool {
        self.namespace == request.snapshot.namespace && self.sequence <= request.snapshot.sequence
    }

    pub(super) fn reuse_score(&self, request: &ResidentRequest<'_>) -> usize {
        request
            .batches
            .keys()
            .filter(|key| self.batches.contains_key(*key))
            .count()
            + self
                .complete
                .iter()
                .filter(|(name, coverage)| {
                    request.lineage.get(*name).is_some_and(|lineage| {
                        lineage.raw_stamp == coverage.raw_stamp
                            || coverage
                                .lineage_ids
                                .iter()
                                .all(|id| lineage.ids.contains(id.as_str()))
                    })
                })
                .map(|(_, coverage)| coverage.ids.len())
                .sum::<usize>()
    }
}

fn coverage_extra_bytes(
    name: &str,
    coverage: &CompleteCoverage,
    request: &ResidentRequest<'_>,
) -> Result<usize> {
    let selected = request
        .snapshot
        .tables
        .iter()
        .any(|table| table.name == name);
    let mut bytes = 0;
    if !selected {
        // The selected-table reserve covers even an empty CompleteCoverage.
        // Carry it explicitly once the query scope no longer selects the table.
        bytes = checked_size(
            bytes,
            checked_size(name.len(), RETAINED_IDENTITY_RESERVE_BYTES)?,
        )?;
    }
    let lineage = request
        .lineage
        .get(name)
        .expect("surviving resident coverage has current lineage");
    let refreshes_lineage = selected && lineage.raw_stamp == coverage.raw_stamp;
    if !refreshes_lineage {
        for id in &coverage.lineage_ids {
            if !lineage.ids.contains(id.as_str()) {
                bytes = checked_size(bytes, checked_size(id.len(), LINEAGE_ID_RESERVE_BYTES)?)?;
            }
        }
    }
    Ok(bytes)
}

fn resident_accounted_bytes(state: &ResidentState, request: &ResidentRequest<'_>) -> Result<usize> {
    let mut bytes = request.retained_bytes;
    for (key, loaded) in &state.batches {
        bytes = charge_with_limit(bytes, loaded.bytes, request.limit)?;
        if !request.batches.contains_key(key) {
            bytes = charge_with_limit(bytes, loaded.identity_bytes, request.limit)?;
        }
    }
    for (name, coverage) in &state.complete {
        bytes = charge_with_limit(
            bytes,
            coverage_extra_bytes(name, coverage, request)?,
            request.limit,
        )?;
    }
    Ok(bytes)
}

struct ResidentInstallPlan<'a> {
    full: bool,
    scope_changed: bool,
    active_batches: BTreeSet<BatchKey>,
    removed_batches: Vec<BatchKey>,
    missing_batches: Vec<(&'a BatchKey, &'a RequestedBatch<'a>)>,
    removed_relations: Vec<RelationKey>,
    changed_relations: Vec<&'a Arc<RelationDescriptor>>,
    previous_relations: Vec<Option<Arc<RelationDescriptor>>>,
    materialized_bytes: usize,
}

impl<'a> ResidentInstallPlan<'a> {
    fn needs_install(&self, state: Option<&ResidentState>) -> bool {
        self.full
            || self.scope_changed
            || !self.removed_batches.is_empty()
            || !self.missing_batches.is_empty()
            || !self.removed_relations.is_empty()
            || !self.changed_relations.is_empty()
            || state.is_none_or(|state| {
                state
                    .batches
                    .iter()
                    .any(|(key, loaded)| loaded.selected != self.active_batches.contains(key))
            })
    }

    fn new(state: Option<&ResidentState>, request: &'a ResidentRequest<'a>) -> Result<Self> {
        if let Some(state) = state {
            ensure!(state.compatible(request), "incompatible resident worker");
        }
        let full = state.is_none();
        let scope_changed =
            state.is_some_and(|state| !state.scope.same_typed_scope(&request.scope));
        let mut complete_tables = BTreeMap::new();
        if let Some(state) = state {
            for (name, coverage) in &state.complete {
                let lineage_compatible = request.lineage.get(name).is_some_and(|lineage| {
                    lineage.raw_stamp == coverage.raw_stamp
                        || coverage
                            .lineage_ids
                            .iter()
                            .all(|id| lineage.ids.contains(id.as_str()))
                });
                let overlapping_identities_match =
                    request.batches.iter().all(|(key, requested)| {
                        key.0 != *name
                            || state
                                .batches
                                .get(key)
                                .is_none_or(|loaded| loaded.matches(requested))
                    });
                if lineage_compatible && overlapping_identities_match {
                    complete_tables.insert(name.as_str(), coverage);
                }
            }
        }

        let mut active_batches = BTreeSet::new();
        for table in &request.snapshot.tables {
            if let Some(coverage) = complete_tables.get(table.name.as_str()) {
                active_batches.extend(coverage.ids.iter().cloned());
                if request.lineage[&table.name].raw_stamp != coverage.raw_stamp {
                    active_batches.extend(
                        request
                            .batches
                            .keys()
                            .filter(|key| {
                                key.0 == table.name && !coverage.lineage_ids.contains(&key.1)
                            })
                            .cloned(),
                    );
                }
            } else {
                active_batches.extend(
                    request
                        .batches
                        .keys()
                        .filter(|key| key.0 == table.name)
                        .cloned(),
                );
            }
        }

        let mut removed_batches = Vec::new();
        if let Some(state) = state {
            for (key, loaded) in &state.batches {
                let preserved_complete = complete_tables
                    .get(key.0.as_str())
                    .is_some_and(|coverage| coverage.ids.contains(key));
                let live = request
                    .lineage
                    .get(&key.0)
                    .is_some_and(|lineage| lineage.ids.contains(key.1.as_str()));
                let remapped = request
                    .batches
                    .get(key)
                    .is_some_and(|requested| !loaded.matches(requested));
                if remapped || (!live && !preserved_complete) {
                    removed_batches.push(key.clone());
                }
            }
        }
        let mut removed_set = removed_batches.iter().cloned().collect::<BTreeSet<_>>();
        let missing_batches = request
            .batches
            .iter()
            .filter(|(key, _)| {
                complete_tables.get(key.0.as_str()).is_none_or(|coverage| {
                    request.lineage[&key.0].raw_stamp != coverage.raw_stamp
                        && !coverage.lineage_ids.contains(&key.1)
                }) && state.is_none_or(|state| {
                    !state.batches.contains_key(*key) || removed_set.contains(*key)
                })
            })
            .collect::<Vec<_>>();

        let mut projected = request.retained_bytes;
        if let Some(state) = state {
            for (key, loaded) in &state.batches {
                if !removed_set.contains(key) {
                    projected = checked_size(projected, loaded.bytes)?;
                    if !request.batches.contains_key(key) {
                        projected = checked_size(projected, loaded.identity_bytes)?;
                    }
                }
            }
        }
        for (_, batch) in &missing_batches {
            projected = checked_size(projected, batch.charged_bytes())?;
        }
        let missing_keys = missing_batches
            .iter()
            .map(|(key, _)| *key)
            .collect::<BTreeSet<_>>();
        let mut projected_coverages = BTreeMap::new();
        if let Some(state) = state {
            for (name, coverage) in &state.complete {
                let lineage_compatible = request.lineage.get(name).is_some_and(|lineage| {
                    lineage.raw_stamp == coverage.raw_stamp
                        || coverage
                            .lineage_ids
                            .iter()
                            .all(|id| lineage.ids.contains(id.as_str()))
                });
                let identities_survive = coverage.ids.iter().all(|key| {
                    (!removed_set.contains(key) && state.batches.contains_key(key))
                        || missing_keys.contains(key)
                });
                if lineage_compatible && identities_survive {
                    let extra = coverage_extra_bytes(name, coverage, request)?;
                    projected = checked_size(projected, extra)?;
                    projected_coverages.insert(name.as_str(), extra);
                }
            }
        }
        if projected > request.limit
            && let Some(state) = state
        {
            for (key, loaded) in &state.batches {
                if projected <= request.limit {
                    break;
                }
                if !active_batches.contains(key) && !removed_set.contains(key) {
                    projected = projected.saturating_sub(loaded.bytes);
                    if !request.batches.contains_key(key) {
                        projected = projected.saturating_sub(loaded.identity_bytes);
                    }
                    let invalidated = projected_coverages
                        .keys()
                        .filter(|name| state.complete[**name].ids.contains(key))
                        .copied()
                        .collect::<Vec<_>>();
                    for name in invalidated {
                        if let Some(extra) = projected_coverages.remove(name) {
                            projected = projected.saturating_sub(extra);
                        }
                    }
                    removed_set.insert(key.clone());
                    removed_batches.push(key.clone());
                }
            }
        }
        ensure_capacity(projected, request.limit)?;

        let changed_relations: Vec<_> = request
            .relations
            .iter()
            .filter_map(|(key, descriptor)| {
                state
                    .and_then(|state| state.relations.get(key))
                    .is_none_or(|installed| installed.as_ref() != descriptor.as_ref())
                    .then_some(descriptor)
            })
            .collect();
        let previous_relations = changed_relations
            .iter()
            .map(|relation| {
                state
                    .and_then(|state| state.relations.get(&relation.key))
                    .cloned()
            })
            .collect();
        let removed_relations = state
            .into_iter()
            .flat_map(|state| state.relations.keys())
            .filter(|key| !request.relations.contains_key(*key))
            .cloned()
            .collect();
        let mut plan = Self {
            full,
            scope_changed,
            active_batches,
            removed_batches,
            missing_batches,
            removed_relations,
            changed_relations,
            previous_relations,
            materialized_bytes: state.map_or(0, |state| state.materialized_bytes),
        };
        // DELETE and COMMIT do not prove that DuckDB reclaimed old storage.
        // Retire a child before cumulative materialization exceeds its allowance.
        if plan.needs_install(state) {
            // Scope/identity metadata stays in the separate live-byte ledger.
            // This allowance tracks inserted data, including replaced values.
            for (_, batch) in &plan.missing_batches {
                plan.materialized_bytes =
                    checked_size(plan.materialized_bytes, batch.charged_bytes())?;
            }
            for (relation, previous) in plan.changed_relations.iter().zip(&plan.previous_relations)
            {
                plan.materialized_bytes = checked_size(
                    plan.materialized_bytes,
                    relation_delta_materialized_bytes(relation, previous.as_deref())?,
                )?;
            }
            for (table, id) in &plan.active_batches {
                plan.materialized_bytes = checked_size(
                    plan.materialized_bytes,
                    retained_batch_identity_bytes(table, id, None)?,
                )?;
            }
        }
        ensure_capacity(plan.materialized_bytes, request.limit)?;
        Ok(plan)
    }
}

fn charge_with_limit(current: usize, additional: usize, limit: usize) -> Result<usize> {
    let total = checked_size(current, additional)?;
    ensure_capacity(total, limit)?;
    Ok(total)
}

#[cfg(test)]
fn charge(current: usize, additional: usize) -> Result<usize> {
    charge_with_limit(current, additional, MAX_QUERY_INPUT_BYTES)
}

fn previous_rollups(previous: Option<&RelationDescriptor>) -> Option<&[PreparedRollupRow]> {
    match previous.map(|relation| &relation.rows) {
        Some(PreparedRelationRows::Rollup(rows)) => Some(rows),
        _ => None,
    }
}

fn relation_delta_materialized_bytes(
    relation: &RelationDescriptor,
    previous: Option<&RelationDescriptor>,
) -> Result<usize> {
    let (PreparedRelationRows::Rollup(rows), Some(old)) =
        (&relation.rows, previous_rollups(previous))
    else {
        return Ok(relation.logical_bytes);
    };
    rows.iter()
        .enumerate()
        .filter(|(index, row)| old.get(*index) != Some(*row))
        .try_fold(0, |total, (_, row)| {
            checked_size(total, row.materialized_bytes()?)
        })
}

#[cfg(test)]
fn append_relation_input(input: &mut Vec<u8>, relation: &RelationDescriptor) -> Result<()> {
    append_relation_delta(input, relation, None, false)
}

fn append_relation_delta(
    input: &mut Vec<u8>,
    relation: &RelationDescriptor,
    previous: Option<&RelationDescriptor>,
    slots: bool,
) -> Result<()> {
    match &relation.rows {
        PreparedRelationRows::Rollup(rows) => {
            let RelationKey::Rollup(table_name) = &relation.key else {
                bail!("rollup descriptor key mismatch");
            };
            let old = previous_rollups(previous);
            if let Some(old) = old {
                for (index, row) in old.iter().enumerate() {
                    if rows.get(index) != Some(row) {
                        append_json_line(
                            input,
                            &SelectedInput {
                                kind: "rollup_delete",
                                table_name,
                                batch_id: &index.to_string(),
                            },
                        )?;
                    }
                }
            }
            for (index, row) in rows.iter().enumerate() {
                if old.is_some_and(|old| old.get(index) == Some(row)) {
                    continue;
                }
                let slot = index.to_string();
                append_json_line(
                    input,
                    &RollupInput {
                        kind: "rollup",
                        table_name,
                        batch_id: slots.then_some(slot.as_str()),
                        width_us: row.width_us,
                        bucket_us: row.bucket_us,
                        tenant: &row.tenant,
                        series: &row.series,
                        tags: serde_json::to_string(&row.tags)?,
                        count: row.count,
                        sum: row.sum.get(),
                        min: row.min.get(),
                        max: row.max.get(),
                        first: row.first.get(),
                        last: row.last.get(),
                        first_timestamp_us: row.first_timestamp_us,
                        last_timestamp_us: row.last_timestamp_us,
                        first_sequence: row.first_sequence,
                        first_ordinal: row.first_ordinal,
                        last_sequence: row.last_sequence,
                        last_ordinal: row.last_ordinal,
                    },
                )?;
            }
        }
        PreparedRelationRows::Catalog(rows) => {
            let RelationKey::Catalog(relation_name) = &relation.key else {
                bail!("catalog descriptor key mismatch");
            };
            for row in rows {
                let values = row
                    .iter()
                    .map(PreparedCatalogValue::to_json)
                    .collect::<Vec<_>>();
                append_json_line(
                    input,
                    &json!({
                        "kind": "catalog", "table_name": relation_name,
                        "tags": serde_json::to_string(&values)?,
                    }),
                )?;
            }
        }
    }
    Ok(())
}

#[derive(Serialize)]
struct SelectedInput<'a> {
    kind: &'static str,
    table_name: &'a str,
    batch_id: &'a str,
}

struct StagedRaw {
    path: tempfile::TempPath,
    bytes: u64,
}

fn vacant_temp_path(inputs: &Path, suffix: &str) -> Result<tempfile::TempPath> {
    let file = tempfile::Builder::new()
        .prefix("resident-raw-")
        .suffix(suffix)
        .tempfile_in(inputs)
        .context("create resident typed input")?;
    let path = file.into_temp_path();
    std::fs::remove_file(&path).context("prepare resident typed input path")?;
    Ok(path)
}

fn stage_raw(inputs: &Path, batch: &RequestedBatch<'_>) -> Result<StagedRaw> {
    let path = vacant_temp_path(inputs, ".parquet")?;
    match batch {
        RequestedBatch::Memory(batch) => {
            crate::segment::write(&path, &batch.rows)
                .context("encode resident Arrow/Parquet batch")?;
        }
        RequestedBatch::File(file) => {
            if let Err(link_error) = std::fs::hard_link(&file.path, &path) {
                std::fs::copy(&file.path, &path).with_context(|| {
                    format!(
                        "copy pinned resident Parquet after hard-link failed ({link_error}): {}",
                        file.path.display()
                    )
                })?;
            }
        }
    }
    let bytes = std::fs::metadata(&path)
        .context("stat resident typed input")?
        .len();
    Ok(StagedRaw { path, bytes })
}

fn append_raw_insert(sql: &mut String, key: &BatchKey, path: &Path) -> Result<()> {
    sql.push_str("INSERT INTO __varve_input (kind, table_name, batch_id, timestamp_us, tenant, series, value, tags, sequence, ordinal) SELECT 'hot', ");
    sql.push_str(&quote_literal(&key.0));
    sql.push_str(", ");
    sql.push_str(&quote_literal(&key.1));
    sql.push_str(
        ", timestamp_us, tenant, series, value, tags, sequence, ordinal FROM read_parquet(",
    );
    sql.push_str(&quote_path(path)?);
    sql.push_str(");\n");
    Ok(())
}

fn append_batch_delete(sql: &mut String, key: &BatchKey) {
    sql.push_str("DELETE FROM __varve_input WHERE kind = 'hot' AND table_name = ");
    sql.push_str(&quote_literal(&key.0));
    sql.push_str(" AND batch_id = ");
    sql.push_str(&quote_literal(&key.1));
    sql.push_str(";\n");
}

impl Worker {
    pub(super) fn install_resident(
        &mut self,
        request: ResidentRequest<'_>,
        options: &QueryOptions,
        deadline: Instant,
        cancelled: &AtomicBool,
    ) -> Result<QueryWorkerStats> {
        self.resident_attempted = true;
        let plan = ResidentInstallPlan::new(self.resident.as_ref(), &request)?;
        let needs_install = plan.needs_install(self.resident.as_ref());
        let mut state = self.resident.take().unwrap_or_else(|| ResidentState {
            namespace: request.snapshot.namespace.clone(),
            sequence: request.snapshot.sequence,
            scope: Arc::clone(&request.scope),
            batches: BTreeMap::new(),
            complete: BTreeMap::new(),
            relations: BTreeMap::new(),
            rows: 0,
            bytes: 0,
            materialized_bytes: 0,
        });

        let mut staged = Vec::new();
        let mut raw_staged_rows = 0_u64;
        let mut raw_staged_bytes = 0_u64;
        for (key, batch) in &plan.missing_batches {
            check_deadline(deadline, cancelled)?;
            let raw = stage_raw(&self.inputs, batch)?;
            raw_staged_rows = raw_staged_rows.saturating_add(batch.rows() as u64);
            raw_staged_bytes = raw_staged_bytes.saturating_add(raw.bytes);
            staged.push(((*key).clone(), *batch, raw));
        }

        let mut input = Vec::new();
        for (relation, previous) in plan.changed_relations.iter().zip(&plan.previous_relations) {
            check_deadline(deadline, cancelled)?;
            append_relation_delta(&mut input, relation, previous.as_deref(), true)?;
        }
        let dynamic_staged_bytes = u64::try_from(input.len()).unwrap_or(u64::MAX);
        if needs_install {
            for (table, id) in &plan.active_batches {
                check_deadline(deadline, cancelled)?;
                append_json_line(
                    &mut input,
                    &SelectedInput {
                        kind: "selected",
                        table_name: table,
                        batch_id: id,
                    },
                )?;
            }
        }
        charge_with_limit(0, input.len(), request.limit)?;

        if needs_install {
            let mut script = self.configuration(options)?;
            script.push_str("BEGIN TRANSACTION;\n");
            if plan.full {
                script.push_str(&request.scope.setup);
                script.push_str(&request.scope.objects);
            } else if plan.scope_changed {
                state.scope.append_drop_objects(&mut script);
            }
            for key in &plan.removed_relations {
                key.append_delete(&mut script);
            }
            for (relation, previous) in plan.changed_relations.iter().zip(&plan.previous_relations)
            {
                if !matches!(relation.key, RelationKey::Rollup(_)) || previous.is_none() {
                    relation.key.append_delete(&mut script);
                }
            }
            for key in &plan.removed_batches {
                append_batch_delete(&mut script, key);
            }
            script.push_str("DELETE FROM __varve_input WHERE kind = 'selected';\n");
            for (key, _, raw) in &staged {
                append_raw_insert(&mut script, key, &raw.path)?;
            }

            let mut dynamic_file = None;
            if !input.is_empty() {
                let mut file = tempfile::Builder::new()
                    .prefix("resident-dynamic-")
                    .suffix(".json")
                    .tempfile_in(&self.inputs)
                    .context("create resident dynamic input")?;
                file.write_all(&input)
                    .context("stage resident dynamic input")?;
                file.flush().context("flush resident dynamic input")?;
                let mut scanner = format!(
                    "read_json({}, format = 'newline_delimited', auto_detect = false, columns = {{",
                    quote_path(file.path())?
                );
                for (index, (name, kind)) in INPUT_COLUMNS.iter().enumerate() {
                    if index > 0 {
                        scanner.push(',');
                    }
                    scanner.push_str(&format!(
                        "{}: {}",
                        quote_identifier(name),
                        quote_literal(kind)
                    ));
                }
                scanner.push_str("})");
                if plan
                    .changed_relations
                    .iter()
                    .zip(&plan.previous_relations)
                    .any(|(relation, previous)| {
                        matches!(relation.key, RelationKey::Rollup(_)) && previous.is_some()
                    })
                {
                    script.push_str("DELETE FROM __varve_input AS old USING ");
                    script.push_str(&scanner);
                    script.push_str(" AS changed WHERE old.kind = 'rollup' AND changed.kind = 'rollup_delete' AND old.table_name = changed.table_name AND old.batch_id = changed.batch_id;\n");
                }
                script.push_str("INSERT INTO __varve_input SELECT * FROM ");
                script.push_str(&scanner);
                script.push_str(" WHERE kind <> 'rollup_delete';\n");
                dynamic_file = Some(file);
            }
            if !plan.full && plan.scope_changed {
                script.push_str(&request.scope.objects);
            }
            script.push_str("COMMIT;\n");
            ensure!(
                script.len() <= MAX_QUERY_SCRIPT_BYTES,
                "resident SQL script exceeds limit"
            );
            ensure!(
                self.run(script, deadline, cancelled, 0)?.is_empty(),
                "unexpected resident setup output"
            );
            if let Some(file) = dynamic_file {
                file.close().context("remove resident dynamic input")?;
            }
            for (_, _, raw) in staged {
                raw.path.close().context("remove resident typed input")?;
            }
        }
        check_deadline(deadline, cancelled)?;

        for key in &plan.removed_batches {
            state.batches.remove(key);
        }
        for (key, batch) in &plan.missing_batches {
            let source = LoadedSource::capture(batch);
            state.batches.insert(
                (*key).clone(),
                LoadedBatch {
                    source,
                    rows: batch.rows(),
                    bytes: batch.charged_bytes(),
                    charged_bytes: batch.charged_bytes(),
                    identity_bytes: batch.retained_identity_bytes(key)?,
                    selected: false,
                },
            );
        }

        state.complete.retain(|name, coverage| {
            request.lineage.get(name).is_some_and(|lineage| {
                (lineage.raw_stamp == coverage.raw_stamp
                    || coverage
                        .lineage_ids
                        .iter()
                        .all(|id| lineage.ids.contains(id.as_str())))
                    && coverage
                        .ids
                        .iter()
                        .all(|key| state.batches.contains_key(key))
            })
        });
        for table in &request.snapshot.tables {
            let lineage = request
                .lineage
                .get(&table.name)
                .expect("validated resident lineage");
            let live = &lineage.ids;
            let selected = request
                .batches
                .keys()
                .filter(|key| key.0 == table.name)
                .map(|key| key.1.as_str())
                .collect::<BTreeSet<_>>();
            if let Some(coverage) = state.complete.get_mut(&table.name) {
                if coverage.raw_stamp == lineage.raw_stamp {
                    coverage.lineage_ids = live.iter().map(|id| (*id).to_owned()).collect();
                } else {
                    let represented = coverage
                        .lineage_ids
                        .iter()
                        .map(String::as_str)
                        .chain(selected.iter().copied())
                        .collect::<BTreeSet<_>>();
                    if represented == *live {
                        coverage.ids.extend(
                            request
                                .batches
                                .keys()
                                .filter(|key| {
                                    key.0 == table.name && state.batches.contains_key(*key)
                                })
                                .cloned(),
                        );
                        coverage.raw_stamp = lineage.raw_stamp;
                        coverage.lineage_ids = live.iter().map(|id| (*id).to_owned()).collect();
                    }
                }
            } else if selected == *live {
                let ids = request
                    .batches
                    .keys()
                    .filter(|key| key.0 == table.name)
                    .cloned()
                    .collect();
                state.complete.insert(
                    table.name.clone(),
                    CompleteCoverage {
                        raw_stamp: lineage.raw_stamp,
                        lineage_ids: live.iter().map(|id| (*id).to_owned()).collect(),
                        ids,
                    },
                );
            }
        }
        let bytes = resident_accounted_bytes(&state, &request)?;
        let mut rows = 0_usize;
        for (key, loaded) in &mut state.batches {
            loaded.selected = plan.active_batches.contains(key);
            rows = rows
                .checked_add(loaded.rows)
                .context("resident row count overflow")?;
        }
        state.namespace = request.snapshot.namespace.clone();
        state.sequence = request.snapshot.sequence;
        state.scope = Arc::clone(&request.scope);
        state.relations = request.relations.clone();
        state.rows = rows;
        state.bytes = bytes;
        state.materialized_bytes = plan.materialized_bytes;
        self.resident = Some(state);
        let additions = plan.missing_batches.len();
        let dynamic_changed = !plan.removed_relations.is_empty()
            || plan
                .changed_relations
                .iter()
                .any(|relation| !plan.full || !relation.is_empty());
        Ok(QueryWorkerStats {
            resident_full_loads: u64::from(plan.full),
            resident_delta_loads: u64::from(!plan.full && additions != 0),
            resident_hits: u64::from(!plan.full && additions == 0),
            resident_raw_staged_rows: raw_staged_rows,
            resident_raw_staged_bytes: raw_staged_bytes,
            resident_dynamic_loads: u64::from(dynamic_changed),
            resident_dynamic_staged_bytes: dynamic_staged_bytes,
            ..QueryWorkerStats::default()
        })
    }

    pub(super) fn configuration(&self, options: &QueryOptions) -> Result<String> {
        let mut script = String::new();
        if !self.initialized {
            let paths = self
                .key
                .files
                .iter()
                .map(|file| quote_path(file))
                .collect::<Result<Vec<_>>>()?;
            script.push_str(&format!("SET memory_limit = '{}MB'; SET threads = {}; SET max_temp_directory_size = '0B'; SET preserve_insertion_order = false; ", options.memory_mb, options.threads));
            script.push_str("SET autoinstall_known_extensions = false; SET autoload_known_extensions = false; SET allow_community_extensions = false; SET allow_unsigned_extensions = false; ");
            script.push_str(&format!("SET allowed_directories = [{}]; SET allowed_paths = [{}]; SET home_directory = {}; SET temp_directory = {}; SET enable_external_access = false; SET lock_configuration = true;\n", quote_path(&self.inputs)?, paths.join(","), quote_path(self.directory.path())?, quote_path(self.directory.path())?));
        }
        Ok(script)
    }
}
