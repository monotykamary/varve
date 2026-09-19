use super::*;
use crate::model::Row;
use std::sync::atomic::AtomicBool;
use std::time::Duration;

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(5)
}

fn stored(value: f64, timestamp_us: i64, sequence: u64) -> StoredRow {
    StoredRow {
        row: Row {
            timestamp_us,
            tenant: "tenant".into(),
            series: "series".into(),
            value,
            tags: BTreeMap::from([("region".into(), "雪".into())]),
        },
        sequence,
        ordinal: u32::MAX,
    }
}

fn inputs() -> (Vec<QueryTable>, ResidentSnapshot, QueryCatalog) {
    let raw = stored(-0.0, -60, u64::MAX);
    let rollup = RollupRow::from_row(60, &raw).unwrap();
    let rows = crate::raw_memory::SharedRawRows::test_rows(vec![raw]);
    let charged_bytes = rows.iter().map(|row| row.row.estimated_bytes()).sum();
    (
        vec![QueryTable {
            name: "metrics".into(),
            hot: vec![],
            files: vec![],
            rollups: vec![rollup],
            cutoff_us: None,
        }],
        ResidentSnapshot {
            namespace: "descriptor-tests".into(),
            sequence: 7,
            tables: vec![ResidentTable {
                name: "metrics".into(),
                batches: vec![ResidentBatch::new("batch-a".into(), rows, charged_bytes)],
                files: vec![],
            }],
            lineage: vec![ResidentLineage {
                name: "metrics".into(),
                raw_stamp: 1,
                ids: vec!["batch-a".into()],
            }],
        },
        QueryCatalog {
            relations: vec![
                CatalogRelation {
                    name: "varve_jobs".into(),
                    columns: vec![
                        ("job".into(), "VARCHAR".into()),
                        ("last_run_us".into(), "BIGINT".into()),
                    ],
                    rows: vec![json!(["flush", i64::MIN])],
                },
                CatalogRelation {
                    name: "varve_status".into(),
                    columns: vec![
                        ("remote_sequence".into(), "UBIGINT".into()),
                        ("healthy".into(), "BOOLEAN".into()),
                    ],
                    rows: vec![json!([u64::MAX, true])],
                },
            ],
            aggregates: vec![AggregateAlias {
                name: "minute".into(),
                source: "metrics".into(),
                width_us: 60,
            }],
        },
    )
}

fn prepare<'a>(
    tables: &'a [QueryTable],
    snapshot: &'a ResidentSnapshot,
    catalog: &'a QueryCatalog,
    candidates: &[&ResidentState],
) -> ResidentRequest<'a> {
    ResidentRequest::new(
        tables,
        snapshot,
        &QueryOptions::default(),
        catalog,
        candidates,
        false,
        deadline(),
        &AtomicBool::new(false),
    )
    .unwrap()
}

fn prepare_with_limit<'a>(
    tables: &'a [QueryTable],
    snapshot: &'a ResidentSnapshot,
    catalog: &'a QueryCatalog,
    candidates: &[&ResidentState],
    limit: usize,
) -> (Result<ResidentRequest<'a>>, DescriptorWork) {
    let cancelled = AtomicBool::new(false);
    let mut context = BuildContext::new(limit, deadline(), &cancelled);
    let result = ResidentRequest::new_with_context(
        tables,
        snapshot,
        &QueryOptions::default(),
        catalog,
        candidates,
        false,
        &mut context,
    );
    (result, context.work)
}

fn accepted(request: &ResidentRequest<'_>) -> ResidentState {
    let batches = request
        .batches
        .iter()
        .map(|(key, batch)| {
            let source = LoadedSource::capture(batch);
            (
                key.clone(),
                LoadedBatch {
                    source,
                    rows: batch.rows(),
                    bytes: batch.charged_bytes(),
                    charged_bytes: batch.charged_bytes(),
                    identity_bytes: batch.retained_identity_bytes(key).unwrap(),
                    selected: true,
                },
            )
        })
        .collect();
    ResidentState {
        namespace: request.snapshot.namespace.clone(),
        sequence: request.snapshot.sequence,
        scope: Arc::clone(&request.scope),
        batches,
        complete: BTreeMap::new(),
        relations: request.relations.clone(),
        materialized_bytes: ResidentInstallPlan::new(None, request)
            .unwrap()
            .materialized_bytes,
        rows: request.batches.values().map(RequestedBatch::rows).sum(),
        bytes: request
            .batches
            .values()
            .try_fold(request.retained_bytes, |bytes, batch| {
                charge(bytes, batch.charged_bytes())
            })
            .unwrap(),
    }
}

fn encoded(relations: &[&Arc<RelationDescriptor>]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for relation in relations {
        append_relation_input(&mut bytes, relation).unwrap();
    }
    bytes
}

#[test]
fn unchanged_scope_reuses_typed_descriptors_before_schema_or_payload_work() {
    let (tables, snapshot, catalog) = inputs();
    let initial = prepare(&tables, &snapshot, &catalog, &[]);
    assert_eq!(initial.work.setup_builds, 1);
    assert_eq!(initial.work.scope_builds, 1);
    assert_eq!(initial.work.relation_builds, 3);

    let mut old_dynamic = Vec::new();
    for table in &tables {
        append_table_input(&mut old_dynamic, table).unwrap();
    }
    for relation in &catalog.relations {
        append_catalog_input(&mut old_dynamic, relation).unwrap();
    }
    assert!(!old_dynamic.is_empty());

    let state = accepted(&initial);
    let repeated = prepare(&tables, &snapshot, &catalog, &[&state]);
    let plan = ResidentInstallPlan::new(Some(&state), &repeated).unwrap();
    assert_eq!(repeated.work.setup_builds, 0);
    assert_eq!(repeated.work.scope_builds, 0);
    assert_eq!(repeated.work.relation_builds, 0);
    assert_eq!(repeated.work.payload_clones, 0);
    assert_eq!(repeated.work.relation_rows_compared, 3);
    assert!(Arc::ptr_eq(&initial.scope, &repeated.scope));
    assert!(plan.missing_batches.is_empty());
    assert!(plan.changed_relations.is_empty());
    assert!(!plan.needs_install(Some(&state)));
    assert!(encoded(&plan.changed_relations).is_empty());
}

#[test]
fn one_metadata_relation_change_plans_and_encodes_only_that_relation() {
    let (tables, snapshot, mut catalog) = inputs();
    let initial = prepare(&tables, &snapshot, &catalog, &[]);
    let state = accepted(&initial);

    catalog.relations[0].rows = vec![json!(["checkpoint", 42])];
    let job_change = prepare(&tables, &snapshot, &catalog, &[&state]);
    let job_plan = ResidentInstallPlan::new(Some(&state), &job_change).unwrap();
    assert_eq!(job_change.work.setup_builds, 0);
    assert_eq!(job_change.work.relation_builds, 1);
    assert_eq!(job_plan.changed_relations.len(), 1);
    assert!(job_plan.needs_install(Some(&state)));
    assert_eq!(
        job_plan.changed_relations[0].key,
        RelationKey::Catalog("varve_jobs".into())
    );
    let mut expected = Vec::new();
    append_catalog_input(&mut expected, &catalog.relations[0]).unwrap();
    assert_eq!(encoded(&job_plan.changed_relations), expected);

    let job_state = accepted(&job_change);
    catalog.relations[1].rows = vec![json!([3_u64, false])];
    let status_change = prepare(&tables, &snapshot, &catalog, &[&job_state]);
    let status_plan = ResidentInstallPlan::new(Some(&job_state), &status_change).unwrap();
    assert_eq!(status_change.work.setup_builds, 0);
    assert_eq!(status_change.work.relation_builds, 1);
    assert_eq!(status_plan.changed_relations.len(), 1);
    assert_eq!(
        status_plan.changed_relations[0].key,
        RelationKey::Catalog("varve_status".into())
    );
}

#[test]
fn append_delta_keeps_relations_and_plans_only_the_new_raw_identity() {
    let (tables, mut snapshot, catalog) = inputs();
    let initial = prepare(&tables, &snapshot, &catalog, &[]);
    let state = accepted(&initial);
    let rows = crate::raw_memory::SharedRawRows::test_rows(vec![stored(1.5, i64::MAX, 8)]);
    let charged_bytes = rows.iter().map(|row| row.row.estimated_bytes()).sum();
    snapshot.tables[0]
        .batches
        .push(ResidentBatch::new("batch-b".into(), rows, charged_bytes));
    snapshot.lineage[0].ids.push("batch-b".into());
    snapshot.lineage[0].raw_stamp += 1;
    snapshot.sequence += 1;

    let appended = prepare(&tables, &snapshot, &catalog, &[&state]);
    let plan = ResidentInstallPlan::new(Some(&state), &appended).unwrap();
    assert_eq!(appended.work.setup_builds, 0);
    assert_eq!(appended.work.relation_builds, 0);
    assert_eq!(plan.missing_batches.len(), 1);
    assert_eq!(plan.missing_batches[0].0.1, "batch-b");
    assert!(plan.changed_relations.is_empty());
}

#[test]
fn signed_zero_rollup_and_catalog_values_are_exact_typed_changes() {
    let (mut tables, snapshot, mut catalog) = inputs();
    catalog.relations[0] = CatalogRelation {
        name: "numbers".into(),
        columns: vec![
            ("float".into(), "DOUBLE".into()),
            ("signed".into(), "BIGINT".into()),
            ("unsigned".into(), "UBIGINT".into()),
        ],
        rows: vec![json!([-0.0, i64::MIN, u64::MAX])],
    };
    catalog.relations.truncate(1);
    let initial = prepare(&tables, &snapshot, &catalog, &[]);
    let state = accepted(&initial);

    catalog.relations[0].rows = vec![json!([0.0, i64::MIN, u64::MAX])];
    let catalog_change = prepare(&tables, &snapshot, &catalog, &[&state]);
    let catalog_plan = ResidentInstallPlan::new(Some(&state), &catalog_change).unwrap();
    assert_eq!(catalog_plan.changed_relations.len(), 1);
    assert_eq!(catalog_change.work.relation_builds, 1);

    let catalog_state = accepted(&catalog_change);
    tables[0].rollups[0].sum = 0.0;
    let rollup_change = prepare(&tables, &snapshot, &catalog, &[&catalog_state]);
    let rollup_plan = ResidentInstallPlan::new(Some(&catalog_state), &rollup_change).unwrap();
    assert_eq!(rollup_plan.changed_relations.len(), 1);
    assert_eq!(
        rollup_plan.changed_relations[0].key,
        RelationKey::Rollup("metrics".into())
    );
}

#[test]
fn descriptor_payload_and_scope_are_equivalent_to_existing_builders() {
    let (tables, snapshot, catalog) = inputs();
    let request = prepare(&tables, &snapshot, &catalog, &[]);

    let empty_tables = tables
        .iter()
        .map(|table| QueryTable {
            name: table.name.clone(),
            hot: vec![],
            files: table.files.clone(),
            rollups: vec![],
            cutoff_us: table.cutoff_us,
        })
        .collect::<Vec<_>>();
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
    let (legacy_setup, legacy_input) = build_query_mode(
        &empty_tables,
        "",
        &QueryOptions::default(),
        &empty_catalog,
        Path::new("."),
        false,
    )
    .unwrap();
    assert!(legacy_input.is_empty());
    let (_, legacy_setup) = legacy_setup
        .split_once("CREATE TEMP TABLE __varve_version_gate")
        .unwrap();
    assert_eq!(
        format!("{}{}", request.scope.setup, request.scope.objects),
        format!("CREATE TEMP TABLE __varve_version_gate{legacy_setup}")
    );

    for table in &tables {
        let descriptor = &request.relations[&RelationKey::Rollup(table.name.clone())];
        let mut expected = Vec::new();
        append_table_input(&mut expected, table).unwrap();
        let mut actual = Vec::new();
        append_relation_input(&mut actual, descriptor).unwrap();
        assert_eq!(actual, expected);
    }
    for relation in &catalog.relations {
        let descriptor = &request.relations[&RelationKey::Catalog(relation.name.clone())];
        let mut expected = Vec::new();
        append_catalog_input(&mut expected, relation).unwrap();
        let mut actual = Vec::new();
        append_relation_input(&mut actual, descriptor).unwrap();
        assert_eq!(actual, expected);
    }
}
#[test]
fn inversion_rebuilds_while_removals_remaps_and_scope_changes_reconcile() {
    let (tables, snapshot, catalog) = inputs();
    let initial = prepare(&tables, &snapshot, &catalog, &[]);
    let state = accepted(&initial);

    let mut inverted = snapshot.clone();
    inverted.sequence -= 1;
    let request = prepare(&tables, &inverted, &catalog, &[&state]);
    assert!(!state.compatible(&request));

    let mut removed = snapshot.clone();
    removed.tables[0].batches.clear();
    removed.lineage[0].ids.clear();
    removed.lineage[0].raw_stamp += 1;
    let request = prepare(&tables, &removed, &catalog, &[&state]);
    assert!(state.compatible(&request));
    let plan = ResidentInstallPlan::new(Some(&state), &request).unwrap();
    assert_eq!(
        plan.removed_batches,
        vec![("metrics".into(), "batch-a".into())]
    );

    let mut remapped = snapshot.clone();
    remapped.tables[0].batches[0].rows =
        crate::raw_memory::SharedRawRows::test_rows(snapshot.tables[0].batches[0].rows.to_vec());
    let request = prepare(&tables, &remapped, &catalog, &[&state]);
    assert!(state.compatible(&request));
    let plan = ResidentInstallPlan::new(Some(&state), &request).unwrap();
    assert_eq!(plan.removed_batches.len(), 1);
    assert_eq!(plan.missing_batches.len(), 1);

    let mut recharged = snapshot.clone();
    recharged.tables[0].batches[0].charged_bytes += 1;
    let request = prepare(&tables, &recharged, &catalog, &[&state]);
    let plan = ResidentInstallPlan::new(Some(&state), &request).unwrap();
    assert_eq!(plan.removed_batches.len(), 1);
    assert_eq!(plan.missing_batches.len(), 1);

    let mut moved_file = tables.clone();
    let path = PathBuf::from("/tmp/descriptor-remap.parquet");
    moved_file[0].files.push(path.clone());
    let mut selected_file = snapshot.clone();
    selected_file.tables[0].files.push(ResidentFile {
        id: "file-b".into(),
        path,
        rows: 1,
        charged_bytes: 128,
        min_timestamp_us: 0,
        max_timestamp_us: 0,
    });
    selected_file.lineage[0].ids.push("file-b".into());
    let request = prepare(&moved_file, &selected_file, &catalog, &[&state]);
    assert_eq!(request.work.setup_builds, 0);
    assert!(state.compatible(&request));

    let mut cutoff = tables.clone();
    cutoff[0].cutoff_us = Some(0);
    let request = prepare(&cutoff, &snapshot, &catalog, &[&state]);
    assert_eq!(request.work.setup_builds, 1);
    assert!(state.compatible(&request));
    assert!(
        ResidentInstallPlan::new(Some(&state), &request)
            .unwrap()
            .scope_changed
    );

    let mut fewer_relations = catalog.clone();
    fewer_relations.relations.pop();
    let request = prepare(&tables, &snapshot, &fewer_relations, &[&state]);
    assert_eq!(request.work.setup_builds, 1);
    assert!(state.compatible(&request));
    assert_eq!(
        ResidentInstallPlan::new(Some(&state), &request)
            .unwrap()
            .removed_relations
            .len(),
        1
    );
}

#[test]
fn admission_rejects_an_oversized_relation_before_payload_clones() {
    let (mut tables, mut snapshot, _) = inputs();
    tables[0].rollups.clear();
    snapshot.tables[0].batches.clear();
    let catalog = QueryCatalog {
        relations: vec![],
        aggregates: vec![],
    };
    let baseline = prepare(&tables, &snapshot, &catalog, &[]);
    let state = accepted(&baseline);

    let row = RollupRow::from_row(60, &stored(1.0, 0, 1)).unwrap();
    tables[0].rollups = (0..24)
        .map(|index| {
            let mut row = row.clone();
            row.bucket_us = index;
            row
        })
        .collect();
    let admitted = prepare(&tables, &snapshot, &catalog, &[&state]);
    let limit = admitted.retained_bytes - 1;
    assert!(limit < MAX_QUERY_INPUT_BYTES);

    let (result, work) = prepare_with_limit(&tables, &snapshot, &catalog, &[&state], limit);
    let error = result.err().unwrap();
    assert!(error.to_string().contains("exceeds"));
    assert_eq!(work.payload_clones, 0);
    assert_eq!(work.relation_builds, 0);
}

#[test]
fn admission_rejects_one_large_text_before_payload_clones() {
    let tables = vec![];
    let snapshot = ResidentSnapshot {
        namespace: "text-budget".into(),
        sequence: 1,
        tables: vec![],
        lineage: vec![],
    };
    let mut catalog = QueryCatalog {
        relations: vec![CatalogRelation {
            name: "large_text".into(),
            columns: vec![("value".into(), "VARCHAR".into())],
            rows: vec![],
        }],
        aggregates: vec![],
    };
    let baseline = prepare(&tables, &snapshot, &catalog, &[]);
    let state = accepted(&baseline);
    catalog.relations[0].rows = vec![json!(["x".repeat(4096)])];
    let admitted = prepare(&tables, &snapshot, &catalog, &[&state]);
    let limit = admitted.retained_bytes - 1;
    assert!(limit < MAX_QUERY_INPUT_BYTES);

    let (result, work) = prepare_with_limit(&tables, &snapshot, &catalog, &[&state], limit);
    let error = result.err().unwrap();
    assert!(error.to_string().contains("exceeds"));
    assert_eq!(work.payload_clones, 0);
    assert_eq!(work.relation_builds, 0);
}

#[test]
fn admission_combines_two_individually_fitting_relations_before_clones() {
    let tables = vec![];
    let snapshot = ResidentSnapshot {
        namespace: "combined-budget".into(),
        sequence: 1,
        tables: vec![],
        lineage: vec![],
    };
    let empty_relation = |name: &str| CatalogRelation {
        name: name.into(),
        columns: vec![("value".into(), "VARCHAR".into())],
        rows: vec![],
    };
    let empty_catalog = QueryCatalog {
        relations: vec![empty_relation("first"), empty_relation("second")],
        aggregates: vec![],
    };
    let mut first = empty_catalog.clone();
    first.relations[0].rows = vec![json!(["a".repeat(768)])];
    let mut second = empty_catalog.clone();
    second.relations[1].rows = vec![json!(["b".repeat(768)])];
    let mut combined = first.clone();
    combined.relations[1].rows = second.relations[1].rows.clone();

    let first_total = prepare(&tables, &snapshot, &first, &[]).retained_bytes;
    let second_total = prepare(&tables, &snapshot, &second, &[]).retained_bytes;
    let combined_total = prepare(&tables, &snapshot, &combined, &[]).retained_bytes;
    let limit = first_total.max(second_total);
    assert!(combined_total > limit);
    assert!(limit < MAX_QUERY_INPUT_BYTES);
    assert!(
        prepare_with_limit(&tables, &snapshot, &first, &[], limit)
            .0
            .is_ok()
    );
    assert!(
        prepare_with_limit(&tables, &snapshot, &second, &[], limit)
            .0
            .is_ok()
    );

    let (result, work) = prepare_with_limit(&tables, &snapshot, &combined, &[], limit);
    let error = result.err().unwrap();
    assert!(error.to_string().contains("exceeds"));
    assert_eq!(work.payload_clones, 0);
    assert_eq!(work.relation_builds, 0);
}

#[test]
fn verified_segment_matching_does_not_trust_id_spelling_or_hot_remaps() {
    let mut batch = ResidentBatch::new(
        "a".repeat(64),
        crate::raw_memory::SharedRawRows::test_rows(vec![StoredRow {
            row: crate::model::Row {
                timestamp_us: 0,
                tenant: "t".into(),
                series: "s".into(),
                value: 1.0,
                tags: Default::default(),
            },
            sequence: 1,
            ordinal: 0,
        }]),
        128,
    );
    let file = ResidentFile {
        id: batch.id.clone(),
        path: PathBuf::from("/unused"),
        rows: 1,
        charged_bytes: 128,
        min_timestamp_us: 0,
        max_timestamp_us: 0,
    };
    let loaded = |source| LoadedBatch {
        source,
        rows: 1,
        bytes: 128,
        charged_bytes: 128,
        identity_bytes: 512,
        selected: true,
    };
    let hot = loaded(LoadedSource::capture(&RequestedBatch::Memory(&batch)));
    assert!(hot.matches(&RequestedBatch::Memory(&batch)));
    assert!(!hot.matches(&RequestedBatch::File(&file)));
    let segment = loaded(LoadedSource::capture(&RequestedBatch::File(&file)));
    assert!(!segment.matches(&RequestedBatch::Memory(&batch)));
    batch.rows = crate::raw_memory::SharedRawRows::test_rows(batch.rows.to_vec());
    assert!(!hot.matches(&RequestedBatch::Memory(&batch)));
    // Only explicit engine provenance, not a 64-character ID, permits reuse.
    batch.verified_segment = true;
    assert!(segment.matches(&RequestedBatch::Memory(&batch)));
    let decoded = loaded(LoadedSource::capture(&RequestedBatch::Memory(&batch)));
    assert!(decoded.matches(&RequestedBatch::File(&file)));
    batch.rows = crate::raw_memory::SharedRawRows::test_rows(batch.rows.to_vec());
    assert!(decoded.matches(&RequestedBatch::Memory(&batch)));
    batch.charged_bytes += 1;
    assert!(!segment.matches(&RequestedBatch::Memory(&batch)));
    batch.charged_bytes -= 1;
    let extra = batch.rows[0].clone();
    let mut rows = batch.rows.to_vec();
    rows.push(extra);
    batch.rows = crate::raw_memory::SharedRawRows::test_rows(rows);
    assert!(!segment.matches(&RequestedBatch::Memory(&batch)));
}

#[test]
fn retained_identity_reserves_cover_all_key_copies_and_empty_coverage() {
    assert!(
        std::mem::size_of::<LoadedBatch>() + 2 * std::mem::size_of::<BatchKey>()
            <= RETAINED_IDENTITY_RESERVE_BYTES
    );
    assert!(
        std::mem::size_of::<CompleteCoverage>() + std::mem::size_of::<String>()
            <= RETAINED_IDENTITY_RESERVE_BYTES
    );
    assert!(std::mem::size_of::<String>() <= LINEAGE_ID_RESERVE_BYTES);

    let large = "x".repeat(4096);
    assert_eq!(
        retained_batch_identity_bytes("metrics", &large, None).unwrap(),
        2 * ("metrics".len() + large.len()) + RETAINED_IDENTITY_RESERVE_BYTES
    );
}

#[test]
fn admission_charges_empty_catalog_relation_map_identities() {
    let tables = vec![];
    let snapshot = ResidentSnapshot {
        namespace: "empty-identities".into(),
        sequence: 1,
        tables: vec![],
        lineage: vec![],
    };
    let catalog = QueryCatalog {
        relations: ["jobs", "status", "fence"]
            .into_iter()
            .map(|name| CatalogRelation {
                name: name.into(),
                columns: vec![("value".into(), "VARCHAR".into())],
                rows: vec![],
            })
            .collect(),
        aggregates: vec![],
    };
    let request = prepare(&tables, &snapshot, &catalog, &[]);
    let descriptor_bytes: usize = request
        .relations
        .values()
        .map(|relation| relation.logical_bytes)
        .sum();
    let identity_bytes: usize = catalog
        .relations
        .iter()
        .map(|relation| RELATION_MAP_ENTRY_BYTES + relation_map_key_bytes(&relation.name).unwrap())
        .sum();
    assert!(identity_bytes > catalog.relations.iter().map(|r| r.name.len()).sum());
    assert_eq!(
        request.retained_bytes,
        request.scope.logical_bytes + snapshot.namespace.len() + descriptor_bytes + identity_bytes
    );
    let state = accepted(&request);
    assert_eq!(state.bytes, request.retained_bytes);
}

#[test]
fn changed_to_empty_relation_keeps_a_targeted_delete() {
    let (tables, snapshot, mut catalog) = inputs();
    let initial = prepare(&tables, &snapshot, &catalog, &[]);
    let state = accepted(&initial);
    catalog.relations[0].rows.clear();
    let changed = prepare(&tables, &snapshot, &catalog, &[&state]);
    let plan = ResidentInstallPlan::new(Some(&state), &changed).unwrap();
    assert_eq!(plan.changed_relations.len(), 1);
    assert!(plan.changed_relations[0].is_empty());
    let mut script = String::new();
    plan.changed_relations[0].key.append_delete(&mut script);
    assert_eq!(
        script,
        "DELETE FROM __varve_input WHERE kind = 'catalog' AND table_name = 'varve_jobs';\n"
    );
}

#[test]
fn indexed_identity_preflight_preserves_large_hot_snapshots() {
    let (tables, mut snapshot, mut catalog) = inputs();
    let sample = snapshot.tables[0].batches[0].clone();
    snapshot.tables[0].batches = (0..1024)
        .map(|index| {
            ResidentBatch::new(
                format!("batch-{index:04}"),
                sample.rows.clone(),
                sample.charged_bytes,
            )
        })
        .collect();
    snapshot.lineage[0].ids = snapshot.tables[0]
        .batches
        .iter()
        .map(|batch| batch.id.clone())
        .collect();
    catalog
        .relations
        .extend((0..128).map(|index| CatalogRelation {
            name: format!("varve_extra_{index}"),
            columns: vec![("value".into(), "BIGINT".into())],
            rows: vec![],
        }));
    let initial = prepare(&tables, &snapshot, &catalog, &[]);
    let state = accepted(&initial);
    let warm = prepare(&tables, &snapshot, &catalog, &[&state]);
    assert_eq!(warm.batches.len(), 1024);
    assert_eq!(warm.relations.len(), 131);
    assert_eq!(warm.work.setup_builds, 0);
    assert_eq!(warm.work.payload_clones, 0);
    let plan = ResidentInstallPlan::new(Some(&state), &warm).unwrap();
    assert!(!plan.full && plan.missing_batches.is_empty() && plan.changed_relations.is_empty());
    let (rejected, work) = prepare_with_limit(
        &tables,
        &snapshot,
        &catalog,
        &[&state],
        warm.retained_bytes - 1,
    );
    assert!(
        rejected.is_err(),
        "interned descriptors must still consume the full request budget"
    );
    assert_eq!(work.payload_clones, 0);
    drop(warm);
    drop(initial);
    let duplicate = snapshot.tables[0].batches[0].clone();
    snapshot.tables[0].batches.push(duplicate);
    let (rejected, work) =
        prepare_with_limit(&tables, &snapshot, &catalog, &[], MAX_QUERY_INPUT_BYTES);
    assert!(
        rejected
            .err()
            .unwrap()
            .to_string()
            .contains("duplicate resident batch identity")
    );
    assert_eq!(work.payload_clones, 0);
}

#[test]
fn rollup_delta_encodes_and_charges_only_exactly_changed_positions() {
    let (mut tables, snapshot, catalog) = inputs();
    let source = tables[0].rollups[0].clone();
    tables[0].rollups = (0..1024)
        .map(|index| {
            let mut row = source.clone();
            row.bucket_us = index * 60;
            row
        })
        .collect();
    let initial = prepare(&tables, &snapshot, &catalog, &[]);
    let state = accepted(&initial);
    let initial_bytes = state.materialized_bytes;
    // Bitwise zero changes must not disappear in typed comparisons.
    tables[0].rollups[513].sum = 0.0;
    let changed = prepare(&tables, &snapshot, &catalog, &[&state]);
    let plan = ResidentInstallPlan::new(Some(&state), &changed).unwrap();
    assert_eq!(plan.changed_relations.len(), 1);
    let relation = plan.changed_relations[0];
    let previous = plan.previous_relations[0].as_deref();
    let mut input = Vec::new();
    append_relation_delta(&mut input, relation, previous, true).unwrap();
    let lines: Vec<serde_json::Value> = serde_json::Deserializer::from_slice(&input)
        .into_iter()
        .map(Result::unwrap)
        .collect();
    assert_eq!(lines.len(), 2);
    assert_eq!(lines[0]["kind"], "rollup_delete");
    assert_eq!(lines[1]["kind"], "rollup");
    assert_eq!(lines[0]["batch_id"], "513");
    assert_eq!(lines[1]["batch_id"], "513");
    let PreparedRelationRows::Rollup(rows) = &relation.rows else {
        unreachable!()
    };
    let delta = rows[513].materialized_bytes().unwrap();
    assert_eq!(
        relation_delta_materialized_bytes(relation, previous).unwrap(),
        delta
    );
    assert!(plan.materialized_bytes >= initial_bytes + delta);
    assert!(plan.materialized_bytes < initial_bytes + 4096);
    assert!(input.len() < 1024);

    let next_state = accepted(&changed);
    tables[0].rollups.pop();
    let removed = prepare(&tables, &snapshot, &catalog, &[&next_state]);
    let removed_plan = ResidentInstallPlan::new(Some(&next_state), &removed).unwrap();
    let mut input = Vec::new();
    append_relation_delta(
        &mut input,
        removed_plan.changed_relations[0],
        removed_plan.previous_relations[0].as_deref(),
        true,
    )
    .unwrap();
    let lines: Vec<serde_json::Value> = serde_json::Deserializer::from_slice(&input)
        .into_iter()
        .map(Result::unwrap)
        .collect();
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["kind"], "rollup_delete");
    assert_eq!(lines[0]["batch_id"], "1023");
    assert_eq!(
        relation_delta_materialized_bytes(
            removed_plan.changed_relations[0],
            removed_plan.previous_relations[0].as_deref()
        )
        .unwrap(),
        0
    );
    assert!(
        removed_plan.materialized_bytes >= next_state.materialized_bytes,
        "deletion cannot refund old allocations"
    );
}

#[test]
fn descriptor_bounds_and_cancellation_fail_before_install() {
    assert!(charge(MAX_QUERY_INPUT_BYTES, 1).is_err());
    assert!(charge(1, usize::MAX).is_err());
    let (tables, snapshot, catalog) = inputs();
    let error = ResidentRequest::new(
        &tables,
        &snapshot,
        &QueryOptions::default(),
        &catalog,
        &[],
        false,
        deadline(),
        &AtomicBool::new(true),
    )
    .err()
    .unwrap();
    assert!(error.to_string().contains("cancelled"));
    let error = ResidentRequest::new(
        &tables,
        &snapshot,
        &QueryOptions::default(),
        &catalog,
        &[],
        false,
        Instant::now() - Duration::from_millis(1),
        &AtomicBool::new(false),
    )
    .err()
    .unwrap();
    assert!(error.to_string().contains("timed out"));
}
