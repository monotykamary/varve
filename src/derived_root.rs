//! Manifest-v2 wire adapter. Domain manifests retain the WAL model's version.
use crate::derived::{self, DerivedRefs, PageLimits, PageRef};
use crate::engine::{self, ControlStamp, Manifest, Segment, Table};
use crate::model::{Config, ContinuousAggregate, JobDefinition, TableConfig};
use crate::wal;
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::{self, Write};

const MAGIC: &[u8; 8] = b"VARVEM02";

#[derive(Clone, Debug)]
pub(crate) struct CheckpointRoot {
    pub catalog: Manifest,
    // Some(empty) is still v2: never infer authority from whether pages exist.
    pub derived: Option<BTreeMap<String, DerivedRefs>>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireTable {
    config: TableConfig,
    creation_config: Option<TableConfig>,
    created_sequence: u64,
    segments: Vec<Segment>,
    cutoff_us: Option<i64>,
    rollup_cutoff_us: Option<i64>,
    idempotency_floor_us: Option<i64>,
    derived: DerivedRefs,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRoot {
    format_version: u32,
    database_id: String,
    checkpoint_sequence: u64,
    #[serde(default, skip_serializing_if = "engine::journal_disabled")]
    segmented_journal: bool,
    tables: BTreeMap<String, WireTable>,
    continuous_aggregates: BTreeMap<String, ContinuousAggregate>,
    jobs: BTreeMap<String, JobDefinition>,
    control_history: Vec<ControlStamp>,
}

pub(crate) fn limits(config: &Config, receipts: bool, max_bytes: usize) -> PageLimits {
    PageLimits {
        page_bytes: config.derived_page_bytes,
        max_bytes,
        max_entries: if receipts {
            config.max_idempotency_keys
        } else {
            config.max_rollup_groups
        },
    }
}

impl CheckpointRoot {
    pub(crate) fn page_refs(&self) -> impl Iterator<Item = &PageRef> {
        self.derived
            .iter()
            .flat_map(|tables| tables.values())
            .flat_map(|sets| sets.rollups.pages.iter().chain(&sets.receipts.pages))
    }

    pub(crate) fn derived_encoded_bytes(&self) -> Result<usize> {
        self.page_refs().try_fold(0usize, |sum, page| {
            sum.checked_add(usize::try_from(page.bytes)?)
                .context("derived encoded bytes overflow")
        })
    }

    pub(crate) fn hydrate(
        &mut self,
        config: &Config,
        load: impl FnMut(&PageRef) -> Result<Vec<u8>>,
    ) -> Result<()> {
        self.hydrate_with_budget(config, config.derived_max_bytes, load)
    }

    pub(crate) fn hydrate_with_budget(
        &mut self,
        config: &Config,
        budget: usize,
        mut load: impl FnMut(&PageRef) -> Result<Vec<u8>>,
    ) -> Result<()> {
        let budget = budget.min(config.derived_max_bytes);
        let Some(sets) = &self.derived else {
            return Ok(());
        };
        ensure!(
            sets.len() == self.catalog.tables.len(),
            "derived root table coverage mismatch"
        );
        ensure!(
            self.derived_encoded_bytes()? <= config.derived_max_bytes,
            "derived encoded budget exceeded"
        );
        let mut resident = 0usize;
        for (name, table) in &mut self.catalog.tables {
            let refs = sets.get(name).context("missing derived table sets")?;
            ensure!(
                table.rollups.is_empty() && table.receipts.is_empty(),
                "derived root hydration requires empty maps"
            );
            table.rollups = derived::hydrate_rollups(
                name,
                &refs.rollups,
                limits(config, false, budget.saturating_sub(resident)),
                &mut load,
            )?;
            for (key, row) in &table.rollups {
                ensure!(
                    table.config.rollup_widths_us.contains(&row.width_us)
                        && row.first_sequence <= self.catalog.checkpoint_sequence
                        && row.last_sequence <= self.catalog.checkpoint_sequence
                        && table
                            .rollup_cutoff_us
                            .is_none_or(|floor| row.bucket_us.saturating_add(row.width_us) > floor),
                    "derived rollup inconsistent with root"
                );
                resident = resident.saturating_add(derived::rollup_resident_bytes(key, row));
            }
            table.receipts = derived::hydrate_receipts(
                name,
                &refs.receipts,
                limits(config, true, budget.saturating_sub(resident)),
                &mut load,
            )?;
            for (key, receipt) in &table.receipts {
                resident = resident.saturating_add(derived::receipt_resident_bytes(key, receipt));
            }
            ensure!(resident <= budget, "derived root resident budget exceeded");
        }
        engine::validate_manifest(&self.catalog)?;
        Ok(())
    }

    pub(crate) fn encode(&self, config: &Config) -> Result<Vec<u8>> {
        let Some(refs) = &self.derived else {
            return engine::encode_manifest(&self.catalog);
        };
        encode_control(
            &self.catalog,
            refs,
            self.catalog.checkpoint_sequence,
            config,
        )
    }
}

/// Encode only control metadata and exact page references for pre-WAL headroom.
/// The borrowed runtime maps are neither copied nor serialized here.
pub(crate) fn encode_control(
    catalog: &Manifest,
    refs: &BTreeMap<String, DerivedRefs>,
    checkpoint_sequence: u64,
    config: &Config,
) -> Result<Vec<u8>> {
    ensure!(
        refs.len() == catalog.tables.len(),
        "derived root table coverage mismatch"
    );
    let mut tables = BTreeMap::new();
    for (name, table) in &catalog.tables {
        let derived = refs
            .get(name)
            .context("missing derived table sets")?
            .clone();
        derived
            .rollups
            .validate(limits(config, false, config.derived_max_bytes))?;
        derived
            .receipts
            .validate(limits(config, true, config.derived_max_bytes))?;
        tables.insert(
            name.clone(),
            WireTable {
                config: table.config.clone(),
                creation_config: table.creation_config.clone(),
                created_sequence: table.created_sequence,
                segments: table.segments.clone(),
                cutoff_us: table.cutoff_us,
                rollup_cutoff_us: table.rollup_cutoff_us,
                idempotency_floor_us: table.idempotency_floor_us,
                derived,
            },
        );
    }
    let wire = WireRoot {
        format_version: 2,
        database_id: catalog.database_id.clone(),
        checkpoint_sequence,
        segmented_journal: catalog.segmented_journal,
        tables,
        continuous_aggregates: catalog.continuous_aggregates.clone(),
        jobs: catalog.jobs.clone(),
        control_history: catalog.control_history.clone(),
    };
    let max = config.metadata_max_bytes.min(wal::MAX_FRAME_BYTES);
    let mut writer = RootBytes {
        bytes: MAGIC.to_vec(),
        max: max.saturating_sub(32),
    };
    serde_json::to_writer(&mut writer, &wire)?;
    let hash = blake3::hash(&writer.bytes);
    writer.bytes.extend_from_slice(hash.as_bytes());
    Ok(writer.bytes)
}

struct RootBytes {
    bytes: Vec<u8>,
    max: usize,
}
impl Write for RootBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.max.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other(
                "derived control root byte budget exceeded",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn decode(bytes: &[u8], config: &Config) -> Result<CheckpointRoot> {
    ensure!(
        bytes.len() >= 40 && bytes.len() <= config.metadata_max_bytes.min(wal::MAX_FRAME_BYTES),
        "checkpoint root size limit"
    );
    if &bytes[..8] == b"VARVEM01" {
        return Ok(CheckpointRoot {
            catalog: engine::decode_manifest(bytes)?,
            derived: None,
        });
    }
    ensure!(&bytes[..8] == MAGIC, "unsupported checkpoint root format");
    let (payload, digest) = bytes.split_at(bytes.len() - 32);
    ensure!(
        blake3::hash(payload).as_bytes() == digest,
        "checkpoint root checksum mismatch"
    );
    let wire: WireRoot = serde_json::from_slice(&bytes[8..bytes.len() - 32])?;
    ensure!(
        wire.format_version == 2,
        "checkpoint root header/version mismatch"
    );
    ensure!(
        wire.tables.len() <= config.max_tables,
        "checkpoint root table budget exceeded"
    );
    let mut tables = BTreeMap::new();
    let mut refs = BTreeMap::new();
    for (name, table) in wire.tables {
        table
            .derived
            .rollups
            .validate(limits(config, false, config.derived_max_bytes))?;
        table
            .derived
            .receipts
            .validate(limits(config, true, config.derived_max_bytes))?;
        refs.insert(name.clone(), table.derived);
        tables.insert(
            name,
            Table {
                config: table.config,
                creation_config: table.creation_config,
                created_sequence: table.created_sequence,
                segments: table.segments,
                receipts: BTreeMap::new(),
                rollups: BTreeMap::new(),
                cutoff_us: table.cutoff_us,
                rollup_cutoff_us: table.rollup_cutoff_us,
                idempotency_floor_us: table.idempotency_floor_us,
            },
        );
    }
    let root = CheckpointRoot {
        catalog: Manifest {
            format_version: crate::model::FORMAT_VERSION,
            database_id: wire.database_id,
            checkpoint_sequence: wire.checkpoint_sequence,
            segmented_journal: wire.segmented_journal,
            tables,
            continuous_aggregates: wire.continuous_aggregates,
            jobs: wire.jobs,
            control_history: wire.control_history,
        },
        derived: Some(refs),
    };
    ensure!(
        root.derived_encoded_bytes()? <= config.derived_max_bytes,
        "derived encoded budget exceeded"
    );
    engine::validate_manifest(&root.catalog)?;
    Ok(root)
}

pub(crate) fn resident_bytes(catalog: &Manifest, include_index: bool) -> usize {
    catalog.tables.values().fold(0usize, |total, table| {
        let rollups = table.rollups.iter().fold(0usize, |total, (key, row)| {
            total
                .saturating_add(derived::rollup_resident_bytes(key, row))
                .saturating_add(if include_index {
                    derived::RollupIndex::entry_bytes(key, row)
                } else {
                    0
                })
        });
        let receipts = table.receipts.iter().fold(0usize, |total, (key, receipt)| {
            total.saturating_add(derived::receipt_resident_bytes(key, receipt))
        });
        total.saturating_add(rollups).saturating_add(receipts)
    })
}

pub(crate) type AppendProjection<'a> = (
    &'a str,
    &'a str,
    &'a engine::ReceiptEntry,
    &'a BTreeMap<String, crate::model::RollupRow>,
);

/// Exact v2 headroom. Re-encoding dirty state is deliberate, not a page-local cost claim.
pub(crate) fn project(
    catalog: &Manifest,
    config: &Config,
    sequence: u64,
    append: Option<AppendProjection<'_>>,
) -> Result<(BTreeMap<String, DerivedRefs>, usize)> {
    let mut refs = BTreeMap::new();
    let mut encoded = 0usize;
    let mut largest = 0usize;
    for (name, table) in &catalog.tables {
        let (rollups, receipts) =
            if let Some((target, id, receipt, updates)) = append.filter(|a| a.0 == name) {
                (
                    derived::project_rollups(
                        target,
                        &table.rollups,
                        updates,
                        limits(config, false, config.derived_max_bytes),
                    )?,
                    derived::project_receipts(
                        target,
                        &table.receipts,
                        id,
                        receipt,
                        limits(config, true, config.derived_max_bytes),
                    )?,
                )
            } else {
                (
                    derived::encode_rollups(
                        name,
                        &table.rollups,
                        limits(config, false, config.derived_max_bytes),
                        |_, _| Ok(()),
                    )?,
                    derived::encode_receipts(
                        name,
                        &table.receipts,
                        limits(config, true, config.derived_max_bytes),
                        |_, _| Ok(()),
                    )?,
                )
            };
        encoded = encoded
            .saturating_add(usize::try_from(rollups.encoded_bytes)?)
            .saturating_add(usize::try_from(receipts.encoded_bytes)?);
        largest = largest.max(
            rollups
                .pages
                .iter()
                .chain(&receipts.pages)
                .map(|p| p.bytes as usize)
                .max()
                .unwrap_or(0),
        );
        ensure!(
            encoded <= config.derived_max_bytes,
            "derived encoded byte budget exceeded"
        );
        refs.insert(name.clone(), DerivedRefs { rollups, receipts });
    }
    let bytes = encode_control(catalog, &refs, sequence, config)?.len();
    // Admit only sets that the bounded reader can hydrate on reopen. Existing
    // resident bytes and indexes are charged separately by the engine caller.
    ensure!(
        largest.saturating_mul(64) <= config.derived_max_bytes,
        "derived recovery working budget exceeded"
    );
    Ok((refs, bytes))
}

pub(crate) fn prepare(
    catalog: Manifest,
    config: &Config,
    mut emit: impl FnMut(&PageRef, &[u8]) -> Result<()>,
) -> Result<CheckpointRoot> {
    engine::validate_manifest(&catalog)?;
    ensure!(
        resident_bytes(&catalog, false).saturating_add(config.derived_page_bytes.saturating_mul(4))
            <= config.derived_max_bytes,
        "derived preparation resident and working budget exceeded"
    );
    // Prepared publication does not run a separate projection pass. Preserve
    // the same recovery workspace check as admission before emitting each page.
    let mut checked_emit = |page: &PageRef, bytes: &[u8]| {
        ensure!(
            (page.bytes as usize).saturating_mul(64) <= config.derived_max_bytes,
            "derived recovery working budget exceeded"
        );
        emit(page, bytes)
    };
    let mut refs = BTreeMap::new();
    let mut encoded = 0usize;
    for (name, table) in &catalog.tables {
        let rollups = derived::encode_rollups(
            name,
            &table.rollups,
            limits(config, false, config.derived_max_bytes),
            &mut checked_emit,
        )?;
        let receipts = derived::encode_receipts(
            name,
            &table.receipts,
            limits(config, true, config.derived_max_bytes),
            &mut checked_emit,
        )?;
        encoded = encoded
            .saturating_add(usize::try_from(rollups.encoded_bytes)?)
            .saturating_add(usize::try_from(receipts.encoded_bytes)?);
        ensure!(
            encoded <= config.derived_max_bytes,
            "derived encoded budget exceeded"
        );
        refs.insert(name.clone(), DerivedRefs { rollups, receipts });
    }
    Ok(CheckpointRoot {
        catalog,
        derived: Some(refs),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Database, Row};

    #[test]
    fn v2_root_roundtrip_omits_maps_and_old_header_refuses() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let config = Config {
            derived_page_bytes: 4096,
            ..Config::default()
        };
        let db = Database::open(dir.path(), config.clone())?;
        db.create_table(
            "metrics",
            TableConfig {
                rollup_widths_us: vec![10],
                ..TableConfig::default()
            },
        )?;
        db.write(
            "metrics",
            "receipt",
            vec![Row {
                timestamp_us: -1,
                tenant: "é".into(),
                series: "cpu".into(),
                value: -0.0,
                tags: BTreeMap::new(),
            }],
            0,
        )?;
        db.checkpoint()?;
        let v1 = std::fs::read(dir.path().join("manifest.bin"))?;
        let original = decode(&v1, &config)?;
        assert!(original.derived.is_none());
        let expected = serde_json::to_vec(&original.catalog)?;
        let mut objects = BTreeMap::new();
        let root = prepare(original.catalog, &config, |page, bytes| {
            objects.insert(page.key(), bytes.to_vec());
            Ok(())
        })?;
        let bytes = root.encode(&config)?;
        assert_eq!(&bytes[..8], b"VARVEM02");
        assert!(engine::decode_manifest(&bytes).is_err());
        let json: serde_json::Value = serde_json::from_slice(&bytes[8..bytes.len() - 32])?;
        assert!(json["tables"]["metrics"].get("rollups").is_none());
        assert!(json["tables"]["metrics"].get("receipts").is_none());
        assert_eq!(json["format_version"], 2);
        let mut restored = decode(
            &bytes,
            &Config {
                derived_pages: false,
                ..config.clone()
            },
        )?;
        assert!(restored.catalog.tables["metrics"].rollups.is_empty());
        restored.hydrate(&config, |page| Ok(objects[&page.key()].clone()))?;
        assert_eq!(serde_json::to_vec(&restored.catalog)?, expected);
        assert_eq!(root.encode(&config)?, restored.encode(&config)?);
        let mut missing = decode(&bytes, &config)?;
        assert!(
            missing
                .hydrate(&config, |_| anyhow::bail!("missing page"))
                .is_err()
        );
        let mut limited = decode(&bytes, &config)?;
        assert!(
            limited
                .hydrate_with_budget(&config, 0, |_| panic!("overbudget loader invoked"))
                .is_err()
        );
        let mut relabeled = bytes;
        relabeled[..8].copy_from_slice(b"VARVEM01");
        let end = relabeled.len() - 32;
        let hash = blake3::hash(&relabeled[..end]);
        relabeled[end..].copy_from_slice(hash.as_bytes());
        assert!(decode(&relabeled, &config).is_err());
        Ok(())
    }

    #[test]
    fn empty_v2_root_retains_format_authority() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let config = Config::default();
        let db = Database::open(dir.path(), config.clone())?;
        let bytes = std::fs::read(dir.path().join("manifest.bin"))?;
        let root = prepare(decode(&bytes, &config)?.catalog, &config, |_, _| {
            panic!("empty page emitted")
        })?;
        let bytes = root.encode(&config)?;
        let decoded = decode(&bytes, &config)?;
        assert!(decoded.derived.as_ref().unwrap().is_empty());
        assert_eq!(decoded.encode(&config)?, bytes);
        drop(db);
        Ok(())
    }
}
