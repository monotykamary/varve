use super::*;
use crate::engine;
use crate::{RollupRow, Row, StoredRow};
use anyhow::Result;
use std::collections::BTreeMap;

fn limits() -> PageLimits {
    PageLimits {
        page_bytes: 4096,
        max_bytes: 8 * 1024 * 1024,
        max_entries: 10_000,
    }
}

fn row(timestamp_us: i64, tenant: &str, series: &str, tag: &str, value: f64) -> RollupRow {
    RollupRow::from_row(
        10,
        &StoredRow {
            sequence: 2,
            ordinal: 0,
            row: Row {
                timestamp_us,
                tenant: tenant.into(),
                series: series.into(),
                value,
                tags: BTreeMap::from([("tag".into(), tag.into())]),
            },
        },
    )
    .unwrap()
}

fn fixture() -> BTreeMap<String, RollupRow> {
    let mut rows = BTreeMap::new();
    for tenant in ["t", "t\\0", "é", "e\u{301}"] {
        for series in ["cpu", "CPU", "雪"] {
            for tag in ["a", "b", "🦀"] {
                for ts in [-101, -10, -1, 0, 9, 10, 100] {
                    let row = row(ts, tenant, series, tag, -0.0);
                    rows.insert(canonical_key(&row).unwrap(), row);
                }
            }
        }
    }
    rows
}

#[test]
fn index_selection_matches_canonical_scan_and_accounts_undo() -> Result<()> {
    let rows = fixture();
    let mut index = RollupIndex::rebuild(&rows, limits().max_bytes)?;
    assert!(index.resident_bytes() > 0);
    for tenant in [None, Some("é"), Some("e\u{301}"), Some("missing")] {
        for series in [None, Some("cpu"), Some("雪")] {
            for width_us in [None, Some(10), Some(20)] {
                for (start_us, end_us) in [(None, None), (Some(-10), Some(10)), (Some(0), Some(0))]
                {
                    let selected = index.select(
                        &rows,
                        RollupSelection {
                            tenant,
                            series,
                            width_us,
                            start_us,
                            end_us,
                        },
                        limits().max_bytes,
                    )?;
                    let expected: Vec<_> = rows
                        .values()
                        .filter(|row| {
                            tenant.is_none_or(|t| row.tenant == t)
                                && series.is_none_or(|s| row.series == s)
                                && width_us.is_none_or(|w| row.width_us == w)
                                && start_us.is_none_or(|s| row.bucket_us >= s)
                                && end_us.is_none_or(|e| row.bucket_us < e)
                        })
                        .collect();
                    assert_eq!(selected, expected);
                }
            }
        }
    }
    assert!(index.select(&rows, RollupSelection::default(), 1).is_err());
    assert!(RollupIndex::rebuild(&rows, 1).is_err());
    let (key, value) = rows.first_key_value().unwrap();
    let before = index.resident_bytes();
    index.insert(key, value, before)?;
    assert_eq!(index.resident_bytes(), before);
    index.remove(key, value);
    assert_eq!(
        index.resident_bytes(),
        before - RollupIndex::entry_bytes(key, value)
    );
    index.insert(key, value, before)?;
    assert_eq!(
        index
            .select(&rows, RollupSelection::default(), limits().max_bytes)?
            .len(),
        rows.len()
    );
    for (key, value) in &rows {
        index.remove(key, value);
    }
    assert_eq!(index.resident_bytes(), 0);
    assert!(
        index
            .select(&rows, RollupSelection::default(), 0)?
            .is_empty()
    );
    Ok(())
}

#[test]
fn independent_bounded_pages_are_reusable_and_bitwise_exact() -> Result<()> {
    let mut rows = fixture();
    let first = rows.values_mut().next().unwrap();
    first.sum = f64::from_bits(0x3fd5555555555555);
    let mut objects = BTreeMap::new();
    let pages = encode_rollups("metrics", &rows, limits(), |reference, bytes| {
        assert!(bytes.len() <= limits().page_bytes);
        reference.verify(bytes)?;
        objects.insert(reference.key(), bytes.to_vec());
        Ok(())
    })?;
    assert!(pages.pages.len() > 1);
    let again = encode_rollups("metrics", &rows, limits(), |reference, bytes| {
        assert_eq!(objects.get(&reference.key()).unwrap(), bytes);
        Ok(())
    })?;
    assert_eq!(pages, again);
    let restored = hydrate_rollups("metrics", &pages, limits(), |p| {
        Ok(objects[&p.key()].clone())
    })?;
    for (key, before) in &rows {
        let after = &restored[key];
        assert_eq!(before, after);
        for (a, b) in [
            before.sum,
            before.min,
            before.max,
            before.first,
            before.last,
        ]
        .into_iter()
        .zip([after.sum, after.min, after.max, after.first, after.last])
        {
            assert_eq!(a.to_bits(), b.to_bits());
        }
    }
    let receipts = BTreeMap::from([(
        "request".into(),
        engine::ReceiptEntry {
            sequence: 2,
            rows: 2,
            digest: "a".repeat(64),
            issued_us: None,
            group_fingerprint: Some("b".repeat(64)),
        },
    )]);
    let receipt_pages = encode_receipts("metrics", &receipts, limits(), |p, b| {
        objects.insert(p.key(), b.to_vec());
        Ok(())
    })?;
    let refs = DerivedRefs {
        rollups: pages.clone(),
        receipts: receipt_pages.clone(),
    };
    assert_ne!(refs.rollups, refs.receipts);
    let hydrated = hydrate_receipts("metrics", &receipt_pages, limits(), |p| {
        Ok(objects[&p.key()].clone())
    })?;
    assert_eq!(
        serde_json::to_vec(&receipts)?,
        serde_json::to_vec(&hydrated)?
    );
    assert!(
        hydrate_rollups("metrics", &receipt_pages, limits(), |p| Ok(objects
            [&p.key()]
            .clone()))
        .is_err()
    );
    assert!(hydrate_rollups("other", &pages, limits(), |p| Ok(objects[&p.key()].clone())).is_err());
    Ok(())
}

#[test]
fn corruption_and_allocation_bounds_fail_closed_before_loader() -> Result<()> {
    let rows = fixture();
    let mut objects = BTreeMap::new();
    let pages = encode_rollups("metrics", &rows, limits(), |p, b| {
        objects.insert(p.key(), b.to_vec());
        Ok(())
    })?;
    let mut duplicate = pages.clone();
    duplicate.pages.push(duplicate.pages[0].clone());
    assert!(
        hydrate_rollups("metrics", &duplicate, limits(), |_| panic!(
            "invalid set loaded"
        ))
        .is_err()
    );
    let mut oversized = pages.clone();
    oversized.pages[0].bytes = (MAX_PAGE_BYTES + 1) as u64;
    assert!(
        hydrate_rollups("metrics", &oversized, limits(), |_| panic!(
            "oversized page loaded"
        ))
        .is_err()
    );
    let tiny = PageLimits {
        max_bytes: 4096,
        ..limits()
    };
    assert!(
        hydrate_rollups("metrics", &pages, tiny, |_| panic!(
            "overbudget page loaded"
        ))
        .is_err()
    );
    assert!(
        hydrate_rollups("metrics", &pages, limits(), |_| anyhow::bail!(
            "missing page"
        ))
        .is_err()
    );
    for truncate in [false, true] {
        assert!(
            hydrate_rollups("metrics", &pages, limits(), |p| {
                let mut bytes = objects[&p.key()].clone();
                if truncate {
                    bytes.pop();
                } else {
                    bytes[10] ^= 1;
                }
                Ok(bytes)
            })
            .is_err()
        );
    }
    let mut huge = rows.values().next().unwrap().clone();
    huge.tenant = "x".repeat(8192);
    let huge = BTreeMap::from([(canonical_key(&huge)?, huge)]);
    assert!(
        encode_rollups("metrics", &huge, limits(), |_, _| panic!(
            "oversized row emitted"
        ))
        .is_err()
    );
    let mut invalid = rows;
    invalid.insert(
        "not canonical".into(),
        invalid.values().next().unwrap().clone(),
    );
    assert!(encode_rollups("metrics", &invalid, limits(), |_, _| Ok(())).is_err());
    Ok(())
}

#[test]
fn projected_overlay_matches_exact_page_encoding() -> Result<()> {
    let base = fixture();
    let mut updates = BTreeMap::new();
    let (key, existing) = base.first_key_value().unwrap();
    let mut changed = existing.clone();
    changed.count += 1;
    updates.insert(key.clone(), changed);
    let new = row(-999, "new", "new", "tags", 0.5);
    updates.insert(canonical_key(&new)?, new);
    let projected = project_rollups("metrics", &base, &updates, limits())?;
    let mut applied = base.clone();
    applied.extend(updates);
    let actual = encode_rollups("metrics", &applied, limits(), |_, _| Ok(()))?;
    assert_eq!(projected, actual);
    let receipt = engine::ReceiptEntry {
        sequence: 3,
        rows: 2,
        digest: "c".repeat(64),
        issued_us: None,
        group_fingerprint: None,
    };
    let receipts = BTreeMap::from([("old".into(), receipt.clone())]);
    let projected = project_receipts("metrics", &receipts, "new", &receipt, limits())?;
    let mut applied = receipts.clone();
    applied.insert("new".into(), receipt.clone());
    assert_eq!(
        projected,
        encode_receipts("metrics", &applied, limits(), |_, _| Ok(()))?
    );
    assert_eq!(
        project_receipts("metrics", &receipts, "old", &receipt, limits())?,
        encode_receipts("metrics", &receipts, limits(), |_, _| Ok(()))?
    );
    for reference in &actual.pages {
        // References never copy arbitrarily large canonical/tag keys.
        assert!(serde_json::to_vec(reference)?.len() <= 128);
    }
    Ok(())
}

#[test]
fn resealed_invalid_dimensions_are_rejected() -> Result<()> {
    for case in 0..8 {
        let mut row = row(-1, "tenant", "series", "tags", 1.0);
        match case {
            0 => row.tenant.clear(),
            1 => row.tenant = "bad\0tenant".into(),
            2 => row.tenant = "x".repeat(257),
            3 => row.series = "x".repeat(1025),
            4 => row.tags = (0..33).map(|i| (format!("k{i}"), "v".into())).collect(),
            5 => row.tags = BTreeMap::from([(String::new(), "v".into())]),
            6 => row.tags = BTreeMap::from([("key".into(), "x".repeat(1025))]),
            _ => row.tags = BTreeMap::from([("key".into(), "bad\0value".into())]),
        }
        let key = canonical_key(&row)?;
        let body = serde_json::json!({"table":"metrics","kind":"rollup","entries":[[key,row]]});
        let mut bytes = b"VARVED01".to_vec();
        bytes.extend(serde_json::to_vec(&body)?);
        bytes.extend_from_slice(blake3::hash(&bytes).as_bytes());
        let reference = PageRef {
            digest: blake3::hash(&bytes).to_hex().to_string(),
            bytes: bytes.len() as u64,
            entries: 1,
        };
        let set = PageSet {
            encoded_bytes: reference.bytes,
            entries: 1,
            pages: vec![reference],
        };
        assert!(
            hydrate_rollups("metrics", &set, limits(), |_| Ok(bytes.clone())).is_err(),
            "invalid dimensions accepted: {case}"
        );
    }
    Ok(())
}

#[test]
fn resealed_duplicate_entries_are_rejected() -> Result<()> {
    let row = row(-1, "tenant", "series", "tags", 1.0);
    let key = canonical_key(&row)?;
    let body =
        serde_json::json!({"table":"metrics","kind":"rollup","entries":[[key,row],[key,row]]});
    let mut bytes = b"VARVED01".to_vec();
    bytes.extend(serde_json::to_vec(&body)?);
    bytes.extend_from_slice(blake3::hash(&bytes).as_bytes());
    let reference = PageRef {
        digest: blake3::hash(&bytes).to_hex().to_string(),
        bytes: bytes.len() as u64,
        entries: 2,
    };
    let set = PageSet {
        encoded_bytes: reference.bytes,
        entries: 2,
        pages: vec![reference],
    };
    assert!(hydrate_rollups("metrics", &set, limits(), |_| Ok(bytes.clone())).is_err());
    Ok(())
}
