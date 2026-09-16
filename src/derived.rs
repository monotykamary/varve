//! Exact resident selection and immutable checkpoint-page codecs.
//!
//! These components do not own a WAL frontier or publish a checkpoint root.
use crate::engine::ReceiptEntry;
use crate::model::{RollupRow, validate_name, validate_request_id, window_start};
use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, Write};

pub(crate) const MAX_PAGE_BYTES: usize = 1024 * 1024;
const MAGIC: &[u8; 8] = b"VARVED01";
const FRAME_BYTES: usize = 40;

#[cfg(test)]
mod tests;

#[cfg(test)]
thread_local! { static CODEC_CALLS: std::cell::Cell<usize> = const {std::cell::Cell::new(0)}; }
#[cfg(test)]
pub(crate) fn codec_calls() -> usize {
    CODEC_CALLS.with(std::cell::Cell::get)
}

pub(crate) fn canonical_key(row: &RollupRow) -> Result<String> {
    Ok(serde_json::to_string(&(
        row.width_us,
        row.bucket_us,
        &row.tenant,
        &row.series,
        &row.tags,
    ))?)
}

type Buckets = BTreeMap<i64, BTreeSet<String>>;
type Series = BTreeMap<String, Buckets>;
type Tenants = BTreeMap<String, Series>;

#[derive(Clone, Debug, Default)]
pub(crate) struct RollupIndex {
    widths: BTreeMap<i64, Tenants>,
    resident_bytes: usize,
}

#[derive(Clone, Copy, Debug, Default)]
/// Exact binary tenant/series filters and a half-open rollup bucket range.
pub struct RollupSelection<'a> {
    pub tenant: Option<&'a str>,
    pub series: Option<&'a str>,
    pub width_us: Option<i64>,
    pub start_us: Option<i64>,
    pub end_us: Option<i64>,
}

impl RollupIndex {
    pub(crate) fn rebuild(rows: &BTreeMap<String, RollupRow>, budget: usize) -> Result<Self> {
        let mut index = Self::default();
        for (key, row) in rows {
            index.insert(key, row, budget)?;
        }
        Ok(index)
    }

    pub(crate) fn resident_bytes(&self) -> usize {
        self.resident_bytes
    }

    pub(crate) fn entry_bytes(key: &str, row: &RollupRow) -> usize {
        // Charge every tree level as unshared, including spare B-tree slots.
        1024usize
            .saturating_add(key.len())
            .saturating_add(row.tenant.len())
            .saturating_add(row.series.len())
    }

    pub(crate) fn insert(&mut self, key: &str, row: &RollupRow, budget: usize) -> Result<()> {
        ensure!(key == canonical_key(row)?, "noncanonical rollup index key");
        let existing = self
            .widths
            .get(&row.width_us)
            .and_then(|t| t.get(&row.tenant))
            .and_then(|s| s.get(&row.series))
            .and_then(|b| b.get(&row.bucket_us))
            .is_some_and(|keys| keys.contains(key));
        if existing {
            return Ok(());
        }
        let bytes = self
            .resident_bytes
            .checked_add(Self::entry_bytes(key, row))
            .context("rollup index accounting overflow")?;
        ensure!(bytes <= budget, "rollup index resident budget exceeded");
        self.widths
            .entry(row.width_us)
            .or_default()
            .entry(row.tenant.clone())
            .or_default()
            .entry(row.series.clone())
            .or_default()
            .entry(row.bucket_us)
            .or_default()
            .insert(key.to_owned());
        self.resident_bytes = bytes;
        Ok(())
    }

    pub(crate) fn remove(&mut self, key: &str, row: &RollupRow) {
        let Some(tenants) = self.widths.get_mut(&row.width_us) else {
            return;
        };
        let Some(series) = tenants.get_mut(&row.tenant) else {
            return;
        };
        let Some(buckets) = series.get_mut(&row.series) else {
            return;
        };
        let Some(keys) = buckets.get_mut(&row.bucket_us) else {
            return;
        };
        if keys.remove(key) {
            self.resident_bytes -= Self::entry_bytes(key, row);
        }
        if keys.is_empty() {
            buckets.remove(&row.bucket_us);
        }
        if buckets.is_empty() {
            series.remove(&row.series);
        }
        if series.is_empty() {
            tenants.remove(&row.tenant);
        }
        if tenants.is_empty() {
            self.widths.remove(&row.width_us);
        }
    }

    pub(crate) fn select<'a>(
        &self,
        rows: &'a BTreeMap<String, RollupRow>,
        selection: RollupSelection<'_>,
        working_budget: usize,
    ) -> Result<Vec<&'a RollupRow>> {
        if let (Some(start), Some(end)) = (selection.start_us, selection.end_us) {
            ensure!(start <= end, "invalid rollup half-open bucket range");
        }
        let mut keys = Vec::new();
        let mut visit = |buckets: &Buckets| -> Result<()> {
            let start = selection.start_us.unwrap_or(i64::MIN);
            for (_, bucket) in buckets
                .range(start..)
                .take_while(|(bucket, _)| selection.end_us.is_none_or(|end| **bucket < end))
            {
                // Bucket filtering below uses the B-tree key, not raw event time.
                for key in bucket {
                    let (key, row) = rows
                        .get_key_value(key)
                        .context("rollup index references missing row")?;
                    if selection.end_us.is_some_and(|end| row.bucket_us >= end) {
                        continue;
                    }
                    let needed = keys
                        .len()
                        .checked_add(1)
                        .and_then(|n| n.checked_mul(4 * std::mem::size_of::<&str>()))
                        .context("rollup selection accounting overflow")?;
                    ensure!(
                        needed <= working_budget,
                        "rollup selection working budget exceeded"
                    );
                    keys.push(key.as_str());
                }
            }
            Ok(())
        };
        for (_, tenants) in self
            .widths
            .range(selection.width_us.unwrap_or(i64::MIN)..=selection.width_us.unwrap_or(i64::MAX))
        {
            if let Some(tenant) = selection.tenant {
                if let Some(series) = tenants.get(tenant) {
                    visit_series(series, selection.series, &mut visit)?;
                }
            } else {
                for series in tenants.values() {
                    visit_series(series, selection.series, &mut visit)?;
                }
            }
        }
        keys.sort_unstable();
        keys.into_iter()
            .map(|key| rows.get(key).context("rollup index references missing row"))
            .collect()
    }
}

fn visit_series(
    series: &Series,
    selected: Option<&str>,
    visit: &mut impl FnMut(&Buckets) -> Result<()>,
) -> Result<()> {
    if let Some(selected) = selected {
        if let Some(buckets) = series.get(selected) {
            visit(buckets)?;
        }
    } else {
        for buckets in series.values() {
            visit(buckets)?;
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PageKind {
    Rollup,
    Receipt,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PageRef {
    pub digest: String,
    pub bytes: u64,
    pub entries: u64,
}
impl PageRef {
    pub(crate) fn key(&self) -> String {
        format!("derived/{}.page", self.digest)
    }

    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(valid_digest(&self.digest), "invalid derived page digest");
        ensure!(
            self.bytes >= FRAME_BYTES as u64 && self.bytes <= MAX_PAGE_BYTES as u64,
            "invalid derived page size"
        );
        ensure!(
            self.entries > 0 && self.entries <= self.bytes,
            "invalid derived page bounds"
        );
        Ok(())
    }

    pub(crate) fn verify(&self, bytes: &[u8]) -> Result<()> {
        self.validate()?;
        ensure!(
            bytes.len() as u64 == self.bytes,
            "derived page size mismatch"
        );
        ensure!(
            blake3::hash(bytes).to_hex().as_str() == self.digest,
            "derived page digest mismatch"
        );
        ensure!(&bytes[..8] == MAGIC, "unsupported derived page format");
        let (payload, checksum) = bytes.split_at(bytes.len() - 32);
        ensure!(
            blake3::hash(payload).as_bytes() == checksum,
            "derived page checksum mismatch"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct PageSet {
    pub pages: Vec<PageRef>,
    pub entries: u64,
    pub encoded_bytes: u64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct DerivedRefs {
    pub rollups: PageSet,
    pub receipts: PageSet,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct PageLimits {
    pub page_bytes: usize,
    pub max_bytes: usize,
    pub max_entries: usize,
}
impl PageLimits {
    fn validate(self) -> Result<()> {
        ensure!(
            (4096..=MAX_PAGE_BYTES).contains(&self.page_bytes),
            "invalid derived page target"
        );
        ensure!(self.max_entries > 0, "invalid derived page limits");
        Ok(())
    }
}

impl PageSet {
    pub(crate) fn validate(&self, limits: PageLimits) -> Result<()> {
        limits.validate()?;
        ensure!(
            self.entries <= limits.max_entries as u64
                && self.encoded_bytes <= limits.max_bytes as u64,
            "derived page set budget exceeded"
        );
        let mut entries = 0u64;
        let mut bytes = 0u64;
        let mut digests = BTreeSet::new();
        for page in &self.pages {
            page.validate()?;
            ensure!(digests.insert(&page.digest), "duplicate derived page");
            entries = entries
                .checked_add(page.entries)
                .context("derived entry count overflow")?;
            bytes = bytes
                .checked_add(page.bytes)
                .context("derived page bytes overflow")?;
        }
        ensure!(
            entries == self.entries && bytes == self.encoded_bytes,
            "inconsistent derived page set totals"
        );
        Ok(())
    }
}

fn valid_digest(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

pub(crate) fn validate_rollup(key: &str, row: &RollupRow) -> Result<()> {
    crate::model::validate_dimensions(&row.tenant, &row.series, &row.tags)?;
    ensure!(
        key == canonical_key(row)?,
        "noncanonical derived rollup key"
    );
    ensure!(
        row.width_us > 0 && window_start(row.bucket_us, row.width_us)? == row.bucket_us,
        "invalid derived rollup bucket"
    );
    ensure!(
        row.count > 0
            && [row.sum, row.min, row.max, row.first, row.last]
                .iter()
                .all(|f| f.is_finite()),
        "invalid derived rollup values"
    );
    ensure!(
        row.min <= row.max
            && row.first >= row.min
            && row.first <= row.max
            && row.last >= row.min
            && row.last <= row.max,
        "inconsistent derived rollup extrema"
    );
    ensure!(
        window_start(row.first_timestamp_us, row.width_us)? == row.bucket_us
            && window_start(row.last_timestamp_us, row.width_us)? == row.bucket_us
            && (
                row.first_timestamp_us,
                row.first_sequence,
                row.first_ordinal
            ) <= (row.last_timestamp_us, row.last_sequence, row.last_ordinal)
            && row.first_sequence > 0
            && row.last_sequence > 0,
        "inconsistent derived rollup ties"
    );
    Ok(())
}

fn validate_receipt(key: &str, receipt: &ReceiptEntry) -> Result<()> {
    validate_request_id(key)?;
    ensure!(
        receipt.sequence > 0 && receipt.rows > 0 && valid_digest(&receipt.digest),
        "invalid derived receipt"
    );
    ensure!(
        receipt
            .group_fingerprint
            .as_deref()
            .is_none_or(valid_digest),
        "invalid derived group receipt fingerprint"
    );
    Ok(())
}

pub(crate) fn rollup_resident_bytes(key: &str, row: &RollupRow) -> usize {
    512usize
        .saturating_add(key.len())
        .saturating_add(row.tenant.len())
        .saturating_add(row.series.len())
        .saturating_add(
            row.tags
                .iter()
                .map(|(k, v)| 256usize.saturating_add(k.len()).saturating_add(v.len()))
                .sum::<usize>(),
        )
}

pub(crate) fn receipt_resident_bytes(key: &str, receipt: &ReceiptEntry) -> usize {
    512usize
        .saturating_add(key.len())
        .saturating_add(receipt.digest.len())
        .saturating_add(receipt.group_fingerprint.as_ref().map_or(0, String::len))
}

struct BoundedBytes {
    bytes: Vec<u8>,
    max: usize,
}
impl Write for BoundedBytes {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.max.saturating_sub(self.bytes.len()) {
            return Err(io::Error::other("derived encoding exceeds page budget"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) fn page_overhead(table: &str, kind: PageKind) -> Result<usize> {
    Ok(FRAME_BYTES
        + format!(
            "{{\"table\":{},\"kind\":{},\"entries\":[",
            serde_json::to_string(table)?,
            serde_json::to_string(&kind)?
        )
        .len()
        + 2)
}

fn encode_set<'a, T: Serialize + 'a>(
    table: &str,
    kind: PageKind,
    rows: impl Iterator<Item = (&'a String, &'a T)>,
    limits: PageLimits,
    validate: impl Fn(&str, &T) -> Result<()>,
    mut emit: impl FnMut(&PageRef, &[u8]) -> Result<()>,
) -> Result<PageSet> {
    #[cfg(test)]
    CODEC_CALLS.with(|calls| calls.set(calls.get() + 1));
    limits.validate()?;
    validate_name(table)?;
    let mut rows = rows.peekable();
    if rows.peek().is_none() {
        return Ok(PageSet::default());
    }
    // At most one page and one candidate entry, with conservative Vec slack.
    ensure!(
        limits.page_bytes.saturating_mul(4) <= limits.max_bytes,
        "derived encoding working budget exceeded"
    );
    let prefix = format!(
        "{{\"table\":{},\"kind\":{},\"entries\":[",
        serde_json::to_string(table)?,
        serde_json::to_string(&kind)?
    );
    let overhead = FRAME_BYTES + prefix.len() + 2;
    let mut payload = Vec::with_capacity(limits.page_bytes.saturating_sub(overhead));
    let mut count = 0u64;
    let mut set = PageSet::default();
    let mut flush = |payload: &mut Vec<u8>, count: u64| -> Result<()> {
        let mut bytes = Vec::with_capacity(overhead + payload.len());
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(prefix.as_bytes());
        bytes.append(payload);
        bytes.extend_from_slice(b"]}");
        bytes.extend_from_slice(blake3::hash(&bytes).as_bytes());
        let page = PageRef {
            digest: blake3::hash(&bytes).to_hex().to_string(),
            bytes: bytes.len() as u64,
            entries: count,
        };
        let total = set
            .encoded_bytes
            .checked_add(page.bytes)
            .context("derived encoded bytes overflow")?;
        ensure!(
            total <= limits.max_bytes as u64,
            "derived encoded byte budget exceeded"
        );
        emit(&page, &bytes)?;
        set.encoded_bytes = total;
        set.entries += count;
        set.pages.push(page);
        Ok(())
    };
    for (index, (key, row)) in rows.enumerate() {
        ensure!(index < limits.max_entries, "derived entry budget exceeded");
        validate(key, row)?;
        let mut entry = BoundedBytes {
            bytes: Vec::with_capacity(limits.page_bytes.saturating_sub(overhead)),
            max: limits.page_bytes.saturating_sub(overhead),
        };
        serde_json::to_writer(&mut entry, &(key, row))?;
        let separator = usize::from(count > 0);
        if count > 0 && overhead + payload.len() + separator + entry.bytes.len() > limits.page_bytes
        {
            flush(&mut payload, count)?;
            count = 0;
        }
        if count > 0 {
            payload.push(b',');
        }
        payload.extend_from_slice(&entry.bytes);
        count += 1;
    }
    if count > 0 {
        flush(&mut payload, count)?;
    }
    set.validate(limits)?;
    Ok(set)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload<T> {
    table: String,
    kind: PageKind,
    entries: Vec<(String, T)>,
}

fn hydrate_set<T: DeserializeOwned>(
    table: &str,
    kind: PageKind,
    set: &PageSet,
    limits: PageLimits,
    validate: impl Fn(&str, &T) -> Result<()>,
    resident: impl Fn(&str, &T) -> usize,
    mut load: impl FnMut(&PageRef) -> Result<Vec<u8>>,
) -> Result<BTreeMap<String, T>> {
    validate_name(table)?;
    set.validate(limits)?;
    let mut rows = BTreeMap::new();
    let mut resident_bytes = 0usize;
    let mut previous: Option<String> = None;
    for page in &set.pages {
        // Check before invoking the loader or allocating decoded objects. JSON tree
        // allocation is conservatively bounded by 64 bytes per encoded byte.
        let working = usize::try_from(page.bytes)?
            .checked_mul(64)
            .context("derived working bytes overflow")?;
        ensure!(
            resident_bytes.saturating_add(working) <= limits.max_bytes,
            "derived hydration working budget exceeded"
        );
        let bytes = load(page)?;
        page.verify(&bytes)?;
        let payload: Payload<T> = serde_json::from_slice(&bytes[8..bytes.len() - 32])?;
        ensure!(
            payload.table == table && payload.kind == kind,
            "derived page table/kind mismatch"
        );
        ensure!(
            payload.entries.len() as u64 == page.entries,
            "derived page entry count mismatch"
        );
        for (key, value) in payload.entries {
            if let Some(previous) = &previous {
                ensure!(previous < &key, "duplicate or unordered derived entry");
            }
            validate(&key, &value)?;
            resident_bytes = resident_bytes
                .checked_add(resident(&key, &value))
                .context("derived resident accounting overflow")?;
            ensure!(
                resident_bytes <= limits.max_bytes,
                "derived resident budget exceeded"
            );
            previous = Some(key.clone());
            ensure!(
                rows.insert(key, value).is_none(),
                "duplicate derived entry across pages"
            );
        }
    }
    Ok(rows)
}

pub(crate) fn encode_rollups(
    table: &str,
    rows: &BTreeMap<String, RollupRow>,
    limits: PageLimits,
    emit: impl FnMut(&PageRef, &[u8]) -> Result<()>,
) -> Result<PageSet> {
    encode_set(
        table,
        PageKind::Rollup,
        rows.iter(),
        limits,
        validate_rollup,
        emit,
    )
}
pub(crate) fn encode_receipts(
    table: &str,
    rows: &BTreeMap<String, ReceiptEntry>,
    limits: PageLimits,
    emit: impl FnMut(&PageRef, &[u8]) -> Result<()>,
) -> Result<PageSet> {
    encode_set(
        table,
        PageKind::Receipt,
        rows.iter(),
        limits,
        validate_receipt,
        emit,
    )
}
fn merged<'a, T>(
    base: &'a BTreeMap<String, T>,
    updates: &'a BTreeMap<String, T>,
) -> impl Iterator<Item = (&'a String, &'a T)> {
    let mut base = base.iter().peekable();
    let mut updates = updates.iter().peekable();
    std::iter::from_fn(move || match (base.peek(), updates.peek()) {
        (Some((left, _)), Some((right, _))) => match left.cmp(right) {
            std::cmp::Ordering::Less => base.next(),
            std::cmp::Ordering::Greater => updates.next(),
            std::cmp::Ordering::Equal => {
                base.next();
                updates.next()
            }
        },
        (Some(_), None) => base.next(),
        (None, Some(_)) => updates.next(),
        (None, None) => None,
    })
}

/// Exact projected page references, without mutating or cloning the resident map.
pub(crate) fn project_rollups(
    table: &str,
    base: &BTreeMap<String, RollupRow>,
    updates: &BTreeMap<String, RollupRow>,
    limits: PageLimits,
) -> Result<PageSet> {
    encode_set(
        table,
        PageKind::Rollup,
        merged(base, updates),
        limits,
        validate_rollup,
        |_, _| Ok(()),
    )
}

pub(crate) fn project_receipts(
    table: &str,
    base: &BTreeMap<String, ReceiptEntry>,
    request_id: &str,
    receipt: &ReceiptEntry,
    limits: PageLimits,
) -> Result<PageSet> {
    let update = BTreeMap::from([(request_id.to_owned(), receipt.clone())]);
    encode_set(
        table,
        PageKind::Receipt,
        merged(base, &update),
        limits,
        validate_receipt,
        |_, _| Ok(()),
    )
}

pub(crate) fn hydrate_rollups(
    table: &str,
    set: &PageSet,
    limits: PageLimits,
    load: impl FnMut(&PageRef) -> Result<Vec<u8>>,
) -> Result<BTreeMap<String, RollupRow>> {
    hydrate_set(
        table,
        PageKind::Rollup,
        set,
        limits,
        validate_rollup,
        rollup_resident_bytes,
        load,
    )
}
pub(crate) fn hydrate_receipts(
    table: &str,
    set: &PageSet,
    limits: PageLimits,
    load: impl FnMut(&PageRef) -> Result<Vec<u8>>,
) -> Result<BTreeMap<String, ReceiptEntry>> {
    hydrate_set(
        table,
        PageKind::Receipt,
        set,
        limits,
        validate_receipt,
        receipt_resident_bytes,
        load,
    )
}
