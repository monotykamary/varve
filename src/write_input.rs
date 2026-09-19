use super::WriteRequest;
use crate::model::{Config, Row, validate_request_id};
use crate::wal::AppendItem;
use anyhow::{Context, Result, ensure};
use std::io::{BufWriter, Write};
use std::ops::Deref;

/// Ownership is the proof: callers cannot mutate rows after admission.
pub(crate) struct AdmittedWrite {
    request: WriteRequest,
    bytes: usize,
    raw_bytes: usize,
    retained_bytes: usize,
    raw: Option<crate::raw_memory::RawReservation>,
}

impl AdmittedWrite {
    pub(crate) fn new(request: WriteRequest) -> Result<Self> {
        let bytes = request.admission_bytes()?;
        let (raw_bytes, retained_bytes) = allocation_envelope(&request)?;
        Ok(Self {
            request,
            bytes,
            raw_bytes,
            retained_bytes,
            raw: None,
        })
    }

    pub(crate) fn reserve(&mut self, budget: &crate::raw_memory::RawMemoryBudget) -> Result<()> {
        if self.raw.is_none() {
            self.raw = Some(budget.reserve(self.raw_bytes)?);
        }
        Ok(())
    }

    pub(crate) fn release_reservation(&mut self) {
        self.raw = None;
    }

    #[cfg(test)]
    pub(crate) fn raw_bytes(&self) -> usize {
        self.raw_bytes
    }

    pub(crate) fn rows(&self) -> usize {
        self.request.rows.len()
    }

    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    pub(crate) fn prepare(self, config: &Config) -> Result<PreparedWrite> {
        ensure!(
            self.rows() <= config.max_batch_rows,
            "batch row admission limit"
        );
        let (encoded_bytes, digest) = row_identity(&self.request.rows, config.max_batch_bytes)?;
        let resident_bytes = resident_bytes(&self.request.rows)?;
        Ok(PreparedWrite {
            item: AppendItem {
                table: self.request.table,
                request_id: self.request.request_id,
                digest,
                rows: self.request.rows,
                now_us: Some(self.request.now_us),
            },
            admission_bytes: self.bytes,
            raw: self.raw,
            retained_bytes: self.retained_bytes,
            encoded_bytes,
            resident_bytes,
        })
    }
}

pub(crate) struct PreparedWrite {
    item: AppendItem,
    raw: Option<crate::raw_memory::RawReservation>,
    retained_bytes: usize,
    admission_bytes: usize,
    encoded_bytes: usize,
    resident_bytes: usize,
}

impl PreparedWrite {
    fn ensure_raw_credit(&mut self, _budget: &crate::raw_memory::RawMemoryBudget) -> Result<()> {
        if self.raw.is_none() {
            #[cfg(test)]
            {
                self.raw = Some(_budget.reserve(self.retained_bytes)?);
            }
            #[cfg(not(test))]
            anyhow::bail!("prepared input lacks raw admission credit");
        }
        Ok(())
    }

    /// Called while the encoded-first owner still holds EVERY input. Separate
    /// the frame/transient portion before any consuming/fallible materialization.
    /// A failed split leaves the original credit in this input. No allocation,
    /// fallible operation or user code separates a successful split and replace.
    pub(super) fn detach_frame_credit(
        &mut self,
        budget: &crate::raw_memory::RawMemoryBudget,
    ) -> Result<crate::raw_memory::RawReservation> {
        self.ensure_raw_credit(budget)?;
        #[cfg(test)]
        materialization_fault(4)?;
        let retained = self
            .raw
            .as_mut()
            .expect("raw credit checked")
            .split(self.retained_bytes)?;
        Ok(self.raw.replace(retained).expect("raw credit checked"))
    }

    pub(super) fn materialize(
        mut self,
        _budget: &crate::raw_memory::RawMemoryBudget,
        sequence: u64,
        ordinal: usize,
    ) -> Result<(
        crate::raw_memory::SharedRawRows,
        crate::raw_memory::RawReservation,
    )> {
        // Production inputs must already own this credit. Only independent test
        // oracles may construct a proof without running public admission.
        self.ensure_raw_credit(_budget)?;
        #[cfg(test)]
        materialization_fault(1)?;
        let stored = self
            .raw
            .as_mut()
            .expect("raw credit checked")
            .split(self.retained_bytes)?;
        let rows = std::mem::take(&mut self.item.rows);
        let shared = crate::raw_memory::SharedRawRows::build(stored, || {
            #[cfg(test)]
            materialization_fault(2)?;
            #[cfg(test)]
            MATERIALIZATION_PASSES.with(|count| count.set(count.get() + 1));
            // Allocate explicitly: collect's in-place specialization must not
            // retain excess caller Vec capacity after the transient credit drops.
            let mut stored = Vec::with_capacity(rows.len());
            for (index, row) in rows.into_iter().enumerate() {
                stored.push(crate::model::StoredRow {
                    row,
                    sequence,
                    ordinal: (ordinal + index) as u32,
                });
                #[cfg(test)]
                if index == 0 {
                    materialization_fault(3)?;
                }
            }
            Ok(stored)
        })?;
        Ok((shared, self.raw.take().expect("raw credit checked")))
    }

    #[cfg(all(test, feature = "fault-injection"))]
    pub(super) fn with_test_admission_hint(mut self, bytes: usize) -> Self {
        self.admission_bytes = bytes;
        self
    }

    pub(crate) fn admission_bytes(&self) -> usize {
        self.admission_bytes
    }

    pub(super) fn validated(&self) -> ValidatedAppend<'_> {
        ValidatedAppend {
            item: &self.item,
            encoded_bytes: self.encoded_bytes,
            resident_bytes: self.resident_bytes,
        }
    }
}

impl Deref for PreparedWrite {
    type Target = AppendItem;
    fn deref(&self) -> &Self::Target {
        &self.item
    }
}

/// A borrowed proof never outlives the immutable owner. Recovery has its own
/// validating constructor; no unchecked/trusted boolean crosses this boundary.
#[derive(Clone, Copy)]
pub(super) struct ValidatedAppend<'a> {
    item: &'a AppendItem,
    encoded_bytes: usize,
    resident_bytes: usize,
}

impl<'a> ValidatedAppend<'a> {
    pub(super) fn recovered(item: &'a AppendItem, config: &Config) -> Result<Self> {
        validate_request_id(&item.request_id)?;
        ensure!(
            !item.rows.is_empty() && item.rows.len() <= config.max_batch_rows,
            "invalid/recovery oversized WAL batch"
        );
        let (encoded_bytes, digest) = row_identity(&item.rows, config.max_batch_bytes)?;
        ensure!(digest == item.digest, "WAL batch digest mismatch");
        for row in &item.rows {
            row.validate()?;
        }
        Ok(Self {
            item,
            encoded_bytes,
            resident_bytes: resident_bytes(&item.rows)?,
        })
    }

    pub(super) fn item(&self) -> &'a AppendItem {
        self.item
    }
    pub(super) fn resident_bytes(&self) -> usize {
        self.resident_bytes
    }
    pub(super) fn check_limits(&self, config: &Config, ordinal: usize) -> Result<()> {
        ensure!(
            !self.item.rows.is_empty()
                && self.item.rows.len() <= config.max_batch_rows
                && ordinal.saturating_add(self.item.rows.len()) <= u32::MAX as usize,
            "invalid/recovery oversized WAL batch"
        );
        ensure!(
            self.encoded_bytes <= config.max_batch_bytes,
            "batch byte admission limit"
        );
        Ok(())
    }
}

/// Counts canonical JSON without allocating a payload (or a tag string).
struct CountWriter(usize);
impl Write for CountWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| std::io::Error::other("JSON length overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn allocation_envelope(request: &WriteRequest) -> Result<(usize, usize)> {
    let logical = resident_bytes(&request.rows)?;
    let mut excess = 0usize;
    for row in &request.rows {
        for text in std::iter::once(&row.tenant)
            .chain(std::iter::once(&row.series))
            .chain(row.tags.iter().flat_map(|(k, v)| [k, v]))
        {
            excess = excess
                .checked_add(text.capacity() - text.len())
                .context("input capacity overflow")?;
        }
    }
    let retained = crate::raw_memory::row_charge(logical)
        .checked_add(excess)
        .context("retained row overflow")?;
    let mut encoded = CountWriter(0);
    serde_json::to_writer(&mut encoded, &request.rows)?;
    // Existing WAL encoder starts at 4096 and doubles Vec capacity. Three
    // times final length covers old+new backing during a growth reallocation. Each item
    // contributes its exact row JSON plus <=6x identifier bytes and 512 bytes
    // for field names, digest, clocks, framing and per-item owner/index metadata.
    let frame = encoded
        .0
        .checked_add(
            (request.table.len() + request.request_id.len())
                .checked_mul(6)
                .context("frame overflow")?,
        )
        .and_then(|n| n.checked_add(512))
        .context("frame overflow")?;
    let envelope = retained
        .checked_add(
            request
                .rows
                .capacity()
                .checked_mul(std::mem::size_of::<Row>())
                .context("input vector overflow")?,
        )
        .and_then(|n| n.checked_add(request.table.capacity()))
        .and_then(|n| n.checked_add(request.request_id.capacity()))
        .and_then(|n| n.checked_add(frame.checked_mul(3)?.max(4096)))
        .and_then(|n| n.checked_add(8192 + 512))
        .context("raw admission envelope overflow")?;
    Ok((envelope, retained))
}

fn resident_bytes(rows: &[Row]) -> Result<usize> {
    rows.iter().try_fold(0usize, |bytes, row| {
        bytes
            .checked_add(row.estimated_bytes())
            .context("row memory accounting overflow")
    })
}

struct DigestWriter {
    hasher: blake3::Hasher,
    bytes: usize,
    limit: usize,
}

impl Write for DigestWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes) {
            return Err(std::io::Error::other("batch byte admission limit"));
        }
        #[cfg(test)]
        ROW_HASH_CHUNKS.with(|count| count.set(count.get() + 1));
        self.hasher.update(bytes);
        self.bytes += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
thread_local! {
    pub(super) static MATERIALIZATION_FAULT: std::cell::Cell<Option<(usize, usize, bool)>> = const { std::cell::Cell::new(None) };
}
#[cfg(test)]
fn materialization_fault(stage: usize) -> Result<()> {
    MATERIALIZATION_FAULT.with(|fault| {
        if let Some((requested, remaining, panic)) = fault.get()
            && requested == stage
        {
            if remaining > 1 {
                fault.set(Some((requested, remaining - 1, panic)));
            } else {
                fault.set(None);
                assert!(!panic, "injected materialization panic");
                anyhow::bail!("injected materialization failure");
            }
        }
        Ok(())
    })
}

#[cfg(test)]
thread_local! {
    pub(super) static MATERIALIZATION_PASSES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(super) static ROW_IDENTITY_PASSES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    pub(super) static ADMISSION_PASSES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static ROW_HASH_CHUNKS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn row_identity(rows: &[Row], limit: usize) -> Result<(usize, String)> {
    #[cfg(test)]
    ROW_IDENTITY_PASSES.with(|count| count.set(count.get() + 1));
    // Serde emits many tiny tokens. Coalesce them in bounded scratch space
    // instead of replacing a whole-buffer hash with one hash update per token.
    let mut writer = BufWriter::with_capacity(
        limit.min(8192),
        DigestWriter {
            hasher: blake3::Hasher::new(),
            bytes: 0,
            limit,
        },
    );
    serde_json::to_writer(&mut writer, rows)?;
    writer.flush()?;
    let writer = writer.into_inner().map_err(|error| error.into_error())?;
    Ok((writer.bytes, writer.hasher.finalize().to_hex().to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn request(value: f64) -> WriteRequest {
        WriteRequest {
            table: "metrics".into(),
            request_id: "id".into(),
            now_us: 20,
            rows: vec![Row {
                timestamp_us: -1,
                tenant: "東京".into(),
                series: "quote\"\\".into(),
                value,
                tags: BTreeMap::from([("line".into(), "a\nb\t".into())]),
            }],
        }
    }

    #[test]
    fn failed_frame_split_keeps_credit_in_unconsumed_input() -> Result<()> {
        let budget = crate::raw_memory::RawMemoryBudget::new(1024 * 1024, 1)?;
        let mut admitted = AdmittedWrite::new(request(1.0))?;
        admitted.reserve(&budget)?;
        let mut prepared = admitted.prepare(&Config::default())?;
        let bytes = budget.status().reserved_bytes;
        // Corrupt only the expected split size, not the actual credit/allocation.
        prepared.retained_bytes = bytes + 1;
        assert!(prepared.detach_frame_credit(&budget).is_err());
        assert_eq!(prepared.raw.as_ref().unwrap().bytes(), bytes);
        assert_eq!(budget.status().reserved_bytes, bytes);
        assert_eq!(prepared.rows.len(), 1);
        drop(prepared);
        assert_eq!(budget.status().reserved_bytes, 0);
        Ok(())
    }

    #[test]
    fn streaming_identity_matches_canonical_bytes_and_exact_bounds() -> Result<()> {
        for value in [-0.0, 0.0, f64::MIN_POSITIVE, f64::MAX, 0.30000000000000004] {
            let request = request(value);
            let expected = serde_json::to_vec(&request.rows)?;
            let (length, digest) = row_identity(&request.rows, expected.len())?;
            assert_eq!(length, expected.len());
            assert_eq!(digest, blake3::hash(&expected).to_hex().as_str());
            assert!(row_identity(&request.rows, expected.len() - 1).is_err());
        }
        Ok(())
    }

    #[test]
    fn admission_moves_owned_rows_and_preparation_runs_identity_once() -> Result<()> {
        let request = request(-0.0);
        let rows_pointer = request.rows.as_ptr();
        let string_pointer = request.rows[0].tenant.as_ptr();
        let admission_before = ADMISSION_PASSES.with(std::cell::Cell::get);
        let admitted = AdmittedWrite::new(request)?;
        let charge = admitted.bytes();
        let before = ROW_IDENTITY_PASSES.with(std::cell::Cell::get);
        let prepared = admitted.prepare(&Config::default())?;
        assert_eq!(prepared.rows.as_ptr(), rows_pointer);
        assert_eq!(prepared.rows[0].tenant.as_ptr(), string_pointer);
        assert_eq!(prepared.admission_bytes(), charge);
        for _ in 0..3 {
            prepared.validated().check_limits(&Config::default(), 0)?;
        }
        assert_eq!(ROW_IDENTITY_PASSES.with(std::cell::Cell::get) - before, 1);
        assert_eq!(
            ADMISSION_PASSES.with(std::cell::Cell::get) - admission_before,
            1
        );
        assert_eq!(prepared.rows[0].value.to_bits(), (-0.0f64).to_bits());
        Ok(())
    }

    #[test]
    fn streaming_hash_coalesces_tokens_in_bounded_scratch() -> Result<()> {
        let row = request(0.30000000000000004).rows.pop().unwrap();
        let rows = vec![row; 512];
        let expected = serde_json::to_vec(&rows)?;
        let before = ROW_HASH_CHUNKS.with(std::cell::Cell::get);
        let (bytes, digest) = row_identity(&rows, expected.len())?;
        let updates = ROW_HASH_CHUNKS.with(std::cell::Cell::get) - before;
        assert_eq!(bytes, expected.len());
        assert_eq!(digest, blake3::hash(&expected).to_hex().as_str());
        assert!(updates > 1 && updates <= expected.len() / 4096 + 2);
        assert!(
            updates < rows.len(),
            "hash work must not amplify to per-row or per-token calls"
        );
        Ok(())
    }

    #[test]
    fn untrusted_recovery_revalidates_digest_and_static_rows() -> Result<()> {
        let prepared = AdmittedWrite::new(request(1.0))?.prepare(&Config::default())?;
        ValidatedAppend::recovered(&prepared, &Config::default())?;
        let mut forged = prepared.item.clone();
        forged.rows[0].value = 9.0;
        assert!(ValidatedAppend::recovered(&forged, &Config::default()).is_err());
        forged.digest = row_identity(&forged.rows, usize::MAX)?.1;
        ValidatedAppend::recovered(&forged, &Config::default())?;
        forged.rows[0].tenant.clear();
        forged.digest = row_identity(&forged.rows, usize::MAX)?.1;
        assert!(ValidatedAppend::recovered(&forged, &Config::default()).is_err());
        assert!(AdmittedWrite::new(request(f64::NAN)).is_err());
        assert!(AdmittedWrite::new(request(f64::INFINITY)).is_err());
        Ok(())
    }
}
