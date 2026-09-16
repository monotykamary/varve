use anyhow::{Context, Result, ensure};
use arrow_array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::model::{Row, StoredRow};

const BATCH_ROWS: usize = 8_192;

struct LimitedWriter<W> {
    inner: W,
    written: u64,
    max_bytes: u64,
}

impl<W: Write> Write for LimitedWriter<W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        crate::wal::injected_io("segment_before_write")?;
        let length = u64::try_from(bytes.len()).map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "segment write is too large",
            )
        })?;
        if self
            .written
            .checked_add(length)
            .is_none_or(|end| end > self.max_bytes)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::FileTooLarge,
                "segment exceeds configured byte limit",
            ));
        }
        let written = self.inner.write(bytes)?;
        self.written = self
            .written
            .checked_add(written as u64)
            .ok_or_else(|| std::io::Error::other("segment size overflow"))?;
        Ok(written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        crate::wal::injected_io("segment_before_flush")?;
        self.inner.flush()
    }
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("timestamp_us", DataType::Int64, false),
        Field::new("tenant", DataType::Utf8, false),
        Field::new("series", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
        Field::new("tags", DataType::Utf8, false),
        Field::new("sequence", DataType::UInt64, false),
        Field::new("ordinal", DataType::UInt32, false),
    ]))
}

fn row_order(left: &StoredRow, right: &StoredRow) -> Ordering {
    left.row
        .tenant
        .cmp(&right.row.tenant)
        .then_with(|| left.row.series.cmp(&right.row.series))
        .then_with(|| left.row.timestamp_us.cmp(&right.row.timestamp_us))
        .then_with(|| left.sequence.cmp(&right.sequence))
        .then_with(|| left.ordinal.cmp(&right.ordinal))
}

fn record_batch(rows: &[&StoredRow], schema: Arc<Schema>) -> Result<RecordBatch> {
    let timestamps = Int64Array::from_iter_values(rows.iter().map(|row| row.row.timestamp_us));
    let tenants = StringArray::from_iter_values(rows.iter().map(|row| row.row.tenant.as_str()));
    let series = StringArray::from_iter_values(rows.iter().map(|row| row.row.series.as_str()));
    let values = Float64Array::from_iter_values(rows.iter().map(|row| row.row.value));
    let tags = rows
        .iter()
        .map(|row| serde_json::to_string(&row.row.tags).context("serialize row tags"))
        .collect::<Result<Vec<_>>>()?;
    let tags = StringArray::from(tags);
    let sequences = UInt64Array::from_iter_values(rows.iter().map(|row| row.sequence));
    let ordinals = UInt32Array::from_iter_values(rows.iter().map(|row| row.ordinal));
    let columns: Vec<ArrayRef> = vec![
        Arc::new(timestamps),
        Arc::new(tenants),
        Arc::new(series),
        Arc::new(values),
        Arc::new(tags),
        Arc::new(sequences),
        Arc::new(ordinals),
    ];
    RecordBatch::try_new(schema, columns).context("construct Arrow record batch")
}

fn encode_to<W: Write + Send>(inner: W, rows: &[StoredRow], max_bytes: u64) -> Result<()> {
    ensure!(max_bytes > 0, "segment byte limit must be positive");
    for row in rows {
        row.row.validate().context("validate segment row")?;
    }
    let mut sorted: Vec<&StoredRow> = rows.iter().collect();
    sorted.sort_by(|left, right| row_order(left, right));
    let limited = LimitedWriter {
        inner,
        written: 0,
        max_bytes,
    };
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let segment_schema = schema();
    let mut writer = ArrowWriter::try_new(limited, Arc::clone(&segment_schema), Some(properties))
        .context("create Parquet writer")?;
    for chunk in sorted.chunks(BATCH_ROWS) {
        writer
            .write(&record_batch(chunk, Arc::clone(&segment_schema))?)
            .context("write Parquet record batch")?;
    }
    writer.close().context("finish Parquet segment")?;
    Ok(())
}

#[derive(Clone)]
struct MemoryWriter(Arc<Mutex<Vec<u8>>>);

impl Write for MemoryWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .map_err(|_| std::io::Error::other("segment memory writer poisoned"))?
            .extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub fn encode_with_limit(rows: &[StoredRow], max_bytes: u64) -> Result<Vec<u8>> {
    let shared = Arc::new(Mutex::new(Vec::new()));
    encode_to(MemoryWriter(Arc::clone(&shared)), rows, max_bytes)?;
    let bytes = Arc::try_unwrap(shared)
        .map_err(|_| anyhow::anyhow!("segment memory writer still shared"))?
        .into_inner()
        .map_err(|_| anyhow::anyhow!("segment memory writer poisoned"))?;
    ensure!(
        bytes.len() as u64 <= max_bytes,
        "segment exceeds configured byte limit"
    );
    Ok(bytes)
}

pub fn write(path: &Path, rows: &[StoredRow]) -> Result<()> {
    write_with_limit(path, rows, u64::MAX)
}

pub fn write_with_limit(path: &Path, rows: &[StoredRow], max_bytes: u64) -> Result<()> {
    ensure!(max_bytes > 0, "segment byte limit must be positive");
    let mut created = false;
    let result = (|| {
        crate::wal::injected_io("segment_before_create")?;
        let file =
            File::create(path).with_context(|| format!("create segment {}", path.display()))?;
        created = true;
        encode_to(file, rows, max_bytes)?;
        ensure!(
            fs::metadata(path)?.len() <= max_bytes,
            "segment exceeds configured byte limit"
        );
        Ok(())
    })();
    if result.is_err() && created {
        let _ = fs::remove_file(path);
    }
    result
}

pub fn read(path: &Path) -> Result<Vec<StoredRow>> {
    let file = File::open(path).with_context(|| format!("open segment {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).context("open Parquet reader")?;
    ensure!(
        builder.schema().as_ref() == schema().as_ref(),
        "unexpected segment Arrow schema"
    );
    let reader = builder.build().context("build Parquet batch reader")?;
    let mut rows = Vec::new();

    for batch in reader {
        let batch = batch.context("read Parquet record batch")?;
        ensure!(batch.num_columns() == 7, "unexpected segment column count");
        for column in batch.columns() {
            ensure!(
                column.null_count() == 0,
                "segment columns may not contain nulls"
            );
        }
        let timestamps = downcast::<Int64Array>(&batch, 0, "timestamp_us")?;
        let tenants = downcast::<StringArray>(&batch, 1, "tenant")?;
        let series = downcast::<StringArray>(&batch, 2, "series")?;
        let values = downcast::<Float64Array>(&batch, 3, "value")?;
        let tags = downcast::<StringArray>(&batch, 4, "tags")?;
        let sequences = downcast::<UInt64Array>(&batch, 5, "sequence")?;
        let ordinals = downcast::<UInt32Array>(&batch, 6, "ordinal")?;

        for index in 0..batch.num_rows() {
            let row = Row {
                timestamp_us: timestamps.value(index),
                tenant: tenants.value(index).to_owned(),
                series: series.value(index).to_owned(),
                value: values.value(index),
                tags: serde_json::from_str::<BTreeMap<String, String>>(tags.value(index))
                    .context("decode segment tags JSON")?,
            };
            row.validate().context("validate decoded segment row")?;
            rows.push(StoredRow {
                row,
                sequence: sequences.value(index),
                ordinal: ordinals.value(index),
            });
        }
    }
    Ok(rows)
}

fn downcast<'a, T: Array + 'static>(
    batch: &'a RecordBatch,
    index: usize,
    name: &str,
) -> Result<&'a T> {
    batch
        .column(index)
        .as_any()
        .downcast_ref::<T>()
        .with_context(|| format!("segment column {name} has an unexpected type"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn stored(
        tenant: &str,
        series: &str,
        timestamp_us: i64,
        sequence: u64,
        ordinal: u32,
    ) -> StoredRow {
        StoredRow {
            row: Row {
                timestamp_us,
                tenant: tenant.to_owned(),
                series: series.to_owned(),
                value: timestamp_us as f64 / 10.0,
                tags: BTreeMap::from([("quote".to_owned(), "雪'\"".to_owned())]),
            },
            sequence,
            ordinal,
        }
    }

    #[test]
    fn parquet_round_trip_is_sorted_and_typed() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("rows.parquet");
        let input = vec![
            stored("b", "cpu", 3, 2, 0),
            stored("a", "cpu", 2, u64::MAX, 1),
            stored("a", "cpu", 2, 1, 2),
            stored("a", "disk", -1, 1, 0),
        ];
        write(&path, &input).unwrap();
        let rows = read(&path).unwrap();
        assert_eq!(
            rows,
            vec![
                input[2].clone(),
                input[1].clone(),
                input[3].clone(),
                input[0].clone()
            ]
        );
        assert!(std::fs::metadata(path).unwrap().len() > 0);
    }

    #[test]
    fn empty_segment_preserves_schema() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("empty.parquet");
        write(&path, &[]).unwrap();
        assert!(read(&path).unwrap().is_empty());
    }
}
