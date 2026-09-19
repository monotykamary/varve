use super::ffi::{
    self, Api, DuckBytes, DuckStr, Handle, HugeInt, OwnedHandle, TYPE_ARRAY, TYPE_BIGINT,
    TYPE_BIGNUM, TYPE_BIT, TYPE_BLOB, TYPE_BOOLEAN, TYPE_DATE, TYPE_DECIMAL, TYPE_DOUBLE,
    TYPE_ENUM, TYPE_FLOAT, TYPE_GEOMETRY, TYPE_HUGEINT, TYPE_INTEGER, TYPE_INTERVAL, TYPE_LIST,
    TYPE_MAP, TYPE_SMALLINT, TYPE_SQLNULL, TYPE_STRUCT, TYPE_TIME, TYPE_TIME_NS, TYPE_TIME_TZ,
    TYPE_TIMESTAMP, TYPE_TIMESTAMP_MS, TYPE_TIMESTAMP_NS, TYPE_TIMESTAMP_SEC, TYPE_TIMESTAMP_TZ,
    TYPE_TIMESTAMP_TZ_NS, TYPE_TINYINT, TYPE_TUPLE, TYPE_UBIGINT, TYPE_UHUGEINT, TYPE_UINTEGER,
    TYPE_UNION, TYPE_USMALLINT, TYPE_UTINYINT, TYPE_UUID, TYPE_VARCHAR, TYPE_VARIANT, UHugeInt,
    VectorView,
};
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Map, Number, Value, json};
use std::ffi::{c_char, c_void};
use std::io::{self, Write};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::str;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

const MAX_VALUE_DEPTH: usize = 128;

struct Column {
    name: String,
    kind: ffi::TypeId,
    json: bool,
}

struct ColumnVector {
    handle: Handle,
    view: Option<VectorView>,
}

pub(super) fn consume(
    api: &Arc<Api>,
    connection: Handle,
    result: &mut OwnedHandle,
    explain: bool,
    max_output_bytes: usize,
    abort: &AtomicBool,
) -> Result<Value> {
    if explain {
        return render_explain(api, result, max_output_bytes);
    }
    let columns = result_columns(api, result.get())?;
    let mut rows = BoundedRows::new(max_output_bytes)?;
    loop {
        let mut error = ptr::null_mut();
        let mut chunk = ptr::null_mut();
        let code = unsafe { (api.result_fetch_chunk)(result.get(), &mut chunk, &mut error) };
        unsafe { api.check(code, &mut error, "fetch native DuckDB result chunk")? };
        if chunk.is_null() {
            break;
        }
        let chunk = OwnedHandle::new(Arc::clone(api), chunk, api.data_chunk_destroy);
        if let Err(error) = append_chunk(api, chunk.get(), &columns, &mut rows) {
            abort.store(true, Ordering::Release);
            let mut interrupt_error = ptr::null_mut();
            let _ = unsafe { (api.connection_interrupt)(connection, &mut interrupt_error) };
            if !interrupt_error.is_null() {
                let _ = unsafe { (api.error_info_destroy)(&mut interrupt_error) };
            }
            return Err(error);
        }
    }
    Ok(Value::Array(rows.rows))
}

fn result_columns(api: &Arc<Api>, result: Handle) -> Result<Vec<Column>> {
    let mut error = ptr::null_mut();
    let mut schema = ptr::null_mut();
    let code = unsafe { (api.result_get_schema)(result, &mut schema, &mut error) };
    unsafe { api.check(code, &mut error, "read native DuckDB result schema")? };
    let schema = OwnedHandle::new(Arc::clone(api), schema, api.schema_destroy);
    let mut count = 0;
    let code = unsafe { (api.schema_get_count)(schema.get(), &mut count, &mut error) };
    unsafe { api.check(code, &mut error, "read native DuckDB result column count")? };
    let mut columns = Vec::with_capacity(count);
    for index in 0..count {
        let mut name = DuckStr {
            ptr: ptr::null(),
            len: 0,
        };
        let mut logical_type = ptr::null_mut();
        let code = unsafe {
            (api.schema_get_field)(
                schema.get(),
                index,
                &mut name,
                &mut logical_type,
                &mut error,
            )
        };
        unsafe { api.check(code, &mut error, "read native DuckDB result column")? };
        ensure!(!logical_type.is_null(), "native DuckDB result type is null");
        let name = copy_utf8(name, "native DuckDB result column name")?;
        let mut kind = 0;
        let code = unsafe { (api.logical_type_get_id)(logical_type, &mut kind, &mut error) };
        unsafe { api.check(code, &mut error, "read native DuckDB result type")? };
        ensure!(
            is_supported_result_type(kind),
            "native DuckDB result type {kind} is not a qualified SQL output type"
        );
        let json = kind == TYPE_VARCHAR && logical_is_json(api, logical_type)?;
        columns.push(Column { name, kind, json });
    }
    Ok(columns)
}

fn is_supported_result_type(kind: ffi::TypeId) -> bool {
    matches!(
        kind,
        TYPE_SQLNULL
            | TYPE_BOOLEAN
            | TYPE_TINYINT
            | TYPE_SMALLINT
            | TYPE_INTEGER
            | TYPE_BIGINT
            | TYPE_DATE
            | TYPE_TIME
            | TYPE_TIMESTAMP_SEC
            | TYPE_TIMESTAMP_MS
            | TYPE_TIMESTAMP
            | TYPE_TIMESTAMP_NS
            | TYPE_DECIMAL
            | TYPE_FLOAT
            | TYPE_DOUBLE
            | TYPE_VARCHAR
            | TYPE_BLOB
            | TYPE_INTERVAL
            | TYPE_UTINYINT
            | TYPE_USMALLINT
            | TYPE_UINTEGER
            | TYPE_UBIGINT
            | TYPE_TIMESTAMP_TZ
            | TYPE_TIMESTAMP_TZ_NS
            | TYPE_TIME_TZ
            | TYPE_TIME_NS
            | TYPE_BIT
            | TYPE_BIGNUM
            | TYPE_UHUGEINT
            | TYPE_HUGEINT
            | TYPE_UUID
            | TYPE_GEOMETRY
            | TYPE_STRUCT
            | TYPE_LIST
            | TYPE_MAP
            | TYPE_ENUM
            | TYPE_UNION
            | TYPE_ARRAY
            | TYPE_VARIANT
            | TYPE_TUPLE
    )
}

fn is_fast_type(kind: ffi::TypeId) -> bool {
    matches!(
        kind,
        TYPE_SQLNULL
            | TYPE_BOOLEAN
            | TYPE_TINYINT
            | TYPE_SMALLINT
            | TYPE_INTEGER
            | TYPE_BIGINT
            | TYPE_FLOAT
            | TYPE_DOUBLE
            | TYPE_VARCHAR
            | TYPE_UTINYINT
            | TYPE_USMALLINT
            | TYPE_UINTEGER
            | TYPE_UBIGINT
            | TYPE_HUGEINT
            | TYPE_UHUGEINT
    )
}

fn append_chunk(
    api: &Arc<Api>,
    chunk: Handle,
    columns: &[Column],
    rows: &mut BoundedRows,
) -> Result<()> {
    let mut error = ptr::null_mut();
    let mut size = 0;
    let code = unsafe { (api.data_chunk_get_size)(chunk, &mut size, &mut error) };
    unsafe { api.check(code, &mut error, "read native DuckDB result chunk size")? };
    let mut vectors = Vec::with_capacity(columns.len());
    for (index, column) in columns.iter().enumerate() {
        let mut vector = ptr::null_mut();
        let code = unsafe { (api.data_chunk_get_vector)(chunk, index, &mut vector, &mut error) };
        unsafe { api.check(code, &mut error, "read native DuckDB result vector")? };
        ensure!(!vector.is_null(), "native DuckDB result vector is null");
        let view = if is_fast_type(column.kind) {
            let code = unsafe { (api.vector_flatten)(vector, &mut error) };
            unsafe { api.check(code, &mut error, "flatten native DuckDB result vector")? };
            let mut view = VectorView::default();
            let code = unsafe { (api.vector_get_view)(vector, &mut view, &mut error) };
            unsafe { api.check(code, &mut error, "view native DuckDB result vector")? };
            ensure!(
                view.count >= size,
                "native DuckDB vector is shorter than its chunk"
            );
            Some(view)
        } else {
            None
        };
        // The vector is borrowed from the chunk and never retained past this call.
        vectors.push(ColumnVector {
            handle: vector,
            view,
        });
    }

    for logical_row in 0..size {
        let mut allocation_remaining = rows.remaining_for_row()?;
        let mut row = Map::new();
        for (index, (column, vector)) in columns.iter().zip(&vectors).enumerate() {
            ensure!(
                column.name.len() <= allocation_remaining,
                "DuckDB output exceeded {} bytes",
                rows.limit
            );
            allocation_remaining -= column.name.len();
            let value = if let Some(view) = vector.view {
                let physical = if view.sel.is_null() {
                    logical_row
                } else {
                    unsafe { *view.sel.add(logical_row) as usize }
                };
                let valid = view.validity.is_null()
                    || unsafe {
                        (*view.validity.add(physical / 64) & (1_u64 << (physical % 64))) != 0
                    };
                if valid {
                    unsafe {
                        decode_fast_value(
                            column.kind,
                            view.data,
                            physical,
                            &mut allocation_remaining,
                            rows.limit,
                        )?
                    }
                } else {
                    Value::Null
                }
            } else {
                decode_vector_value(
                    api,
                    vector.handle,
                    logical_row,
                    &mut allocation_remaining,
                    rows.limit,
                )
                .with_context(|| format!("decode native DuckDB result column {index}"))?
            };
            let value = if column.json && !value.is_null() {
                serde_json::from_str(value.as_str().context("native JSON alias is not text")?)
                    .context("decode native DuckDB JSON alias")?
            } else {
                value
            };
            row.insert(column.name.clone(), value);
        }
        rows.push(Value::Object(row))?;
    }
    Ok(())
}

unsafe fn decode_fast_value(
    kind: ffi::TypeId,
    data: *const c_void,
    row: usize,
    allocation_remaining: &mut usize,
    max_output_bytes: usize,
) -> Result<Value> {
    if kind == TYPE_SQLNULL {
        return Ok(Value::Null);
    }
    ensure!(!data.is_null(), "native DuckDB result vector data is null");
    let value = match kind {
        TYPE_BOOLEAN => Value::Bool(unsafe { *data.cast::<u8>().add(row) != 0 }),
        TYPE_TINYINT => Value::Number(Number::from(unsafe { *data.cast::<i8>().add(row) } as i64)),
        TYPE_SMALLINT => {
            Value::Number(Number::from(unsafe { *data.cast::<i16>().add(row) } as i64))
        }
        TYPE_INTEGER => Value::Number(Number::from(unsafe { *data.cast::<i32>().add(row) } as i64)),
        TYPE_BIGINT => Value::Number(Number::from(unsafe { *data.cast::<i64>().add(row) })),
        TYPE_UTINYINT => Value::Number(Number::from(unsafe { *data.cast::<u8>().add(row) } as u64)),
        TYPE_USMALLINT => {
            Value::Number(Number::from(unsafe { *data.cast::<u16>().add(row) } as u64))
        }
        TYPE_UINTEGER => {
            Value::Number(Number::from(unsafe { *data.cast::<u32>().add(row) } as u64))
        }
        // DuckDB CLI -json emits UBIGINT and both 128-bit integers as strings.
        TYPE_UBIGINT => Value::String(unsafe { *data.cast::<u64>().add(row) }.to_string()),
        TYPE_HUGEINT => {
            let value = unsafe { *data.cast::<HugeInt>().add(row) };
            let value = ((value.upper as i128) << 64) | i128::from(value.lower);
            Value::String(value.to_string())
        }
        TYPE_UHUGEINT => {
            let value = unsafe { *data.cast::<UHugeInt>().add(row) };
            let value = (u128::from(value.upper) << 64) | u128::from(value.lower);
            Value::String(value.to_string())
        }
        // The CLI widens FLOAT to DOUBLE in top-level JSON result cells.
        TYPE_FLOAT => finite_number(unsafe { *data.cast::<f32>().add(row) } as f64)?,
        TYPE_DOUBLE => finite_number(unsafe { *data.cast::<f64>().add(row) })?,
        TYPE_VARCHAR => {
            let encoded = unsafe { &*data.cast::<DuckBytes>().add(row) };
            let bytes = unsafe { encoded.as_slice() };
            charge(allocation_remaining, bytes.len(), max_output_bytes)?;
            Value::String(
                str::from_utf8(bytes)
                    .context("native DuckDB VARCHAR result is not UTF-8")?
                    .to_owned(),
            )
        }
        _ => bail!("native DuckDB result type {kind} left the primitive fast path"),
    };
    if matches!(kind, TYPE_UBIGINT | TYPE_HUGEINT | TYPE_UHUGEINT) {
        let text = value
            .as_str()
            .context("native DuckDB integer string is not text")?;
        charge(allocation_remaining, text.len(), max_output_bytes)?;
    }
    Ok(value)
}

fn decode_vector_value(
    api: &Arc<Api>,
    vector: Handle,
    row: usize,
    allocation_remaining: &mut usize,
    max_output_bytes: usize,
) -> Result<Value> {
    let mut error = ptr::null_mut();
    let mut value = ptr::null_mut();
    let code = unsafe { (api.vector_get_value)(vector, row, &mut value, &mut error) };
    unsafe { api.check(code, &mut error, "read native DuckDB vector value")? };
    ensure!(!value.is_null(), "native DuckDB vector value is null");
    // vector_get_value returns caller ownership, including for SQL NULL cells.
    let value = OwnedHandle::new(Arc::clone(api), value, api.value_destroy);
    decode_owned_value(api, value.get(), allocation_remaining, max_output_bytes, 0)
}

fn decode_owned_value(
    api: &Arc<Api>,
    value: Handle,
    allocation_remaining: &mut usize,
    max_output_bytes: usize,
    depth: usize,
) -> Result<Value> {
    ensure!(
        depth <= MAX_VALUE_DEPTH,
        "native DuckDB result exceeds the maximum nested value depth"
    );
    let mut error = ptr::null_mut();
    let mut is_null = false;
    let code = unsafe { (api.value_is_null)(value, &mut is_null, &mut error) };
    unsafe { api.check(code, &mut error, "inspect native DuckDB value nullability")? };
    if is_null {
        return Ok(Value::Null);
    }

    let mut logical_type = ptr::null_mut();
    let code = unsafe { (api.value_get_logical_type)(value, &mut logical_type, &mut error) };
    unsafe { api.check(code, &mut error, "read native DuckDB value type")? };
    ensure!(!logical_type.is_null(), "native DuckDB value type is null");
    // value_get_logical_type returns an independent caller-owned type.
    let logical_type = OwnedHandle::new(Arc::clone(api), logical_type, api.logical_type_destroy);
    let mut kind = 0;
    let code = unsafe { (api.logical_type_get_id)(logical_type.get(), &mut kind, &mut error) };
    unsafe { api.check(code, &mut error, "read native DuckDB value type id")? };

    match kind {
        TYPE_STRUCT => decode_struct(
            api,
            value,
            logical_type.get(),
            allocation_remaining,
            max_output_bytes,
            depth + 1,
        ),
        TYPE_LIST | TYPE_ARRAY | TYPE_TUPLE => decode_sequence(
            api,
            value,
            allocation_remaining,
            max_output_bytes,
            depth + 1,
        ),
        TYPE_MAP => decode_map(
            api,
            value,
            allocation_remaining,
            max_output_bytes,
            depth + 1,
        ),
        TYPE_UNION => decode_union(
            api,
            value,
            logical_type.get(),
            allocation_remaining,
            max_output_bytes,
            depth + 1,
        ),
        TYPE_BOOLEAN | TYPE_TINYINT | TYPE_SMALLINT | TYPE_INTEGER | TYPE_BIGINT
        | TYPE_UTINYINT | TYPE_USMALLINT | TYPE_UINTEGER => {
            let text = value_text(api, value, allocation_remaining, max_output_bytes)?;
            let decoded: Value = serde_json::from_str(&text)
                .with_context(|| format!("decode native DuckDB numeric value {text:?}"))?;
            ensure!(
                (kind == TYPE_BOOLEAN && decoded.is_boolean())
                    || (kind != TYPE_BOOLEAN && decoded.is_number()),
                "native DuckDB scalar formatter returned the wrong JSON kind"
            );
            Ok(decoded)
        }
        TYPE_FLOAT | TYPE_DOUBLE => {
            let mut number = 0.0;
            let code = unsafe { (api.value_get_double)(value, &mut number, &mut error) };
            unsafe { api.check(code, &mut error, "read native floating JSON value")? };
            finite_number(number)
        }
        TYPE_VARCHAR if logical_is_json(api, logical_type.get())? => {
            let text = value_text(api, value, allocation_remaining, max_output_bytes)?;
            serde_json::from_str(&text).context("decode nested DuckDB JSON alias")
        }
        TYPE_DECIMAL | TYPE_UBIGINT | TYPE_HUGEINT | TYPE_UHUGEINT | TYPE_BIGNUM if depth > 0 => {
            // Nested values use DuckDB's JSON formatter, whose numeric contract
            // differs from top-level result cells. Match the CLI parse exactly.
            let text = value_text(api, value, allocation_remaining, max_output_bytes)?;
            serde_json::from_str(&text).context("decode nested DuckDB numeric JSON")
        }
        // Top-level DECIMAL and wide integers are exact JSON strings.
        TYPE_DECIMAL | TYPE_UBIGINT | TYPE_HUGEINT | TYPE_UHUGEINT | TYPE_BIGNUM => {
            Ok(Value::String(value_text(
                api,
                value,
                allocation_remaining,
                max_output_bytes,
            )?))
        }
        TYPE_VARCHAR | TYPE_DATE | TYPE_TIME | TYPE_TIMESTAMP_SEC | TYPE_TIMESTAMP_MS
        | TYPE_TIMESTAMP | TYPE_TIMESTAMP_NS | TYPE_BLOB | TYPE_INTERVAL | TYPE_TIMESTAMP_TZ
        | TYPE_TIMESTAMP_TZ_NS | TYPE_TIME_TZ | TYPE_TIME_NS | TYPE_BIT | TYPE_UUID
        | TYPE_GEOMETRY | TYPE_ENUM | TYPE_VARIANT => Ok(Value::String(value_text(
            api,
            value,
            allocation_remaining,
            max_output_bytes,
        )?)),
        TYPE_SQLNULL => Ok(Value::Null),
        _ => bail!("native DuckDB value type {kind} is not a qualified SQL output type"),
    }
}

fn decode_sequence(
    api: &Arc<Api>,
    value: Handle,
    allocation_remaining: &mut usize,
    max_output_bytes: usize,
    depth: usize,
) -> Result<Value> {
    let count = child_count(api, value)?;
    charge(allocation_remaining, count, max_output_bytes)?;
    let mut values = Vec::with_capacity(count);
    for index in 0..count {
        let child = child_value(api, value, index)?;
        values.push(decode_owned_value(
            api,
            child.get(),
            allocation_remaining,
            max_output_bytes,
            depth,
        )?);
    }
    Ok(Value::Array(values))
}

fn decode_struct(
    api: &Arc<Api>,
    value: Handle,
    logical_type: Handle,
    allocation_remaining: &mut usize,
    max_output_bytes: usize,
    depth: usize,
) -> Result<Value> {
    let count = child_count(api, value)?;
    ensure!(
        logical_param_count(api, logical_type)? == count,
        "native DuckDB STRUCT metadata does not match its value"
    );
    charge(allocation_remaining, count, max_output_bytes)?;
    let mut fields = Map::new();
    for index in 0..count {
        let name = logical_param_name(api, logical_type, index)?;
        charge(allocation_remaining, name.len(), max_output_bytes)?;
        let child = child_value(api, value, index)?;
        let decoded = decode_owned_value(
            api,
            child.get(),
            allocation_remaining,
            max_output_bytes,
            depth,
        )?;
        fields.insert(name, decoded);
    }
    Ok(Value::Object(fields))
}

fn decode_map(
    api: &Arc<Api>,
    value: Handle,
    allocation_remaining: &mut usize,
    max_output_bytes: usize,
    depth: usize,
) -> Result<Value> {
    let children = child_count(api, value)?;
    ensure!(
        children % 2 == 0,
        "native DuckDB MAP has an odd child count"
    );
    let entries = children / 2;
    charge(allocation_remaining, entries, max_output_bytes)?;
    let mut map = Map::new();
    for index in 0..entries {
        let key = child_value(api, value, index * 2)?;
        let mut error = ptr::null_mut();
        let mut is_null = false;
        let code = unsafe { (api.value_is_null)(key.get(), &mut is_null, &mut error) };
        unsafe { api.check(code, &mut error, "inspect native DuckDB MAP key")? };
        ensure!(!is_null, "native DuckDB MAP contains a NULL key");
        let key = value_text(api, key.get(), allocation_remaining, max_output_bytes)?;
        let child = child_value(api, value, index * 2 + 1)?;
        let decoded = decode_owned_value(
            api,
            child.get(),
            allocation_remaining,
            max_output_bytes,
            depth,
        )?;
        map.insert(key, decoded);
    }
    Ok(Value::Object(map))
}

fn decode_union(
    api: &Arc<Api>,
    value: Handle,
    logical_type: Handle,
    allocation_remaining: &mut usize,
    max_output_bytes: usize,
    depth: usize,
) -> Result<Value> {
    ensure!(
        child_count(api, value)? == 2,
        "native DuckDB UNION does not contain a tag and active member"
    );
    let tag = child_value(api, value, 0)?;
    let tag = value_text_unaccounted(api, tag.get(), *allocation_remaining, max_output_bytes)?
        .parse::<usize>()
        .context("decode native DuckDB UNION tag")?;
    ensure!(
        tag < logical_param_count(api, logical_type)?,
        "native DuckDB UNION tag is outside its member metadata"
    );
    let name = logical_param_name(api, logical_type, tag)?;
    charge(allocation_remaining, 1 + name.len(), max_output_bytes)?;
    let child = child_value(api, value, 1)?;
    let decoded = decode_owned_value(
        api,
        child.get(),
        allocation_remaining,
        max_output_bytes,
        depth,
    )?;
    Ok(Value::Object(Map::from_iter([(name, decoded)])))
}

fn child_count(api: &Arc<Api>, value: Handle) -> Result<usize> {
    let mut error = ptr::null_mut();
    let mut count = 0;
    let code = unsafe { (api.value_get_child_count)(value, &mut count, &mut error) };
    unsafe { api.check(code, &mut error, "read native DuckDB value child count")? };
    Ok(count)
}

fn child_value(api: &Arc<Api>, value: Handle, index: usize) -> Result<OwnedHandle> {
    let mut error = ptr::null_mut();
    let mut child = ptr::null_mut();
    let code = unsafe { (api.value_get_child)(value, index, &mut child, &mut error) };
    unsafe { api.check(code, &mut error, "read native DuckDB child value")? };
    ensure!(!child.is_null(), "native DuckDB child value is null");
    // value_get_child returns an owned copy, independent of its parent value.
    Ok(OwnedHandle::new(Arc::clone(api), child, api.value_destroy))
}

fn logical_param_count(api: &Arc<Api>, logical_type: Handle) -> Result<usize> {
    let mut error = ptr::null_mut();
    let mut count = 0;
    let code = unsafe { (api.logical_type_get_param_count)(logical_type, &mut count, &mut error) };
    unsafe { api.check(code, &mut error, "read native DuckDB type parameter count")? };
    Ok(count)
}

fn logical_param_name(api: &Arc<Api>, logical_type: Handle, index: usize) -> Result<String> {
    let mut error = ptr::null_mut();
    let mut name = DuckStr {
        ptr: ptr::null(),
        len: 0,
    };
    let mut parameter = ptr::null_mut();
    let code = unsafe {
        (api.logical_type_get_param)(logical_type, index, &mut name, &mut parameter, &mut error)
    };
    unsafe { api.check(code, &mut error, "read native DuckDB type parameter")? };
    ensure!(!parameter.is_null(), "native DuckDB type parameter is null");
    // Parameter values are caller-owned; the name remains borrowed from logical_type.
    let _parameter = OwnedHandle::new(Arc::clone(api), parameter, api.value_destroy);
    copy_utf8(name, "native DuckDB type parameter name")
}

fn value_text(
    api: &Arc<Api>,
    value: Handle,
    allocation_remaining: &mut usize,
    max_output_bytes: usize,
) -> Result<String> {
    let text = value_text_unaccounted(api, value, *allocation_remaining, max_output_bytes)?;
    charge(allocation_remaining, text.len(), max_output_bytes)?;
    Ok(text)
}

fn value_text_unaccounted(
    api: &Arc<Api>,
    value: Handle,
    allocation_remaining: usize,
    max_output_bytes: usize,
) -> Result<String> {
    let mut error = ptr::null_mut();
    let mut length = 0;
    let code = unsafe { (api.value_to_string)(value, ptr::null_mut(), 0, &mut length, &mut error) };
    unsafe { api.check(code, &mut error, "size native DuckDB scalar text")? };
    ensure!(
        length <= allocation_remaining,
        "DuckDB output exceeded {max_output_bytes} bytes"
    );
    let capacity = length
        .checked_add(1)
        .context("native DuckDB scalar text length overflow")?;
    let mut bytes = vec![0_u8; capacity];
    let mut written = 0;
    let code = unsafe {
        (api.value_to_string)(
            value,
            bytes.as_mut_ptr().cast::<c_char>(),
            capacity,
            &mut written,
            &mut error,
        )
    };
    unsafe { api.check(code, &mut error, "render native DuckDB scalar text")? };
    ensure!(
        written == length,
        "native DuckDB scalar text length changed"
    );
    ensure!(
        bytes[length] == 0,
        "native DuckDB scalar text is not terminated"
    );
    bytes.truncate(length);
    String::from_utf8(bytes).context("native DuckDB scalar text is not UTF-8")
}

fn charge(remaining: &mut usize, amount: usize, max_output_bytes: usize) -> Result<()> {
    ensure!(
        amount <= *remaining,
        "DuckDB output exceeded {max_output_bytes} bytes"
    );
    *remaining -= amount;
    Ok(())
}

fn logical_is_json(api: &Arc<Api>, logical_type: Handle) -> Result<bool> {
    let mut name = DuckStr {
        ptr: ptr::null(),
        len: 0,
    };
    let mut error = ptr::null_mut();
    let code = unsafe { (api.logical_type_get_name)(logical_type, &mut name, &mut error) };
    unsafe { api.check(code, &mut error, "read native DuckDB type alias")? };
    if name.len != 4 || name.ptr.is_null() {
        return Ok(false);
    }
    // The name is borrowed from the still-live logical type; do not retain it.
    Ok(
        unsafe { std::slice::from_raw_parts(name.ptr.cast::<u8>(), name.len) }
            .eq_ignore_ascii_case(b"JSON"),
    )
}

fn finite_number(value: f64) -> Result<Value> {
    ensure!(
        value.is_finite(),
        "native DuckDB produced a non-finite JSON number"
    );
    let number = Number::from_f64(value).context("encode native DuckDB double")?;
    Ok(Value::Number(number))
}

struct BoundedRows {
    rows: Vec<Value>,
    encoded_bytes: usize,
    limit: usize,
}

impl BoundedRows {
    fn new(limit: usize) -> Result<Self> {
        ensure!(limit >= 2, "DuckDB output exceeded {limit} bytes");
        Ok(Self {
            rows: Vec::new(),
            encoded_bytes: 2,
            limit,
        })
    }

    fn remaining_for_row(&self) -> Result<usize> {
        let comma = usize::from(!self.rows.is_empty());
        ensure!(
            self.encoded_bytes.saturating_add(comma) <= self.limit,
            "DuckDB output exceeded {} bytes",
            self.limit
        );
        Ok(self.limit - self.encoded_bytes - comma)
    }

    fn push(&mut self, row: Value) -> Result<()> {
        let comma = usize::from(!self.rows.is_empty());
        ensure!(
            self.encoded_bytes.saturating_add(comma) <= self.limit,
            "DuckDB output exceeded {} bytes",
            self.limit
        );
        let remaining = self.limit - self.encoded_bytes - comma;
        let mut counter = CountingWriter {
            written: 0,
            limit: remaining,
        };
        serde_json::to_writer(&mut counter, &row).map_err(|error| {
            if error.is_io() {
                anyhow::anyhow!("DuckDB output exceeded {} bytes", self.limit)
            } else {
                anyhow::Error::new(error).context("encode native DuckDB JSON row")
            }
        })?;
        self.encoded_bytes += comma + counter.written;
        self.rows.push(row);
        Ok(())
    }
}

struct CountingWriter {
    written: usize,
    limit: usize,
}

impl Write for CountingWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.written) {
            return Err(io::Error::other("native DuckDB JSON output limit"));
        }
        self.written += bytes.len();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

struct RenderState {
    bytes: Vec<u8>,
    limit: usize,
    overflow: bool,
    panicked: bool,
}

fn render_explain(
    api: &Arc<Api>,
    result: &mut OwnedHandle,
    max_output_bytes: usize,
) -> Result<Value> {
    let mut state = RenderState {
        bytes: Vec::new(),
        limit: max_output_bytes,
        overflow: false,
        panicked: false,
    };
    let mut error = ptr::null_mut();
    let code = unsafe {
        (api.result_render_box)(
            result.slot(),
            0,
            0,
            0,
            DuckStr {
                ptr: ptr::null(),
                len: 0,
            },
            0,
            0,
            render_sink,
            (&mut state as *mut RenderState).cast(),
            &mut error,
        )
    };
    if state.overflow {
        if !error.is_null() {
            let _ = unsafe { (api.error_info_destroy)(&mut error) };
        }
        bail!("DuckDB output exceeded {max_output_bytes} bytes");
    }
    ensure!(!state.panicked, "panic in native DuckDB EXPLAIN renderer");
    unsafe { api.check(code, &mut error, "render native DuckDB EXPLAIN result")? };
    ensure!(
        result.is_null(),
        "native DuckDB EXPLAIN result was not consumed"
    );
    let plan = str::from_utf8(&state.bytes)
        .context("DuckDB plan is not UTF-8")?
        .trim_end();
    let mut rows = BoundedRows::new(max_output_bytes)?;
    rows.push(json!({ "plan": plan }))?;
    Ok(Value::Array(rows.rows))
}

extern "C" fn render_sink(text: DuckStr, user_data: *mut c_void, error: *mut Handle) {
    let outcome = catch_unwind(AssertUnwindSafe(|| {
        if user_data.is_null() {
            return Err("native DuckDB EXPLAIN render state is null".to_owned());
        }
        let state = unsafe { &mut *user_data.cast::<RenderState>() };
        let bytes = if text.len == 0 {
            &[][..]
        } else if text.ptr.is_null() {
            return Err("native DuckDB EXPLAIN text is null".to_owned());
        } else {
            unsafe { std::slice::from_raw_parts(text.ptr.cast::<u8>(), text.len) }
        };
        if bytes.len() > state.limit.saturating_sub(state.bytes.len()) {
            state.overflow = true;
            return Err("native DuckDB EXPLAIN output limit".to_owned());
        }
        state.bytes.extend_from_slice(bytes);
        Ok(())
    }));
    let message = match outcome {
        Ok(Ok(())) => return,
        Ok(Err(message)) => message,
        Err(_) => {
            if !user_data.is_null() {
                unsafe { (*user_data.cast::<RenderState>()).panicked = true };
            }
            "panic in native DuckDB EXPLAIN renderer".to_owned()
        }
    };
    if let Some(api) = ffi::callback_api() {
        unsafe { api.callback_error(error, &message) };
    }
}

fn copy_utf8(view: DuckStr, context: &str) -> Result<String> {
    if view.len == 0 {
        return Ok(String::new());
    }
    ensure!(!view.ptr.is_null(), "{context} is null");
    let bytes = unsafe { std::slice::from_raw_parts(view.ptr.cast::<u8>(), view.len) };
    Ok(str::from_utf8(bytes)
        .with_context(|| context.to_owned())?
        .to_owned())
}
