use anyhow::{Context, Result, bail, ensure};
use libloading::Library;
use sha2::{Digest, Sha256};
use std::ffi::{c_char, c_void};
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::{Arc, OnceLock};

pub(super) type Handle = *mut c_void;
pub(super) type ErrorCode = i32;
pub(super) type TypeId = i32;
pub(super) type Callback = extern "C" fn(Handle, Handle, *mut Handle);
pub(super) type OpaqueDestroy = extern "C" fn(*mut c_void);
pub(super) type TextSink = extern "C" fn(DuckStr, *mut c_void, *mut Handle);

pub(super) const ERROR_NONE: ErrorCode = 0;
pub(super) const ERROR_API: ErrorCode = 1;
pub(super) const TYPE_SQLNULL: TypeId = 1;
pub(super) const TYPE_BOOLEAN: TypeId = 10;
pub(super) const TYPE_TINYINT: TypeId = 11;
pub(super) const TYPE_SMALLINT: TypeId = 12;
pub(super) const TYPE_INTEGER: TypeId = 13;
pub(super) const TYPE_BIGINT: TypeId = 14;
pub(super) const TYPE_DATE: TypeId = 15;
pub(super) const TYPE_TIME: TypeId = 16;
pub(super) const TYPE_TIMESTAMP_SEC: TypeId = 17;
pub(super) const TYPE_TIMESTAMP_MS: TypeId = 18;
pub(super) const TYPE_TIMESTAMP: TypeId = 19;
pub(super) const TYPE_TIMESTAMP_NS: TypeId = 20;
pub(super) const TYPE_DECIMAL: TypeId = 21;
pub(super) const TYPE_FLOAT: TypeId = 22;
pub(super) const TYPE_DOUBLE: TypeId = 23;
pub(super) const TYPE_VARCHAR: TypeId = 25;
pub(super) const TYPE_BLOB: TypeId = 26;
pub(super) const TYPE_INTERVAL: TypeId = 27;
pub(super) const TYPE_UTINYINT: TypeId = 28;
pub(super) const TYPE_USMALLINT: TypeId = 29;
pub(super) const TYPE_UINTEGER: TypeId = 30;
pub(super) const TYPE_UBIGINT: TypeId = 31;
pub(super) const TYPE_TIMESTAMP_TZ: TypeId = 32;
pub(super) const TYPE_TIMESTAMP_TZ_NS: TypeId = 33;
pub(super) const TYPE_TIME_TZ: TypeId = 34;
pub(super) const TYPE_TIME_NS: TypeId = 35;
pub(super) const TYPE_BIT: TypeId = 36;
pub(super) const TYPE_BIGNUM: TypeId = 39;
pub(super) const TYPE_UHUGEINT: TypeId = 49;
pub(super) const TYPE_HUGEINT: TypeId = 50;
pub(super) const TYPE_UUID: TypeId = 54;
pub(super) const TYPE_GEOMETRY: TypeId = 60;
pub(super) const TYPE_STRUCT: TypeId = 100;
pub(super) const TYPE_LIST: TypeId = 101;
pub(super) const TYPE_MAP: TypeId = 102;
pub(super) const TYPE_ENUM: TypeId = 104;
pub(super) const TYPE_UNION: TypeId = 107;
pub(super) const TYPE_ARRAY: TypeId = 108;
pub(super) const TYPE_VARIANT: TypeId = 109;
pub(super) const TYPE_TUPLE: TypeId = 110;

const EXPECTED_VERSION: &str = "v2.0.0-alpha41533";
const EXPECTED_LINUX_LIBRARY_SHA256: &str =
    "69bdd44e0d2426e7ba44ed14644b54d8bd99a9703e5cbb4aec3f8beef40f817d";
pub(super) const HEADER_SHA256: &str =
    "62ad0df66b9f4193a657540d2429ba23cb32c41c5c98ea9f3f4e7a3b64e30544";

#[repr(C)]
#[derive(Clone, Copy)]
pub(super) struct DuckStr {
    pub ptr: *const c_char,
    pub len: usize,
}

impl DuckStr {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            ptr: bytes.as_ptr().cast(),
            len: bytes.len(),
        }
    }
}

#[repr(C)]
pub(super) struct DuckOpaque {
    pub ptr: *mut c_void,
    pub destroy: Option<OpaqueDestroy>,
    pub equals: Option<extern "C" fn(*mut c_void, *mut c_void) -> bool>,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub(super) struct VectorView {
    pub data: *const c_void,
    pub validity: *const u64,
    pub sel: *const u32,
    pub count: usize,
}

impl Default for VectorView {
    fn default() -> Self {
        Self {
            data: ptr::null(),
            validity: ptr::null(),
            sel: ptr::null(),
            count: 0,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
pub(super) struct HugeInt {
    pub lower: u64,
    pub upper: i64,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub(super) struct UHugeInt {
    pub lower: u64,
    pub upper: u64,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub(super) struct DuckBytesPointer {
    pub length: u32,
    pub prefix: [u8; 4],
    pub ptr: *mut u8,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub(super) struct DuckBytesInline {
    pub length: u32,
    pub inlined: [u8; 12],
}

#[repr(C)]
#[derive(Clone, Copy)]
pub(super) union DuckBytesValue {
    pub pointer: DuckBytesPointer,
    pub inlined: DuckBytesInline,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub(super) struct DuckBytes {
    pub value: DuckBytesValue,
}

impl DuckBytes {
    pub unsafe fn as_slice(&self) -> &[u8] {
        // The pinned header makes length the common first field of both union arms.
        let length = unsafe { self.value.inlined.length as usize };
        if length <= 12 {
            unsafe { &self.value.inlined.inlined[..length] }
        } else {
            unsafe { std::slice::from_raw_parts(self.value.pointer.ptr.cast_const(), length) }
        }
    }
}

type Destroy = unsafe extern "C" fn(*mut Handle) -> ErrorCode;

pub(super) struct OwnedHandle {
    _api: Arc<Api>,
    handle: Handle,
    destroy: Destroy,
}

impl OwnedHandle {
    pub fn new(api: Arc<Api>, handle: Handle, destroy: Destroy) -> Self {
        Self {
            _api: api,
            handle,
            destroy,
        }
    }

    pub fn get(&self) -> Handle {
        self.handle
    }

    pub fn slot(&mut self) -> *mut Handle {
        &mut self.handle
    }

    pub fn is_null(&self) -> bool {
        self.handle.is_null()
    }
}

impl Drop for OwnedHandle {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            // Every pinned v2 destroy/close function is null-safe and clears the slot.
            let _ = unsafe { (self.destroy)(&mut self.handle) };
        }
    }
}

#[allow(clippy::type_complexity)]
pub(super) struct Api {
    pub path: PathBuf,
    pub digest: String,
    pub version: String,
    pub error_info_get_text: unsafe extern "C" fn(Handle, *mut DuckStr) -> ErrorCode,
    pub error_info_set_code: unsafe extern "C" fn(Handle, ErrorCode) -> ErrorCode,
    pub error_info_set_text: unsafe extern "C" fn(Handle, DuckStr) -> ErrorCode,
    pub error_info_destroy: unsafe extern "C" fn(*mut Handle) -> ErrorCode,
    pub create_environment: unsafe extern "C" fn(*mut Handle, *mut Handle) -> ErrorCode,
    pub destroy_environment: Destroy,
    pub option_create:
        unsafe extern "C" fn(DuckStr, DuckStr, *mut Handle, *mut Handle) -> ErrorCode,
    pub option_destroy: Destroy,
    pub open: unsafe extern "C" fn(
        Handle,
        DuckStr,
        *mut Handle,
        usize,
        *mut Handle,
        *mut Handle,
    ) -> ErrorCode,
    pub close: Destroy,
    pub connect: unsafe extern "C" fn(Handle, *mut Handle, *mut Handle) -> ErrorCode,
    pub disconnect: Destroy,
    pub connection_interrupt: unsafe extern "C" fn(Handle, *mut Handle) -> ErrorCode,
    pub parse_sql:
        unsafe extern "C" fn(Handle, *const c_char, *mut Handle, *mut Handle) -> ErrorCode,
    pub statement_iterator_next:
        unsafe extern "C" fn(Handle, *mut Handle, *mut Handle) -> ErrorCode,
    pub statement_iterator_destroy: Destroy,
    pub sql_statement_destroy: Destroy,
    pub statement_execute: unsafe extern "C" fn(
        Handle,
        Handle,
        *const DuckStr,
        *const Handle,
        usize,
        *mut Handle,
        *mut Handle,
    ) -> ErrorCode,
    pub result_destroy: Destroy,
    pub result_fetch_chunk: unsafe extern "C" fn(Handle, *mut Handle, *mut Handle) -> ErrorCode,
    pub result_drain: unsafe extern "C" fn(Handle, *mut usize, *mut Handle) -> ErrorCode,
    pub result_get_schema: unsafe extern "C" fn(Handle, *mut Handle, *mut Handle) -> ErrorCode,
    pub result_render_box: unsafe extern "C" fn(
        *mut Handle,
        usize,
        usize,
        usize,
        DuckStr,
        usize,
        usize,
        TextSink,
        *mut c_void,
        *mut Handle,
    ) -> ErrorCode,
    pub schema_get_count: unsafe extern "C" fn(Handle, *mut usize, *mut Handle) -> ErrorCode,
    pub schema_get_field:
        unsafe extern "C" fn(Handle, usize, *mut DuckStr, *mut Handle, *mut Handle) -> ErrorCode,
    pub schema_destroy: Destroy,
    pub logical_type_get_id: unsafe extern "C" fn(Handle, *mut TypeId, *mut Handle) -> ErrorCode,
    pub logical_type_get_name: unsafe extern "C" fn(Handle, *mut DuckStr, *mut Handle) -> ErrorCode,
    pub logical_type_get_param_count:
        unsafe extern "C" fn(Handle, *mut usize, *mut Handle) -> ErrorCode,
    pub logical_type_get_param:
        unsafe extern "C" fn(Handle, usize, *mut DuckStr, *mut Handle, *mut Handle) -> ErrorCode,
    pub logical_type_destroy: Destroy,
    pub context_create_type_from_id: unsafe extern "C" fn(
        Handle,
        TypeId,
        *const DuckStr,
        *const Handle,
        usize,
        *mut Handle,
        *mut Handle,
    ) -> ErrorCode,
    pub data_chunk_destroy: Destroy,
    pub data_chunk_get_size: unsafe extern "C" fn(Handle, *mut usize, *mut Handle) -> ErrorCode,
    pub data_chunk_get_vector:
        unsafe extern "C" fn(Handle, usize, *mut Handle, *mut Handle) -> ErrorCode,
    pub vector_get_view: unsafe extern "C" fn(Handle, *mut VectorView, *mut Handle) -> ErrorCode,
    pub vector_get_value:
        unsafe extern "C" fn(Handle, usize, *mut Handle, *mut Handle) -> ErrorCode,
    pub vector_flatten: unsafe extern "C" fn(Handle, *mut Handle) -> ErrorCode,
    pub value_destroy: Destroy,
    pub value_is_null: unsafe extern "C" fn(Handle, *mut bool, *mut Handle) -> ErrorCode,
    pub value_get_double: unsafe extern "C" fn(Handle, *mut f64, *mut Handle) -> ErrorCode,
    pub value_get_logical_type: unsafe extern "C" fn(Handle, *mut Handle, *mut Handle) -> ErrorCode,
    pub value_get_child_count: unsafe extern "C" fn(Handle, *mut usize, *mut Handle) -> ErrorCode,
    pub value_get_child: unsafe extern "C" fn(Handle, usize, *mut Handle, *mut Handle) -> ErrorCode,
    pub value_to_string:
        unsafe extern "C" fn(Handle, *mut c_char, usize, *mut usize, *mut Handle) -> ErrorCode,
    pub vector_get_data_mutable:
        unsafe extern "C" fn(Handle, *mut *mut c_void, *mut Handle) -> ErrorCode,
    pub vector_set_size: unsafe extern "C" fn(Handle, usize, *mut Handle) -> ErrorCode,
    pub vector_flat_get_validity_mutable:
        unsafe extern "C" fn(Handle, *mut *mut u64, *mut Handle) -> ErrorCode,
    pub vector_get_arena: unsafe extern "C" fn(Handle, *mut Handle, *mut Handle) -> ErrorCode,
    pub arena_allocate: unsafe extern "C" fn(Handle, usize, *mut *mut u8, *mut Handle) -> ErrorCode,
    pub table_function_create_with_connection:
        unsafe extern "C" fn(Handle, *mut Handle, *mut Handle) -> ErrorCode,
    pub table_function_set_name:
        unsafe extern "C" fn(Handle, *mut DuckStr, *mut Handle) -> ErrorCode,
    pub table_function_set_user_data:
        unsafe extern "C" fn(Handle, *mut DuckOpaque, *mut Handle) -> ErrorCode,
    pub table_function_set_bind_callback:
        unsafe extern "C" fn(Handle, Callback, *mut Handle) -> ErrorCode,
    pub table_function_set_init_global_callback:
        unsafe extern "C" fn(Handle, Callback, *mut Handle) -> ErrorCode,
    pub table_function_set_init_local_callback:
        unsafe extern "C" fn(Handle, Callback, *mut Handle) -> ErrorCode,
    pub table_function_set_exec_callback:
        unsafe extern "C" fn(Handle, Callback, *mut Handle) -> ErrorCode,
    pub table_function_set_projection_pushdown:
        unsafe extern "C" fn(Handle, bool, *mut Handle) -> ErrorCode,
    pub table_function_register: unsafe extern "C" fn(Handle, *mut Handle) -> ErrorCode,
    pub table_function_destroy: Destroy,
    pub table_function_bind_get_user_data:
        unsafe extern "C" fn(Handle, *mut *mut c_void, *mut Handle) -> ErrorCode,
    pub table_function_bind_set_bind_data:
        unsafe extern "C" fn(Handle, *mut DuckOpaque, *mut Handle) -> ErrorCode,
    pub table_function_bind_add_result_column:
        unsafe extern "C" fn(Handle, DuckStr, Handle, *mut Handle) -> ErrorCode,
    pub table_function_bind_set_cardinality:
        unsafe extern "C" fn(Handle, usize, bool, *mut Handle) -> ErrorCode,
    pub table_function_init_global_get_bind_data:
        unsafe extern "C" fn(Handle, *mut *mut c_void, *mut Handle) -> ErrorCode,
    pub table_function_init_global_set_global_state:
        unsafe extern "C" fn(Handle, *mut DuckOpaque, *mut Handle) -> ErrorCode,
    pub table_function_init_global_set_max_threads:
        unsafe extern "C" fn(Handle, usize, *mut Handle) -> ErrorCode,
    pub table_function_init_local_get_global_state:
        unsafe extern "C" fn(Handle, *mut *mut c_void, *mut Handle) -> ErrorCode,
    pub table_function_init_local_set_local_state:
        unsafe extern "C" fn(Handle, *mut DuckOpaque, *mut Handle) -> ErrorCode,
    pub table_function_exec_get_global_state:
        unsafe extern "C" fn(Handle, *mut *mut c_void, *mut Handle) -> ErrorCode,
    pub table_function_exec_get_local_state:
        unsafe extern "C" fn(Handle, *mut *mut c_void, *mut Handle) -> ErrorCode,
    pub table_function_exec_get_output_chunk:
        unsafe extern "C" fn(Handle, *mut Handle, *mut Handle) -> ErrorCode,
    pub table_function_exec_get_column_count:
        unsafe extern "C" fn(Handle, *mut usize, *mut Handle) -> ErrorCode,
    pub table_function_exec_get_column_index:
        unsafe extern "C" fn(Handle, usize, *mut usize, *mut Handle) -> ErrorCode,
    _library: Library,
}

// Function pointers and the process-pinned Library are immutable after loading.
unsafe impl Send for Api {}
unsafe impl Sync for Api {}

impl Api {
    unsafe fn load(path: &Path, digest: String) -> Result<Arc<Self>> {
        let library = unsafe { Library::new(path) }
            .with_context(|| format!("load pinned DuckDB v2 library {}", path.display()))?;
        let post_load_digest = sha256_file(path)?;
        ensure!(
            post_load_digest == digest,
            "DuckDB native library changed while it was being loaded"
        );
        macro_rules! sym {
            ($name:literal, $ty:ty) => {{
                let symbol = unsafe { library.get::<$ty>(concat!($name, "\0").as_bytes()) }
                    .with_context(|| format!("load required DuckDB v2 symbol {}", $name))?;
                *symbol
            }};
        }

        let library_version = sym!(
            "duckdb_v2_library_version",
            unsafe extern "C" fn(*mut DuckStr, *mut Handle) -> ErrorCode
        );
        let error_info_get_text = sym!(
            "duckdb_v2_error_info_get_text",
            unsafe extern "C" fn(Handle, *mut DuckStr) -> ErrorCode
        );
        let error_info_set_code = sym!(
            "duckdb_v2_error_info_set_code",
            unsafe extern "C" fn(Handle, ErrorCode) -> ErrorCode
        );
        let error_info_set_text = sym!(
            "duckdb_v2_error_info_set_text",
            unsafe extern "C" fn(Handle, DuckStr) -> ErrorCode
        );
        let error_info_destroy = sym!("duckdb_v2_error_info_destroy", Destroy);
        let create_environment = sym!(
            "duckdb_v2_create_environment",
            unsafe extern "C" fn(*mut Handle, *mut Handle) -> ErrorCode
        );
        let destroy_environment = sym!("duckdb_v2_destroy_environment", Destroy);
        let option_create = sym!(
            "duckdb_v2_option_create",
            unsafe extern "C" fn(DuckStr, DuckStr, *mut Handle, *mut Handle) -> ErrorCode
        );
        let option_destroy = sym!("duckdb_v2_option_destroy", Destroy);
        let open = sym!(
            "duckdb_v2_open",
            unsafe extern "C" fn(
                Handle,
                DuckStr,
                *mut Handle,
                usize,
                *mut Handle,
                *mut Handle,
            ) -> ErrorCode
        );
        let close = sym!("duckdb_v2_close", Destroy);
        let connect = sym!(
            "duckdb_v2_connect",
            unsafe extern "C" fn(Handle, *mut Handle, *mut Handle) -> ErrorCode
        );
        let disconnect = sym!("duckdb_v2_disconnect", Destroy);
        let connection_interrupt = sym!(
            "duckdb_v2_connection_interrupt",
            unsafe extern "C" fn(Handle, *mut Handle) -> ErrorCode
        );
        let parse_sql = sym!(
            "duckdb_v2_parse_sql",
            unsafe extern "C" fn(Handle, *const c_char, *mut Handle, *mut Handle) -> ErrorCode
        );
        let statement_iterator_next = sym!(
            "duckdb_v2_statement_iterator_next",
            unsafe extern "C" fn(Handle, *mut Handle, *mut Handle) -> ErrorCode
        );
        let statement_iterator_destroy = sym!("duckdb_v2_statement_iterator_destroy", Destroy);
        let sql_statement_destroy = sym!("duckdb_v2_sql_statement_destroy", Destroy);
        let statement_execute = sym!(
            "duckdb_v2_statement_execute",
            unsafe extern "C" fn(
                Handle,
                Handle,
                *const DuckStr,
                *const Handle,
                usize,
                *mut Handle,
                *mut Handle,
            ) -> ErrorCode
        );
        let result_destroy = sym!("duckdb_v2_result_destroy", Destroy);
        let result_fetch_chunk = sym!(
            "duckdb_v2_result_fetch_chunk",
            unsafe extern "C" fn(Handle, *mut Handle, *mut Handle) -> ErrorCode
        );
        let result_drain = sym!(
            "duckdb_v2_result_drain",
            unsafe extern "C" fn(Handle, *mut usize, *mut Handle) -> ErrorCode
        );
        let result_get_schema = sym!(
            "duckdb_v2_result_get_schema",
            unsafe extern "C" fn(Handle, *mut Handle, *mut Handle) -> ErrorCode
        );
        let result_render_box = sym!(
            "duckdb_v2_result_render_box",
            unsafe extern "C" fn(
                *mut Handle,
                usize,
                usize,
                usize,
                DuckStr,
                usize,
                usize,
                TextSink,
                *mut c_void,
                *mut Handle,
            ) -> ErrorCode
        );
        let schema_get_count = sym!(
            "duckdb_v2_schema_get_count",
            unsafe extern "C" fn(Handle, *mut usize, *mut Handle) -> ErrorCode
        );
        let schema_get_field = sym!(
            "duckdb_v2_schema_get_field",
            unsafe extern "C" fn(
                Handle,
                usize,
                *mut DuckStr,
                *mut Handle,
                *mut Handle,
            ) -> ErrorCode
        );
        let schema_destroy = sym!("duckdb_v2_schema_destroy", Destroy);
        let logical_type_get_id = sym!(
            "duckdb_v2_logical_type_get_id",
            unsafe extern "C" fn(Handle, *mut TypeId, *mut Handle) -> ErrorCode
        );
        let logical_type_get_name = sym!(
            "duckdb_v2_logical_type_get_name",
            unsafe extern "C" fn(Handle, *mut DuckStr, *mut Handle) -> ErrorCode
        );
        let logical_type_get_param_count = sym!(
            "duckdb_v2_logical_type_get_param_count",
            unsafe extern "C" fn(Handle, *mut usize, *mut Handle) -> ErrorCode
        );
        let logical_type_get_param = sym!(
            "duckdb_v2_logical_type_get_param",
            unsafe extern "C" fn(
                Handle,
                usize,
                *mut DuckStr,
                *mut Handle,
                *mut Handle,
            ) -> ErrorCode
        );
        let logical_type_destroy = sym!("duckdb_v2_logical_type_destroy", Destroy);
        let context_create_type_from_id = sym!(
            "duckdb_v2_context_create_type_from_id",
            unsafe extern "C" fn(
                Handle,
                TypeId,
                *const DuckStr,
                *const Handle,
                usize,
                *mut Handle,
                *mut Handle,
            ) -> ErrorCode
        );
        let data_chunk_destroy = sym!("duckdb_v2_data_chunk_destroy", Destroy);
        let data_chunk_get_size = sym!(
            "duckdb_v2_data_chunk_get_size",
            unsafe extern "C" fn(Handle, *mut usize, *mut Handle) -> ErrorCode
        );
        let data_chunk_get_vector = sym!(
            "duckdb_v2_data_chunk_get_vector",
            unsafe extern "C" fn(Handle, usize, *mut Handle, *mut Handle) -> ErrorCode
        );
        let vector_get_view = sym!(
            "duckdb_v2_vector_get_view",
            unsafe extern "C" fn(Handle, *mut VectorView, *mut Handle) -> ErrorCode
        );
        let vector_get_value = sym!(
            "duckdb_v2_vector_get_value",
            unsafe extern "C" fn(Handle, usize, *mut Handle, *mut Handle) -> ErrorCode
        );
        let vector_flatten = sym!(
            "duckdb_v2_vector_flatten",
            unsafe extern "C" fn(Handle, *mut Handle) -> ErrorCode
        );
        let value_destroy = sym!("duckdb_v2_value_destroy", Destroy);
        let value_is_null = sym!(
            "duckdb_v2_value_is_null",
            unsafe extern "C" fn(Handle, *mut bool, *mut Handle) -> ErrorCode
        );
        let value_get_double = sym!(
            "duckdb_v2_value_get_double",
            unsafe extern "C" fn(Handle, *mut f64, *mut Handle) -> ErrorCode
        );
        let value_get_logical_type = sym!(
            "duckdb_v2_value_get_logical_type",
            unsafe extern "C" fn(Handle, *mut Handle, *mut Handle) -> ErrorCode
        );
        let value_get_child_count = sym!(
            "duckdb_v2_value_get_child_count",
            unsafe extern "C" fn(Handle, *mut usize, *mut Handle) -> ErrorCode
        );
        let value_get_child = sym!(
            "duckdb_v2_value_get_child",
            unsafe extern "C" fn(Handle, usize, *mut Handle, *mut Handle) -> ErrorCode
        );
        let value_to_string = sym!(
            "duckdb_v2_value_to_string",
            unsafe extern "C" fn(Handle, *mut c_char, usize, *mut usize, *mut Handle) -> ErrorCode
        );
        let vector_get_data_mutable = sym!(
            "duckdb_v2_vector_get_data_mutable",
            unsafe extern "C" fn(Handle, *mut *mut c_void, *mut Handle) -> ErrorCode
        );
        let vector_set_size = sym!(
            "duckdb_v2_vector_set_size",
            unsafe extern "C" fn(Handle, usize, *mut Handle) -> ErrorCode
        );
        let vector_flat_get_validity_mutable = sym!(
            "duckdb_v2_vector_flat_get_validity_mutable",
            unsafe extern "C" fn(Handle, *mut *mut u64, *mut Handle) -> ErrorCode
        );
        let vector_get_arena = sym!(
            "duckdb_v2_vector_get_arena",
            unsafe extern "C" fn(Handle, *mut Handle, *mut Handle) -> ErrorCode
        );
        let arena_allocate = sym!(
            "duckdb_v2_arena_allocate",
            unsafe extern "C" fn(Handle, usize, *mut *mut u8, *mut Handle) -> ErrorCode
        );
        let table_function_create_with_connection = sym!(
            "duckdb_v2_table_function_create_with_connection",
            unsafe extern "C" fn(Handle, *mut Handle, *mut Handle) -> ErrorCode
        );
        let table_function_set_name = sym!(
            "duckdb_v2_table_function_set_name",
            unsafe extern "C" fn(Handle, *mut DuckStr, *mut Handle) -> ErrorCode
        );
        let table_function_set_user_data = sym!(
            "duckdb_v2_table_function_set_user_data",
            unsafe extern "C" fn(Handle, *mut DuckOpaque, *mut Handle) -> ErrorCode
        );
        let table_function_set_bind_callback = sym!(
            "duckdb_v2_table_function_set_bind_callback",
            unsafe extern "C" fn(Handle, Callback, *mut Handle) -> ErrorCode
        );
        let table_function_set_init_global_callback = sym!(
            "duckdb_v2_table_function_set_init_global_callback",
            unsafe extern "C" fn(Handle, Callback, *mut Handle) -> ErrorCode
        );
        let table_function_set_init_local_callback = sym!(
            "duckdb_v2_table_function_set_init_local_callback",
            unsafe extern "C" fn(Handle, Callback, *mut Handle) -> ErrorCode
        );
        let table_function_set_exec_callback = sym!(
            "duckdb_v2_table_function_set_exec_callback",
            unsafe extern "C" fn(Handle, Callback, *mut Handle) -> ErrorCode
        );
        let table_function_set_projection_pushdown = sym!(
            "duckdb_v2_table_function_set_projection_pushdown",
            unsafe extern "C" fn(Handle, bool, *mut Handle) -> ErrorCode
        );
        let table_function_register = sym!(
            "duckdb_v2_table_function_register",
            unsafe extern "C" fn(Handle, *mut Handle) -> ErrorCode
        );
        let table_function_destroy = sym!("duckdb_v2_table_function_destroy", Destroy);
        let table_function_bind_get_user_data = sym!(
            "duckdb_v2_table_function_bind_get_user_data",
            unsafe extern "C" fn(Handle, *mut *mut c_void, *mut Handle) -> ErrorCode
        );
        let table_function_bind_set_bind_data = sym!(
            "duckdb_v2_table_function_bind_set_bind_data",
            unsafe extern "C" fn(Handle, *mut DuckOpaque, *mut Handle) -> ErrorCode
        );
        let table_function_bind_add_result_column = sym!(
            "duckdb_v2_table_function_bind_add_result_column",
            unsafe extern "C" fn(Handle, DuckStr, Handle, *mut Handle) -> ErrorCode
        );
        let table_function_bind_set_cardinality = sym!(
            "duckdb_v2_table_function_bind_set_cardinality",
            unsafe extern "C" fn(Handle, usize, bool, *mut Handle) -> ErrorCode
        );
        let table_function_init_global_get_bind_data = sym!(
            "duckdb_v2_table_function_init_global_get_bind_data",
            unsafe extern "C" fn(Handle, *mut *mut c_void, *mut Handle) -> ErrorCode
        );
        let table_function_init_global_set_global_state = sym!(
            "duckdb_v2_table_function_init_global_set_global_state",
            unsafe extern "C" fn(Handle, *mut DuckOpaque, *mut Handle) -> ErrorCode
        );
        let table_function_init_global_set_max_threads = sym!(
            "duckdb_v2_table_function_init_global_set_max_threads",
            unsafe extern "C" fn(Handle, usize, *mut Handle) -> ErrorCode
        );
        let table_function_init_local_get_global_state = sym!(
            "duckdb_v2_table_function_init_local_get_global_state",
            unsafe extern "C" fn(Handle, *mut *mut c_void, *mut Handle) -> ErrorCode
        );
        let table_function_init_local_set_local_state = sym!(
            "duckdb_v2_table_function_init_local_set_local_state",
            unsafe extern "C" fn(Handle, *mut DuckOpaque, *mut Handle) -> ErrorCode
        );
        let table_function_exec_get_global_state = sym!(
            "duckdb_v2_table_function_exec_get_global_state",
            unsafe extern "C" fn(Handle, *mut *mut c_void, *mut Handle) -> ErrorCode
        );
        let table_function_exec_get_local_state = sym!(
            "duckdb_v2_table_function_exec_get_local_state",
            unsafe extern "C" fn(Handle, *mut *mut c_void, *mut Handle) -> ErrorCode
        );
        let table_function_exec_get_output_chunk = sym!(
            "duckdb_v2_table_function_exec_get_output_chunk",
            unsafe extern "C" fn(Handle, *mut Handle, *mut Handle) -> ErrorCode
        );
        let table_function_exec_get_column_count = sym!(
            "duckdb_v2_table_function_exec_get_column_count",
            unsafe extern "C" fn(Handle, *mut usize, *mut Handle) -> ErrorCode
        );
        let table_function_exec_get_column_index = sym!(
            "duckdb_v2_table_function_exec_get_column_index",
            unsafe extern "C" fn(Handle, usize, *mut usize, *mut Handle) -> ErrorCode
        );

        let mut version_view = DuckStr {
            ptr: ptr::null(),
            len: 0,
        };
        let mut error = ptr::null_mut();
        let code = unsafe { library_version(&mut version_view, &mut error) };
        if code != ERROR_NONE {
            let message =
                unsafe { take_error_raw(error_info_get_text, error_info_destroy, &mut error) };
            bail!("read DuckDB v2 library version ({code}): {message}");
        }
        let version_bytes = if version_view.len == 0 {
            &[][..]
        } else {
            unsafe { std::slice::from_raw_parts(version_view.ptr.cast::<u8>(), version_view.len) }
        };
        let version = std::str::from_utf8(version_bytes)
            .context("DuckDB v2 library version is not UTF-8")?
            .to_owned();
        ensure!(
            version == EXPECTED_VERSION,
            "DuckDB native library version mismatch: expected {EXPECTED_VERSION}, found {version:?}"
        );

        Ok(Arc::new(Self {
            path: path.to_owned(),
            digest,
            version,
            error_info_get_text,
            error_info_set_code,
            error_info_set_text,
            error_info_destroy,
            create_environment,
            destroy_environment,
            option_create,
            option_destroy,
            open,
            close,
            connect,
            disconnect,
            connection_interrupt,
            parse_sql,
            statement_iterator_next,
            statement_iterator_destroy,
            sql_statement_destroy,
            statement_execute,
            result_destroy,
            result_fetch_chunk,
            result_drain,
            result_get_schema,
            result_render_box,
            schema_get_count,
            schema_get_field,
            schema_destroy,
            logical_type_get_id,
            logical_type_get_name,
            logical_type_get_param_count,
            logical_type_get_param,
            logical_type_destroy,
            context_create_type_from_id,
            data_chunk_destroy,
            data_chunk_get_size,
            data_chunk_get_vector,
            vector_get_view,
            vector_get_value,
            vector_flatten,
            value_destroy,
            value_is_null,
            value_get_double,
            value_get_logical_type,
            value_get_child_count,
            value_get_child,
            value_to_string,
            vector_get_data_mutable,
            vector_set_size,
            vector_flat_get_validity_mutable,
            vector_get_arena,
            arena_allocate,
            table_function_create_with_connection,
            table_function_set_name,
            table_function_set_user_data,
            table_function_set_bind_callback,
            table_function_set_init_global_callback,
            table_function_set_init_local_callback,
            table_function_set_exec_callback,
            table_function_set_projection_pushdown,
            table_function_register,
            table_function_destroy,
            table_function_bind_get_user_data,
            table_function_bind_set_bind_data,
            table_function_bind_add_result_column,
            table_function_bind_set_cardinality,
            table_function_init_global_get_bind_data,
            table_function_init_global_set_global_state,
            table_function_init_global_set_max_threads,
            table_function_init_local_get_global_state,
            table_function_init_local_set_local_state,
            table_function_exec_get_global_state,
            table_function_exec_get_local_state,
            table_function_exec_get_output_chunk,
            table_function_exec_get_column_count,
            table_function_exec_get_column_index,
            _library: library,
        }))
    }

    pub unsafe fn check(&self, code: ErrorCode, error: &mut Handle, operation: &str) -> Result<()> {
        if code == ERROR_NONE {
            if !error.is_null() {
                let _ = unsafe { (self.error_info_destroy)(error) };
            }
            return Ok(());
        }
        let message =
            unsafe { take_error_raw(self.error_info_get_text, self.error_info_destroy, error) };
        bail!("{operation} ({code}): {message}")
    }

    pub unsafe fn callback_error(&self, slot: *mut Handle, message: &str) {
        if slot.is_null() {
            return;
        }
        let info = unsafe { *slot };
        if info.is_null() {
            return;
        }
        let _ = unsafe { (self.error_info_set_code)(info, ERROR_API) };
        let text = DuckStr::from_bytes(message.as_bytes());
        let _ = unsafe { (self.error_info_set_text)(info, text) };
    }
}

static API: OnceLock<Arc<Api>> = OnceLock::new();

pub(super) fn load(path: &Path) -> Result<Arc<Api>> {
    ensure!(
        path.is_absolute(),
        "DuckDB native library path must be absolute"
    );
    let resolved = path
        .canonicalize()
        .with_context(|| format!("resolve DuckDB native library {}", path.display()))?;
    let digest = sha256_file(&resolved)?;
    #[cfg(target_os = "linux")]
    ensure!(
        digest == EXPECTED_LINUX_LIBRARY_SHA256,
        "DuckDB native library digest mismatch: expected {EXPECTED_LINUX_LIBRARY_SHA256}, found {digest}"
    );
    #[cfg(not(target_os = "linux"))]
    bail!("the pinned DuckDB native adapter is qualified only for the reviewed Linux library");

    if let Some(api) = API.get() {
        ensure!(
            api.path == resolved && api.digest == digest,
            "a different DuckDB native library is already loaded"
        );
        return Ok(Arc::clone(api));
    }
    let candidate = unsafe { Api::load(&resolved, digest)? };
    match API.set(Arc::clone(&candidate)) {
        Ok(()) => Ok(candidate),
        Err(_) => {
            let api = API
                .get()
                .context("DuckDB callback API initialization raced")?;
            ensure!(
                api.path == resolved && api.digest == candidate.digest,
                "a different DuckDB native library won initialization"
            );
            Ok(Arc::clone(api))
        }
    }
}

pub(super) fn callback_api() -> Option<&'static Arc<Api>> {
    API.get()
}

unsafe fn take_error_raw(
    get_text: unsafe extern "C" fn(Handle, *mut DuckStr) -> ErrorCode,
    destroy: Destroy,
    error: &mut Handle,
) -> String {
    if error.is_null() {
        return "no DuckDB error detail".to_owned();
    }
    let mut text = DuckStr {
        ptr: ptr::null(),
        len: 0,
    };
    let _ = unsafe { get_text(*error, &mut text) };
    let message = if text.ptr.is_null() || text.len == 0 {
        "no DuckDB error detail".to_owned()
    } else {
        let bytes = unsafe { std::slice::from_raw_parts(text.ptr.cast::<u8>(), text.len) };
        String::from_utf8_lossy(bytes).into_owned()
    };
    let _ = unsafe { destroy(error) };
    message
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut file = File::open(path)
        .with_context(|| format!("open DuckDB native library {}", path.display()))?;
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .with_context(|| format!("hash DuckDB native library {}", path.display()))?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(hash
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}
