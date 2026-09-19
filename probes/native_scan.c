/* Stage A only: ABI scanner, projection, mutation, and ownership.
 * Excludes multi-chunk, mixed Parquet, cancellation, and private-database isolation. */
#include "duckdb_v2.h"
#include <dirent.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#define EXPECTED_VERSION "v2.0.0-alpha41533"
#define EXPECTED_PROJECTION ((1u << 1) | (1u << 2))
typedef struct { int64_t timestamp_us; double value; uint64_t sequence; } HotRow;
typedef struct { unsigned user, bind, global, local; } Counters;
typedef struct {
	HotRow *rows;
	idx_t count;
	Counters *counters;
	unsigned projection_mask, exec_calls;
} Owner;
typedef struct { Owner *owner; } BindState;
typedef struct { Owner *owner; idx_t cursor; } GlobalState;
typedef struct { Owner *owner; unsigned calls; } LocalState;
static duckdb_v2_str str_view(const char *text) {
	duckdb_v2_str result = {text, (idx_t)strlen(text)};
	return result;
}
static void fail(const char *message) {
	fprintf(stderr, "native_scan probe failed: %s\n", message);
	exit(1);
}
static void api_check(DUCKDB_V2_ERROR code, duckdb_v2_error_info_handle *error, const char *operation) {
	if (code == DUCKDB_V2_ERROR_NONE) return;
	duckdb_v2_str text = {0};
	if (error && *error) {
		(void)duckdb_v2_error_info_get_text(*error, &text);
	}
	fprintf(stderr, "native_scan probe failed: %s (%d): %.*s\n", operation, (int)code, (int)text.len,
	        text.ptr ? text.ptr : "");
	if (error) {
		(void)duckdb_v2_error_info_destroy(error);
	}
	exit(1);
}
static void plain_check(DUCKDB_V2_ERROR code, const char *operation) {
	if (code != DUCKDB_V2_ERROR_NONE) {
		fprintf(stderr, "native_scan probe failed: %s (%d)\n", operation, (int)code);
		exit(1);
	}
}
static void callback_error(duckdb_v2_error_info_handle *error, const char *message) {
	if (!error || !*error) return;
	(void)duckdb_v2_error_info_set_code(*error, DUCKDB_V2_ERROR_API);
	(void)duckdb_v2_error_info_set_text(*error, str_view(message));
}
static void user_destroy(void *data) {
	((Owner *)data)->counters->user++;
}
static void bind_destroy(void *data) {
	BindState *state = data;
	state->owner->counters->bind++;
	free(state);
}
static void global_destroy(void *data) {
	GlobalState *state = data;
	state->owner->counters->global++;
	free(state);
}
static void local_destroy(void *data) {
	LocalState *state = data;
	state->owner->counters->local++;
	free(state);
}
static void bind_scan(duckdb_v2_table_function_bind_info_handle info, duckdb_v2_context_handle context,
                      duckdb_v2_error_info_handle *error) {
	void *raw_owner = NULL;
	if (duckdb_v2_table_function_bind_get_user_data(info, &raw_owner, error) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	Owner *owner = raw_owner;
	const DUCKDB_V2_LOGICAL_TYPE_ID ids[] = {DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT,
	                                          DUCKDB_V2_LOGICAL_TYPE_ID_DOUBLE,
	                                          DUCKDB_V2_LOGICAL_TYPE_ID_UBIGINT};
	const char *names[] = {"timestamp_us", "value", "sequence"};
	for (idx_t i = 0; i < 3; i++) {
		duckdb_v2_logical_type_handle type = NULL;
		if (duckdb_v2_context_create_type_from_id(context, ids[i], NULL, NULL, 0, &type, error) !=
		    DUCKDB_V2_ERROR_NONE) {
			return;
		}
		duckdb_v2_identifier_t name = str_view(names[i]);
		DUCKDB_V2_ERROR code = duckdb_v2_table_function_bind_add_result_column(info, name, type, error);
		(void)duckdb_v2_logical_type_destroy(&type);
		if (code != DUCKDB_V2_ERROR_NONE) {
			return;
		}
	}
	BindState *state = calloc(1, sizeof(*state));
	if (!state) {
		callback_error(error, "allocate bind state");
		return;
	}
	state->owner = owner;
	duckdb_v2_opaque opaque = {state, bind_destroy, NULL};
	if (duckdb_v2_table_function_bind_set_bind_data(info, &opaque, error) != DUCKDB_V2_ERROR_NONE) {
		free(state);
		return;
	}
	(void)duckdb_v2_table_function_bind_set_cardinality(info, owner->count, true, error);
}
static void init_global(duckdb_v2_table_function_init_global_info_handle info, duckdb_v2_context_handle context,
                        duckdb_v2_error_info_handle *error) {
	(void)context;
	void *raw_bind = NULL;
	if (duckdb_v2_table_function_init_global_get_bind_data(info, &raw_bind, error) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	GlobalState *state = calloc(1, sizeof(*state));
	if (!state) {
		callback_error(error, "allocate global state");
		return;
	}
	state->owner = ((BindState *)raw_bind)->owner;
	duckdb_v2_opaque opaque = {state, global_destroy, NULL};
	if (duckdb_v2_table_function_init_global_set_global_state(info, &opaque, error) != DUCKDB_V2_ERROR_NONE) {
		free(state);
		return;
	}
	(void)duckdb_v2_table_function_init_global_set_max_threads(info, 1, error);
}
static void init_local(duckdb_v2_table_function_init_local_info_handle info, duckdb_v2_context_handle context,
                       duckdb_v2_error_info_handle *error) {
	(void)context;
	void *raw_global = NULL;
	if (duckdb_v2_table_function_init_local_get_global_state(info, &raw_global, error) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	LocalState *state = calloc(1, sizeof(*state));
	if (!state) {
		callback_error(error, "allocate local state");
		return;
	}
	state->owner = ((GlobalState *)raw_global)->owner;
	duckdb_v2_opaque opaque = {state, local_destroy, NULL};
	if (duckdb_v2_table_function_init_local_set_local_state(info, &opaque, error) != DUCKDB_V2_ERROR_NONE) {
		free(state);
	}
}
static void execute_scan(duckdb_v2_table_function_exec_info_handle info, duckdb_v2_context_handle context,
                         duckdb_v2_error_info_handle *error) {
	(void)context;
	void *raw_global = NULL;
	void *raw_local = NULL;
	duckdb_v2_data_chunk_handle output = NULL;
	idx_t column_count = 0;
	if (duckdb_v2_table_function_exec_get_global_state(info, &raw_global, error) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_exec_get_local_state(info, &raw_local, error) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_exec_get_output_chunk(info, &output, error) != DUCKDB_V2_ERROR_NONE ||
	    duckdb_v2_table_function_exec_get_column_count(info, &column_count, error) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	GlobalState *global = raw_global;
	LocalState *local = raw_local;
	Owner *owner = global->owner;
	owner->exec_calls++;
	local->calls++;
	if (global->cursor >= owner->count) return;
	if (column_count == 0) {
		callback_error(error, "aggregate unexpectedly projected zero columns");
		return;
	}
	for (idx_t out_index = 0; out_index < column_count; out_index++) {
		idx_t source_index = 0;
		duckdb_v2_vector_handle vector = NULL;
		void *data = NULL;
		if (duckdb_v2_table_function_exec_get_column_index(info, out_index, &source_index, error) !=
		        DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_data_chunk_get_vector(output, out_index, &vector, error) != DUCKDB_V2_ERROR_NONE ||
		    duckdb_v2_vector_get_data_mutable(vector, &data, error) != DUCKDB_V2_ERROR_NONE) {
			return;
		}
		owner->projection_mask |= 1u << source_index;
		for (idx_t row = 0; row < owner->count; row++) {
			if (source_index == 0) {
				((int64_t *)data)[row] = owner->rows[row].timestamp_us;
			} else if (source_index == 1) {
				((double *)data)[row] = owner->rows[row].value;
			} else if (source_index == 2) {
				((uint64_t *)data)[row] = owner->rows[row].sequence;
			} else {
				callback_error(error, "unknown projected column");
				return;
			}
		}
	}
	duckdb_v2_vector_handle first = NULL;
	if (duckdb_v2_data_chunk_get_vector(output, 0, &first, error) != DUCKDB_V2_ERROR_NONE) {
		return;
	}
	if (duckdb_v2_vector_set_size(first, owner->count, error) == DUCKDB_V2_ERROR_NONE) {
		global->cursor = owner->count;
	}
}
static idx_t physical_row(const duckdb_v2_vector_view *view, idx_t row) { return view->sel ? view->sel[row] : row; }
static void run_query(duckdb_v2_connection_handle connection, uint64_t expected_count, double expected_sum,
                      uint64_t expected_max) {
	const char *sql = "SELECT count(*)::UBIGINT, sum(value)::DOUBLE, max(sequence)::UBIGINT "
	                  "FROM native_hot_scan()";
	duckdb_v2_error_info_handle error = NULL;
	duckdb_v2_statement_iterator_handle iterator = NULL;
	duckdb_v2_sql_statement_handle statement = NULL;
	duckdb_v2_result_handle result = NULL;
	duckdb_v2_data_chunk_handle chunk = NULL;
	DUCKDB_V2_ERROR code = duckdb_v2_parse_sql(connection, sql, &iterator, &error);
	api_check(code, &error, "parse aggregate SQL");
	code = duckdb_v2_statement_iterator_next(iterator, &statement, &error);
	api_check(code, &error, "read aggregate statement");
	if (!statement) {
		fail("aggregate SQL produced no statement");
	}
	plain_check(duckdb_v2_statement_iterator_destroy(&iterator), "destroy statement iterator");
	code = duckdb_v2_statement_execute(connection, statement, NULL, NULL, 0, &result, &error);
	api_check(code, &error, "execute aggregate SQL");
	code = duckdb_v2_result_fetch_chunk(result, &chunk, &error);
	api_check(code, &error, "fetch aggregate chunk");
	if (!chunk) {
		fail("aggregate returned no row");
	}
	idx_t size = 0;
	code = duckdb_v2_data_chunk_get_size(chunk, &size, &error);
	api_check(code, &error, "read aggregate chunk size");
	if (size != 1) {
		fail("aggregate returned an unexpected row count");
	}
	duckdb_v2_vector_view views[3] = {0};
	for (idx_t i = 0; i < 3; i++) {
		duckdb_v2_vector_handle vector = NULL;
		code = duckdb_v2_data_chunk_get_vector(chunk, i, &vector, &error);
		api_check(code, &error, "read aggregate vector");
		code = duckdb_v2_vector_get_view(vector, &views[i], &error);
		api_check(code, &error, "view aggregate vector");
	}
	uint64_t count = ((const uint64_t *)views[0].data)[physical_row(&views[0], 0)];
	double sum = ((const double *)views[1].data)[physical_row(&views[1], 0)];
	uint64_t maximum = ((const uint64_t *)views[2].data)[physical_row(&views[2], 0)];
	if (count != expected_count || sum != expected_sum || maximum != expected_max) {
		fprintf(stderr, "unexpected aggregate: count=%" PRIu64 " sum=%.17g max=%" PRIu64 "\n", count, sum,
		        maximum);
		exit(1);
	}
	plain_check(duckdb_v2_data_chunk_destroy(&chunk), "destroy aggregate chunk");
	code = duckdb_v2_result_fetch_chunk(result, &chunk, &error);
	api_check(code, &error, "finish aggregate result");
	if (chunk) {
		fail("aggregate returned more than one chunk");
	}
	plain_check(duckdb_v2_result_destroy(&result), "destroy aggregate result");
	plain_check(duckdb_v2_sql_statement_destroy(&statement), "destroy aggregate statement");
}
static unsigned directory_entries(void) {
	DIR *directory = opendir(".");
	if (!directory) {
		fail("open current directory");
	}
	unsigned count = 0;
	struct dirent *entry;
	while ((entry = readdir(directory)) != NULL) {
		if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0) {
			count++;
		}
	}
	closedir(directory);
	return count;
}
int main(void) {
	unsigned files_before = directory_entries();
	duckdb_v2_error_info_handle error = NULL;
	duckdb_v2_str version = {0};
	DUCKDB_V2_ERROR code = duckdb_v2_library_version(&version, &error);
	api_check(code, &error, "read library version");
	if (version.len != strlen(EXPECTED_VERSION) || memcmp(version.ptr, EXPECTED_VERSION, version.len) != 0) {
		fail("linked DuckDB version does not match the pin");
	}
	HotRow rows[] = {{100, 1.5, 7}, {200, 2.25, 9}, {300, -0.75, 8}};
	Counters counters = {0};
	Owner owner = {rows, 3, &counters, 0, 0};
	duckdb_v2_environment_handle environment = NULL;
	duckdb_v2_database_handle database = NULL;
	duckdb_v2_connection_handle connection = NULL;
	duckdb_v2_table_function_handle function = NULL;
	code = duckdb_v2_create_environment(&environment, &error);
	api_check(code, &error, "create environment");
	code = duckdb_v2_open(environment, str_view(":memory:"), NULL, 0, &database, &error);
	api_check(code, &error, "open in-memory database");
	code = duckdb_v2_connect(database, &connection, &error);
	api_check(code, &error, "connect database");
	code = duckdb_v2_table_function_create_with_connection(connection, &function, &error);
	api_check(code, &error, "create table function");
	duckdb_v2_str name = str_view("native_hot_scan");
	code = duckdb_v2_table_function_set_name(function, &name, &error);
	api_check(code, &error, "name table function");
	duckdb_v2_opaque user_data = {&owner, user_destroy, NULL};
	code = duckdb_v2_table_function_set_user_data(function, &user_data, &error);
	api_check(code, &error, "set table function user data");
	code = duckdb_v2_table_function_set_bind_callback(function, bind_scan, &error);
	api_check(code, &error, "set bind callback");
	code = duckdb_v2_table_function_set_init_global_callback(function, init_global, &error);
	api_check(code, &error, "set global init callback");
	code = duckdb_v2_table_function_set_init_local_callback(function, init_local, &error);
	api_check(code, &error, "set local init callback");
	code = duckdb_v2_table_function_set_exec_callback(function, execute_scan, &error);
	api_check(code, &error, "set exec callback");
	code = duckdb_v2_table_function_set_projection_pushdown(function, true, &error);
	api_check(code, &error, "enable projection pushdown");
	code = duckdb_v2_table_function_register(function, &error);
	api_check(code, &error, "register table function");
	plain_check(duckdb_v2_table_function_destroy(&function), "destroy table function builder");
	run_query(connection, 3, 3.0, 9);
	if (owner.projection_mask != EXPECTED_PROJECTION) {
		fail("first query did not use exact projection pushdown");
	}
	owner.projection_mask = 0;
	rows[1].value = 12.25;
	rows[1].sequence = 42;
	run_query(connection, 3, 13.0, 42);
	if (owner.projection_mask != EXPECTED_PROJECTION) {
		fail("second query did not use exact projection pushdown");
	}
	if (counters.bind != 2 || counters.global != 2 || counters.local != 2 || counters.user != 0) {
		fail("query callback destructor counters are not exact");
	}
	plain_check(duckdb_v2_disconnect(&connection), "disconnect database");
	plain_check(duckdb_v2_close(&database), "close database");
	plain_check(duckdb_v2_destroy_environment(&environment), "destroy environment");
	if (counters.user != 1 || counters.bind != 2 || counters.global != 2 || counters.local != 2) {
		fail("final callback destructor counters are not exact");
	}
	if (directory_entries() != files_before) {
		fail("query created a file in the working directory");
	}
	printf("native_scan Stage A passed: version=%s projection=0x%x destructors=%u/%u/%u/%u exec_calls=%u\n",
	       EXPECTED_VERSION, EXPECTED_PROJECTION, counters.user, counters.bind, counters.global, counters.local,
	       owner.exec_calls);
	return 0;
}
