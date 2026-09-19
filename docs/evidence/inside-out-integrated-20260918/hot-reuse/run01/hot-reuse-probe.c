#define _POSIX_C_SOURCE 200809L
#include "duckdb_v2.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

static duckdb_v2_str view(const char *s) {
    duckdb_v2_str v = {s, (idx_t)strlen(s)};
    return v;
}
static double now_ms(void) {
    struct timespec t;
    if (clock_gettime(CLOCK_MONOTONIC, &t)) exit(2);
    return 1000.0 * (double)t.tv_sec + (double)t.tv_nsec / 1000000.0;
}
static void checked(DUCKDB_V2_ERROR code, duckdb_v2_error_info_handle *error) {
    if (code == DUCKDB_V2_ERROR_NONE) return;
    duckdb_v2_str text = {0};
    if (error && *error) (void)duckdb_v2_error_info_get_text(*error, &text);
    fprintf(stderr, "native API error %d: %.*s\n", (int)code, (int)text.len, text.ptr ? text.ptr : "");
    exit(1);
}
static int query(duckdb_v2_connection_handle conn, const char *sql, int64_t *scalar, char *message, size_t capacity) {
    duckdb_v2_error_info_handle error = NULL;
    duckdb_v2_statement_iterator_handle iterator = NULL;
    duckdb_v2_sql_statement_handle statement = NULL, extra = NULL;
    duckdb_v2_result_handle result = NULL;
    duckdb_v2_data_chunk_handle chunk = NULL;
    duckdb_v2_value_handle value = NULL;
    DUCKDB_V2_ERROR code;
    size_t cells = 0;
#define TRY(call) do { code = (call); if (code != DUCKDB_V2_ERROR_NONE) goto done; } while (0)
    TRY(duckdb_v2_parse_sql(conn, sql, &iterator, &error));
    TRY(duckdb_v2_statement_iterator_next(iterator, &statement, &error));
    if (!statement) exit(2);
    TRY(duckdb_v2_statement_iterator_next(iterator, &extra, &error));
    if (extra) exit(2);
    TRY(duckdb_v2_statement_execute(conn, statement, NULL, NULL, 0, &result, &error));
    for (;;) {
        TRY(duckdb_v2_result_fetch_chunk(result, &chunk, &error));
        if (!chunk) break;
        if (scalar) {
            idx_t rows = 0, columns = 0;
            duckdb_v2_vector_handle vector = NULL;
            TRY(duckdb_v2_data_chunk_get_size(chunk, &rows, &error));
            TRY(duckdb_v2_data_chunk_get_vector_count(chunk, &columns, &error));
            if (rows != 1 || columns != 1 || cells++) exit(2);
            TRY(duckdb_v2_data_chunk_get_vector(chunk, 0, &vector, &error));
            TRY(duckdb_v2_vector_get_value(vector, 0, &value, &error));
            TRY(duckdb_v2_value_get_bigint(value, scalar, &error));
            checked(duckdb_v2_value_destroy(&value), NULL);
        }
        checked(duckdb_v2_data_chunk_destroy(&chunk), NULL);
    }
    if (scalar && cells != 1) exit(2);
done:
    if (capacity) message[0] = 0;
    if (error && capacity) {
        duckdb_v2_str text = {0};
        checked(duckdb_v2_error_info_get_text(error, &text), NULL);
        size_t n = text.len < capacity - 1 ? (size_t)text.len : capacity - 1;
        memcpy(message, text.ptr, n);
        message[n] = 0;
    }
    checked(duckdb_v2_value_destroy(&value), NULL);
    checked(duckdb_v2_data_chunk_destroy(&chunk), NULL);
    checked(duckdb_v2_result_destroy(&result), NULL);
    checked(duckdb_v2_sql_statement_destroy(&extra), NULL);
    checked(duckdb_v2_sql_statement_destroy(&statement), NULL);
    checked(duckdb_v2_statement_iterator_destroy(&iterator), NULL);
    checked(duckdb_v2_error_info_destroy(&error), NULL);
    return (int)code;
#undef TRY
}
static void ok(duckdb_v2_connection_handle conn, const char *sql) {
    char message[2048];
    int code = query(conn, sql, NULL, message, sizeof(message));
    if (code) { fprintf(stderr, "SQL failed: %s\n%s\n", sql, message); exit(1); }
}
static void equal(duckdb_v2_connection_handle conn, const char *name, const char *sql, int64_t expected) {
    char message[2048];
    int64_t actual = -1;
    int code = query(conn, sql, &actual, message, sizeof(message));
    if (code || actual != expected) { fprintf(stderr, "%s: code=%d actual=%lld expected=%lld %s\n", name, code, (long long)actual, (long long)expected, message); exit(1); }
    printf("{\"case\":\"%s\",\"passed\":true,\"value\":%lld}\n", name, (long long)actual);
}
static void denied(duckdb_v2_connection_handle conn, const char *name, const char *sql) {
    char message[2048];
    int code = query(conn, sql, NULL, message, sizeof(message));
    int reason = strstr(message, "disabled") || strstr(message, "locked") || strstr(message, "Permission") || strstr(message, "permission") || strstr(message, "not allowed");
    if (!code || !reason) { fprintf(stderr, "%s: not a witnessed authority rejection: code=%d %s\n", name, code, message); exit(1); }
    printf("{\"case\":\"%s\",\"passed\":true,\"error_code\":%d,\"authority_reason\":true}\n", name, code);
}
int main(void) {
    duckdb_v2_error_info_handle error = NULL;
    duckdb_v2_str version = {0};
    checked(duckdb_v2_library_version(&version, &error), &error);
    if (version.len != strlen("v2.0.0-alpha41533") || memcmp(version.ptr, "v2.0.0-alpha41533", version.len)) return 3;
    duckdb_v2_environment_handle environment = NULL;
    duckdb_v2_database_handle database = NULL;
    duckdb_v2_connection_handle connection = NULL;
    duckdb_v2_option_handle options[2] = {NULL, NULL};
    checked(duckdb_v2_create_environment(&environment, &error), &error);
    checked(duckdb_v2_option_create(view("threads"), view("2"), &options[0], &error), &error);
    checked(duckdb_v2_option_create(view("memory_limit"), view("256MB"), &options[1], &error), &error);
    checked(duckdb_v2_open(environment, view(":memory:"), options, 2, &database, &error), &error);
    checked(duckdb_v2_connect(database, &connection, &error), &error);
    const char *settings[] = {
        "SET max_temp_directory_size = '0B'", "SET preserve_insertion_order = false",
        "SET autoinstall_known_extensions = false", "SET autoload_known_extensions = false",
        "SET allow_community_extensions = false", "SET allow_unsigned_extensions = false",
        "SET allowed_directories = []", "SET allowed_paths = []", "SET enable_external_access = false",
        "SET enable_logging = false", "SET lock_configuration = true"
    };
    for (size_t i = 0; i < sizeof(settings) / sizeof(settings[0]); i++) ok(connection, settings[i]);
    ok(connection, "CREATE TEMP VIEW epoch_a AS SELECT 41::BIGINT AS value");
    ok(connection, "CREATE TEMP MACRO epoch_m(x) AS x + 40");
    ok(connection, "SET VARIABLE epoch_token = 'epoch_a'");
    equal(connection, "epoch_a_value", "SELECT value FROM epoch_a", 41);
    equal(connection, "epoch_a_macro", "SELECT epoch_m(1)::BIGINT", 41);
    checked(duckdb_v2_disconnect(&connection), NULL);
    checked(duckdb_v2_connect(database, &connection, &error), &error);
    equal(connection, "old_view_absent", "SELECT count(*)::BIGINT FROM duckdb_views() WHERE view_name = 'epoch_a'", 0);
    equal(connection, "old_macro_absent", "SELECT count(*)::BIGINT FROM duckdb_functions() WHERE function_name = 'epoch_m'", 0);
    equal(connection, "old_variable_absent", "SELECT (getvariable('epoch_token') IS NULL)::BIGINT", 1);
    equal(connection, "old_query_log_absent", "SELECT count(*)::BIGINT FROM duckdb_logs() WHERE message LIKE '%epoch_a%' OR message LIKE '%epoch_m%'", 0);
    equal(connection, "threads_fixed", "SELECT current_setting('threads')::BIGINT", 2);
    denied(connection, "file_read_denied", "SELECT * FROM read_blob('/workspace/varve-rebuild/hot-reuse-probe/run01/hot-reuse-probe.c')");
    denied(connection, "external_access_locked", "SET enable_external_access = true");
    denied(connection, "allowlist_locked", "SET allowed_paths = ['/workspace/varve-rebuild/hot-reuse-probe/run01/hot-reuse-probe.c']");
    ok(connection, "CREATE TEMP VIEW epoch_a AS SELECT 99::BIGINT AS value");
    equal(connection, "epoch_b_same_name", "SELECT value FROM epoch_a", 99);
    checked(duckdb_v2_disconnect(&connection), NULL);
    for (int trial = 0; trial < 8; trial++) {
        double start = now_ms();
        checked(duckdb_v2_connect(database, &connection, &error), &error);
        char message[2048];
        int64_t answer = 0;
        int code = query(connection, "SELECT 42::BIGINT", &answer, message, sizeof(message));
        if (code || answer != 42) { fprintf(stderr, "scalar trial failed: %s\n", message); return 1; }
        checked(duckdb_v2_disconnect(&connection), NULL);
        printf("{\"trial\":%d,\"fresh_connection_reused_database_ms\":%.6f}\n", trial, now_ms() - start);
    }
    checked(duckdb_v2_close(&database), NULL);
    checked(duckdb_v2_destroy_environment(&environment), NULL);
    for (int i = 0; i < 2; i++) checked(duckdb_v2_option_destroy(&options[i]), NULL);
    puts("{\"state\":\"passed\",\"boundary\":\"pinned empty-file-authority C diagnostic only; no Varve callbacks, borrow-epoch, cancellation, capacity, cold reuse, or matched-resource qualification\"}");
    return 0;
}
