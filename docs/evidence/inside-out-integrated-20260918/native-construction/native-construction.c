#define _POSIX_C_SOURCE 200809L
#include "duckdb_v2.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/resource.h>

static duckdb_v2_str view(const char *s) {
    duckdb_v2_str v = {s, (idx_t)strlen(s)};
    return v;
}
static double wall_ms(void) {
    struct timespec t;
    if (clock_gettime(CLOCK_MONOTONIC, &t)) exit(2);
    return 1000.0 * (double)t.tv_sec + (double)t.tv_nsec / 1000000.0;
}
static double cpu_ms(void) {
    struct rusage r;
    if (getrusage(RUSAGE_SELF, &r)) exit(2);
    return 1000.0 * (double)(r.ru_utime.tv_sec + r.ru_stime.tv_sec) + (double)(r.ru_utime.tv_usec + r.ru_stime.tv_usec) / 1000.0;
}
static void check(DUCKDB_V2_ERROR code, duckdb_v2_error_info_handle *error) {
    if (code == DUCKDB_V2_ERROR_NONE) return;
    duckdb_v2_str text = {0};
    if (error && *error) (void)duckdb_v2_error_info_get_text(*error, &text);
    fprintf(stderr, "native construction failed (%d): %.*s\n", (int)code, (int)text.len, text.ptr ? text.ptr : "");
    if (error) (void)duckdb_v2_error_info_destroy(error);
    exit(1);
}
int main(void) {
    duckdb_v2_error_info_handle error = NULL;
    duckdb_v2_str version = {0};
    check(duckdb_v2_library_version(&version, &error), &error);
    if (version.len != strlen("v2.0.0-alpha41533") || memcmp(version.ptr, "v2.0.0-alpha41533", version.len)) return 3;
    for (int trial = 0; trial < 8; trial++) {
        const int configured = trial % 2;
        duckdb_v2_environment_handle environment = NULL;
        duckdb_v2_database_handle database = NULL;
        duckdb_v2_connection_handle connection = NULL;
        duckdb_v2_option_handle options[2] = {NULL, NULL};
        double cpu_start = cpu_ms(), start = wall_ms();
        check(duckdb_v2_create_environment(&environment, &error), &error);
        double environment_done = wall_ms();
        if (configured) {
            check(duckdb_v2_option_create(view("threads"), view("2"), &options[0], &error), &error);
            check(duckdb_v2_option_create(view("memory_limit"), view("256MB"), &options[1], &error), &error);
        }
        double options_done = wall_ms();
        check(duckdb_v2_open(environment, view(":memory:"), configured ? options : NULL, configured ? 2 : 0, &database, &error), &error);
        double open_done = wall_ms();
        check(duckdb_v2_connect(database, &connection, &error), &error);
        double connect_done = wall_ms();
        check(duckdb_v2_disconnect(&connection), NULL);
        check(duckdb_v2_close(&database), NULL);
        check(duckdb_v2_destroy_environment(&environment), NULL);
        for (int i = 0; i < 2; i++) if (options[i]) check(duckdb_v2_option_destroy(&options[i]), NULL);
        double done = wall_ms();
        printf("{\"trial\":%d,\"preopen_options\":%s,\"environment_ms\":%.6f,\"options_ms\":%.6f,\"open_ms\":%.6f,\"connect_ms\":%.6f,\"teardown_ms\":%.6f,\"total_ms\":%.6f,\"self_cpu_ms\":%.6f}\n", trial, configured ? "true" : "false", environment_done-start, options_done-environment_done, open_done-options_done, connect_done-open_done, done-connect_done, done-start, cpu_ms()-cpu_start);
    }
    return 0;
}
