#!/usr/bin/env python3
"""Read-only exact-data supplement to the strict Varve/Timescale benchmark."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import itertools
import json
import math
import os
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, TypeVar

from psycopg import sql

from benchmark import (
    Names,
    names_for,
    TimescaleClient,
    VarveClient,
    canonical_json_sha256,
    checked_identifier,
    load_credentials,
    qualified,
    redactor,
)
from core import (
    BUCKET_US,
    DATASET_SEED,
    SERIES_COUNT,
    TENANT_COUNT,
    Deadline,
    checked_run_id,
    late_measurement,
    measurement,
    parse_bounded_int,
)

CLEANUP_TIMEOUT = 1.0
PAGE_ROWS = 4096
MAX_ROWS = 1_000_000
MAX_AGGREGATE_GROUPS = 100_000
MAX_REPORT_BYTES = 64 * 1024 * 1024
ARTIFACT_FILES = ("benchmark.py", "core.py", "requirements.txt", "Dockerfile")
T = TypeVar("T")


@dataclass(frozen=True, order=True, slots=True)
class ExactRow:
    timestamp_us: int
    tenant: str
    series: str
    value_q: int
    tags: str = "{}"


@dataclass(slots=True)
class IterationBounds:
    max_late_class_values: int = 0


@dataclass(slots=True)
class QuarterStats:
    count: int = 0
    sum_q: int = 0
    min_q: int | None = None
    max_q: int | None = None

    def add(self, row: ExactRow) -> None:
        self.count += 1
        self.sum_q += row.value_q
        self.min_q = row.value_q if self.min_q is None else min(self.min_q, row.value_q)
        self.max_q = row.value_q if self.max_q is None else max(self.max_q, row.value_q)

    def report(self) -> dict[str, int | float | None]:
        return {
            "count": self.count,
            "sum": self.sum_q / 4,
            "min": None if self.min_q is None else self.min_q / 4,
            "max": None if self.max_q is None else self.max_q / 4,
        }


@dataclass(frozen=True, slots=True)
class SourcePlan:
    source: dict[str, Any]
    names: Names
    base_us: int
    initial_rows: int
    late_rows: int
    source_state: str
    source_report_sha256: str
    dependency_hashes: dict[str, str]

    @property
    def total_rows(self) -> int:
        return self.initial_rows + self.late_rows


AggregateRow = tuple[int, str, str, str, int, int, int, int]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _integer(value: object, label: str, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise RuntimeError(f"source report {label} is invalid")
    return value


def _complete_ingest(source: dict[str, Any], initial_rows: int) -> None:
    ingest = source.get("initial_ingest")
    if not isinstance(ingest, dict):
        raise RuntimeError("source report lacks initial ingest accounting")
    for backend in ("varve", "timescale"):
        item = ingest.get(backend)
        if not isinstance(item, dict) or item.get("state") != "passed":
            raise RuntimeError(f"source report {backend} initial ingest is incomplete")
        for field in ("offered_rows", "assigned_rows", "rows"):
            if _integer(item.get(field), f"initial_ingest.{backend}.{field}") != initial_rows:
                raise RuntimeError(f"source report {backend} initial row accounting differs")
        for field in ("never_submitted_rows", "failed_or_ambiguous_rows"):
            if _integer(item.get(field), f"initial_ingest.{backend}.{field}") != 0:
                raise RuntimeError(f"source report {backend} initial work is incomplete")


def _complete_mixed(source: dict[str, Any], manifest: dict[str, Any], total_limit: int) -> int:
    mixed = source.get("mixed_workload")
    config = source.get("configuration")
    if not isinstance(mixed, dict) or not isinstance(config, dict):
        raise RuntimeError("source report lacks mixed workload/configuration")
    target = _integer(mixed.get("target_rows"), "mixed_workload.target_rows")
    rate = config.get("offered_rate_rows_per_second")
    seconds = config.get("mixed_seconds")
    if isinstance(rate, bool) or not isinstance(rate, (int, float)) or not math.isfinite(rate):
        raise RuntimeError("source report mixed rate is invalid")
    if type(seconds) is not int or seconds < 1 or target != int(rate * seconds):
        raise RuntimeError("source report mixed target differs from configuration")
    acknowledged = _integer(mixed.get("acknowledged_rows"), "mixed_workload.acknowledged_rows")
    if acknowledged != target or acknowledged > total_limit:
        raise RuntimeError("source report does not contain a complete bounded mixed workload")
    for field in ("varve_acknowledged_rows", "timescale_acknowledged_rows"):
        if _integer(mixed.get(field), f"mixed_workload.{field}") != acknowledged:
            raise RuntimeError("source report paired acknowledgements differ")
    for field in ("failed_or_ambiguous_rows", "pending_rows", "never_submitted_rows"):
        if _integer(mixed.get(field), f"mixed_workload.{field}") != 0:
            raise RuntimeError("source report has failed, ambiguous, pending, or unsubmitted work")
    offered_rows = _integer(mixed.get("offered_rows"), "mixed_workload.offered_rows")
    dropped_rows = _integer(mixed.get("dropped_rows"), "mixed_workload.dropped_rows")
    accounted = acknowledged + mixed["failed_or_ambiguous_rows"] + mixed["pending_rows"] + dropped_rows
    if offered_rows != target or dropped_rows != 0 or accounted != offered_rows:
        raise RuntimeError("source report mixed write conservation differs")
    reads = mixed.get("reads")
    if not isinstance(reads, dict):
        raise RuntimeError("source report mixed read accounting is absent")
    offered = _integer(reads.get("offered"), "mixed_workload.reads.offered")
    if (
        _integer(reads.get("completed"), "mixed_workload.reads.completed") != offered
        or _integer(reads.get("failed"), "mixed_workload.reads.failed") != 0
        or _integer(reads.get("pending"), "mixed_workload.reads.pending") != 0
        or mixed.get("data_complete") is not True
    ):
        raise RuntimeError("source report mixed reads are incomplete")
    checks = {
        "mixed_paired_acknowledged_rows": acknowledged,
        "mixed_varve_acknowledged_rows": acknowledged,
        "mixed_timescale_acknowledged_rows": acknowledged,
        "mixed_failed_or_ambiguous_rows": 0,
        "mixed_pending_rows": 0,
    }
    for field, expected in checks.items():
        if _integer(manifest.get(field), f"manifest.{field}") != expected:
            raise RuntimeError("source report manifest mixed accounting differs")
    return acknowledged


def _validate_verdict_and_barrier(source: dict[str, Any], total_rows: int) -> str:
    state = source.get("state")
    mixed = source["mixed_workload"]
    if state == "passed":
        if (
            source.get("overloaded") is not None
            or mixed.get("state") != "passed"
            or mixed.get("schedule_met") is not True
            or mixed.get("clean") is not True
        ):
            raise RuntimeError("source passed verdict conflicts with workload diagnostics")
    elif state == "overloaded":
        overloaded = source.get("overloaded")
        if (
            not isinstance(overloaded, dict)
            or overloaded.get("data_complete") is not True
            or overloaded.get("schedule_met") is not False
            or mixed.get("state") != "overloaded"
            or mixed.get("schedule_met") is not False
            or mixed.get("clean") is not False
        ):
            raise RuntimeError("only a complete-data overloaded diagnostic is verifiable")
    else:
        raise RuntimeError("source report verdict is not passed or complete-data overloaded")
    barrier = source.get("mixed_freshness_barrier")
    if (
        not isinstance(barrier, dict)
        or barrier.get("varve_eager_aggregate") is not True
        or _integer(barrier.get("same_reconciled_watermark_rows"), "freshness watermark") != total_rows
        or isinstance(barrier.get("timescale_explicit_refresh_seconds"), bool)
        or not isinstance(barrier.get("timescale_explicit_refresh_seconds"), (int, float))
        or not math.isfinite(barrier["timescale_explicit_refresh_seconds"])
        or barrier["timescale_explicit_refresh_seconds"] < 0
    ):
        raise RuntimeError("source report lacks the completed fresh aggregate barrier")
    phases = source.get("phases")
    if (
        not isinstance(phases, list)
        or not phases
        or phases[-1].get("name") != "mixed_barrier_retry_drills_and_final_correctness"
        or phases[-1].get("state") != "passed"
    ):
        raise RuntimeError("source report final correctness phase is incomplete")
    return state


def load_source(path: Path) -> SourcePlan:
    if not path.is_file() or path.stat().st_size > MAX_REPORT_BYTES:
        raise RuntimeError("--report must be a bounded regular file")
    raw = path.read_bytes()
    try:
        source = json.loads(raw, parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        raise RuntimeError("source report is not strict JSON") from None
    if not isinstance(source, dict) or source.get("mode") != "benchmark":
        raise RuntimeError("source report must be a completed benchmark report")
    manifest = source.get("manifest")
    config = source.get("configuration")
    if not isinstance(manifest, dict) or not isinstance(config, dict):
        raise RuntimeError("source report lacks manifest/configuration")
    initial = _integer(manifest.get("initial_rows"), "manifest.initial_rows", 1)
    if initial != _integer(config.get("rows"), "configuration.rows", 1) or initial > MAX_ROWS:
        raise RuntimeError("source report initial row bounds differ")
    base_us = _integer(manifest.get("base_timestamp_us"), "manifest.base_timestamp_us")
    if base_us % BUCKET_US != 0:
        raise RuntimeError("source report base timestamp is not minute aligned")
    expected_manifest = {
        "seed": DATASET_SEED,
        "series": SERIES_COUNT,
        "tenants": TENANT_COUNT,
        "aggregate_width_us": BUCKET_US,
        "tags": {},
        "value_rule": "((i * 17) % 10000) / 4",
        "timestamp_rule": "base + (i//1024)*1 second + (i%1024) microseconds",
    }
    for field, expected in expected_manifest.items():
        if manifest.get(field) != expected:
            raise RuntimeError(f"source report deterministic manifest field {field} differs")
    _complete_ingest(source, initial)
    late = _complete_mixed(source, manifest, MAX_ROWS - initial)
    total = initial + late
    if _integer(manifest.get("total_reconciled_watermark_rows"), "manifest.total rows") != total:
        raise RuntimeError("source report total watermark differs")
    state = _validate_verdict_and_barrier(source, total)

    run_id = source.get("run_id")
    try:
        if not isinstance(run_id, str):
            raise ValueError("run identity is not text")
        checked_run_id(run_id)
    except ValueError:
        raise RuntimeError("source report run identity is invalid") from None
    raw_names = source.get("namespaces")
    if not isinstance(raw_names, dict):
        raise RuntimeError("source report lacks namespaces")
    try:
        names = Names(**raw_names)
    except (TypeError, ValueError):
        raise RuntimeError("source report namespaces are invalid") from None
    for value in names.json().values():
        checked_identifier(value)
    if names != names_for(run_id):
        raise RuntimeError("source report namespaces differ from its run identity")

    artifact = source.get("artifact")
    files = artifact.get("files") if isinstance(artifact, dict) else None
    if not isinstance(files, dict) or artifact.get("format_version") != 1:
        raise RuntimeError("source report artifact hashes are absent")
    root = Path(__file__).resolve().parent
    current = {name: file_sha256(root / name) for name in ARTIFACT_FILES}
    for name in ARTIFACT_FILES:
        if files.get(name) != current[name]:
            raise RuntimeError(f"source report {name} hash differs from verifier dependency")
    combined = hashlib.sha256("".join(current[name] for name in ARTIFACT_FILES).encode("ascii")).hexdigest()
    if artifact.get("sha256") != combined:
        raise RuntimeError("source report combined artifact hash differs")
    return SourcePlan(source, names, base_us, initial, late, state, hashlib.sha256(raw).hexdigest(), current)


def expected_rows(plan: SourcePlan, bounds: IterationBounds | None = None) -> Iterable[ExactRow]:
    remainders = list(range(min(SERIES_COUNT, plan.late_rows)))
    remainders.sort(key=lambda index: (
        late_measurement(index, plan.base_us).timestamp_us,
        late_measurement(index, plan.base_us).tenant,
        late_measurement(index, plan.base_us).series,
    ))
    for remainder in remainders:
        values = [
            late_measurement(index, plan.base_us)
            for index in range(remainder, plan.late_rows, SERIES_COUNT)
        ]
        values.sort(key=lambda row: row.value_q)
        if bounds is not None:
            bounds.max_late_class_values = max(bounds.max_late_class_values, len(values))
        for row in values:
            yield ExactRow(row.timestamp_us, row.tenant, row.series, row.value_q)
    for index in range(plan.initial_rows):
        row = measurement(index, plan.base_us)
        yield ExactRow(row.timestamp_us, row.tenant, row.series, row.value_q)


def _quarter(value: object, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)):
        raise RuntimeError(f"{label} is not numeric")
    try:
        quarter = Decimal(str(value)) * 4
    except (InvalidOperation, ValueError):
        raise RuntimeError(f"{label} is not finite exact-quarter data") from None
    if not quarter.is_finite() or quarter != quarter.to_integral_value():
        raise RuntimeError(f"{label} is not finite exact-quarter data")
    return int(quarter)


def _empty_tags(value: object, label: str) -> str:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            raise RuntimeError(f"{label} tags are not valid JSON") from None
    if not isinstance(value, Mapping) or value:
        raise RuntimeError(f"{label} tags differ from the deterministic empty map")
    return "{}"


def normalize_raw(row: Mapping[str, object]) -> ExactRow:
    timestamp = row.get("timestamp_us")
    if type(timestamp) is not int:
        raise RuntimeError("raw timestamp_us is not an integer")
    tenant, series = row.get("tenant"), row.get("series")
    if not isinstance(tenant, str) or not isinstance(series, str):
        raise RuntimeError("raw tenant/series is not text")
    return ExactRow(
        timestamp,
        tenant,
        series,
        _quarter(row.get("value"), "raw value"),
        _empty_tags(row.get("tags"), "raw"),
    )


def normalize_aggregate(row: Mapping[str, object]) -> AggregateRow:
    bucket, count = row.get("bucket_us"), row.get("count")
    tenant, series = row.get("tenant"), row.get("series")
    if type(bucket) is not int or type(count) is not int or count <= 0:
        raise RuntimeError("aggregate bucket/count is invalid")
    if not isinstance(tenant, str) or not isinstance(series, str):
        raise RuntimeError("aggregate tenant/series is not text")
    return (
        bucket,
        tenant,
        series,
        _empty_tags(row.get("tags"), "aggregate"),
        count,
        _quarter(row.get("sum"), "aggregate sum"),
        _quarter(row.get("min"), "aggregate min"),
        _quarter(row.get("max"), "aggregate max"),
    )


async def verify_paged(
    label: str,
    expected: Iterable[T],
    fetches: Iterable[tuple[str, Callable[[int, int], Any]]],
    normalize: Callable[[Mapping[str, object]], T],
    page_size: int = PAGE_ROWS,
) -> dict[str, int]:
    if page_size <= 0 or page_size > PAGE_ROWS:
        raise ValueError("page size is outside the verifier bound")
    iterator = iter(expected)
    offset = 0
    pages = 0
    backends = tuple(fetches)
    while True:
        wanted = list(itertools.islice(iterator, page_size))
        limit = page_size if wanted else 1
        for backend, fetch in backends:
            observed = [normalize(row) for row in await fetch(offset, limit)]
            if observed != wanted:
                mismatch = next(
                    (index for index, pair in enumerate(itertools.zip_longest(wanted, observed)) if pair[0] != pair[1]),
                    0,
                )
                expected_item = wanted[mismatch] if mismatch < len(wanted) else "<end>"
                observed_item = observed[mismatch] if mismatch < len(observed) else "<end>"
                raise RuntimeError(
                    f"{backend} {label} differs at sorted row {offset + mismatch}: "
                    f"expected {expected_item!r}, observed {observed_item!r}"
                )
        if not wanted:
            break
        offset += len(wanted)
        pages += 1
    return {"rows": offset, "pages": pages, "page_rows_max": page_size}


def build_aggregates(plan: SourcePlan, deadline: Deadline) -> tuple[list[AggregateRow], QuarterStats, IterationBounds]:
    groups: dict[tuple[int, str, str, str], QuarterStats] = {}
    global_stats = QuarterStats()
    bounds = IterationBounds()
    for index, row in enumerate(expected_rows(plan, bounds), 1):
        global_stats.add(row)
        key = ((row.timestamp_us // BUCKET_US) * BUCKET_US, row.tenant, row.series, row.tags)
        stats = groups.setdefault(key, QuarterStats())
        stats.add(row)
        if len(groups) > MAX_AGGREGATE_GROUPS:
            raise RuntimeError("independent aggregate expectation exceeded its group bound")
        if index % PAGE_ROWS == 0:
            deadline.timeout(1)
    result = [
        (*key, stats.count, stats.sum_q, int(stats.min_q), int(stats.max_q))
        for key, stats in sorted(groups.items())
    ]
    deadline.timeout(1)
    return result, global_stats, bounds


def verify_report_fingerprints(plan: SourcePlan, stats: QuarterStats) -> None:
    fingerprints = plan.source.get("final_raw_fingerprints")
    if not isinstance(fingerprints, dict):
        raise RuntimeError("source report lacks final raw fingerprints")
    expected_raw = stats.report()
    oracle = plan.source.get("oracles")
    if not isinstance(oracle, dict) or oracle.get("global") != expected_raw:
        raise RuntimeError("source global oracle differs from independent reconstruction")
    common_raw: object = None
    for backend in ("varve", "timescale"):
        item = fingerprints.get(backend)
        if not isinstance(item, dict) or not isinstance(item.get("database_identity"), dict):
            raise RuntimeError("source final fingerprint identity is absent")
        raw = item.get("raw")
        if not isinstance(raw, dict):
            raise RuntimeError("source final raw fingerprint is absent")
        for field, value in expected_raw.items():
            if raw.get(field) != value:
                raise RuntimeError("source final fingerprint differs from reconstruction")
        if item.get("raw_sha256") != canonical_json_sha256(raw):
            raise RuntimeError("source final raw hash differs")
        material = {"database_identity": item["database_identity"], "raw": raw}
        if item.get("fingerprint_sha256") != canonical_json_sha256(material):
            raise RuntimeError("source final identity fingerprint differs")
        common_raw = raw if common_raw is None else common_raw
        if raw != common_raw:
            raise RuntimeError("source backend raw fingerprints differ")


async def current_identities(varve: VarveClient, pg: TimescaleClient, names: Names) -> dict[str, dict[str, object]]:
    status = await varve.request("GET", "/v1/status")
    database_id = status.get("database_id") if isinstance(status, dict) else None
    if not isinstance(database_id, str) or not database_id:
        raise RuntimeError("Varve database identity is absent")
    rows = await pg.query(
        "SELECT current_database() AS database_name, d.oid::bigint AS database_oid, "
        "n.nspname AS schema_name, n.oid::bigint AS schema_oid "
        "FROM pg_database d CROSS JOIN pg_namespace n "
        "WHERE d.datname = current_database() AND n.nspname = %s",
        (names.pg_schema,),
    )
    if len(rows) != 1:
        raise RuntimeError("Timescale database identity is absent")
    return {"varve": {"database_id": database_id}, "timescale": dict(rows[0])}


async def _close_clients(*clients: Any) -> None:
    results = await asyncio.wait_for(
        asyncio.gather(*(client.close() for client in clients), return_exceptions=True),
        timeout=CLEANUP_TIMEOUT,
    )
    for result in results:
        if isinstance(result, BaseException):
            raise RuntimeError("verifier client cleanup failed") from result


async def verify_remote(plan: SourcePlan, credentials: dict[str, str], deadline: Deadline) -> dict[str, object]:
    aggregates, global_stats, bounds = build_aggregates(plan, deadline)
    verify_report_fingerprints(plan, global_stats)
    varve = VarveClient(credentials["VARVE_URL"], credentials["VARVE_API_TOKEN"], deadline, 1)
    pg = TimescaleClient(credentials, deadline)
    primary_failed = False
    try:
        await pg.open(0)
        identities = await current_identities(varve, pg, plan.names)
        expected_fingerprints = plan.source["final_raw_fingerprints"]
        for backend in ("varve", "timescale"):
            if identities[backend] != expected_fingerprints[backend]["database_identity"]:
                raise RuntimeError(f"{backend} database identity differs from source final fingerprint")

        async def varve_raw(offset: int, limit: int) -> list[dict[str, Any]]:
            return await varve.sql(
                f"SELECT timestamp_us, tenant, series, value, tags FROM {plan.names.varve_table} "
                "ORDER BY timestamp_us, tenant, series, value, tags "
                f"LIMIT {limit} OFFSET {offset}"
            )

        pg_table = qualified(plan.names.pg_schema, plan.names.pg_table)

        async def pg_raw(offset: int, limit: int) -> list[dict[str, Any]]:
            return await pg.query(sql.SQL(
                "SELECT (extract(epoch FROM ts)*1000000)::bigint AS timestamp_us, "
                "tenant, series, value, tags FROM {} "
                "ORDER BY ts, tenant, series, value, tags LIMIT {} OFFSET {}"
            ).format(pg_table, sql.Literal(limit), sql.Literal(offset)))

        raw_result = await verify_paged(
            "raw identity/multiplicity",
            expected_rows(plan),
            (("Varve", varve_raw), ("Timescale", pg_raw)),
            normalize_raw,
        )

        async def varve_aggregate(offset: int, limit: int) -> list[dict[str, Any]]:
            return await varve.sql(
                f"SELECT bucket_us, tenant, series, tags, count, sum, min, max "
                f"FROM {plan.names.varve_aggregate} "
                "ORDER BY bucket_us, tenant, series, tags "
                f"LIMIT {limit} OFFSET {offset}"
            )

        pg_aggregate = qualified(plan.names.pg_schema, plan.names.pg_aggregate)

        async def pg_aggregate_page(offset: int, limit: int) -> list[dict[str, Any]]:
            return await pg.query(sql.SQL(
                "SELECT (extract(epoch FROM bucket)*1000000)::bigint AS bucket_us, "
                "tenant, series, tags, count, sum, min, max FROM {} "
                "ORDER BY bucket, tenant, series, tags LIMIT {} OFFSET {}"
            ).format(pg_aggregate, sql.Literal(limit), sql.Literal(offset)))

        aggregate_result = await verify_paged(
            "minute aggregate group",
            aggregates,
            (("Varve", varve_aggregate), ("Timescale", pg_aggregate_page)),
            normalize_aggregate,
        )
        return {
            "database_identities": identities,
            "raw": raw_result,
            "aggregates": aggregate_result,
            "expected_iteration_bounds": {
                "late_remainder_classes": min(SERIES_COUNT, plan.late_rows),
                "max_late_class_values": bounds.max_late_class_values,
                "aggregate_groups": len(aggregates),
                "aggregate_group_limit": MAX_AGGREGATE_GROUPS,
            },
        }
    except BaseException:
        primary_failed = True
        raise
    finally:
        try:
            await _close_clients(varve, pg)
        except Exception:
            if not primary_failed:
                raise



def result_base(plan: SourcePlan) -> dict[str, object]:
    return {
        "format_version": 1,
        "kind": "supplemental_exact_verification",
        "state": "running",
        "started_at": utc_now(),
        "run_id": plan.source.get("run_id"),
        "source_report_sha256": plan.source_report_sha256,
        "source_final_fingerprints_sha256": canonical_json_sha256(
            plan.source.get("final_raw_fingerprints")
        ),
        "source_artifact": plan.source["artifact"],
        "verifier_artifact": {
            "verify_exact.py": file_sha256(Path(__file__).resolve()),
            **plan.dependency_hashes,
        },
        "workload": {
            "initial_rows": plan.initial_rows,
            "acknowledged_late_rows": plan.late_rows,
            "total_rows": plan.total_rows,
        },
        "verdict": {
            "source_benchmark": plan.source_state,
            "source_benchmark_preserved": True,
            "performance_approval": False,
            "statement": "Exact verification is diagnostic only and does not promote or approve benchmark performance.",
        },
        "execution": {
            "query_workers": 1,
            "query_order": "sequential Varve then Timescale pages",
            "freshness": "uses the source report existing fresh barrier; performs no refresh",
        },
        "claim_boundary": {
            "supplemental_not_replacement": True,
            "verified": "every deterministic raw identity, exact quarter value, empty tags, multiplicity, and every named minute count/sum/min/max group",
            "excluded": "first/last/OHLC because independent receipt ordering does not prove equal-timestamp ties",
            "mutation_or_refresh": False,
            "automatic_write_retries": 0,
        },
    }


def arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--max-seconds",
        type=lambda value: parse_bounded_int(value, "max-seconds", 1, 1800),
        default=600,
    )
    args = parser.parse_args(argv)
    if args.output.exists():
        parser.error("--output must be a new path")
    if args.report.resolve() == args.output.resolve():
        parser.error("--report and --output must differ")
    return args


def _reserve_output(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    os.close(descriptor)


def _write_output(path: Path, body: dict[str, object]) -> None:
    path.write_text(json.dumps(body, indent=2, sort_keys=True, allow_nan=False) + "\n")


def main(argv: list[str] | None = None) -> int:
    args = arguments(argv)
    _reserve_output(args.output)
    deadline = Deadline(args.max_seconds)
    credentials: dict[str, str] = {}
    body: dict[str, object] = {
        "format_version": 1,
        "kind": "supplemental_exact_verification",
        "state": "running",
        "started_at": utc_now(),
    }
    try:
        plan = load_source(args.report)
        body = result_base(plan)
        _write_output(args.output, body)
        credentials = load_credentials()
        checks = asyncio.run(asyncio.wait_for(
            verify_remote(plan, credentials, deadline),
            timeout=deadline.timeout(args.max_seconds),
        ))
        body["checks"] = checks
        body["state"] = "passed"
        body["finished_at"] = utc_now()
        _write_output(args.output, body)
        return 0
    except BaseException as error:
        clean = redactor(credentials)(error) if credentials else f"{type(error).__name__}: {error}"
        body["state"] = "failed"
        body["finished_at"] = utc_now()
        body["failure"] = clean[:2000]
        _write_output(args.output, body)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
