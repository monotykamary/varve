#!/usr/bin/env python3
"""Bounded, correctness-first Varve and TimescaleDB synthetic benchmark."""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlsplit

import aiohttp
import psycopg
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from core import (
    BUCKET_US,
    DATASET_SEED,
    Deadline,
    Oracle,
    Stats,
    aligned_base_us,
    canonical_json_sha256,
    canonical_payload_digest,
    checked_identifier,
    checked_run_id,
    cpu_snapshot,
    generator_platform,
    latency_summary,
    late_measurement,
    measurement,
    parse_bounded_float,
    parse_bounded_int,
)

REQUIRED_ENV = (
    "VARVE_URL",
    "VARVE_API_TOKEN",
    "PGHOST",
    "PGPORT",
    "PGUSER",
    "PGPASSWORD",
    "PGDATABASE",
)
CALL_TIMEOUT = 35.0
CONNECT_TIMEOUT = 15.0
MAX_RESPONSE_BYTES = 16 * 1024 * 1024
TIMESCALE_IMAGE = "timescale/timescaledb:2.30.0-pg17 (official full/non-oss)"
TIMESCALE_DIGEST = "sha256:3113d12b78392c064aa7475caf7a52b447b29ddd4f9bfd23526733fcb03e3459"
EXPECTED_NATIVE_VERSION = "v2.0.0-alpha41533"
EXPECTED_NATIVE_LIBRARY_SHA256 = "69bdd44e0d2426e7ba44ed14644b54d8bd99a9703e5cbb4aec3f8beef40f817d"
EXPECTED_NATIVE_HEADER_SHA256 = "62ad0df66b9f4193a657540d2429ba23cb32c41c5c98ea9f3f4e7a3b64e30544"
ATTESTATION_MAX_BYTES = 64 * 1024


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def emit(event: str, **fields: object) -> None:
    safe = {"event": event, "at": utc_now()}
    safe.update(fields)
    print(json.dumps(safe, separators=(",", ":"), allow_nan=False), flush=True)


def artifact_metadata() -> dict[str, object]:
    root = Path(__file__).resolve().parent
    files: dict[str, str] = {}
    for name in ("benchmark.py", "core.py", "requirements.txt", "Dockerfile"):
        files[name] = hashlib.sha256((root / name).read_bytes()).hexdigest()
    combined = hashlib.sha256("".join(files.values()).encode("ascii")).hexdigest()
    return {"format_version": 1, "sha256": combined, "files": files}


def load_credentials() -> dict[str, str]:
    missing = [name for name in REQUIRED_ENV if not os.environ.get(name)]
    if missing:
        raise ValueError("missing required credential environment variables: " + ", ".join(missing))
    values = {name: os.environ[name] for name in REQUIRED_ENV}
    parsed = urlsplit(values["VARVE_URL"])
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        raise ValueError("VARVE_URL must be an absolute HTTP(S) URL")
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("VARVE_URL must not contain credentials, query, or fragment")
    if not values["PGPORT"].isdigit() or not 1 <= int(values["PGPORT"]) <= 65535:
        raise ValueError("PGPORT must be an integer in 1..65535")
    token = values["VARVE_API_TOKEN"]
    if not 32 <= len(token) <= 4096 or not token.isascii() or not token.isprintable():
        raise ValueError("VARVE_API_TOKEN must contain 32..4096 visible ASCII bytes")
    return values


def redactor(credentials: dict[str, str]) -> Callable[[BaseException], str]:
    secrets = sorted((value for value in credentials.values() if value), key=len, reverse=True)

    def clean(error: BaseException) -> str:
        pending = [error]
        seen: set[int] = set()
        descriptions: list[str] = []
        while pending and len(descriptions) < 16:
            item = pending.pop()
            if id(item) in seen:
                continue
            seen.add(id(item))
            descriptions.append(f"{type(item).__name__}: {item}")
            if isinstance(item, BaseExceptionGroup):
                pending.extend(reversed(item.exceptions))
            if item.__cause__ is not None:
                pending.append(item.__cause__)
            elif not item.__suppress_context__ and item.__context__ is not None:
                pending.append(item.__context__)
        text = " | ".join(descriptions)
        for secret in secrets:
            text = text.replace(secret, "<redacted>")
        return text[:2000]

    return clean


class VarveHttpError(RuntimeError):
    def __init__(self, status: int, message: object) -> None:
        super().__init__(f"Varve HTTP {status}: {str(message)[:500]}")
        self.status = status


def validate_varve_receipt(receipt: object, expected_rows: int, duplicate: bool) -> dict[str, object]:
    if not isinstance(receipt, dict):
        raise RuntimeError("Varve write receipt is not an object")
    if type(receipt.get("rows")) is not int or receipt["rows"] != expected_rows:
        raise RuntimeError("Varve write receipt row count mismatch")
    if receipt.get("duplicate") is not duplicate:
        raise RuntimeError("Varve write receipt duplicate flag mismatch")
    if receipt.get("durability") != "local_fsync":
        raise RuntimeError("Varve write omitted local_fsync durability receipt")
    if type(receipt.get("sequence")) is not int or receipt["sequence"] <= 0:
        raise RuntimeError("Varve write receipt sequence must be positive")
    return receipt


def parse_attestation(value: str) -> dict[str, object]:
    if len(value.encode("utf-8")) > ATTESTATION_MAX_BYTES:
        raise argparse.ArgumentTypeError("attestation JSON exceeds 64 KiB")
    def reject_constant(constant: str) -> object:
        raise ValueError(f"non-finite JSON value {constant}")

    try:
        parsed = json.loads(value, parse_constant=reject_constant)
    except (json.JSONDecodeError, ValueError) as error:
        message = error.msg if isinstance(error, json.JSONDecodeError) else str(error)
        raise argparse.ArgumentTypeError(f"invalid attestation JSON: {message}") from None
    if not isinstance(parsed, dict):
        raise argparse.ArgumentTypeError("attestation JSON must be an object")
    return parsed


class IngestError(RuntimeError):
    def __init__(self, message: str, partial: dict[str, object]) -> None:
        super().__init__(message)
        self.partial = partial


class MixedWorkloadError(RuntimeError):
    def __init__(self, message: str, partial: dict[str, object]) -> None:
        super().__init__(message)
        self.partial = partial


class Report:
    def __init__(self, path: Path, body: dict[str, object]) -> None:
        self.path = path
        self.body = body
        self._started_monotonic = time.monotonic()
        self._phase_monotonic = self._started_monotonic

    def phase(self, name: str, state: str, **details: object) -> None:
        phases = self.body.setdefault("phases", [])
        assert isinstance(phases, list)
        started_at = (
            phases[-1]["finished_at"] if phases else self.body["started_at"]
        )
        now = time.monotonic()
        phases.append(
            {
                "name": name,
                "state": state,
                "started_at": started_at,
                "finished_at": utc_now(),
                "duration_seconds": now - self._phase_monotonic,
                "elapsed_seconds": now - self._started_monotonic,
                **details,
            }
        )
        self._phase_monotonic = now
        self.flush()
        emit("phase", name=name, state=state)

    def flush(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary.write_text(json.dumps(self.body, indent=2, sort_keys=True, allow_nan=False) + "\n")
        temporary.replace(self.path)


class VarveClient:
    def __init__(self, base: str, token: str, deadline: Deadline, writers: int) -> None:
        self.base = base.rstrip("/")
        self.deadline = deadline
        self.session = aiohttp.ClientSession(
            headers={"Authorization": f"Bearer {token}"},
            connector=aiohttp.TCPConnector(limit=writers + 8, ttl_dns_cache=60),
        )

    async def close(self) -> None:
        await self.session.close()

    async def request(self, method: str, path: str, body: bytes | None = None) -> Any:
        timeout = aiohttp.ClientTimeout(total=self.deadline.timeout(CALL_TIMEOUT))
        headers = {"Content-Type": "application/json"} if body is not None else None
        async with self.session.request(
            method,
            self.base + path,
            data=body,
            headers=headers,
            timeout=timeout,
            allow_redirects=False,
        ) as response:
            raw = bytearray()
            async for chunk in response.content.iter_chunked(64 * 1024):
                raw.extend(chunk)
                if len(raw) > MAX_RESPONSE_BYTES:
                    raise RuntimeError("Varve response exceeded benchmark limit")
            try:
                value = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError):
                raise RuntimeError(f"Varve returned non-JSON status {response.status}") from None
            if response.status < 200 or response.status >= 300:
                message = value.get("error") if isinstance(value, dict) else None
                raise VarveHttpError(response.status, message)
            return value

    async def text(self, method: str, path: str) -> str:
        timeout = aiohttp.ClientTimeout(total=self.deadline.timeout(CALL_TIMEOUT))
        async with self.session.request(
            method, self.base + path, timeout=timeout, allow_redirects=False
        ) as response:
            raw = bytearray()
            async for chunk in response.content.iter_chunked(64 * 1024):
                raw.extend(chunk)
                if len(raw) > MAX_RESPONSE_BYTES:
                    raise RuntimeError("Varve response exceeded benchmark limit")
            if response.status < 200 or response.status >= 300:
                raise VarveHttpError(response.status, raw.decode("utf-8", "replace"))
            return raw.decode("utf-8")

    async def json(self, method: str, path: str, value: object) -> Any:
        body = json.dumps(value, separators=(",", ":"), allow_nan=False).encode("utf-8")
        return await self.request(method, path, body)

    async def sql(self, statement: str) -> list[dict[str, Any]]:
        value = await self.json("POST", "/v1/query", {"sql": statement})
        if not isinstance(value, list):
            if isinstance(value, dict):
                return [value]
            raise RuntimeError("Varve SQL response contract changed")
        return value


class TimescaleClient:
    def __init__(self, credentials: dict[str, str], deadline: Deadline) -> None:
        self.credentials = credentials
        self.deadline = deadline
        self.control: psycopg.AsyncConnection[Any] | None = None
        self.writers: list[psycopg.AsyncConnection[Any]] = []

    async def _connect(self) -> psycopg.AsyncConnection[Any]:
        async with asyncio.timeout(self.deadline.timeout(CONNECT_TIMEOUT)):
            connection = await psycopg.AsyncConnection.connect(
                host=self.credentials["PGHOST"],
                port=int(self.credentials["PGPORT"]),
                user=self.credentials["PGUSER"],
                password=self.credentials["PGPASSWORD"],
                dbname=self.credentials["PGDATABASE"],
                connect_timeout=int(CONNECT_TIMEOUT),
                row_factory=dict_row,
            )
        await connection.set_autocommit(True)
        async with connection.cursor() as cursor:
            await cursor.execute("SET statement_timeout = '30s'")
            await cursor.execute("SET lock_timeout = '10s'")
        return connection

    async def open(self, writers: int) -> None:
        self.control = await self._connect()
        for _ in range(writers):
            self.writers.append(await self._connect())

    async def close(self) -> None:
        for connection in self.writers:
            await connection.close()
        if self.control is not None:
            await self.control.close()

    def conn(self) -> psycopg.AsyncConnection[Any]:
        if self.control is None:
            raise RuntimeError("Timescale control connection is not open")
        return self.control

    async def query(self, statement: Any, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        async with asyncio.timeout(self.deadline.timeout(CALL_TIMEOUT)):
            async with self.conn().cursor() as cursor:
                await cursor.execute(statement, params)
                if cursor.description is None:
                    return []
                return list(await cursor.fetchall())

    async def execute(self, statement: Any, params: tuple[Any, ...] = ()) -> None:
        await self.query(statement, params)


@dataclass(frozen=True)
class Names:
    varve_table: str
    varve_aggregate: str
    checkpoint_job: str
    compact_job: str
    pg_schema: str
    pg_table: str = "measurements"
    pg_receipts: str = "batch_receipts"
    pg_aggregate: str = "minute_rollup"

    def json(self) -> dict[str, str]:
        return self.__dict__.copy()


def names_for(run_id: str) -> Names:
    suffix = hashlib.sha256(run_id.encode("ascii")).hexdigest()[:8]
    return Names(
        varve_table=checked_identifier(f"vb_{run_id}_{suffix}"),
        varve_aggregate=checked_identifier(f"va_{run_id}_{suffix}"),
        checkpoint_job=checked_identifier(f"vc_{run_id}_{suffix}"),
        compact_job=checked_identifier(f"vm_{run_id}_{suffix}"),
        pg_schema=checked_identifier(f"vb_{run_id}_{suffix}"),
    )


def qualified(schema: str, relation: str) -> Any:
    return sql.Identifier(schema, relation)


def rebuilt_status(status: object) -> dict[str, object]:
    actual = status if isinstance(status, dict) else {}
    native = actual.get("native_query")
    native_actual = native if isinstance(native, dict) else {}
    expected = {
        "segmented_journal": True,
        "native_query": {
            "version": EXPECTED_NATIVE_VERSION,
            "library_sha256": EXPECTED_NATIVE_LIBRARY_SHA256,
            "header_sha256": EXPECTED_NATIVE_HEADER_SHA256,
        },
    }
    checks = {
        "persisted_segmented_journal": actual.get("segmented_journal") is True,
        "native_version": native_actual.get("version") == EXPECTED_NATIVE_VERSION,
        "native_library_sha256": native_actual.get("library_sha256") == EXPECTED_NATIVE_LIBRARY_SHA256,
        "native_header_sha256": native_actual.get("header_sha256") == EXPECTED_NATIVE_HEADER_SHA256,
    }
    return {
        "passed": all(checks.values()),
        "checks": checks,
        "expected": expected,
        "actual": {
            "database_id": actual.get("database_id"),
            "sequence": actual.get("sequence"),
            "checkpoint_sequence": actual.get("checkpoint_sequence"),
            "segmented_journal": actual.get("segmented_journal"),
            "native_query": native,
        },
        "evidence": "measured from Varve /v1/status; segmented_journal is the persisted catalog marker",
    }


async def preflight(
    varve: VarveClient,
    pg: TimescaleClient,
    names: Names,
    status: dict[str, object] | None = None,
) -> dict[str, object]:
    if status is None:
        varve_tables, varve_aggregates, measured_status = await asyncio.gather(
            varve.request("GET", "/v1/tables"),
            varve.request("GET", "/v1/aggregates"),
            varve.request("GET", "/v1/status"),
        )
        status = measured_status
    else:
        varve_tables, varve_aggregates = await asyncio.gather(
            varve.request("GET", "/v1/tables"),
            varve.request("GET", "/v1/aggregates"),
        )
    table_exists = any(row.get("name") == names.varve_table for row in varve_tables)
    aggregate_exists = any(row.get("name") == names.varve_aggregate for row in varve_aggregates)
    if table_exists or aggregate_exists:
        raise RuntimeError("owned Varve table or aggregate already exists; choose a new run-id")
    schema_exists = await pg.query(
        "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = %s) AS present",
        (names.pg_schema,),
    )
    if schema_exists[0]["present"]:
        raise RuntimeError("owned PostgreSQL schema already exists; choose a new run-id")
    extension = await pg.query(
        "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"
    )
    if len(extension) != 1:
        raise RuntimeError("TimescaleDB extension is not already installed; refusing CREATE EXTENSION")
    settings: dict[str, str] = {}
    for setting in ("fsync", "synchronous_commit", "full_page_writes"):
        rows = await pg.query(sql.SQL("SHOW {} ").format(sql.Identifier(setting)))
        settings[setting] = str(next(iter(rows[0].values())))
    if settings != {"fsync": "on", "synchronous_commit": "on", "full_page_writes": "on"}:
        raise RuntimeError(f"durability settings are not fully enabled: {settings}")
    versions = await pg.query("SELECT current_setting('server_version') AS postgres_version")
    identity = await pg.query(
        "SELECT current_database() AS database_name, oid::bigint AS database_oid "
        "FROM pg_database WHERE datname = current_database()"
    )
    return {
        "timescaledb_version": extension[0]["extversion"],
        "postgresql_version": versions[0]["postgres_version"],
        "durability": settings,
        "extension_preinstalled": True,
        "timescale_database_identity": identity[0],
        "varve_status": status,
        "rebuilt": rebuilt_status(status),
    }


async def setup_varve(varve: VarveClient, names: Names) -> None:
    config = {
        "shards": 8,
        "window_us": 3_600_000_000,
        "late_after_us": None,
        "retention_us": None,
        "archive_after_us": None,
        "rollup_widths_us": [BUCKET_US],
        "rollup_retention_us": None,
        "idempotency_window_us": None,
    }
    await varve.json("POST", "/v1/tables", {"name": names.varve_table, "config": config})
    await varve.sql(
        "CALL varve_create_continuous_aggregate("
        f"{sql_literal(names.varve_aggregate)},{sql_literal(names.varve_table)},{BUCKET_US})"
    )
    for job, kind in ((names.checkpoint_job, "checkpoint"), (names.compact_job, "compact")):
        await varve.sql(
            f"CALL varve_create_job({sql_literal(job)},{sql_literal(kind)},86400000000)"
        )
        await varve.sql(
            f"CALL varve_alter_job({sql_literal(job)},{sql_literal(json.dumps({'paused': True}))})"
        )


async def setup_timescale(pg: TimescaleClient, names: Names) -> list[str]:
    commands: list[str] = []
    await pg.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(names.pg_schema)))
    await pg.execute(
        sql.SQL(
            "CREATE TABLE {} (ts timestamptz NOT NULL, tenant text NOT NULL, "
            "series text NOT NULL, value double precision NOT NULL CHECK (value <> 'NaN'::double precision AND value <> 'Infinity'::double precision AND value <> '-Infinity'::double precision), "
            "tags jsonb NOT NULL)"
        ).format(qualified(names.pg_schema, names.pg_table))
    )
    await pg.execute(
        sql.SQL(
            "CREATE TABLE {} (batch_id text PRIMARY KEY, "
            "row_count integer NOT NULL CHECK (row_count > 0), "
            "payload_sha256 text NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{{64}}$'))"
        ).format(qualified(names.pg_schema, names.pg_receipts))
    )
    await pg.query(
        "SELECT create_hypertable(%s::regclass, 'ts', chunk_time_interval => INTERVAL '1 hour', if_not_exists => FALSE)",
        (f"{names.pg_schema}.{names.pg_table}",),
    )
    index_name = checked_identifier(f"{names.pg_table}_tenant_series_ts_idx")
    await pg.execute(
        sql.SQL("CREATE INDEX {} ON {} (tenant, series, ts DESC) INCLUDE (value)").format(
            sql.Identifier(index_name), qualified(names.pg_schema, names.pg_table)
        )
    )
    await pg.execute(
        sql.SQL(
            "CREATE MATERIALIZED VIEW {} WITH (timescaledb.continuous) AS "
            "SELECT time_bucket(INTERVAL '1 minute', ts) AS bucket, tenant, series, tags, "
            "count(*)::bigint AS count, sum(value)::double precision AS sum, "
            "min(value)::double precision AS min, max(value)::double precision AS max "
            "FROM {} GROUP BY bucket, tenant, series, tags WITH NO DATA"
        ).format(
            qualified(names.pg_schema, names.pg_aggregate),
            qualified(names.pg_schema, names.pg_table),
        )
    )
    commands.extend([
        "CREATE TABLE logged measurements",
        "CREATE TABLE batch_receipts (stable ID, row count, canonical payload SHA-256)",
        "SELECT create_hypertable(..., chunk_time_interval => INTERVAL '1 hour')",
        f"CREATE INDEX {index_name} ON measurements (tenant, series, ts DESC) INCLUDE (value)",
        "CREATE MATERIALIZED VIEW minute_rollup WITH (timescaledb.continuous) ... WITH NO DATA",
        "explicit refresh_continuous_aggregate barriers; no background refresh policy",
    ])
    return commands


async def refresh_timescale(pg: TimescaleClient, names: Names) -> float:
    started = time.monotonic()
    await pg.execute(
        sql.SQL("CALL refresh_continuous_aggregate({}, NULL, NULL)").format(
            sql.Literal(f"{names.pg_schema}.{names.pg_aggregate}")
        )
    )
    return time.monotonic() - started


async def analyze_timescale(pg: TimescaleClient, names: Names) -> float:
    started = time.monotonic()
    await pg.execute(sql.SQL("ANALYZE {}").format(qualified(names.pg_schema, names.pg_table)))
    return time.monotonic() - started


def build_oracle(rows: int, base_us: int, deadline: Deadline, cancelled: threading.Event) -> Oracle:
    oracle = Oracle()
    for index in range(rows):
        if index % 1024 == 0:
            if cancelled.is_set():
                raise TimeoutError("oracle preparation cancelled")
            deadline.timeout(1)
        oracle.add(measurement(index, base_us))
    return oracle


async def prepare_oracle(rows: int, base_us: int, deadline: Deadline) -> Oracle:
    cancelled = threading.Event()
    try:
        return await asyncio.to_thread(build_oracle, rows, base_us, deadline, cancelled)
    finally:
        cancelled.set()


def initial_rows(offset: int, count: int, base_us: int) -> list[Any]:
    return [measurement(index, base_us) for index in range(offset, offset + count)]


def mixed_rows(offset: int, count: int, base_us: int) -> list[Any]:
    return [late_measurement(index, base_us) for index in range(offset, offset + count)]


async def ingest_varve(
    varve: VarveClient,
    names: Names,
    rows: int,
    batch: int,
    writers: int,
    base_us: int,
    run_id: str,
    clean_error: Callable[[BaseException], str] | None = None,
) -> dict[str, object]:
    describe_error = clean_error or (lambda error: f"{type(error).__name__}: {str(error)[:500]}")
    latencies: list[float] = []
    encoding: list[float] = []
    acknowledged = 0
    next_offset = 0
    next_batch = 0
    cursor_lock = asyncio.Lock()

    async def worker() -> None:
        nonlocal acknowledged, next_offset, next_batch
        while True:
            async with cursor_lock:
                if next_offset >= rows:
                    return
                batch_index = next_batch
                offset = next_offset
                count = min(batch, rows - offset)
                next_batch += 1
                next_offset += count
            encode_started = time.monotonic()
            body = json.dumps(
                {
                    "table": names.varve_table,
                    "request_id": f"{run_id}-initial-{batch_index:08d}",
                    "rows": [row.varve() for row in initial_rows(offset, count, base_us)],
                },
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
            encoding.append((time.monotonic() - encode_started) * 1000)
            started = time.monotonic()
            receipt = await varve.request("POST", "/v1/write", body)
            latencies.append((time.monotonic() - started) * 1000)
            validate_varve_receipt(receipt, count, duplicate=False)
            acknowledged += count

    started = time.monotonic()
    failure: BaseException | None = None
    try:
        async with asyncio.TaskGroup() as group:
            for _ in range(writers):
                group.create_task(worker())
    except BaseException as error:
        failure = error
    elapsed = time.monotonic() - started
    result: dict[str, object] = {
        "state": "failed" if failure is not None else "passed",
        "rows": acknowledged,
        "offered_rows": rows,
        "assigned_rows": next_offset,
        "never_submitted_rows": rows - next_offset,
        "failed_or_ambiguous_rows": next_offset - acknowledged,
        "seconds": elapsed,
        "rows_per_second": acknowledged / elapsed,
        "ack_latency_ms": {"raw": latencies, "summary": latency_summary(latencies)},
        "encoding_ms": {"raw": encoding, "summary": latency_summary(encoding)},
        "durability_receipt": "local_fsync",
        "automatic_retries": 0,
        "errors": [] if failure is None else [describe_error(failure)],
    }
    if failure is not None:
        raise IngestError("Varve initial ingestion failed or became ambiguous", result) from failure
    if acknowledged != rows:
        raise IngestError("Varve initial acknowledgement conservation failed", result)
    return result


async def copy_batch(
    connection: psycopg.AsyncConnection[Any],
    names: Names,
    batch_id: str,
    rows: list[Any],
    deadline: Deadline,
) -> bool:
    async with asyncio.timeout(deadline.timeout(CALL_TIMEOUT)):
        if len(rows) == 1:
            if not connection.autocommit:
                raise RuntimeError("single-row comparison requires autocommit durability")
            payload_sha256 = canonical_payload_digest(rows)
            timestamp, tenant, series, value, tags = rows[0].postgres()
            async with connection.cursor() as cursor:
                result = await cursor.execute(
                    sql.SQL(
                        "WITH receipt AS (INSERT INTO {} (batch_id, row_count, payload_sha256) "
                        "VALUES (%s, 1, %s) ON CONFLICT DO NOTHING RETURNING batch_id) "
                        "INSERT INTO {} (ts, tenant, series, value, tags) "
                        "SELECT %s, %s, %s, %s, %s WHERE EXISTS (SELECT 1 FROM receipt) "
                        "RETURNING 1 AS inserted"
                    ).format(
                        qualified(names.pg_schema, names.pg_receipts),
                        qualified(names.pg_schema, names.pg_table),
                    ),
                    (batch_id, payload_sha256, timestamp, tenant, series, value, Jsonb(tags)),
                )
                if await result.fetchone() is not None:
                    return True
                existing = await cursor.execute(
                    sql.SQL("SELECT row_count, payload_sha256 FROM {} WHERE batch_id = %s").format(
                        qualified(names.pg_schema, names.pg_receipts)
                    ),
                    (batch_id,),
                )
                stored = await existing.fetchone()
                if stored is None or stored["row_count"] != 1 or stored["payload_sha256"] != payload_sha256:
                    raise RuntimeError(f"Timescale batch_id {batch_id!r} conflicts with its canonical payload digest")
                return False
        async with connection.transaction():
            async with connection.cursor() as cursor:
                payload_sha256 = canonical_payload_digest(rows)
                receipt = await cursor.execute(
                    sql.SQL(
                        "INSERT INTO {} (batch_id, row_count, payload_sha256) VALUES (%s, %s, %s) "
                        "ON CONFLICT DO NOTHING RETURNING batch_id"
                    ).format(qualified(names.pg_schema, names.pg_receipts)),
                    (batch_id, len(rows), payload_sha256),
                )
                if await receipt.fetchone() is None:
                    existing = await cursor.execute(
                        sql.SQL(
                            "SELECT row_count, payload_sha256 FROM {} WHERE batch_id = %s FOR UPDATE"
                        ).format(qualified(names.pg_schema, names.pg_receipts)),
                        (batch_id,),
                    )
                    stored = await existing.fetchone()
                    if (
                        stored is None
                        or stored["row_count"] != len(rows)
                        or stored["payload_sha256"] != payload_sha256
                    ):
                        raise RuntimeError(
                            f"Timescale batch_id {batch_id!r} conflicts with its canonical payload digest"
                        )
                    return False
                copy_sql = sql.SQL("COPY {} (ts, tenant, series, value, tags) FROM STDIN").format(
                    qualified(names.pg_schema, names.pg_table)
                )
                async with cursor.copy(copy_sql) as copy:
                    for row in rows:
                        timestamp, tenant, series, value, tags = row.postgres()
                        await copy.write_row((timestamp, tenant, series, value, Jsonb(tags)))
    return True


async def ingest_timescale(
    pg: TimescaleClient,
    names: Names,
    rows: int,
    batch: int,
    writers: int,
    base_us: int,
    run_id: str,
    deadline: Deadline,
    clean_error: Callable[[BaseException], str] | None = None,
) -> dict[str, object]:
    describe_error = clean_error or (lambda error: f"{type(error).__name__}: {str(error)[:500]}")
    latencies: list[float] = []
    generation: list[float] = []
    acknowledged = 0
    next_offset = 0
    next_batch = 0
    cursor_lock = asyncio.Lock()

    async def worker(worker_index: int) -> None:
        nonlocal acknowledged, next_offset, next_batch
        connection = pg.writers[worker_index]
        while True:
            async with cursor_lock:
                if next_offset >= rows:
                    return
                batch_index = next_batch
                offset = next_offset
                count = min(batch, rows - offset)
                next_batch += 1
                next_offset += count
            generated = time.monotonic()
            batch_rows = initial_rows(offset, count, base_us)
            generation.append((time.monotonic() - generated) * 1000)
            started = time.monotonic()
            inserted = await copy_batch(
                connection,
                names,
                f"{run_id}-initial-{batch_index:08d}",
                batch_rows,
                deadline,
            )
            latencies.append((time.monotonic() - started) * 1000)
            if not inserted:
                raise RuntimeError("unexpected duplicate Timescale initial batch receipt")
            acknowledged += count

    started = time.monotonic()
    failure: BaseException | None = None
    try:
        async with asyncio.TaskGroup() as group:
            for index in range(writers):
                group.create_task(worker(index))
    except BaseException as error:
        failure = error
    elapsed = time.monotonic() - started
    result: dict[str, object] = {
        "state": "failed" if failure is not None else "passed",
        "rows": acknowledged,
        "offered_rows": rows,
        "assigned_rows": next_offset,
        "never_submitted_rows": rows - next_offset,
        "failed_or_ambiguous_rows": next_offset - acknowledged,
        "seconds": elapsed,
        "rows_per_second": acknowledged / elapsed,
        "ack_latency_ms": {"raw": latencies, "summary": latency_summary(latencies)},
        "generation_ms": {"raw": generation, "summary": latency_summary(generation)},
        "durability_receipt": "transactional COPY plus receipt commit",
        "automatic_retries": 0,
        "errors": [] if failure is None else [describe_error(failure)],
    }
    if failure is not None:
        raise IngestError("Timescale initial ingestion failed or became ambiguous", result) from failure
    if acknowledged != rows:
        raise IngestError("Timescale initial acknowledgement conservation failed", result)
    return result


def assert_stats(actual: dict[str, object], expected: Stats, label: str) -> None:
    count = int(actual.get("n", actual.get("count", 0)))
    sum_key = "total" if "total" in actual else "sum"
    observed_sum = actual.get(sum_key)
    observed_min = actual.get("min")
    observed_max = actual.get("max")
    expected_sum = expected.sum_q / 4.0
    expected_min = None if expected.min_q is None else expected.min_q / 4.0
    expected_max = None if expected.max_q is None else expected.max_q / 4.0
    if (
        count != expected.count
        or (None if observed_sum is None else float(observed_sum)) != expected_sum
        or (None if observed_min is None else float(observed_min)) != expected_min
        or (None if observed_max is None else float(observed_max)) != expected_max
    ):
        observed = {
            "count": count,
            "sum": observed_sum,
            "min": observed_min,
            "max": observed_max,
        }
        raise RuntimeError(
            f"{label} correctness mismatch: expected {expected.json()}, observed {observed}"
        )


def expected_stats(snapshot: dict[str, object]) -> Stats:
    return Stats(
        count=int(snapshot["count"]),
        sum_q=round(float(snapshot["sum"]) * 4),
        min_q=None if snapshot.get("min") is None else round(float(snapshot["min"]) * 4),
        max_q=None if snapshot.get("max") is None else round(float(snapshot["max"]) * 4),
    )


@dataclass(frozen=True)
class QueryCase:
    name: str
    varve_sql: str
    postgres_sql: Any
    verify: Callable[[list[dict[str, Any]]], None]


def query_cases(names: Names, oracle: Oracle, base_us: int, initial_count: int) -> list[QueryCase]:
    table = names.varve_table
    aggregate = names.varve_aggregate
    pg_table = qualified(names.pg_schema, names.pg_table)
    pg_aggregate = qualified(names.pg_schema, names.pg_aggregate)
    end_us = base_us + ((max(0, initial_count - 1) // 1024) + 2) * 1_000_000
    start_us = max(base_us, end_us - 60_000_000)
    recent = oracle.selected_recent(start_us, end_us)

    def verify_single(expected: Stats, label: str) -> Callable[[list[dict[str, Any]]], None]:
        def verify(rows: list[dict[str, Any]]) -> None:
            if len(rows) != 1:
                raise RuntimeError(f"{label} returned {len(rows)} rows instead of one")
            assert_stats(rows[0], expected, label)
        return verify

    def verify_tenants(rows: list[dict[str, Any]]) -> None:
        actual = {str(row["tenant"]): row for row in rows}
        if len(actual) != len(rows) or set(actual) != set(oracle.tenants):
            raise RuntimeError("cross-series tenant groups differ from oracle")
        for tenant, stats in oracle.tenants.items():
            assert_stats(actual[tenant], stats, f"tenant group {tenant}")

    def verify_buckets(rows: list[dict[str, Any]]) -> None:
        actual = {int(row["bucket_us"]): row for row in rows}
        if len(actual) != len(rows) or set(actual) != set(oracle.selected_buckets):
            raise RuntimeError("minute aggregate buckets differ from oracle")
        for bucket, stats in oracle.selected_buckets.items():
            assert_stats(actual[bucket], stats, f"minute bucket {bucket}")

    expected_window = oracle.selected_window()

    def verify_window(rows: list[dict[str, Any]]) -> None:
        normalized = [
            {"timestamp_us": int(row["timestamp_us"]), "value": float(row["value"])}
            for row in rows
        ]
        if normalized != expected_window:
            raise RuntimeError("window query differs from oracle")

    raw_projection = "count(*) AS n, sum(value) AS total, min(value) AS min, max(value) AS max"
    return [
        QueryCase(
            "recent_series_time_filter",
            f"SELECT {raw_projection} FROM {table} WHERE tenant='tenant_0' AND series='series_0000' AND timestamp_us >= {start_us} AND timestamp_us < {end_us}",
            sql.SQL(
                "SELECT count(*)::bigint AS n, sum(value)::double precision AS total, "
                "min(value)::double precision AS min, max(value)::double precision AS max FROM {} "
                "WHERE tenant='tenant_0' AND series='series_0000' "
                "AND ts >= to_timestamp({}/1000000.0) AND ts < to_timestamp({}/1000000.0)"
            ).format(pg_table, sql.Literal(start_us), sql.Literal(end_us)),
            verify_single(recent, "recent selective query"),
        ),
        QueryCase(
            "full_count_sum",
            f"SELECT {raw_projection} FROM {table}",
            sql.SQL(
                "SELECT count(*)::bigint AS n, sum(value)::double precision AS total, "
                "min(value)::double precision AS min, max(value)::double precision AS max FROM {}"
            ).format(pg_table),
            verify_single(oracle.all, "full count/sum"),
        ),
        QueryCase(
            "cross_series_group",
            f"SELECT tenant, {raw_projection} FROM {table} GROUP BY tenant ORDER BY tenant",
            sql.SQL(
                "SELECT tenant, count(*)::bigint AS n, sum(value)::double precision AS total, "
                "min(value)::double precision AS min, max(value)::double precision AS max "
                "FROM {} GROUP BY tenant ORDER BY tenant"
            ).format(pg_table),
            verify_tenants,
        ),
        QueryCase(
            "minute_continuous_aggregate",
            f"SELECT bucket_us, count AS n, sum AS total, min, max FROM {aggregate} "
            "WHERE tenant='tenant_0' AND series='series_0000' ORDER BY bucket_us",
            sql.SQL(
                "SELECT (extract(epoch FROM bucket)*1000000)::bigint AS bucket_us, count AS n, "
                "sum AS total, min, max FROM {} WHERE tenant='tenant_0' AND series='series_0000' "
                "ORDER BY bucket"
            ).format(pg_aggregate),
            verify_buckets,
        ),
        QueryCase(
            "window_top_five",
            f"SELECT timestamp_us, value FROM (SELECT timestamp_us, value, row_number() OVER "
            f"(ORDER BY timestamp_us DESC, value DESC) AS rn FROM {table} WHERE tenant='tenant_0' "
            "AND series='series_0000') ranked WHERE rn <= 5 ORDER BY timestamp_us DESC, value DESC",
            sql.SQL(
                "SELECT (extract(epoch FROM ts)*1000000)::bigint AS timestamp_us, value FROM "
                "(SELECT ts, value, row_number() OVER (ORDER BY ts DESC, value DESC) AS rn FROM {} "
                "WHERE tenant='tenant_0' AND series='series_0000') ranked "
                "WHERE rn <= 5 ORDER BY ts DESC, value DESC"
            ).format(pg_table),
            verify_window,
        ),
    ]


async def run_query_suite(
    varve: VarveClient,
    pg: TimescaleClient,
    cases: list[QueryCase],
    samples: int,
    report: Report | None = None,
    stage: str | None = None,
    backend_order: tuple[str, str] = ("varve", "timescale"),
    clean_error: Callable[[BaseException], str] | None = None,
) -> dict[str, object]:
    describe_error = clean_error or (lambda error: f"{type(error).__name__}: {str(error)[:500]}")
    if set(backend_order) != {"varve", "timescale"}:
        raise ValueError("query backend order must contain Varve and Timescale exactly once")
    output: dict[str, object] = {
        "warmup_per_query": 1,
        "samples_per_query": samples,
        "backend_order": list(backend_order),
        "percentile_scope": "observational pilot samples; not p99 qualification",
        "backends": {},
    }
    if report is not None and stage is not None:
        report.body.setdefault("query_stages", {})[stage] = output
        report.flush()
    backends = output["backends"]
    assert isinstance(backends, dict)
    for backend in backend_order:
        backend_results: dict[str, object] = {}
        backends[backend] = backend_results
        for case in cases:
            async def execute() -> list[dict[str, Any]]:
                if backend == "varve":
                    return await varve.sql(case.varve_sql)
                return await pg.query(case.postgres_sql)

            latencies: list[float] = []
            result_state = {"state": "running", "latency_ms": {"raw": latencies}, "verified_samples": 0}
            backend_results[case.name] = result_state
            try:
                case.verify(await execute())
                for _ in range(samples):
                    started = time.monotonic()
                    result = await execute()
                    latencies.append((time.monotonic() - started) * 1000)
                    case.verify(result)
                    result_state["verified_samples"] += 1
                result_state["state"] = "passed"
            except BaseException as error:
                result_state["state"] = "failed"
                result_state["error"] = describe_error(error)
                raise
            finally:
                result_state["latency_ms"]["summary"] = latency_summary(latencies)
                if report is not None:
                    report.flush()
    return output


async def checkpoint_varve(varve: VarveClient, names: Names) -> dict[str, object]:
    before = await varve.request("GET", "/v1/status")
    checkpoint_started = time.monotonic()
    await varve.sql(f"CALL varve_run_job({sql_literal(names.checkpoint_job)})")
    checkpoint_seconds = time.monotonic() - checkpoint_started
    compact_started = time.monotonic()
    await varve.sql(f"CALL varve_run_job({sql_literal(names.compact_job)})")
    compact_seconds = time.monotonic() - compact_started
    after = await varve.request("GET", "/v1/status")
    return {
        "checkpoint_seconds": checkpoint_seconds,
        "compaction_seconds": compact_seconds,
        "commands": [
            f"CALL varve_run_job('{names.checkpoint_job}')",
            f"CALL varve_run_job('{names.compact_job}')",
        ],
        "status_before": {
            key: before.get(key) for key in ("sequence", "checkpoint_sequence", "hot_rows", "segments")
        },
        "status_after": {
            key: after.get(key) for key in ("sequence", "checkpoint_sequence", "hot_rows", "segments")
        },
    }


async def convert_timescale(pg: TimescaleClient, names: Names) -> dict[str, object]:
    procedures = await pg.query(
        "SELECT p.proname, p.prokind, p.oid::regprocedure::text AS signature "
        "FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace "
        "WHERE p.proname IN ('convert_to_columnstore','compress_chunk') ORDER BY p.proname"
    )
    available = {str(row["proname"]): row for row in procedures}
    chunks = await pg.query(
        "SELECT show_chunks(%s::regclass)::text AS chunk",
        (f"{names.pg_schema}.{names.pg_table}",),
    )
    started = time.monotonic()
    commands: list[str] = []
    try:
        if "convert_to_columnstore" in available:
            path = "hypercore_columnstore"
            await pg.execute(
                sql.SQL(
                    "ALTER TABLE {} SET (timescaledb.enable_columnstore=true, "
                    "timescaledb.segmentby='tenant,series', timescaledb.orderby='ts DESC')"
                ).format(qualified(names.pg_schema, names.pg_table))
            )
            commands.append(
                "ALTER TABLE measurements SET (timescaledb.enable_columnstore=true, timescaledb.segmentby='tenant,series', timescaledb.orderby='ts DESC')"
            )
            convert = available["convert_to_columnstore"]
            for row in chunks:
                if convert["prokind"] == "p":
                    await pg.execute("CALL convert_to_columnstore(%s::regclass)", (row["chunk"],))
                    commands.append(f"CALL convert_to_columnstore('{row['chunk']}'::regclass)")
                else:
                    await pg.query("SELECT convert_to_columnstore(%s::regclass)", (row["chunk"],))
                    commands.append(f"SELECT convert_to_columnstore('{row['chunk']}'::regclass)")
        elif "compress_chunk" in available:
            path = "legacy_compression"
            await pg.execute(
                sql.SQL(
                    "ALTER TABLE {} SET (timescaledb.compress=true, "
                    "timescaledb.compress_segmentby='tenant,series', timescaledb.compress_orderby='ts DESC')"
                ).format(qualified(names.pg_schema, names.pg_table))
            )
            commands.append(
                "ALTER TABLE measurements SET (timescaledb.compress=true, timescaledb.compress_segmentby='tenant,series', timescaledb.compress_orderby='ts DESC')"
            )
            compress = available["compress_chunk"]
            for row in chunks:
                if compress["prokind"] == "p":
                    await pg.execute("CALL compress_chunk(%s::regclass)", (row["chunk"],))
                    commands.append(f"CALL compress_chunk('{row['chunk']}'::regclass)")
                else:
                    await pg.query(
                        "SELECT compress_chunk(%s::regclass, if_not_compressed => true)",
                        (row["chunk"],),
                    )
                    commands.append(f"SELECT compress_chunk('{row['chunk']}'::regclass, if_not_compressed => true)")
        else:
            return {
                "status": "unsupported",
                "reason": "neither convert_to_columnstore nor compress_chunk exists",
                "procedure_introspection": procedures,
                "commands": [],
                "seconds": time.monotonic() - started,
            }
        return {
            "status": "converted",
            "path": path,
            "segmentby": "tenant,series",
            "orderby": "ts DESC",
            "chunks": len(chunks),
            "procedure_introspection": procedures,
            "commands": commands,
            "seconds": time.monotonic() - started,
        }
    except Exception as error:
        return {
            "status": "failed",
            "reason": f"{type(error).__name__}: columnar conversion command failed; inspect database logs",
            "procedure_introspection": procedures,
            "commands": commands,
            "seconds": time.monotonic() - started,
        }


async def stable_mixed_read(
    varve: VarveClient,
    pg: TimescaleClient,
    names: Names,
    baseline: Stats,
    base_us: int,
    end_us: int,
) -> tuple[float, float]:
    projection = "count(*) AS n, sum(value) AS total, min(value) AS min, max(value) AS max"
    varve_sql = f"SELECT {projection} FROM {names.varve_table} WHERE timestamp_us >= {base_us} AND timestamp_us < {end_us}"
    pg_sql = sql.SQL(
        "SELECT count(*)::bigint AS n, sum(value)::double precision AS total, "
        "min(value)::double precision AS min, max(value)::double precision AS max FROM {} "
        "WHERE ts >= to_timestamp({}/1000000.0) AND ts < to_timestamp({}/1000000.0)"
    ).format(qualified(names.pg_schema, names.pg_table), sql.Literal(base_us), sql.Literal(end_us))
    async def timed_read(call: Any, label: str) -> float:
        started = time.monotonic()
        rows = await call
        elapsed = (time.monotonic() - started) * 1000
        if len(rows) != 1:
            raise RuntimeError(f"{label} returned an unexpected row count")
        assert_stats(rows[0], baseline, label)
        return elapsed

    latencies = await asyncio.gather(
        timed_read(varve.sql(varve_sql), "mixed Varve stable-watermark read"),
        timed_read(pg.query(pg_sql), "mixed Timescale stable-watermark read"),
    )
    return latencies[0], latencies[1]


async def runtime_snapshot(
    varve: VarveClient,
    pg: TimescaleClient,
    label: str,
) -> dict[str, object]:
    observed_at = utc_now()
    status, metrics, database = await asyncio.gather(
        varve.request("GET", "/v1/status"),
        varve.text("GET", "/metrics"),
        pg.query(
            "SELECT datname AS database_name, numbackends, xact_commit, xact_rollback, "
            "blks_read, blks_hit, tup_returned, tup_fetched, tup_inserted, temp_files, "
            "temp_bytes FROM pg_stat_database WHERE datname = current_database()"
        ),
    )
    return {
        "label": label,
        "observed_at": observed_at,
        "varve_status": status,
        "varve_metrics_text": metrics,
        "timescale_pg_stat_database": database[0] if database else None,
        "scope": "database API/status snapshot; platform CPU, memory, disk, and process identity are external",
    }


def _normalize_moments(rows: list[dict[str, Any]]) -> list[dict[str, object]]:
    return [
        {
            "timestamp_us": int(row["timestamp_us"]),
            "tenant": str(row["tenant"]),
            "series": str(row["series"]),
            "value": float(row["value"]),
        }
        for row in rows
    ]


async def raw_fingerprints(
    varve: VarveClient,
    pg: TimescaleClient,
    names: Names,
) -> dict[str, object]:
    projection = (
        "count(*) AS n, sum(value) AS total, min(value) AS min, max(value) AS max, "
        "min(timestamp_us) AS min_timestamp_us, max(timestamp_us) AS max_timestamp_us"
    )
    # Verification runs after drain and must respect even a one-query profile.
    varve_summary = await varve.sql(f"SELECT {projection} FROM {names.varve_table}")
    varve_first = await varve.sql(
        f"SELECT timestamp_us, tenant, series, value FROM {names.varve_table} "
        "ORDER BY timestamp_us ASC, tenant ASC, series ASC, value ASC LIMIT 8"
    )
    varve_last = await varve.sql(
        f"SELECT timestamp_us, tenant, series, value FROM {names.varve_table} "
        "ORDER BY timestamp_us DESC, tenant DESC, series DESC, value DESC LIMIT 8"
    )
    status = await varve.request("GET", "/v1/status")
    pg_table = qualified(names.pg_schema, names.pg_table)
    pg_summary = await pg.query(
        sql.SQL(
            "SELECT count(*)::bigint AS n, sum(value)::double precision AS total, "
            "min(value)::double precision AS min, max(value)::double precision AS max, "
            "(extract(epoch FROM min(ts))*1000000)::bigint AS min_timestamp_us, "
            "(extract(epoch FROM max(ts))*1000000)::bigint AS max_timestamp_us FROM {}"
        ).format(pg_table)
    )
    moment_projection = (
        "(extract(epoch FROM ts)*1000000)::bigint AS timestamp_us, tenant, series, value"
    )
    pg_first = await pg.query(
        sql.SQL(
            f"SELECT {moment_projection} FROM {{}} "
            "ORDER BY ts ASC, tenant ASC, series ASC, value ASC LIMIT 8"
        ).format(pg_table)
    )
    pg_last = await pg.query(
        sql.SQL(
            f"SELECT {moment_projection} FROM {{}} "
            "ORDER BY ts DESC, tenant DESC, series DESC, value DESC LIMIT 8"
        ).format(pg_table)
    )
    pg_identity_rows = await pg.query(
        "SELECT current_database() AS database_name, d.oid::bigint AS database_oid, "
        "n.nspname AS schema_name, n.oid::bigint AS schema_oid "
        "FROM pg_database d CROSS JOIN pg_namespace n "
        "WHERE d.datname = current_database() AND n.nspname = %s",
        (names.pg_schema,),
    )

    def common(summary_rows: list[dict[str, Any]], first: list[dict[str, Any]], last: list[dict[str, Any]]) -> dict[str, object]:
        if len(summary_rows) != 1:
            raise RuntimeError("raw fingerprint summary returned an unexpected row count")
        row = summary_rows[0]
        return {
            "count": int(row["n"]),
            "sum": None if row["total"] is None else float(row["total"]),
            "min": None if row["min"] is None else float(row["min"]),
            "max": None if row["max"] is None else float(row["max"]),
            "min_timestamp_us": None if row["min_timestamp_us"] is None else int(row["min_timestamp_us"]),
            "max_timestamp_us": None if row["max_timestamp_us"] is None else int(row["max_timestamp_us"]),
            "first_exact_moments": _normalize_moments(first),
            "last_exact_moments": _normalize_moments(last),
        }

    varve_raw = common(varve_summary, varve_first, varve_last)
    timescale_raw = common(pg_summary, pg_first, pg_last)
    if varve_raw != timescale_raw:
        raise RuntimeError("Varve and Timescale raw exact-moment fingerprints differ")
    database_id = status.get("database_id")
    if not isinstance(database_id, str) or not database_id:
        raise RuntimeError("Varve raw fingerprint lacks database identity")
    if len(pg_identity_rows) != 1:
        raise RuntimeError("Timescale raw fingerprint lacks database identity")
    varve_identity = {"database_id": database_id}
    timescale_identity = pg_identity_rows[0]
    result = {
        "varve": {
            "database_identity": varve_identity,
            "raw": varve_raw,
            "raw_sha256": canonical_json_sha256(varve_raw),
            "fingerprint_sha256": canonical_json_sha256({"database_identity": varve_identity, "raw": varve_raw}),
        },
        "timescale": {
            "database_identity": timescale_identity,
            "raw": timescale_raw,
            "raw_sha256": canonical_json_sha256(timescale_raw),
            "fingerprint_sha256": canonical_json_sha256({"database_identity": timescale_identity, "raw": timescale_raw}),
        },
    }
    return result


async def stable_retry_drill(
    varve: VarveClient,
    pg: TimescaleClient,
    names: Names,
    run_id: str,
    base_us: int,
    initial_count: int,
    batch: int,
    deadline: Deadline,
    include_conflicting: bool,
) -> dict[str, object]:
    count = min(batch, initial_count)
    rows = initial_rows(0, count, base_us)
    batch_id = f"{run_id}-initial-00000000"
    before = await raw_fingerprints(varve, pg, names)
    body = json.dumps(
        {"table": names.varve_table, "request_id": batch_id, "rows": [row.varve() for row in rows]},
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    receipt = validate_varve_receipt(
        await varve.request("POST", "/v1/write", body), count, duplicate=True
    )
    if await copy_batch(pg.writers[0], names, batch_id, rows, deadline):
        raise RuntimeError("Timescale identical retry unexpectedly inserted rows")
    outcomes: dict[str, object] = {
        "batch_id": batch_id,
        "rows": count,
        "identical": {
            "varve_duplicate": receipt["duplicate"],
            "varve_sequence": receipt["sequence"],
            "timescale_duplicate": True,
        },
        "conflicting": "not_requested",
    }
    if include_conflicting:
        first = rows[0]
        conflicting = list(rows)
        conflicting[0] = type(first)(
            first.timestamp_us, first.tenant, first.series, first.value_q + 1
        )
        conflict_body = json.dumps(
            {
                "table": names.varve_table,
                "request_id": batch_id,
                "rows": [row.varve() for row in conflicting],
            },
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        try:
            await varve.request("POST", "/v1/write", conflict_body)
        except VarveHttpError as error:
            if error.status != 409:
                raise RuntimeError("Varve conflicting retry did not return HTTP 409") from error
        else:
            raise RuntimeError("Varve accepted a conflicting stable request ID")
        try:
            await copy_batch(pg.writers[0], names, batch_id, conflicting, deadline)
        except RuntimeError as error:
            if "conflicts with its canonical payload digest" not in str(error):
                raise
        else:
            raise RuntimeError("Timescale accepted a conflicting stable batch ID")
        outcomes["conflicting"] = {
            "varve_http_status": 409,
            "timescale_payload_digest_conflict": True,
        }
    after = await raw_fingerprints(varve, pg, names)
    if before != after:
        raise RuntimeError("raw fingerprint changed across stable-ID retry drill")
    outcomes["fingerprints_before"] = before
    outcomes["fingerprints_after"] = after
    outcomes["unchanged"] = True
    return outcomes


async def mixed_workload(
    varve: VarveClient,
    pg: TimescaleClient,
    names: Names,
    args: argparse.Namespace,
    base_us: int,
    baseline: Stats,
    oracle: Oracle,
    deadline: Deadline,
    clean_error: Callable[[BaseException], str] | None = None,
) -> dict[str, object]:
    """Run finite intended write/read traces with bounded queues and no shedding or retries."""
    describe_error = clean_error or (lambda error: f"{type(error).__name__}: {str(error)[:500]}")
    write_queue: asyncio.Queue[tuple[int, int, int, float] | None] = asyncio.Queue(
        maxsize=args.writers * 2
    )
    readers = getattr(args, "mixed_readers", 1)
    read_interval = getattr(args, "mixed_read_interval", 1.0)
    read_queue: asyncio.Queue[tuple[int, float] | None] = asyncio.Queue(maxsize=readers * 2)
    drain_seconds = getattr(args, "drain_seconds", 60.0)
    target_rows = int(args.rate * args.mixed_seconds)
    read_target = max(0, math.ceil(args.mixed_seconds / read_interval) - 1)
    varve_ack: list[float] = []
    pg_ack: list[float] = []
    queue_delays: list[float] = []
    end_to_end: list[float] = []
    generation: list[float] = []
    read_arrival: list[float] = []
    read_observations: list[dict[str, object]] = []
    varve_reads: list[float] = []
    pg_reads: list[float] = []
    errors: list[str] = []
    counts = {
        "offered_rows": target_rows,
        "acknowledged_rows": 0,
        "varve_acknowledged_rows": 0,
        "timescale_acknowledged_rows": 0,
        "failed_or_ambiguous_rows": 0,
    }
    read_counts = {"offered": read_target, "completed": 0, "failed": 0}
    flow: dict[str, int | float] = {
        "write_enqueued_rows": 0,
        "read_enqueued": 0,
        "peak_write_queue_requests": 0,
        "peak_read_queue_requests": 0,
        "max_write_enqueue_lag_ms": 0.0,
        "max_read_enqueue_lag_ms": 0.0,
        "max_write_start_lag_ms": 0.0,
        "max_read_start_lag_ms": 0.0,
    }
    stop = asyncio.Event()
    initial_end = base_us + ((max(0, args.rows - 1) // 1024) + 2) * 1_000_000
    started = time.monotonic()

    async def worker(worker_index: int) -> None:
        connection = pg.writers[worker_index]
        while True:
            item = await write_queue.get()
            if item is None:
                write_queue.task_done()
                return
            batch_index, offset, count, intended = item
            picked = time.monotonic()
            flow["max_write_start_lag_ms"] = max(
                float(flow["max_write_start_lag_ms"]), (picked - intended) * 1000
            )
            queue_delays.append((picked - intended) * 1000)
            submitted = False
            reconciled = False
            failure_counted = False
            try:
                generated = time.monotonic()
                rows = mixed_rows(offset, count, base_us)
                request = json.dumps(
                    {
                        "table": names.varve_table,
                        "request_id": f"{args.run_id}-mixed-{batch_index:08d}",
                        "rows": [row.varve() for row in rows],
                    },
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("utf-8")
                generation.append((time.monotonic() - generated) * 1000)

                async def write_varve() -> bool:
                    before = time.monotonic()
                    receipt = await varve.request("POST", "/v1/write", request)
                    varve_ack.append((time.monotonic() - before) * 1000)
                    validate_varve_receipt(receipt, count, duplicate=False)
                    return True

                async def write_pg() -> bool:
                    before = time.monotonic()
                    inserted = await copy_batch(
                        connection,
                        names,
                        f"{args.run_id}-mixed-{batch_index:08d}",
                        rows,
                        deadline,
                    )
                    pg_ack.append((time.monotonic() - before) * 1000)
                    if not inserted:
                        raise RuntimeError("unexpected duplicate mixed Timescale receipt")
                    return True

                submitted = True
                paired_tasks = [
                    asyncio.create_task(write_varve()),
                    asyncio.create_task(write_pg()),
                ]
                try:
                    done, pending = await asyncio.wait(
                        paired_tasks, return_when=asyncio.FIRST_EXCEPTION
                    )
                    if any(
                        not task.cancelled() and task.exception() is not None
                        for task in done
                    ):
                        for task in pending:
                            task.cancel()
                    outcomes = await asyncio.gather(*paired_tasks, return_exceptions=True)
                except BaseException:
                    for task in paired_tasks:
                        task.cancel()
                    cancelled_outcomes = await asyncio.gather(
                        *paired_tasks, return_exceptions=True
                    )
                    if cancelled_outcomes[0] is True:
                        counts["varve_acknowledged_rows"] += count
                    if cancelled_outcomes[1] is True:
                        counts["timescale_acknowledged_rows"] += count
                    raise
                if outcomes[0] is True:
                    counts["varve_acknowledged_rows"] += count
                if outcomes[1] is True:
                    counts["timescale_acknowledged_rows"] += count
                failures = [result for result in outcomes if isinstance(result, BaseException)]
                if failures:
                    counts["failed_or_ambiguous_rows"] += count
                    failure_counted = True
                    errors.extend(describe_error(error) for error in failures)
                    stop.set()
                    raise RuntimeError("paired mixed write failed or became ambiguous") from failures[0]
                oracle.extend(rows)
                counts["acknowledged_rows"] += count
                reconciled = True
                end_to_end.append((time.monotonic() - intended) * 1000)
            except BaseException as error:
                if submitted and not reconciled and not failure_counted:
                    counts["failed_or_ambiguous_rows"] += count
                    failure_counted = True
                diagnostic = describe_error(error)
                if diagnostic not in errors:
                    errors.append(diagnostic)
                stop.set()
                raise
            finally:
                write_queue.task_done()

    async def reader() -> None:
        while True:
            item = await read_queue.get()
            if item is None:
                read_queue.task_done()
                return
            ordinal, intended = item
            before = time.monotonic()
            flow["max_read_start_lag_ms"] = max(
                float(flow["max_read_start_lag_ms"]), (before - intended) * 1000
            )
            try:
                varve_ms, pg_ms = await stable_mixed_read(
                    varve, pg, names, baseline, base_us, initial_end
                )
                completed = time.monotonic()
                varve_reads.append(varve_ms)
                pg_reads.append(pg_ms)
                arrival_ms = (completed - intended) * 1000
                read_arrival.append(arrival_ms)
                read_observations.append({
                    "ordinal": ordinal,
                    "intended_seconds": ordinal * read_interval,
                    "queue_delay_ms": (before - intended) * 1000,
                    "varve_service_ms": varve_ms,
                    "timescale_service_ms": pg_ms,
                    "arrival_latency_ms": arrival_ms,
                    "state": "completed",
                })
                read_counts["completed"] += 1
            except BaseException as error:
                read_counts["failed"] += 1
                diagnostic = describe_error(error)
                read_observations.append({
                    "ordinal": ordinal,
                    "intended_seconds": ordinal * read_interval,
                    "queue_delay_ms": (before - intended) * 1000,
                    "state": "failed_or_ambiguous",
                    "error": diagnostic,
                })
                errors.append(f"read {ordinal}: {diagnostic}")
                stop.set()
                raise
            finally:
                read_queue.task_done()

    async def produce_writes() -> None:
        batch_index = 0
        for offset in range(0, target_rows, args.batch):
            if stop.is_set():
                return
            intended = started + offset / args.rate
            await asyncio.sleep(max(0.0, intended - time.monotonic()))
            count = min(args.batch, target_rows - offset)
            await write_queue.put((batch_index, offset, count, intended))
            flow["write_enqueued_rows"] = int(flow["write_enqueued_rows"]) + count
            flow["peak_write_queue_requests"] = max(
                int(flow["peak_write_queue_requests"]), write_queue.qsize()
            )
            flow["max_write_enqueue_lag_ms"] = max(
                float(flow["max_write_enqueue_lag_ms"]),
                (time.monotonic() - intended) * 1000,
            )
            batch_index += 1
        await asyncio.sleep(max(0.0, started + args.mixed_seconds - time.monotonic()))
        await write_queue.join()
        for _ in range(args.writers):
            await write_queue.put(None)

    async def produce_reads() -> None:
        for ordinal in range(1, read_target + 1):
            if stop.is_set():
                return
            intended = started + ordinal * read_interval
            await asyncio.sleep(max(0.0, intended - time.monotonic()))
            await read_queue.put((ordinal, intended))
            flow["read_enqueued"] = int(flow["read_enqueued"]) + 1
            flow["peak_read_queue_requests"] = max(
                int(flow["peak_read_queue_requests"]), read_queue.qsize()
            )
            flow["max_read_enqueue_lag_ms"] = max(
                float(flow["max_read_enqueue_lag_ms"]),
                (time.monotonic() - intended) * 1000,
            )
        await read_queue.join()
        for _ in range(readers):
            await read_queue.put(None)

    failure: BaseException | None = None
    try:
        async with asyncio.timeout(args.mixed_seconds + drain_seconds):
            async with asyncio.TaskGroup() as group:
                for index in range(args.writers):
                    group.create_task(worker(index))
                group.create_task(produce_writes())
                for _ in range(readers):
                    group.create_task(reader())
                group.create_task(produce_reads())
    except BaseException as error:
        failure = error
    finally:
        elapsed = time.monotonic() - started
        flow["write_unsubmitted_rows"] = target_rows - int(flow["write_enqueued_rows"])
        flow["read_unsubmitted"] = read_target - int(flow["read_enqueued"])
        flow["elapsed_with_drain_seconds"] = elapsed
        flow["drain_seconds"] = max(0.0, elapsed - args.mixed_seconds)
        counts["pending_rows"] = (
            target_rows
            - counts["acknowledged_rows"]
            - counts["failed_or_ambiguous_rows"]
        )
        read_counts["pending"] = (
            read_target - read_counts["completed"] - read_counts["failed"]
        )
        flow["pending_enqueued_rows"] = max(
            0, counts["pending_rows"] - int(flow["write_unsubmitted_rows"])
        )

    if counts["pending_rows"] < 0 or read_counts["pending"] < 0:
        raise RuntimeError("mixed workload conservation violated")
    schedule_met = (
        float(flow["max_write_start_lag_ms"]) <= args.batch / args.rate * 1000
        and (
            read_target == 0
            or float(flow["max_read_start_lag_ms"]) <= read_interval * 1000
        )
    )
    data_complete = (
        counts["acknowledged_rows"] == target_rows
        and counts["failed_or_ambiguous_rows"] == 0
        and counts["pending_rows"] == 0
        and read_counts["completed"] == read_target
        and read_counts["failed"] == 0
        and read_counts["pending"] == 0
    )
    result: dict[str, object] = {
        "state": "failed" if failure is not None else ("passed" if data_complete and schedule_met else "overloaded"),
        "seconds": elapsed,
        "writers": args.writers,
        "readers": readers,
        "read_interval_seconds": read_interval,
        "offered_rate_rows_per_second": args.rate,
        "target_rows": target_rows,
        **counts,
        "dropped_rows": 0,
        "never_submitted_rows": flow["write_unsubmitted_rows"],
        "reads": read_counts,
        "read_observations": read_observations,
        "scheduling": flow,
        "data_complete": data_complete,
        "schedule_met": schedule_met,
        "clean": data_complete and schedule_met,
        "errors": errors,
        "queue_delay_ms": {"raw": queue_delays, "summary": latency_summary(queue_delays)},
        "end_to_end_from_intended_arrival_ms": {
            "raw": end_to_end,
            "summary": latency_summary(end_to_end),
        },
        "varve_ack_latency_ms": {"raw": varve_ack, "summary": latency_summary(varve_ack)},
        "timescale_ack_latency_ms": {"raw": pg_ack, "summary": latency_summary(pg_ack)},
        "generation_encoding_ms": {"raw": generation, "summary": latency_summary(generation)},
        "varve_concurrent_read_ms": {"raw": varve_reads, "summary": latency_summary(varve_reads)},
        "timescale_concurrent_read_ms": {"raw": pg_reads, "summary": latency_summary(pg_reads)},
        "read_arrival_latency_ms": {
            "raw": read_arrival,
            "summary": latency_summary(read_arrival),
        },
        "concurrent_read_pair_ms": {
            "raw": read_arrival,
            "summary": latency_summary(read_arrival),
        },
        "percentile_scope": "observational pilot samples; not p99 qualification",
        "late_event_rule": "fixed deterministic timestamps strictly before initial base; out-of-order by construction",
    }
    if failure is not None:
        raise MixedWorkloadError(
            f"mixed workload aborted with conserved diagnostic accounting: {type(failure).__name__}",
            result,
        ) from failure
    return result


async def verify_all(
    varve: VarveClient,
    pg: TimescaleClient,
    names: Names,
    oracle: Oracle,
    base_us: int,
    initial_count: int,
) -> None:
    for case in query_cases(names, oracle, base_us, initial_count):
        case.verify(await varve.sql(case.varve_sql))
        case.verify(await pg.query(case.postgres_sql))


async def run_fresh(
    args: argparse.Namespace,
    credentials: dict[str, str],
    report: Report,
    deadline: Deadline,
) -> None:
    names = names_for(args.run_id)
    report.body["namespaces"] = names.json()
    report.flush()
    varve = VarveClient(credentials["VARVE_URL"], credentials["VARVE_API_TOKEN"], deadline, args.writers)
    pg = TimescaleClient(credentials, deadline)
    try:
        await pg.open(args.writers)
        actual_status = await varve.request("GET", "/v1/status")
        measured_rebuilt = rebuilt_status(actual_status)
        report.body["actual_varve_preflight_status"] = actual_status
        report.body["rebuilt_preflight"] = measured_rebuilt
        report.flush()
        if args.require_rebuilt and not measured_rebuilt["passed"]:
            report.phase("preflight", "failed", require_rebuilt=True)
            raise RuntimeError("--require-rebuilt exact persisted journal/native identity check failed")
        diagnostics = await preflight(varve, pg, names, actual_status)
        report.body["matched_durability"] = {
            "varve": "every write receipt is checked for local_fsync, rows, duplicate=false, and positive sequence",
            "timescale": diagnostics["durability"],
            "scope": "single-node local durable acknowledgement; no replicated durability claim",
        }
        report.body["database_versions"] = diagnostics
        report.flush()
        report.phase("preflight", "passed", require_rebuilt=args.require_rebuilt)

        await setup_varve(varve, names)
        commands = await setup_timescale(pg, names)
        report.body["timescale_setup_commands"] = commands
        report.body.setdefault("runtime_snapshots", []).append(
            await runtime_snapshot(varve, pg, "after_isolated_setup")
        )
        report.phase("isolated_setup", "passed")

        base_us = aligned_base_us(int(time.time() * 1_000_000))
        oracle = await prepare_oracle(args.rows, base_us, deadline)
        report.body["manifest"] = {
            "seed": DATASET_SEED,
            "base_timestamp_us": base_us,
            "initial_rows": args.rows,
            "series": 1024,
            "tenants": 4,
            "value_rule": "((i * 17) % 10000) / 4",
            "timestamp_rule": "base + (i//1024)*1 second + (i%1024) microseconds",
            "tags": {},
            "aggregate_width_us": BUCKET_US,
        }
        report.body["oracles"] = oracle.snapshot()
        report.flush()

        order = (
            ["varve", "timescale"]
            if int(hashlib.sha256(args.run_id.encode()).hexdigest(), 16) % 2 == 0
            else ["timescale", "varve"]
        )
        query_order = tuple(reversed(order))
        report.body["initial_ingest_order"] = order
        report.body["query_backend_order"] = list(query_order)
        ingest: dict[str, object] = {}
        for backend in order:
            freshness_started = time.monotonic()
            try:
                if backend == "varve":
                    ingest[backend] = await ingest_varve(
                        varve, names, args.rows, args.batch, args.writers, base_us, args.run_id,
                        redactor(credentials),
                    )
                else:
                    ingest[backend] = await ingest_timescale(
                        pg, names, args.rows, args.batch, args.writers, base_us, args.run_id,
                        deadline, redactor(credentials),
                    )
            except IngestError as error:
                ingest[backend] = error.partial
                report.body["initial_ingest"] = ingest
                report.flush()
                raise
            result = ingest[backend]
            assert isinstance(result, dict)
            if backend == "timescale":
                result["analyze_seconds"] = await analyze_timescale(pg, names)
                result["aggregate_refresh_seconds"] = await refresh_timescale(pg, names)
            result["time_to_durable_data_plus_fresh_aggregate_seconds"] = (
                time.monotonic() - freshness_started
            )
            result["freshness_timing_scope"] = (
                "elapsed ingestion plus immediate maintenance barrier; includes ANALYZE for Timescale"
            )
            report.body["initial_ingest"] = ingest
            report.flush()
        report.body.setdefault("runtime_snapshots", []).append(
            await runtime_snapshot(varve, pg, "after_initial_drain")
        )
        report.phase("initial_durable_ingest_and_freshness_barrier", "passed", order=order)

        await verify_all(varve, pg, names, oracle, base_us, args.rows)
        status = await varve.request("GET", "/v1/status")
        report.body["initial_tier_state"] = {
            "varve": {
                key: status.get(key)
                for key in ("hot_rows", "segments", "sequence", "checkpoint_sequence")
            },
            "timescale": "rowstore before explicit conversion",
            "claim": "Varve default automatic tiering, not a hot-only or cold-cache benchmark",
        }
        report.body.setdefault("query_stages", {})["default_tiers"] = await run_query_suite(
            varve,
            pg,
            query_cases(names, oracle, base_us, args.rows),
            args.query_samples,
            report,
            "default_tiers",
            query_order,
            redactor(credentials),
        )
        report.phase("default_tiers_queries", "passed")

        report.body["varve_checkpoint_compaction"] = await checkpoint_varve(varve, names)
        conversion = await convert_timescale(pg, names)
        report.body["timescale_columnar_conversion"] = conversion
        report.phase("tier_transition", str(conversion["status"]))
        if conversion["status"] == "converted":
            await analyze_timescale(pg, names)
            await verify_all(varve, pg, names, oracle, base_us, args.rows)
            report.body.setdefault("query_stages", {})["columnar_checkpointed"] = (
                await run_query_suite(
                    varve,
                    pg,
                    query_cases(names, oracle, base_us, args.rows),
                    args.query_samples,
                    report,
                    "columnar_checkpointed",
                    query_order,
                    redactor(credentials),
                )
            )
            report.phase("columnar_checkpointed_queries", "passed")
        else:
            report.body.setdefault("query_stages", {})["columnar_checkpointed"] = {
                "state": "not_run",
                "reason": f"Timescale conversion {conversion['status']}",
            }
            report.body["unsupported_capabilities"] = [
                f"Timescale columnar conversion {conversion['status']}"
            ]
            report.phase("columnar_checkpointed_queries", "not_run")

        baseline = Stats(
            count=oracle.all.count,
            sum_q=oracle.all.sum_q,
            min_q=oracle.all.min_q,
            max_q=oracle.all.max_q,
        )
        report.body.setdefault("runtime_snapshots", []).append(
            await runtime_snapshot(varve, pg, "before_mixed_arrivals")
        )
        report.flush()
        try:
            mixed = await mixed_workload(
                varve, pg, names, args, base_us, baseline, oracle, deadline,
                redactor(credentials),
            )
        except MixedWorkloadError as error:
            partial = error.partial
            report.body["mixed_workload"] = partial
            report.body["manifest"]["mixed_paired_acknowledged_rows"] = partial[
                "acknowledged_rows"
            ]
            report.body["manifest"]["mixed_varve_acknowledged_rows"] = partial[
                "varve_acknowledged_rows"
            ]
            report.body["manifest"]["mixed_timescale_acknowledged_rows"] = partial[
                "timescale_acknowledged_rows"
            ]
            report.body["manifest"]["mixed_failed_or_ambiguous_rows"] = partial[
                "failed_or_ambiguous_rows"
            ]
            report.body["manifest"]["mixed_pending_rows"] = partial["pending_rows"]
            report.body["manifest"]["total_reconciled_watermark_rows"] = oracle.all.count
            report.body["oracles"] = oracle.snapshot()
            report.flush()
            raise
        report.body["mixed_workload"] = mixed
        if not mixed["clean"]:
            report.body["overloaded"] = {
                "reason": "finite intended arrivals missed the strict bounded scheduling gate",
                "data_complete": mixed["data_complete"],
                "schedule_met": mixed["schedule_met"],
                "never_submitted_rows": mixed["never_submitted_rows"],
                "pending_rows": mixed["pending_rows"],
            }
        report.body["manifest"]["mixed_paired_acknowledged_rows"] = mixed[
            "acknowledged_rows"
        ]
        report.body["manifest"]["mixed_varve_acknowledged_rows"] = mixed[
            "varve_acknowledged_rows"
        ]
        report.body["manifest"]["mixed_timescale_acknowledged_rows"] = mixed[
            "timescale_acknowledged_rows"
        ]
        report.body["manifest"]["mixed_failed_or_ambiguous_rows"] = mixed[
            "failed_or_ambiguous_rows"
        ]
        report.body["manifest"]["mixed_pending_rows"] = mixed["pending_rows"]
        report.body["manifest"]["total_reconciled_watermark_rows"] = oracle.all.count
        report.body["oracles"] = oracle.snapshot()
        report.body.setdefault("runtime_snapshots", []).append(
            await runtime_snapshot(varve, pg, "after_mixed_full_drain")
        )
        report.flush()

        mixed_refresh = await refresh_timescale(pg, names)
        mixed_checkpoint = await checkpoint_varve(varve, names)
        report.body["mixed_freshness_barrier"] = {
            "timescale_explicit_refresh_seconds": mixed_refresh,
            "varve_eager_aggregate": True,
            "varve_checkpoint": mixed_checkpoint,
            "same_reconciled_watermark_rows": oracle.all.count,
        }
        await analyze_timescale(pg, names)
        report.body["stable_id_retry_drill"] = await stable_retry_drill(
            varve,
            pg,
            names,
            args.run_id,
            base_us,
            args.rows,
            args.batch,
            deadline,
            include_conflicting=True,
        )
        await verify_all(varve, pg, names, oracle, base_us, args.rows)
        report.body["final_raw_fingerprints"] = report.body["stable_id_retry_drill"][
            "fingerprints_after"
        ]
        report.body.setdefault("runtime_snapshots", []).append(
            await runtime_snapshot(varve, pg, "after_final_verification")
        )
        report.phase("mixed_barrier_retry_drills_and_final_correctness", "passed")
    finally:
        await varve.close()
        await pg.close()


async def run_verify_only(
    args: argparse.Namespace,
    credentials: dict[str, str],
    report: Report,
    deadline: Deadline,
) -> None:
    source = json.loads(Path(args.verify_only).read_text())
    raw_names = source["namespaces"]
    names = Names(**raw_names)
    for value in names.json().values():
        checked_identifier(value)
    expected = expected_stats(source["oracles"]["global"])
    varve = VarveClient(credentials["VARVE_URL"], credentials["VARVE_API_TOKEN"], deadline, 1)
    pg = TimescaleClient(credentials, deadline)
    try:
        await pg.open(1)
        status = await varve.request("GET", "/v1/status")
        rebuilt = rebuilt_status(status)
        report.body["actual_varve_preflight_status"] = status
        report.body["rebuilt_preflight"] = rebuilt
        report.flush()
        report.body.setdefault("runtime_snapshots", []).append(
            await runtime_snapshot(varve, pg, "restart_before_retry")
        )
        report.flush()
        if args.require_rebuilt and not rebuilt["passed"]:
            report.phase("restart_preflight", "failed", require_rebuilt=True)
            raise RuntimeError("--require-rebuilt exact persisted journal/native identity check failed")
        report.phase("restart_preflight", "passed", require_rebuilt=args.require_rebuilt)

        expected_fingerprints = source["final_raw_fingerprints"]
        observed_before = await raw_fingerprints(varve, pg, names)
        if observed_before != expected_fingerprints:
            raise RuntimeError("restart raw fingerprint or database identity differs from source report")
        source_configuration = source["configuration"]
        source_manifest = source["manifest"]
        source_run_id = checked_run_id(str(source["run_id"]))
        source_initial_rows = int(source_manifest["initial_rows"])
        source_batch = int(source_configuration["batch"])
        if not 1 <= source_initial_rows <= 1_000_000:
            raise RuntimeError("source report initial row bound is invalid")
        if not 1 <= source_batch <= min(10_000, source_initial_rows):
            raise RuntimeError("source report initial batch bound is invalid")
        retry = await stable_retry_drill(
            varve,
            pg,
            names,
            source_run_id,
            int(source_manifest["base_timestamp_us"]),
            source_initial_rows,
            source_batch,
            deadline,
            include_conflicting=False,
        )
        if retry["fingerprints_before"] != expected_fingerprints:
            raise RuntimeError("restart fingerprint changed before identical retry")
        report.body["restart_identical_retry_drill"] = retry
        report.body["restart_raw_fingerprints"] = retry["fingerprints_after"]

        projection = "count(*) AS n, sum(value) AS total, min(value) AS min, max(value) AS max"
        varve_raw = await varve.sql(f"SELECT {projection} FROM {names.varve_table}")
        varve_rollup = await varve.sql(
            f"SELECT CAST(sum(count) AS BIGINT) AS n, sum(sum) AS total, min(min) AS min, max(max) AS max FROM {names.varve_aggregate}"
        )
        pg_raw = await pg.query(
            sql.SQL(
                "SELECT count(*)::bigint AS n, sum(value)::double precision AS total, "
                "min(value)::double precision AS min, max(value)::double precision AS max FROM {}"
            ).format(qualified(names.pg_schema, names.pg_table))
        )
        pg_rollup = await pg.query(
            sql.SQL(
                "SELECT sum(count)::bigint AS n, sum(sum)::double precision AS total, "
                "min(min)::double precision AS min, max(max)::double precision AS max FROM {}"
            ).format(qualified(names.pg_schema, names.pg_aggregate))
        )
        for label, rows in (
            ("Varve raw", varve_raw),
            ("Varve aggregate", varve_rollup),
            ("Timescale raw", pg_raw),
            ("Timescale continuous aggregate", pg_rollup),
        ):
            if len(rows) != 1:
                raise RuntimeError(f"{label} returned an unexpected row count")
            assert_stats(rows[0], expected, label)
        report.body.setdefault("runtime_snapshots", []).append(
            await runtime_snapshot(varve, pg, "restart_after_identical_retry")
        )
        report.body["source_report"] = str(Path(args.verify_only).resolve())
        report.body["expected_watermark_rows"] = expected.count
        report.body["checks"] = [
            "Varve raw count/sum/min/max",
            "Varve eager aggregate count/sum/min/max",
            "Timescale raw count/sum/min/max",
            "Timescale retained fresh continuous aggregate count/sum/min/max",
            "exact timestamp moments and both database identities unchanged across restart",
            "stable-ID identical retry remained duplicate on both backends without raw change",
        ]
        report.phase("restart_verify_only", "passed")
    finally:
        await varve.close()
        await pg.close()


def bounded_int(name: str, minimum: int, maximum: int) -> Callable[[str], int]:
    def parse(value: str) -> int:
        try:
            return parse_bounded_int(value, name, minimum, maximum)
        except ValueError as error:
            raise argparse.ArgumentTypeError(str(error)) from None
    return parse


def bounded_float(name: str, minimum: float, maximum: float) -> Callable[[str], float]:
    def parse(value: str) -> float:
        try:
            return parse_bounded_float(value, name, minimum, maximum)
        except ValueError as error:
            raise argparse.ArgumentTypeError(str(error)) from None
    return parse


def arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=bounded_int("rows", 1, 1_000_000), default=250_000)
    parser.add_argument("--batch", type=bounded_int("batch", 1, 10_000), default=1_000)
    parser.add_argument("--writers", type=bounded_int("writers", 1, 16), default=4)
    parser.add_argument("--query-samples", type=bounded_int("query-samples", 1, 200), default=50)
    parser.add_argument("--mixed-seconds", type=bounded_int("mixed-seconds", 1, 180), default=60)
    parser.add_argument("--rate", type=bounded_float("rate", 1, 20_000), default=5_000.0)
    parser.add_argument(
        "--mixed-read-interval",
        type=bounded_float("mixed-read-interval", 0.01, 10.0),
        default=1.0,
    )
    parser.add_argument("--mixed-readers", type=bounded_int("mixed-readers", 1, 2), default=1)
    parser.add_argument("--drain-seconds", type=bounded_int("drain-seconds", 1, 300), default=60)
    parser.add_argument("--max-seconds", type=bounded_int("max-seconds", 1, 1_800), default=1_200)
    parser.add_argument("--require-rebuilt", action="store_true")
    parser.add_argument("--attestation", type=parse_attestation, metavar="JSON")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--run-id")
    parser.add_argument("--verify-only", type=Path, metavar="REPORT")
    args = parser.parse_args(argv)
    if args.verify_only is None:
        args.run_id = checked_run_id(args.run_id or ("r" + uuid.uuid4().hex[:15]))
        args.output = args.output or Path(f"/results/{args.run_id}.json")
        if args.output.exists():
            parser.error("output already exists; choose a new --run-id or --output")
    else:
        if not args.verify_only.is_file():
            parser.error("--verify-only report does not exist")
        args.run_id = checked_run_id(args.run_id) if args.run_id else "verify"
        args.output = args.output or args.verify_only.with_suffix(".verify.json")
        if args.output.exists():
            parser.error("verification output already exists; choose a new --output")
    if args.batch > args.rows and args.verify_only is None:
        parser.error("batch must not exceed rows")
    if args.verify_only is None:
        mixed_rows_count = int(args.rate * args.mixed_seconds)
        if args.rows + mixed_rows_count > 1_000_000:
            parser.error("initial plus intended mixed rows must not exceed 1,000,000")
        request_count = math.ceil(args.rows / args.batch) + math.ceil(
            mixed_rows_count / args.batch
        )
        if request_count > 200_000:
            parser.error("intended request IDs exceed the 200,000-key benchmark profile")
    return args


def base_report(args: argparse.Namespace) -> dict[str, object]:
    return {
        "state": "running",
        "started_at": utc_now(),
        "mode": "verify_only" if args.verify_only else "benchmark",
        "run_id": args.run_id,
        "workload_stage": (
            "baseline_250k_synthetic_capability"
            if args.rows == 250_000 and args.batch == 1_000 and args.writers == 4
            else "bounded_custom_synthetic_capability"
        ),
        "artifact": artifact_metadata(),
        "external_attestation": {
            "value": args.attestation,
            "supplied": args.attestation is not None,
            "provenance": "client-supplied by Main; retained verbatim and not measured by this process",
            "measured_runtime_proof": False,
        },
        "generator": {
            "platform": generator_platform(),
            "cpu_start": cpu_snapshot(),
            "scope": "driver process/driver cgroup only; database platform metrics and process identities are external",
        },
        "configuration": {
            "rows": args.rows,
            "batch": args.batch,
            "writers": args.writers,
            "query_samples": args.query_samples,
            "mixed_seconds": args.mixed_seconds,
            "offered_rate_rows_per_second": args.rate,
            "mixed_read_interval_seconds": args.mixed_read_interval,
            "mixed_readers": args.mixed_readers,
            "drain_seconds": args.drain_seconds,
            "outer_wall_clock_seconds": args.max_seconds,
            "require_rebuilt": args.require_rebuilt,
            "per_call_timeout_seconds": CALL_TIMEOUT,
            "automatic_retries_after_ambiguous_ack": 0,
        },
        "infrastructure_assumptions": {
            "region": "Singapore; supplied by infrastructure owner, not independently detected",
            "varve_database": "2 vCPU / 2,000,000,000 bytes; externally supplied",
            "timescale_database": "2 vCPU / 2,000,000,000 bytes; externally supplied",
            "generator": "2 vCPU / 1,000,000,000 bytes; externally supplied",
            "timescale_image": TIMESCALE_IMAGE,
            "timescale_image_digest": TIMESCALE_DIGEST,
            "varve_benchmark_config": {
                "max_batch_rows": 10000,
                "max_batch_bytes": 4194304,
                "max_idempotency_keys": 200000,
                "hot_max_bytes": 134217728,
                "metadata_max_bytes": 67108864,
                "query_memory_bytes_per_worker": 268435456,
                "query_threads": 2,
                "query_workers": 2,
                "query_timeout_seconds": 30,
            },
            "ownership": "database CPU/memory/disk metrics, database process identities, infrastructure, secrets, and cleanup are external; runtime snapshots expose timestamps for Main to join",
        },
        "claim_boundary": (
            "Generic synthetic single-node capability evidence only; not user workload compatibility, "
            "distributed behavior, high availability, or production certification. Short-run percentiles "
            "are observational and are not p99 qualification."
        ),
        "phases": [],
    }


async def async_main(args: argparse.Namespace, credentials: dict[str, str], report: Report) -> None:
    deadline = Deadline(args.max_seconds)
    if args.verify_only:
        await run_verify_only(args, credentials, report, deadline)
    else:
        await run_fresh(args, credentials, report, deadline)




def finish_generator_metrics(body: dict[str, object]) -> None:
    generator = body["generator"]
    assert isinstance(generator, dict)
    start = generator["cpu_start"]
    assert isinstance(start, dict)
    end = cpu_snapshot()
    generator["cpu_end"] = end
    generator["cpu_delta"] = {
        "user_seconds": end["user_seconds"] - float(start["user_seconds"]),
        "system_seconds": end["system_seconds"] - float(start["system_seconds"]),
    }

def main(argv: list[str] | None = None) -> int:
    args = arguments(argv)
    body = base_report(args)
    report = Report(args.output, body)
    report.flush()
    credentials: dict[str, str] = {}
    try:
        credentials = load_credentials()
        asyncio.run(asyncio.wait_for(async_main(args, credentials, report), timeout=args.max_seconds))
        if body.get("unsupported_capabilities"):
            terminal_state = "partial_unsupported"
        elif body.get("overloaded"):
            terminal_state = "overloaded"
        else:
            terminal_state = "passed"
        body["state"] = terminal_state
        body["finished_at"] = utc_now()
        finish_generator_metrics(body)
        report.flush()
        emit("final", state=terminal_state, output=str(args.output))
        return 0 if terminal_state == "passed" else 1
    except BaseException as error:
        clean = redactor(credentials)(error) if credentials else f"{type(error).__name__}: {error}"
        body["state"] = "failed"
        body["finished_at"] = utc_now()
        body["failure"] = clean
        finish_generator_metrics(body)
        report.flush()
        emit("final", state="failed", output=str(args.output), error=clean)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
