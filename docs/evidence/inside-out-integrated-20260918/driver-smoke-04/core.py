"""Pure, offline-testable primitives for the Timescale comparison runner."""
from __future__ import annotations

import hashlib
import json
import math
import os
import platform
import re
import resource
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterable

SERIES_COUNT = 1024
TENANT_COUNT = 4
BUCKET_US = 60_000_000
DATASET_SEED = 20260915
_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_RUN_ID = re.compile(r"^[a-z0-9][a-z0-9_]{0,23}$")
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True, slots=True)
class Measurement:
    timestamp_us: int
    tenant: str
    series: str
    value_q: int

    @property
    def value(self) -> float:
        return self.value_q / 4.0

    def varve(self) -> dict[str, object]:
        return {
            "timestamp_us": self.timestamp_us,
            "tenant": self.tenant,
            "series": self.series,
            "value": self.value,
            "tags": {},
        }

    def postgres(self) -> tuple[datetime, str, str, float, dict[str, str]]:
        return (
            _EPOCH + timedelta(microseconds=self.timestamp_us),
            self.tenant,
            self.series,
            self.value,
            {},
        )


def aligned_base_us(now_us: int) -> int:
    """Minute-aligned base approximately 24 hours before the supplied clock."""
    return ((now_us - 24 * 60 * 60 * 1_000_000) // BUCKET_US) * BUCKET_US


def measurement(index: int, base_us: int) -> Measurement:
    if index < 0:
        raise ValueError("measurement index must be nonnegative")
    series_index = index % SERIES_COUNT
    return Measurement(
        timestamp_us=base_us + (index // SERIES_COUNT) * 1_000_000 + series_index,
        tenant=f"tenant_{(index // SERIES_COUNT) % TENANT_COUNT}",
        series=f"series_{series_index:04d}",
        value_q=(index * 17) % 10_000,
    )


def late_measurement(index: int, base_us: int) -> Measurement:
    """A fixed, deliberately out-of-order stream strictly before the initial watermark."""
    if index < 0:
        raise ValueError("late measurement index must be nonnegative")
    series_index = (index * 73) % SERIES_COUNT
    return Measurement(
        timestamp_us=base_us - 60_000_000 - (index % 16) * 1_000_000 + series_index,
        tenant=f"tenant_{index % TENANT_COUNT}",
        series=f"series_{series_index:04d}",
        value_q=-4_000 + (index * 29) % 8_000,
    )


@dataclass(slots=True)
class Stats:
    count: int = 0
    sum_q: int = 0
    min_q: int | None = None
    max_q: int | None = None

    def add(self, row: Measurement) -> None:
        self.count += 1
        self.sum_q += row.value_q
        self.min_q = row.value_q if self.min_q is None else min(self.min_q, row.value_q)
        self.max_q = row.value_q if self.max_q is None else max(self.max_q, row.value_q)

    def json(self) -> dict[str, int | float | None]:
        return {
            "count": self.count,
            "sum": self.sum_q / 4.0,
            "min": None if self.min_q is None else self.min_q / 4.0,
            "max": None if self.max_q is None else self.max_q / 4.0,
        }


class Oracle:
    """Independent integer-quarter oracles; only one selected group's raw rows are retained."""

    selected = ("tenant_0", "series_0000")

    def __init__(self) -> None:
        self.all = Stats()
        self.tenants: dict[str, Stats] = {}
        self.groups: dict[tuple[str, str], Stats] = {}
        self.selected_buckets: dict[int, Stats] = {}
        self.selected_rows: list[Measurement] = []

    def add(self, row: Measurement) -> None:
        self.all.add(row)
        self.tenants.setdefault(row.tenant, Stats()).add(row)
        self.groups.setdefault((row.tenant, row.series), Stats()).add(row)
        if (row.tenant, row.series) == self.selected:
            bucket = (row.timestamp_us // BUCKET_US) * BUCKET_US
            self.selected_buckets.setdefault(bucket, Stats()).add(row)
            self.selected_rows.append(row)

    def extend(self, rows: Iterable[Measurement]) -> None:
        for row in rows:
            self.add(row)

    def selected_window(self, limit: int = 5) -> list[dict[str, int | float]]:
        ordered = sorted(self.selected_rows, key=lambda row: (row.timestamp_us, row.value_q), reverse=True)[:limit]
        return [{"timestamp_us": row.timestamp_us, "value": row.value} for row in ordered]

    def selected_recent(self, start_us: int, end_us: int) -> Stats:
        result = Stats()
        for row in self.selected_rows:
            if start_us <= row.timestamp_us < end_us:
                result.add(row)
        return result

    def snapshot(self) -> dict[str, object]:
        return {
            "global": self.all.json(),
            "tenants": {key: self.tenants[key].json() for key in sorted(self.tenants)},
            "selected_group": self.groups.get(self.selected, Stats()).json(),
            "selected_buckets": {
                str(key): self.selected_buckets[key].json() for key in sorted(self.selected_buckets)
            },
            "selected_window": self.selected_window(),
        }


def canonical_json_sha256(value: object) -> str:
    """Hash JSON with a pinned, finite canonical encoding."""
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def canonical_payload_digest(rows: Iterable[Measurement]) -> str:
    """Digest the exact logical batch payload, including order and quarter values."""
    payload = [
        [row.timestamp_us, row.tenant, row.series, row.value_q, {}]
        for row in rows
    ]
    return canonical_json_sha256({"format": "varve-timescale-batch-v1", "rows": payload})


def percentile(values: Iterable[float], fraction: float) -> float | None:
    if not 0 < fraction <= 1:
        raise ValueError("fraction must be in (0, 1]")
    ordered = sorted(values)
    if not ordered:
        return None
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def latency_summary(values: list[float]) -> dict[str, int | float | None]:
    return {
        "samples": len(values),
        "p50_ms": percentile(values, 0.50),
        "p95_ms": percentile(values, 0.95),
        "p99_ms": percentile(values, 0.99),
        "max_ms": max(values) if values else None,
    }




def parse_bounded_int(value: str, name: str, minimum: int, maximum: int) -> int:
    try:
        parsed = int(value)
    except ValueError:
        raise ValueError(f"{name} must be an integer") from None
    if not minimum <= parsed <= maximum:
        raise ValueError(f"{name} must be in {minimum}..{maximum}")
    return parsed


def parse_bounded_float(value: str, name: str, minimum: float, maximum: float) -> float:
    try:
        parsed = float(value)
    except ValueError:
        raise ValueError(f"{name} must be numeric") from None
    if not math.isfinite(parsed) or not minimum <= parsed <= maximum:
        raise ValueError(f"{name} must be finite and in {minimum}..{maximum}")
    return parsed

def checked_identifier(value: str) -> str:
    if not _IDENTIFIER.fullmatch(value):
        raise ValueError("unsafe SQL identifier")
    return value


def checked_run_id(value: str) -> str:
    if not _RUN_ID.fullmatch(value):
        raise ValueError("run-id must match [a-z0-9][a-z0-9_]{0,23}")
    return value


class Deadline:
    def __init__(self, seconds: float, clock: Callable[[], float] = time.monotonic) -> None:
        if not 0 < seconds <= 1800:
            raise ValueError("deadline seconds must be in (0, 1800]")
        self._clock = clock
        self.started = clock()
        self.ends = self.started + seconds

    def remaining(self) -> float:
        return max(0.0, self.ends - self._clock())

    def timeout(self, per_call: float) -> float:
        remaining = self.remaining()
        if remaining <= 0:
            raise TimeoutError("outer benchmark wall-clock ceiling reached")
        return min(per_call, remaining)


def cpu_snapshot() -> dict[str, float]:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return {"user_seconds": usage.ru_utime, "system_seconds": usage.ru_stime}


def generator_platform() -> dict[str, object]:
    cgroup: dict[str, str] = {}
    for name in ("cpu.max", "memory.max"):
        path = f"/sys/fs/cgroup/{name}"
        try:
            with open(path, encoding="ascii") as handle:
                cgroup[name] = handle.read(128).strip()
        except (OSError, UnicodeError):
            cgroup[name] = "unavailable"
    return {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "logical_cpus": os.cpu_count(),
        "cgroup_v2": cgroup,
    }
