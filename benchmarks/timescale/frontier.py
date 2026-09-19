"""Bounded local HTTP performance diagnosis, not a Timescale or cloud qualification.

A fresh owned process/root per invocation; no write retries. Service timers and
client intended-arrival timestamps are distinct. Run traced diagnosis separately
from untraced counterbalanced performance measurements.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import secrets
import signal
import socket
import subprocess
import time

import aiohttp
from benchmark import redactor
from core import Oracle, aligned_base_us, measurement

MAX_RESPONSE = 8 * 1024 * 1024


def digest(path):
    hasher = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


def summary(values):
    values = sorted(values)
    if not values:
        return {"samples": 0}
    def quantile(q):
        return values[max(0, math.ceil(q * len(values)) - 1)]
    return {"samples": len(values), "p50_ms": quantile(.5), "p95_ms": quantile(.95),
            "p99_ms": quantile(.99), "max_ms": values[-1],
            "p99_minimum_sample_gate": len(values) >= 1000}


def conservation(offered, acknowledged, dropped, failed, pending):
    if min(offered, acknowledged, dropped, failed, pending) < 0:
        raise ValueError("negative work accounting")
    if offered != acknowledged + dropped + failed + pending:
        raise ValueError("work conservation violated")
    return dropped == failed == pending == 0


def metrics_delta(before, after):
    def parse(text):
        result = {}
        for line in text.splitlines():
            if line and not line.startswith("#"):
                name, value = line.rsplit(" ", 1)
                if "_seconds_sum{" in name or "_seconds_count{" in name or name.endswith("_total"):
                    result[name] = float(value)
        return result
    left, right = parse(before), parse(after)
    result = {}
    for name, value in right.items():
        old = left.get(name, 0)
        if value < old:
            raise ValueError("counter reset during measurement")
        result[name] = value - old
    return result


class Client:
    def __init__(self, port, token, connections):
        self.describe_error = redactor({"token": token})
        self.base = f"http://127.0.0.1:{port}"
        self.session = aiohttp.ClientSession(headers={"Authorization": f"Bearer {token}"},
            connector=aiohttp.TCPConnector(limit=connections + 4),
            timeout=aiohttp.ClientTimeout(total=15), trust_env=False)

    async def request(self, method, path, body=None, optional=False):
        async with self.session.request(method, self.base + path, json=body,
                allow_redirects=False) as response:
            raw = bytearray()
            async for block in response.content.iter_chunked(65536):
                raw.extend(block)
                if len(raw) > MAX_RESPONSE:
                    raise RuntimeError("bounded response exceeded")
            if optional and response.status == 404:
                return None
            if response.status != 200:
                raise RuntimeError(f"HTTP {response.status}: {raw[:300].decode(errors='replace')}")
            return raw.decode() if path == "/metrics" else json.loads(raw)

    async def sql(self, sql):
        return await self.request("POST", "/v1/query", {"sql": sql})


FRESH_SQL = """SELECT raw.n AS n, raw.total AS total, raw.lo AS lo, raw.hi AS hi,
    rollup.n AS rollup_n, rollup.total AS rollup_total, rollup.lo AS rollup_lo, rollup.hi AS rollup_hi
FROM (SELECT count(*) AS n, sum(value) AS total, min(value) AS lo, max(value) AS hi FROM metrics) raw
CROSS JOIN (SELECT CAST(sum(count) AS BIGINT) AS n, sum(sum) AS total, min(min) AS lo, max(max) AS hi FROM minute_rollup) rollup"""


def validate_fresh_read(result, acknowledged_floor, offered_ceiling):
    if not isinstance(result, list) or len(result) != 1 or not isinstance(result[0], dict):
        raise RuntimeError("fresh mixed-read shape mismatch")
    row = result[0]
    if type(row.get("n")) is not int or not acknowledged_floor <= row["n"] <= offered_ceiling:
        raise RuntimeError(f"fresh mixed-read acknowledgment bounds violated: {row.get('n')} not in [{acknowledged_floor}, {offered_ceiling}]")
    for name in ("n", "total", "lo", "hi"):
        value = row.get(name)
        if type(value) not in (int, float) or not math.isfinite(value) or value != row.get("rollup_" + name):
            raise RuntimeError(f"fresh raw/rollup snapshot mismatch: {name}={str(value)[:80]} rollup={str(row.get('rollup_' + name))[:80]}")
    if row["lo"] > row["hi"]:
        raise RuntimeError("fresh mixed-read extrema inverted")
    return row["n"]


async def scheduled_load(client, args, base, events, prefix_sum=None, progress=None):
    """A finite intended trace, bounded producers, and no load shedding/retries.

    Offered counts describe the complete intended trace, including unsubmitted
    work if the run is cancelled. Logical backlog is represented by a cursor,
    never an unbounded list of payloads or tasks. Original arrival times survive
    backpressure, and a completed drain alone does not make a schedule pass.
    """
    target = int(args.rate * args.seconds)
    readers = getattr(args, "readers", 1)
    read_target = max(0, math.ceil(args.seconds / args.read_interval) - 1) if args.read_interval else 0
    queue = asyncio.Queue(maxsize=args.writers * 2)
    read_queue = asyncio.Queue(maxsize=readers * 2)
    samples, reads, acknowledged_offsets = [], [], []
    counts = {"offered": target, "acknowledged": 0, "dropped": 0, "failed": 0}
    read_counts = {"offered": read_target, "completed": 0, "failed": 0}
    flow = {"write_enqueued_rows": 0, "read_enqueued": 0,
            "peak_write_queue_requests": 0, "peak_read_queue_requests": 0,
            "max_write_enqueue_lag_ms": 0.0, "max_read_enqueue_lag_ms": 0.0,
            "max_write_start_lag_ms": 0.0, "max_read_start_lag_ms": 0.0}
    bound = measurement(args.rows, base).timestamp_us
    read_mode = getattr(args, "read_mode", "prefix")
    if args.read_interval and read_mode == "prefix" and prefix_sum is None:
        prefix_sum = sum(measurement(i, base).value_q for i in range(args.rows)) / 4
    if progress is not None:
        progress.update(partial_load=counts, partial_reads=read_counts, scheduling=flow)
    start = time.monotonic()

    def event(value):
        events.write(json.dumps(value, separators=(",", ":")) + "\n")
        events.flush()

    async def writer():
        while True:
            item = await queue.get()
            if item is None:
                queue.task_done()
                return
            offset, count, intended = item
            picked = time.monotonic()
            flow["max_write_start_lag_ms"] = max(flow["max_write_start_lag_ms"], (picked - intended) * 1000)
            record = {"kind": "write", "offset": offset, "rows": count,
                      "intended_s": intended - start, "queue_ms": (picked - intended) * 1000}
            try:
                rows = [measurement(i, base).varve() for i in range(offset, offset + count)]
                body = {"table": "metrics", "request_id": f"batch-{offset}", "rows": rows}
                submitted = time.monotonic()
                record["generation_ms"] = (submitted - picked) * 1000
                receipt = await client.request("POST", "/v1/write", body)
                if receipt.get("durability") != "local_fsync" or receipt.get("duplicate") is not False or receipt.get("rows") != count:
                    raise RuntimeError("invalid fresh durable receipt")
                completed = time.monotonic()
                record.update(ack_ms=(completed - submitted) * 1000,
                              end_to_end_ms=(completed - intended) * 1000, receipt=receipt)
                counts["acknowledged"] += count
                acknowledged_offsets.append((offset, count))
                samples.append(record)
                event(record)
            except BaseException as error:
                counts["failed"] += count
                event({**record, "failed_or_ambiguous": getattr(client, "describe_error", redactor({}))(error)})
                raise
            finally:
                queue.task_done()

    async def reader():
        sql = FRESH_SQL if read_mode == "fresh" else f"SELECT count(*) AS n, sum(value) AS total FROM metrics WHERE timestamp_us < {bound}"
        while True:
            item = await read_queue.get()
            if item is None:
                read_queue.task_done()
                return
            ordinal, intended = item
            before = time.monotonic()
            flow["max_read_start_lag_ms"] = max(flow["max_read_start_lag_ms"], (before - intended) * 1000)
            acknowledged_floor = args.rows + counts["acknowledged"]
            record = {"kind": "read", "mode": read_mode, "ordinal": ordinal,
                      "intended_s": intended - start, "queue_ms": (before - intended) * 1000}
            try:
                result = await client.sql(sql)
                completed = time.monotonic()
                # Enqueued writes may commit before their receipt arrives. Never
                # substitute future, not-yet-enqueued planned work as an oracle.
                offered_ceiling = args.rows + flow["write_enqueued_rows"]
                if read_mode == "fresh":
                    n = validate_fresh_read(result, acknowledged_floor, offered_ceiling)
                else:
                    if result != [{"n": args.rows, "total": prefix_sum}]:
                        raise RuntimeError("stable-prefix mixed-read oracle mismatch")
                    n = args.rows
                record.update(ms=(completed - before) * 1000,
                              end_to_end_ms=(completed - intended) * 1000, rows=n,
                              acknowledged_floor_rows=acknowledged_floor, offered_ceiling_rows=offered_ceiling)
                read_counts["completed"] += 1
                reads.append(record)
                event(record)
            except BaseException as error:
                read_counts["failed"] += 1
                failure = {**record, "ms": (time.monotonic() - before) * 1000,
                           "failed": getattr(client, "describe_error", redactor({}))(error)}
                event(failure)
                if progress is not None:
                    progress["failed_read"] = failure
                raise
            finally:
                read_queue.task_done()

    async def produce_writes():
        for relative in range(0, target, args.batch):
            intended = start + relative / args.rate
            await asyncio.sleep(max(0, intended - time.monotonic()))
            count = min(args.batch, target - relative)
            await queue.put((args.rows + relative, count, intended))
            flow["write_enqueued_rows"] += count
            flow["peak_write_queue_requests"] = max(flow["peak_write_queue_requests"], queue.qsize())
            flow["max_write_enqueue_lag_ms"] = max(flow["max_write_enqueue_lag_ms"], (time.monotonic() - intended) * 1000)
        await asyncio.sleep(max(0, start + args.seconds - time.monotonic()))
        await queue.join()
        for _ in range(args.writers):
            await queue.put(None)

    async def produce_reads():
        for ordinal in range(1, read_target + 1):
            intended = start + ordinal * args.read_interval
            await asyncio.sleep(max(0, intended - time.monotonic()))
            await read_queue.put((ordinal, intended))
            flow["read_enqueued"] += 1
            flow["peak_read_queue_requests"] = max(flow["peak_read_queue_requests"], read_queue.qsize())
            flow["max_read_enqueue_lag_ms"] = max(flow["max_read_enqueue_lag_ms"], (time.monotonic() - intended) * 1000)
        await read_queue.join()
        for _ in range(readers):
            await read_queue.put(None)

    try:
        async with asyncio.timeout(args.seconds + getattr(args, "drain_seconds", 30)):
            async with asyncio.TaskGroup() as group:
                for _ in range(args.writers):
                    group.create_task(writer())
                group.create_task(produce_writes())
                if read_target:
                    for _ in range(readers):
                        group.create_task(reader())
                    group.create_task(produce_reads())
    finally:
        counts["pending"] = target - counts["acknowledged"] - counts["failed"]
        read_counts["pending"] = read_target - read_counts["completed"] - read_counts["failed"]
        flow["write_unsubmitted_rows"] = target - flow["write_enqueued_rows"]
        flow["read_unsubmitted"] = read_target - flow["read_enqueued"]
        flow["elapsed_with_drain_s"] = time.monotonic() - start
        conservation(**counts)
        if read_counts["pending"] < 0:
            raise RuntimeError("read conservation violated")
    elapsed = flow["elapsed_with_drain_s"]
    data_complete = conservation(**counts) and read_counts["failed"] == read_counts["pending"] == 0
    # More than one full arrival interval of scheduling debt is an overload
    # diagnostic, even if every byte is eventually acknowledged during drain.
    schedule_met = (flow["max_write_start_lag_ms"] <= args.batch / args.rate * 1000
                    and (not read_target or flow["max_read_start_lag_ms"] <= args.read_interval * 1000))
    return {**counts, "clean": data_complete and schedule_met, "data_complete": data_complete,
            "schedule_met": schedule_met, "reads": read_counts, "scheduling": flow,
            "elapsed_with_drain_s": elapsed, "drain_s": max(0, elapsed - args.seconds),
            "acknowledged_rows_per_s": counts["acknowledged"] / elapsed,
            "ack_latency": summary([s["ack_ms"] for s in samples]),
            "arrival_latency": summary([s["end_to_end_ms"] for s in samples]),
            "queue_latency": summary([s["queue_ms"] for s in samples]),
            "read_latency": summary([r["ms"] for r in reads]),
            "read_arrival_latency": summary([r["end_to_end_ms"] for r in reads])}, acknowledged_offsets

async def verify(client, oracle):
    expected = {"n": oracle.all.count, "total": oracle.all.sum_q / 4,
                "min": oracle.all.min_q / 4, "max": oracle.all.max_q / 4}
    raw = await client.sql("SELECT count(*) AS n, sum(value) AS total, min(value) AS min, max(value) AS max FROM metrics")
    rollup = await client.sql("SELECT CAST(sum(count) AS BIGINT) AS n, sum(sum) AS total, min(min) AS min, max(max) AS max FROM minute_rollup")
    if raw != [expected] or rollup != [expected]:
        raise RuntimeError("raw/rollup quarter oracle mismatch")
    return expected


async def campaign(args, output, report):
    root = output / "data"
    profile = Path(args.profile).resolve(strict=True)
    config = json.loads(profile.read_text())
    binary = Path(args.binary).resolve(strict=True)
    duckdb = Path(config["query_executable"]).resolve(strict=True)
    if digest(binary) != args.expected_binary_sha256:
        raise ValueError("binary hash does not match supplied qualification")
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    token = secrets.token_hex(32)
    environment = {k: v for k, v in os.environ.items() if not k.startswith(("VARVE_", "PG"))}
    environment.update(VARVE_API_TOKEN=token, VARVE_S3_ENABLED="false",
                       VARVE_INGEST_TRACE_CAPACITY=str(args.trace_capacity))
    command = [str(binary), "--data", str(root), "--config", str(profile), "serve", "--bind", "127.0.0.1", "--port", str(port)]
    report["identity"] = {"binary": str(binary), "binary_sha256": digest(binary),
        "profile_sha256": digest(profile), "profile": config, "duckdb_sha256": digest(duckdb),
        "probe_sha256": digest(__file__), "core_sha256": digest(Path(__file__).with_name("core.py")),
        "benchmark_sha256": digest(Path(__file__).with_name("benchmark.py")),
        "platform": platform.platform(), "python": platform.python_version(),
        "resource_scope": "local host; no cgroup or dedicated-CPU parity claim"}
    process = None
    client = None
    with (output / "server.log").open("wb") as server_log:
        try:
            process = subprocess.Popen(command, env=environment, stdin=subprocess.DEVNULL,
                stdout=server_log, stderr=subprocess.STDOUT, start_new_session=True)
            report["server_pid"] = process.pid
            client = Client(port, token, args.writers)
            startup = time.monotonic() + 15
            while True:
                if process.poll() is not None:
                    raise RuntimeError("owned server exited before readiness")
                try:
                    status = await client.request("GET", "/v1/status")
                    break
                except aiohttp.ClientConnectorError:
                    if time.monotonic() > startup:
                        raise TimeoutError("owned server startup deadline") from None
                    await asyncio.sleep(.025)
            if status["sequence"] != 0 or status["tables"] != 0:
                raise RuntimeError("root was not fresh")
            report["database_id"] = status["database_id"]
            await client.request("POST", "/v1/tables", {"name": "metrics", "config": {}})
            await client.sql("CALL varve_create_continuous_aggregate('minute_rollup','metrics',60000000)")
            base = aligned_base_us(time.time_ns() // 1000)
            report["base_us"] = base
            oracle = Oracle()
            initial_offset = 0
            preload_receipts = []
            async def preload():
                nonlocal initial_offset
                while initial_offset < args.rows:
                    offset = initial_offset
                    count = min(args.batch, args.rows - offset)
                    initial_offset += count
                    rows = [measurement(i, base) for i in range(offset, offset + count)]
                    receipt = await client.request("POST", "/v1/write", {
                        "table": "metrics", "request_id": f"batch-{offset}", "rows": [r.varve() for r in rows]})
                    if receipt.get("durability") != "local_fsync" or receipt.get("duplicate") is not False or receipt.get("rows") != count:
                        raise RuntimeError("preload receipt mismatch")
                    oracle.extend(rows)
                    preload_receipts.append(receipt)
            started = time.monotonic()
            async with asyncio.TaskGroup() as tasks:
                for _ in range(args.writers):
                    tasks.create_task(preload())
            elapsed = time.monotonic() - started
            report["preload"] = {"rows": args.rows, "seconds": elapsed, "rows_per_s": args.rows / elapsed,
                                 "receipts": preload_receipts}
            await verify(client, oracle)
            before = await client.request("GET", "/metrics")
            (output / "metrics-before.txt").write_text(before)
            traces_before = await client.request("GET", "/v1/diagnostics/ingest", optional=True)
            if args.trace_capacity and (traces_before is None or traces_before["capacity"] != args.trace_capacity):
                raise RuntimeError("required trace capability unavailable")
            with (output / "events.jsonl").open("x") as events:
                load, offsets = await scheduled_load(client, args, base, events,
                    prefix_sum=oracle.all.sum_q / 4, progress=report)
            report["load"] = load
            for offset, count in offsets:
                oracle.extend(measurement(i, base) for i in range(offset, offset + count))
            after = await client.request("GET", "/metrics")
            (output / "metrics-after.txt").write_text(after)
            report["load_phase_deltas"] = metrics_delta(before, after)
            traces = await client.request("GET", "/v1/diagnostics/ingest", optional=True)
            (output / "traces.json").write_text(json.dumps(traces, indent=2) + "\n")
            report["oracles"] = await verify(client, oracle)
            report["final_status"] = await client.request("GET", "/v1/status")
            if report["final_status"]["fenced"] or report["final_status"]["last_maintenance_error"]:
                raise RuntimeError("fenced or unhealthy maintenance")
            if report["final_status"]["database_id"] != report["database_id"]:
                raise RuntimeError("database identity changed")
            report["status"] = "passed" if load["clean"] else "overloaded"
            if digest(binary) != report["identity"]["binary_sha256"] or digest(profile) != report["identity"]["profile_sha256"]:
                raise RuntimeError("input bytes changed during run")
        except BaseException as error:
            report["error"] = (client.describe_error if client else redactor({}))(error)
            raise
        finally:
            if client:
                await client.session.close()
            if process is not None:
                # Kill only the session we created. This also stops inherited DuckDB workers.
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                process.wait(timeout=5)
                report["owned_server_reaped"] = True
                report["server_returncode"] = process.returncode


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--binary", required=True)
    p.add_argument("--expected-binary-sha256", required=True)
    p.add_argument("--profile", required=True)
    p.add_argument("--output", required=True)
    p.add_argument("--rows", type=int, default=102400)
    p.add_argument("--batch", type=int, default=1000)
    p.add_argument("--writers", type=int, default=4)
    p.add_argument("--rate", type=float, default=5000)
    p.add_argument("--seconds", type=float, default=20)
    p.add_argument("--read-interval", type=float, default=.1)
    p.add_argument("--read-mode", choices=("prefix", "fresh"), default="prefix")
    p.add_argument("--readers", type=int, default=1)
    p.add_argument("--drain-seconds", type=float, default=30)
    p.add_argument("--trace-capacity", type=int, default=0)
    p.add_argument("--max-seconds", type=float, default=180)
    return p


def validate(args):
    numeric = [args.rate, args.seconds, args.read_interval, args.max_seconds, args.drain_seconds]
    if not all(math.isfinite(x) for x in numeric):
        raise ValueError("non-finite workload bound")
    if not (1 <= args.batch <= 1000 and 1 <= args.writers <= 16 and args.rows >= args.batch
            and 0 < args.rate <= 100000 and 0 < args.seconds <= 60
            and 0 <= args.read_interval <= 10 and 0 <= args.trace_capacity <= 512
            and 1 <= args.readers <= 2 and 0 < args.drain_seconds <= 120
            and 0 < args.max_seconds <= 600 and args.rows + int(args.rate * args.seconds) <= 1000000
            and args.rows % 1024 == 0):
        raise ValueError("workload exceeds bounded profile or initial prefix is not series-aligned")
    if args.read_interval and args.read_interval < .01:
        raise ValueError("read interval below bounded minimum")
    if math.ceil(args.rows / args.batch) + math.ceil(args.rate * args.seconds / args.batch) > 20000:
        raise ValueError("request/evidence budget exceeded")


def main():
    args = parser().parse_args()
    validate(args)
    output = Path(args.output).absolute()
    output.mkdir(mode=0o700)  # Never reuse a namespace, including failed runs.
    report = {"status": "failed", "workload": {k: getattr(args, k) for k in
        ("rows", "batch", "writers", "rate", "seconds", "read_interval", "read_mode", "readers", "drain_seconds", "trace_capacity")},
        "claims": {"timescale_win": False, "cloud_qualification": False, "power_loss": False}}
    async def bounded():
        async with asyncio.timeout(args.max_seconds):
            await campaign(args, output, report)
    try:
        asyncio.run(bounded())
    except BaseException as error:
        report["status"] = "failed"
        report.setdefault("error", redactor({})(error))
    finally:
        (output / "report.json").write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
        inventory = {p.name: digest(p) for p in output.iterdir() if p.is_file()}
        (output / "sha256.json").write_text(json.dumps(inventory, indent=2) + "\n")
    print(json.dumps({"status": report["status"], "output": str(output),
                      "load": report.get("load"), "error": report.get("error")}))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
