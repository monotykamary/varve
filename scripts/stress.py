#!/usr/bin/env python3
"""Bounded synthetic HTTP stress with exact raw/rollup oracles; no third-party packages."""
import argparse
import concurrent.futures
import ipaddress
import json
import math
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def identifier(prefix):
    return prefix + uuid.uuid4().hex[:16]


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def percentile(values, fraction):
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def latency_summary(values):
    return {"samples": len(values), "p50_ms": percentile(values, .50), "p95_ms": percentile(values, .95), "p99_ms": percentile(values, .99), "max_ms": max(values) if values else None}


def rows_for(offset, count, base_us):
    return [{"timestamp_us": base_us - i * 100, "tenant": "stress", "series": "cpu_" + str(i % 8), "value": (i % 1000) / 8.0, "tags": {"region": str(i % 3)}} for i in range(offset, offset + count)]


def timed_request_id(issued_us=None, nonce=None):
    issued_us = int(time.time() * 1000000) if issued_us is None else issued_us
    nonce = uuid.uuid4().hex if nonce is None else nonce
    return "v1:" + str(issued_us) + ":" + nonce


def validate_target(url):
    parsed = urllib.parse.urlsplit(url)
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise ValueError("URL must not contain credentials, query or fragment")
    if parsed.scheme == "https" and parsed.hostname:
        return
    if parsed.scheme == "http":
        try:
            if ipaddress.ip_address(parsed.hostname).is_loopback:
                return
        except ValueError:
            if parsed.hostname == "localhost":
                return
    raise ValueError("HTTPS required except for loopback testing")


class HTTPStatusError(RuntimeError):
    def __init__(self, status, server_message=None):
        super().__init__("HTTP request failed with status " + str(status))
        self.status = status
        self.server_message = server_message


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise RuntimeError("redirect refused to protect operator credentials")


class Client:
    def __init__(self, base, token):
        validate_target(base)
        if len(token) < 32 or any(ord(character) < 33 or ord(character) > 126 for character in token):
            raise ValueError("VARVE_API_TOKEN must contain at least 32 visible ASCII characters")
        self.base, self.token = base.rstrip("/"), token
        self.opener = urllib.request.build_opener(NoRedirect())
        self.lock = threading.Lock()
        self.retries = 0

    def request(self, path, body=None, timeout=20, retry=False):
        encoded = None if body is None else json.dumps(body, separators=(",", ":"), allow_nan=False).encode()
        for attempt in range(4 if retry else 1):
            request = urllib.request.Request(self.base + path, data=encoded, headers={"Authorization": "Bearer " + self.token, "Content-Type": "application/json"})
            try:
                with self.opener.open(request, timeout=timeout) as response:
                    raw = response.read(2 * 1024 * 1024 + 1)
                    if len(raw) > 2 * 1024 * 1024:
                        raise RuntimeError("response exceeds probe limit")
                    return json.loads(raw)
            except urllib.error.HTTPError as error:
                # Never echo request headers or credentials into the report.
                if not retry or attempt == 3 or error.code not in (429, 502, 503, 504):
                    message = None
                    try:
                        raw = error.read(2049) if error.fp is not None else b""
                        if len(raw) <= 2048:
                            value = json.loads(raw)
                            if isinstance(value, dict) and isinstance(value.get("error"), str):
                                message = value["error"]
                    except (ValueError, OSError):
                        pass
                    finally:
                        error.close()
                    raise HTTPStatusError(error.code, message) from None
            except (urllib.error.URLError, TimeoutError):
                if not retry or attempt == 3:
                    raise RuntimeError("request failed after bounded retries; write outcome may be ambiguous") from None
            with self.lock:
                self.retries += 1
            time.sleep(.05 * (2 ** attempt))
        raise RuntimeError("retry bound reached")

    def sql(self, statement, retry=False):
        return self.request("/v1/query", {"sql": statement}, retry=retry)


def run(args):
    client = Client(args.url, os.environ.get("VARVE_API_TOKEN", ""))
    table, aggregate, job = identifier("stress_"), identifier("agg_"), identifier("checkpoint_")
    config = {"shards": 4, "window_us": 60000000, "rollup_widths_us": [1000000, 60000000], "archive_after_us": 15000000, "retention_us": 3600000000, "idempotency_window_us": args.idempotency_window_seconds * 1000000}
    client.sql("CALL varve_create_table(" + literal(table) + "," + literal(json.dumps(config)) + ")")
    client.sql("CALL varve_create_continuous_aggregate(" + literal(aggregate) + "," + literal(table) + ",1000000)")
    client.sql("CALL varve_create_job(" + literal(job) + ",'checkpoint',2500000)")
    # Unique table creation must succeed before this probe is allowed to alter retention.
    base_us = int(time.time() * 1000000) - 120000000
    started, deadline = time.monotonic(), time.monotonic() + args.seconds
    lock, stop = threading.Lock(), threading.Event()
    write_times, query_times, failures = [], [], []
    expected_rows, expected_sum, next_offset = 0, 0.0, 0
    first_body = None

    def writer():
        nonlocal expected_rows, expected_sum, next_offset, first_body
        while not stop.is_set() and time.monotonic() < deadline:
            with lock:
                offset = next_offset
                count = min(args.batch, args.rows - offset)
                if count <= 0:
                    return
                next_offset += count
            target_time = started + offset / args.rate
            delay = min(target_time - time.monotonic(), deadline - time.monotonic())
            if delay > 0:
                stop.wait(delay)
            if stop.is_set() or time.monotonic() >= deadline:
                return
            batch = rows_for(offset, count, base_us)
            body = {"table": table, "request_id": timed_request_id(), "rows": batch}
            before = time.monotonic()
            try:
                receipt = client.request("/v1/write", body, retry=True)
                if receipt.get("durability") != "local_fsync":
                    raise RuntimeError("missing local fsync receipt")
                if offset == 0:
                    first_body = body
                    duplicate = client.request("/v1/write", body, retry=True)
                    if not duplicate.get("duplicate") or duplicate.get("sequence") != receipt.get("sequence"):
                        raise RuntimeError("idempotency oracle failed")
                with lock:
                    write_times.append((time.monotonic() - before) * 1000)
                    expected_rows += count
                    expected_sum += sum(row["value"] for row in batch)
            except Exception as error:
                with lock:
                    failures.append(str(error))
                stop.set()
                return

    def reader():
        while not stop.wait(.75):
            before = time.monotonic()
            try:
                client.sql("SELECT count(*) AS n, sum(value) AS total FROM " + table, retry=True)
                with lock:
                    query_times.append((time.monotonic() - before) * 1000)
            except Exception as error:
                with lock:
                    failures.append("concurrent query: " + str(error))
                stop.set()

    reader_thread = threading.Thread(target=reader)
    reader_thread.start()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        list(pool.map(lambda _: writer(), range(args.concurrency)))
    stop.set()
    reader_thread.join(timeout=25)
    if reader_thread.is_alive():
        raise RuntimeError("query worker did not stop within its bound")
    elapsed = time.monotonic() - started
    if failures:
        raise RuntimeError("stress aborted: " + "; ".join(failures[:4]))
    if expected_rows == 0:
        raise RuntimeError("probe did not complete any writes")
    raw = client.sql("SELECT count(*) AS n, sum(value) AS total FROM " + table)[0]
    rolled = client.sql("SELECT CAST(sum(count) AS BIGINT) AS n, sum(sum) AS total FROM " + aggregate)[0]
    for label, value in [("raw", raw), ("continuous aggregate", rolled)]:
        if int(value["n"]) != expected_rows or float(value["total"]) != expected_sum:
            raise RuntimeError(label + " correctness oracle failed")
    expired_id_rejected = False
    if first_body is not None:
        issued_us = int(first_body["request_id"].split(":", 2)[1])
        if int(time.time() * 1000000) > issued_us + args.idempotency_window_seconds * 1000000:
            try:
                client.request("/v1/write", first_body)
            except HTTPStatusError as error:
                if error.status != 400 or "idempotency window" not in (error.server_message or ""):
                    raise RuntimeError("expired request ID did not report the expected rejection") from None
                expired_id_rejected = True
            else:
                raise RuntimeError("expired request ID was accepted")
            if client.sql("SELECT count(*) AS n, sum(value) AS total FROM " + table)[0] != raw:
                raise RuntimeError("expired retry changed retained raw data")
    client.sql("CALL varve_alter_job(" + literal(job) + ",'{}')".format(json.dumps({"paused": True})))
    client.sql("CALL varve_run_job(" + literal(job) + ")")
    jobs = client.sql("SELECT * FROM varve_jobs()")
    client.request("/v1/maintain", {})
    status = client.request("/v1/status")
    retained = None
    if args.expire:
        policy = {"retention_us": 1, "idempotency_window_us": args.idempotency_window_seconds * 1000000}
        client.sql("CALL varve_set_policy(" + literal(table) + "," + literal(json.dumps(policy)) + ")")
        client.request("/v1/maintain", {})
        raw_after = client.sql("SELECT count(*) AS n FROM " + table)[0]
        retained = client.sql("SELECT CAST(sum(count) AS BIGINT) AS n FROM " + aggregate)[0]
        if int(raw_after["n"]) != 0 or int(retained["n"]) != expected_rows:
            raise RuntimeError("independent raw/aggregate retention oracle failed")
    client.sql("CALL varve_drop_job(" + literal(job) + ")")
    return {"target": urllib.parse.urlsplit(args.url).netloc, "table": table, "aggregate": aggregate, "completed_rows": expected_rows, "expected_sum": expected_sum, "elapsed_seconds": elapsed, "observed_rows_per_second": expected_rows / elapsed, "concurrency": args.concurrency, "bounded_retries": client.retries, "idempotency_window_seconds": args.idempotency_window_seconds, "expired_request_id_rejected": expired_id_rejected, "writes": latency_summary(write_times), "queries": latency_summary(query_times), "raw": raw, "rollup": rolled, "retained_rollup_after_raw_expiration": retained, "jobs_observed": len(jobs), "status_before_expiration": status, "verified": ["auth", "sql_controls", "out_of_order_ingest", "idempotency", "concurrent_sql", "exact_count_sum", "named_continuous_aggregate", "job_pause_run_drop", "maintenance"] + (["independent_retention"] if args.expire else []), "note": "Synthetic data and unique namespaces only. This is bounded workload evidence, not a production certification or competitive benchmark."}


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--rows", type=int, default=100000)
    parser.add_argument("--batch", type=int, default=256)
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--seconds", type=float, default=60)
    parser.add_argument("--rate", type=float, default=4000, help="maximum offered rows/second")
    parser.add_argument("--idempotency-window-seconds", type=int, default=300)
    parser.add_argument("--expire", action="store_true", help="expire this probe's newly-created raw table, preserving rollups")
    args = parser.parse_args()
    if not (1 <= args.rows <= 500000 and 1 <= args.batch <= 1000 and 1 <= args.concurrency <= 8 and 1 <= args.seconds <= 300 and 1 <= args.rate <= 20000 and 1 <= args.idempotency_window_seconds <= 3600):
        parser.error("limits: rows<=500000, batch<=1000, concurrency<=8, seconds<=300, rate<=20000, idempotency-window-seconds<=3600; all positive")
    return args


if __name__ == "__main__":
    print(json.dumps(run(arguments()), indent=2, allow_nan=False))
