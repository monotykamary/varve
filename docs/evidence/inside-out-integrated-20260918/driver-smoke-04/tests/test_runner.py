import argparse
import asyncio
import json
import shutil
import tempfile
import threading
import unittest
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import benchmark
from core import Deadline, Measurement, Oracle, Stats, canonical_payload_digest, measurement


class Response:
    status = 200

    def __init__(self, chunks):
        self.content = self
        self.chunks = chunks

    async def iter_chunked(self, size):
        for chunk in self.chunks:
            yield chunk

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False


class RunnerTests(unittest.IsolatedAsyncioTestCase):
    def test_rebuilt_status_requires_persisted_journal_and_exact_native_identity(self):
        status = {
            "database_id": "db-1",
            "sequence": 7,
            "checkpoint_sequence": 6,
            "segmented_journal": True,
            "native_query": {
                "version": benchmark.EXPECTED_NATIVE_VERSION,
                "library_sha256": benchmark.EXPECTED_NATIVE_LIBRARY_SHA256,
                "header_sha256": benchmark.EXPECTED_NATIVE_HEADER_SHA256,
                "library_path": "/usr/local/lib/varve/libduckdb.so",
            },
        }
        measured = benchmark.rebuilt_status(status)
        self.assertTrue(measured["passed"])
        self.assertEqual(measured["actual"]["database_id"], "db-1")
        for key in ("segmented_journal", "native_query"):
            changed = json.loads(json.dumps(status))
            if key == "segmented_journal":
                changed[key] = False
            else:
                changed[key]["header_sha256"] = "0" * 64
            self.assertFalse(benchmark.rebuilt_status(changed)["passed"])

    def test_varve_receipts_require_all_exact_fields(self):
        good = {"rows": 3, "duplicate": False, "durability": "local_fsync", "sequence": 9}
        self.assertIs(benchmark.validate_varve_receipt(good, 3, False), good)
        duplicate = dict(good, duplicate=True)
        self.assertIs(benchmark.validate_varve_receipt(duplicate, 3, True), duplicate)
        for field, value in (
            ("rows", 2),
            ("duplicate", True),
            ("durability", "memory"),
            ("sequence", 0),
            ("sequence", True),
        ):
            invalid = dict(good)
            invalid[field] = value
            with self.assertRaises(RuntimeError, msg=field):
                benchmark.validate_varve_receipt(invalid, 3, False)

    def test_attestation_is_bounded_external_metadata_not_runtime_proof(self):
        with tempfile.TemporaryDirectory() as temporary:
            args = benchmark.arguments([
                "--run-id", "attested", "--output", str(Path(temporary) / "report.json"),
                "--attestation", '{"candidate_image":"sha256:abc","cpu":"external"}',
                "--require-rebuilt",
            ])
            body = benchmark.base_report(args)
        self.assertTrue(body["external_attestation"]["supplied"])
        self.assertFalse(body["external_attestation"]["measured_runtime_proof"])
        self.assertTrue(body["configuration"]["require_rebuilt"])
        for value in ("[]", '{"cpu":NaN}'):
            with self.assertRaises(argparse.ArgumentTypeError):
                benchmark.parse_attestation(value)

    async def test_initial_ingest_failure_preserves_receipt_diagnostics(self):
        calls = 0

        async def request(method, path, body):
            nonlocal calls
            calls += 1
            payload = json.loads(body)
            receipt = {
                "rows": len(payload["rows"]),
                "duplicate": False,
                "durability": "local_fsync",
                "sequence": calls,
            }
            if calls == 2:
                receipt["sequence"] = 0
            return receipt

        with self.assertRaises(benchmark.IngestError) as caught:
            await benchmark.ingest_varve(
                SimpleNamespace(request=request),
                benchmark.names_for("initialdiag"),
                2,
                1,
                1,
                0,
                "initialdiag",
            )
        partial = caught.exception.partial
        self.assertEqual(partial["state"], "failed")
        self.assertEqual(partial["rows"], 1)
        self.assertEqual(partial["assigned_rows"], 2)
        self.assertEqual(partial["failed_or_ambiguous_rows"], 1)
        self.assertEqual(len(partial["ack_latency_ms"]["raw"]), 2)
        self.assertTrue(partial["errors"])

    async def test_timescale_single_row_uses_one_atomic_autocommit_statement(self):
        row = measurement(0, 0)

        class Cursor:
            def __init__(self, fresh=True, digest=None):
                self.executions = []
                self.fresh = fresh
                self.digest = digest

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

            async def execute(self, statement, params=()):
                self.executions.append((statement.as_string(), params))
                return self

            async def fetchone(self):
                if len(self.executions) == 1:
                    return {"inserted": 1} if self.fresh else None
                return {"row_count": 1, "payload_sha256": self.digest}

        class Connection:
            autocommit = True

            def __init__(self, cursor):
                self._cursor = cursor

            def cursor(self):
                return self._cursor

            def transaction(self):
                raise AssertionError("ordinary single inserts must not pay a separate BEGIN/COPY/COMMIT")

        names = benchmark.names_for("single")
        cursor = Cursor()
        self.assertTrue(await benchmark.copy_batch(Connection(cursor), names, "id", [row], Deadline(5)))
        self.assertEqual(len(cursor.executions), 1)
        self.assertIn("WITH receipt AS", cursor.executions[0][0])
        self.assertEqual(cursor.executions[0][0].count("INSERT INTO"), 2)
        duplicate = Cursor(False, canonical_payload_digest([row]))
        self.assertFalse(await benchmark.copy_batch(Connection(duplicate), names, "id", [row], Deadline(5)))
        with self.assertRaisesRegex(RuntimeError, "canonical payload digest"):
            await benchmark.copy_batch(Connection(Cursor(False, "0" * 64)), names, "id", [row], Deadline(5))
        nonautocommit = Connection(Cursor())
        nonautocommit.autocommit = False
        with self.assertRaisesRegex(RuntimeError, "autocommit durability"):
            await benchmark.copy_batch(nonautocommit, names, "id", [row], Deadline(5))

    async def test_timescale_setup_preserves_digest_regex_in_composed_sql(self):
        statements = []

        class Pg:
            async def execute(self, statement, params=()):
                statements.append(statement.as_string() if hasattr(statement, "as_string") else statement)

            async def query(self, statement, params=()):
                return []

        await benchmark.setup_timescale(Pg(), benchmark.names_for("ddlcheck"))
        receipt = next(statement for statement in statements if "payload_sha256" in statement)
        self.assertIn("'^[0-9a-f]{64}$'", receipt)
        self.assertFalse(any("CREATE EXTENSION" in statement or "ALTER SYSTEM" in statement for statement in statements))

    async def test_timescale_duplicate_checks_canonical_digest_inside_transaction(self):
        rows = [measurement(0, 0), measurement(1, 0)]

        class Context:
            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return False

        class Cursor(Context):
            def __init__(self, stored):
                self.stored = stored
                self.next_row = None
                self.executions = []
                self.copy_entered = False

            async def execute(self, statement, params=()):
                rendered = statement.as_string()
                self.executions.append((rendered, params))
                self.next_row = None if "INSERT INTO" in rendered else self.stored
                return self

            async def fetchone(self):
                return self.next_row

            def copy(self, statement):
                self.copy_entered = True
                raise AssertionError("duplicate retry must not enter COPY")

        class Connection:
            def __init__(self, cursor):
                self._cursor = cursor

            def transaction(self):
                return Context()

            def cursor(self):
                return self._cursor

        stored = {"row_count": 2, "payload_sha256": canonical_payload_digest(rows)}
        cursor = Cursor(stored)
        inserted = await benchmark.copy_batch(
            Connection(cursor), benchmark.names_for("digest"), "stable-id", rows, Deadline(5)
        )
        self.assertFalse(inserted)
        self.assertFalse(cursor.copy_entered)
        self.assertIn("FOR UPDATE", cursor.executions[1][0])
        conflicting = list(rows)
        row = conflicting[0]
        conflicting[0] = Measurement(row.timestamp_us, row.tenant, row.series, row.value_q + 1)
        with self.assertRaisesRegex(RuntimeError, "canonical payload digest"):
            await benchmark.copy_batch(
                Connection(Cursor(stored)),
                benchmark.names_for("digest"),
                "stable-id",
                conflicting,
                Deadline(5),
            )

    async def test_raw_fingerprints_include_exact_moments_and_database_identity(self):
        summary = [{
            "n": 2,
            "total": 1.25,
            "min": 0.5,
            "max": 0.75,
            "min_timestamp_us": 1000001,
            "max_timestamp_us": 1000002,
        }]
        first = [
            {"timestamp_us": 1000001, "tenant": "tenant_0", "series": "series_0000", "value": 0.5},
            {"timestamp_us": 1000002, "tenant": "tenant_0", "series": "series_0001", "value": 0.75},
        ]
        last = list(reversed(first))

        active = 0
        peak = 0

        async def varve_sql(statement):
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            try:
                await asyncio.sleep(0)
                if "count(*)" in statement:
                    return summary
                return first if " ASC" in statement else last
            finally:
                active -= 1

        async def varve_request(method, path):
            return {"database_id": "varve-db"}

        async def pg_query(statement, params=()):
            rendered = statement.as_string() if hasattr(statement, "as_string") else statement
            if "pg_database" in rendered:
                return [{
                    "database_name": "postgres", "database_oid": 5,
                    "schema_name": benchmark.names_for("fingerprint").pg_schema, "schema_oid": 42,
                }]
            if "count(*)" in rendered:
                return summary
            return first if " ASC" in rendered else last

        fingerprints = await benchmark.raw_fingerprints(
            SimpleNamespace(sql=varve_sql, request=varve_request),
            SimpleNamespace(query=pg_query),
            benchmark.names_for("fingerprint"),
        )
        self.assertEqual(peak, 1, "fingerprint verification must not exceed a one-query budget")
        self.assertEqual(
            fingerprints["varve"]["raw_sha256"],
            fingerprints["timescale"]["raw_sha256"],
        )
        self.assertEqual(
            fingerprints["varve"]["raw"]["first_exact_moments"][0]["timestamp_us"],
            1000001,
        )
        self.assertEqual(
            fingerprints["varve"]["database_identity"], {"database_id": "varve-db"}
        )
        self.assertEqual(
            fingerprints["timescale"]["database_identity"]["database_oid"], 5
        )
        self.assertEqual(
            fingerprints["timescale"]["database_identity"]["schema_oid"], 42
        )

    async def test_stable_retry_drill_rejects_conflicts_and_preserves_fingerprints(self):
        fingerprint = {
            "varve": {"fingerprint_sha256": "a"},
            "timescale": {"fingerprint_sha256": "b"},
        }
        requests = 0

        async def request(method, path, body):
            nonlocal requests
            requests += 1
            if requests == 1:
                return {"rows": 2, "duplicate": True, "durability": "local_fsync", "sequence": 4}
            raise benchmark.VarveHttpError(409, "request_id conflicts")

        copied = AsyncMock(side_effect=[False, RuntimeError("conflicts with its canonical payload digest")])
        with patch.object(benchmark, "raw_fingerprints", AsyncMock(side_effect=[fingerprint, fingerprint])), patch.object(benchmark, "copy_batch", copied):
            result = await benchmark.stable_retry_drill(
                SimpleNamespace(request=request),
                SimpleNamespace(writers=[object()]),
                benchmark.names_for("retry"),
                "retry",
                0,
                2,
                2,
                Deadline(5),
                include_conflicting=True,
            )
        self.assertTrue(result["unchanged"])
        self.assertEqual(result["conflicting"]["varve_http_status"], 409)
        self.assertEqual(copied.await_count, 2)

    async def test_query_backend_order_is_explicit_and_counterbalanced(self):
        calls = []

        async def varve_query(statement):
            calls.append("varve")
            return [{"ok": 1}]

        async def pg_query(statement):
            calls.append("timescale")
            return [{"ok": 1}]

        case = benchmark.QueryCase("ordered", "SELECT 1", "SELECT 1", lambda rows: None)
        result = await benchmark.run_query_suite(
            SimpleNamespace(sql=varve_query),
            SimpleNamespace(query=pg_query),
            [case],
            1,
            backend_order=("timescale", "varve"),
        )
        self.assertEqual(result["backend_order"], ["timescale", "varve"])
        self.assertEqual(calls, ["timescale", "timescale", "varve", "varve"])
        self.assertIn("not p99", result["percentile_scope"])

    async def test_mixed_arrivals_are_lossless_bounded_and_reads_are_independent(self):
        sequence = 0

        async def request(method, path, body):
            nonlocal sequence
            sequence += 1
            payload = json.loads(body)
            return {
                "rows": len(payload["rows"]),
                "duplicate": False,
                "durability": "local_fsync",
                "sequence": sequence,
            }

        async def copied(*args):
            return True

        async def read(*args):
            await asyncio.sleep(0.015)
            return 2.0, 3.0

        args = argparse.Namespace(
            writers=1,
            rate=1000,
            mixed_seconds=0.04,
            rows=10,
            batch=10,
            run_id="lossless",
            mixed_read_interval=0.01,
            mixed_readers=1,
            drain_seconds=1,
        )
        with patch.object(benchmark, "copy_batch", copied), patch.object(benchmark, "stable_mixed_read", read):
            result = await benchmark.mixed_workload(
                SimpleNamespace(request=request),
                SimpleNamespace(writers=[object()]),
                benchmark.names_for("lossless"),
                args,
                0,
                Stats(),
                Oracle(),
                Deadline(5),
            )
        self.assertTrue(result["data_complete"])
        self.assertEqual(result["offered_rows"], 40)
        self.assertEqual(result["acknowledged_rows"], 40)
        self.assertEqual(result["dropped_rows"], 0)
        self.assertEqual(result["never_submitted_rows"], 0)
        self.assertEqual(result["pending_rows"], 0)
        self.assertEqual(result["reads"], {"offered": 3, "completed": 3, "failed": 0, "pending": 0})
        self.assertLessEqual(result["scheduling"]["peak_write_queue_requests"], 2)
        self.assertLessEqual(result["scheduling"]["peak_read_queue_requests"], 2)
        self.assertEqual(result["read_arrival_latency_ms"]["summary"]["samples"], 3)
        self.assertEqual(
            [row["intended_seconds"] for row in result["read_observations"]],
            [0.01, 0.02, 0.03],
        )
        self.assertTrue(all("arrival_latency_ms" in row for row in result["read_observations"]))
        self.assertIn("not p99", result["percentile_scope"])

    def client(self, chunks):
        client = object.__new__(benchmark.VarveClient)
        client.base = "http://example.invalid"
        client.deadline = Deadline(5)
        client.session = SimpleNamespace(request=lambda *args, **kwargs: Response(chunks))
        return client

    async def test_fragmented_json_and_utf8_are_read_to_eof(self):
        result = await self.client([b'[{"value":"\xe2', b'\x82\xac"}', b']']).request("GET", "/test")
        self.assertEqual(result, [{"value": "\u20ac"}])

    async def test_response_limit_is_cumulative(self):
        with patch.object(benchmark, "MAX_RESPONSE_BYTES", 5):
            with self.assertRaisesRegex(RuntimeError, "exceeded"):
                await self.client([b"123", b"456"]).request("GET", "/test")

    async def test_mixed_rejection_cancels_the_producer_promptly(self):
        async def reject(*args):
            raise RuntimeError("injected rejection")

        async def copied(*args):
            await asyncio.sleep(0.01)
            return True

        args = argparse.Namespace(
            writers=1, rate=100, mixed_seconds=10, rows=10, batch=1, run_id="test",
            mixed_read_interval=1, mixed_readers=1, drain_seconds=1,
        )
        with patch.object(benchmark, "copy_batch", copied):
            with self.assertRaises(benchmark.MixedWorkloadError) as caught:
                await asyncio.wait_for(benchmark.mixed_workload(
                    SimpleNamespace(request=reject), SimpleNamespace(writers=[object()]),
                    benchmark.names_for("test"), args, 0, Stats(), Oracle(), Deadline(10),
                ), timeout=0.5)
        self.assertIsInstance(caught.exception.__cause__, ExceptionGroup)
        self.assertIn("injected rejection", benchmark.redactor({})(caught.exception))
        partial = caught.exception.partial
        self.assertEqual(partial["failed_or_ambiguous_rows"], 1)
        self.assertEqual(
            partial["offered_rows"],
            partial["acknowledged_rows"] + partial["failed_or_ambiguous_rows"] + partial["pending_rows"],
        )
        self.assertEqual(partial["dropped_rows"], 0)
        self.assertGreater(partial["never_submitted_rows"], 0)
        for name in ("varve_concurrent_read_ms", "timescale_concurrent_read_ms", "concurrent_read_pair_ms"):
            self.assertEqual(caught.exception.partial[name]["raw"], [])
            self.assertEqual(caught.exception.partial[name]["summary"]["samples"], 0)

    async def test_oracle_preparation_runs_off_the_event_loop(self):
        owner = threading.get_ident()
        observed = []

        class TrackedOracle(Oracle):
            def __init__(self):
                observed.append(threading.get_ident())
                super().__init__()

        with patch.object(benchmark, "Oracle", TrackedOracle):
            oracle = await benchmark.prepare_oracle(16, 0, Deadline(5))
        self.assertEqual(oracle.all.count, 16)
        self.assertNotEqual(observed, [owner])
        self.assertEqual(len(observed), 1)

    def test_oracle_preparation_honors_cancellation_and_deadline(self):
        cancelled = threading.Event()
        cancelled.set()
        with self.assertRaisesRegex(TimeoutError, "cancelled"):
            benchmark.build_oracle(16, 0, Deadline(5), cancelled)
        clock = [0.0]
        deadline = Deadline(1, lambda: clock[0])
        clock[0] = 2.0
        with self.assertRaises(TimeoutError):
            benchmark.build_oracle(16, 0, deadline, threading.Event())

    def test_exception_group_leaves_are_preserved_and_redacted(self):
        secret = "private-token-value"
        error = ExceptionGroup("group", [RuntimeError("actual cause " + secret), ValueError("second cause")])
        message = benchmark.redactor({"VARVE_API_TOKEN": secret})(error)
        self.assertIn("actual cause", message)
        self.assertIn("second cause", message)
        self.assertNotIn(secret, message)

    def test_redaction_traverses_causes_and_bounds_cycles(self):
        secret = "private-causal-secret"
        leaf = RuntimeError("nested disconnect " + secret)
        group = ExceptionGroup("pair", [leaf, ValueError("other cause")])
        outer = benchmark.MixedWorkloadError("aborted", {})
        outer.__cause__ = group
        leaf.__cause__ = outer
        message = benchmark.redactor({"VARVE_API_TOKEN": secret})(outer)
        self.assertIn("nested disconnect", message)
        self.assertIn("other cause", message)
        self.assertNotIn(secret, message)
        self.assertEqual(message.count("MixedWorkloadError:"), 1)
        self.assertLessEqual(len(message), 2000)

    def test_redaction_honors_suppressed_context_and_description_limit(self):
        outer = RuntimeError("outer")
        outer.__context__ = ValueError("hidden")
        outer.__suppress_context__ = True
        self.assertNotIn("hidden", benchmark.redactor({})(outer))
        outer.__suppress_context__ = False
        self.assertIn("hidden", benchmark.redactor({})(outer))
        group = ExceptionGroup("many", [ValueError(str(index)) for index in range(100)])
        self.assertEqual(benchmark.redactor({})(group).count("ValueError:"), 15)
        outer.__cause__ = ValueError("explicit cause")
        message = benchmark.redactor({})(outer)
        self.assertIn("explicit cause", message)
        self.assertNotIn("hidden", message)
        secret = "long-message-secret"
        message = benchmark.redactor({"VARVE_API_TOKEN": secret})(RuntimeError("x" * 1970 + secret + "y" * 100))
        self.assertEqual(len(message), 2000)
        self.assertNotIn(secret, message)
        self.assertIn("<redacted>", message)

    async def test_mixed_failure_preserves_completed_read_samples(self):
        collected = asyncio.Event()
        blocked = asyncio.Event()
        real_sleep = asyncio.sleep
        reads = 0

        async def yield_only(delay):
            await real_sleep(0)

        async def query(*args):
            nonlocal reads
            reads += 1
            if reads >= 2:
                collected.set()
            return 11.0, 22.0

        async def reject(*args):
            await collected.wait()
            raise RuntimeError("injected after completed reads")

        async def copied(*args):
            await blocked.wait()
            return True

        args = argparse.Namespace(
            writers=1, rate=1, mixed_seconds=1, rows=1, batch=1, run_id="partial",
            mixed_read_interval=0.1, mixed_readers=1, drain_seconds=2,
        )
        with patch.object(benchmark, "copy_batch", copied), patch.object(benchmark, "stable_mixed_read", query), patch.object(benchmark.asyncio, "sleep", yield_only):
            with self.assertRaises(benchmark.MixedWorkloadError) as caught:
                await asyncio.wait_for(benchmark.mixed_workload(
                    SimpleNamespace(request=reject), SimpleNamespace(writers=[object()]),
                    benchmark.names_for("partial"), args, 0, Stats(), Oracle(), Deadline(5),
                ), timeout=1)
        partial = caught.exception.partial
        self.assertGreaterEqual(reads, 2)
        self.assertEqual(partial["varve_concurrent_read_ms"]["raw"], [11.0] * reads)
        self.assertEqual(partial["timescale_concurrent_read_ms"]["raw"], [22.0] * reads)
        for name in ("varve_concurrent_read_ms", "timescale_concurrent_read_ms", "concurrent_read_pair_ms"):
            self.assertEqual(partial[name]["summary"], benchmark.latency_summary(partial[name]["raw"]))
        self.assertEqual(len(partial["concurrent_read_pair_ms"]["raw"]), reads)
        self.assertEqual(len(partial["generation_encoding_ms"]["raw"]), 1)
        self.assertEqual(partial["generation_encoding_ms"]["summary"], benchmark.latency_summary(partial["generation_encoding_ms"]["raw"]))
        self.assertEqual(partial["failed_or_ambiguous_rows"], 1)
        self.assertEqual(partial["acknowledged_rows"], 0)
        self.assertEqual(
            partial["offered_rows"],
            partial["acknowledged_rows"] + partial["failed_or_ambiguous_rows"] + partial["pending_rows"],
        )
        self.assertIn("injected after completed reads", " ".join(partial["errors"]))
        self.assertIn("injected after completed reads", benchmark.redactor({})(caught.exception))

    def test_mixed_failure_reaches_persisted_and_emitted_terminal_report(self):
        secret = "terminal-report-private-token"
        credentials = {"VARVE_URL": "http://example.invalid", "VARVE_API_TOKEN": secret}
        oracle = Oracle()
        oracle.add(measurement(0, 0))

        async def request(method, path, body=None):
            if method == "GET":
                return {}
            raise RuntimeError("nested original disconnect " + secret)

        async def copied(*args):
            await asyncio.Event().wait()

        varve = SimpleNamespace(request=request, close=AsyncMock())
        pg = SimpleNamespace(writers=[object()], open=AsyncMock(), close=AsyncMock())
        helpers = {
            "preflight": {
                "durability": {}, "varve_status": {"database_id": "db"},
                "rebuilt": {"passed": True},
            },
            "runtime_snapshot": {"observed_at": "test"},
            "setup_varve": None, "setup_timescale": [],
            "prepare_oracle": oracle, "ingest_varve": {}, "ingest_timescale": {},
            "analyze_timescale": 0, "refresh_timescale": 0, "verify_all": None,
            "run_query_suite": {}, "checkpoint_varve": {},
            "convert_timescale": {"status": "converted"},
        }
        with tempfile.TemporaryDirectory() as temporary, ExitStack() as stack:
            path = Path(temporary) / "report.json"
            args = benchmark.arguments(["--run-id", "diagreport", "--output", str(path)])
            args.rows = args.batch = args.writers = args.query_samples = args.mixed_seconds = args.rate = 1
            args.mixed_read_interval = args.drain_seconds = 1
            args.mixed_readers = 1
            stack.enter_context(patch.object(benchmark, "arguments", return_value=args))
            stack.enter_context(patch.object(benchmark, "load_credentials", return_value=credentials))
            stack.enter_context(patch.object(benchmark, "VarveClient", return_value=varve))
            stack.enter_context(patch.object(benchmark, "TimescaleClient", return_value=pg))
            stack.enter_context(patch.object(benchmark, "copy_batch", copied))
            events = stack.enter_context(patch.object(benchmark, "emit"))
            for name, result in helpers.items():
                stack.enter_context(patch.object(benchmark, name, AsyncMock(return_value=result)))
            self.assertEqual(benchmark.main(), 1)
            text = path.read_text()
            saved = json.loads(text)
            self.assertEqual(saved["state"], "failed")
            self.assertTrue(saved["finished_at"])
            self.assertIn("nested original disconnect", saved["failure"])
            self.assertNotIn(secret, text)
            self.assertEqual(saved["mixed_workload"]["failed_or_ambiguous_rows"], 1)
            self.assertEqual(saved["mixed_workload"]["varve_concurrent_read_ms"]["raw"], [])
            self.assertEqual(saved["manifest"]["mixed_paired_acknowledged_rows"], 0)
            self.assertEqual(saved["manifest"]["mixed_failed_or_ambiguous_rows"], 1)
            self.assertEqual(saved["manifest"]["total_reconciled_watermark_rows"], 1)
            final = [call.kwargs for call in events.call_args_list if call.args[0] == "final"]
            self.assertEqual(len(final), 1)
            self.assertEqual(final[0]["state"], "failed")
            self.assertEqual(final[0]["error"], saved["failure"])
            varve.close.assert_awaited_once()
            pg.close.assert_awaited_once()

    async def test_partial_query_samples_survive_failure(self):
        calls = 0

        async def query(statement):
            nonlocal calls
            calls += 1
            if calls == 3:
                raise RuntimeError("injected disconnect")
            return [{"ok": 1}]

        case = benchmark.QueryCase("probe", "SELECT 1", "SELECT 1", lambda rows: self.assertEqual(rows, [{"ok": 1}]))
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "report.json"
            report = benchmark.Report(path, {"started_at": "test", "phases": []})
            with self.assertRaisesRegex(RuntimeError, "injected disconnect"):
                await benchmark.run_query_suite(SimpleNamespace(sql=query), SimpleNamespace(query=query), [case], 3, report, "probe_stage")
            saved = json.loads(path.read_text())["query_stages"]["probe_stage"]["backends"]["varve"]["probe"]
            self.assertEqual(saved["state"], "failed")
            self.assertEqual(saved["verified_samples"], 1)
            self.assertEqual(len(saved["latency_ms"]["raw"]), 1)

    def test_container_copies_every_artifact_hash_input(self):
        root = Path(benchmark.__file__).parent
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary)
            for line in (root / "Dockerfile").read_text().splitlines():
                if line.startswith("COPY ") and line.split()[-1] == "./":
                    for name in line.split()[1:-1]:
                        shutil.copyfile(root / name, destination / name)
            with patch.object(benchmark, "__file__", str(destination / "benchmark.py")):
                metadata = benchmark.artifact_metadata()
            self.assertEqual(set(metadata["files"]), {"benchmark.py", "core.py", "requirements.txt", "Dockerfile"})

    def test_window_ties_are_deterministic(self):
        oracle = Oracle()
        oracle.extend(Measurement(10, "tenant_0", "series_0000", value) for value in (4, 12, 8))
        self.assertEqual([row["value"] for row in oracle.selected_window()], [3.0, 2.0, 1.0])
        case = benchmark.query_cases(benchmark.names_for("test"), oracle, 0, 3)[-1]
        self.assertIn("timestamp_us DESC, value DESC", case.varve_sql)
        self.assertIn("ts DESC, value DESC", case.postgres_sql.as_string())

    def test_duplicate_groups_and_buckets_are_rejected(self):
        oracle = Oracle()
        oracle.add(measurement(0, 0))
        cases = benchmark.query_cases(benchmark.names_for("test"), oracle, 0, 1)
        tenant = {"tenant": "tenant_0", "n": 1, "total": 0, "min": 0, "max": 0}
        bucket = {"bucket_us": 0, "n": 1, "total": 0, "min": 0, "max": 0}
        with self.assertRaises(RuntimeError):
            cases[2].verify([tenant, tenant])
        with self.assertRaises(RuntimeError):
            cases[3].verify([bucket, bucket])


if __name__ == "__main__":
    unittest.main()
