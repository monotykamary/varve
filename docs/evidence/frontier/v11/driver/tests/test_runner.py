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
from core import Deadline, Measurement, Oracle, Stats, measurement


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

        args = argparse.Namespace(writers=1, rate=100, mixed_seconds=10, rows=10, batch=1, run_id="test")
        with patch.object(benchmark, "copy_batch", copied):
            with self.assertRaises(benchmark.MixedWorkloadError) as caught:
                await asyncio.wait_for(benchmark.mixed_workload(
                    SimpleNamespace(request=reject), SimpleNamespace(writers=[object()]),
                    benchmark.names_for("test"), args, 0, Stats(), Oracle(), Deadline(10),
                ), timeout=0.5)
        self.assertIsInstance(caught.exception.__cause__, ExceptionGroup)
        self.assertIn("injected rejection", benchmark.redactor({})(caught.exception))
        self.assertEqual(caught.exception.partial["failed_or_ambiguous_rows"], 1)
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

        args = argparse.Namespace(writers=1, rate=1, mixed_seconds=1, rows=1, batch=1, run_id="partial")
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
            "preflight": {"durability": {}}, "setup_varve": None, "setup_timescale": [],
            "prepare_oracle": oracle, "ingest_varve": {}, "ingest_timescale": {},
            "analyze_timescale": 0, "refresh_timescale": 0, "verify_all": None,
            "run_query_suite": {}, "checkpoint_varve": {},
            "convert_timescale": {"status": "converted"},
        }
        with tempfile.TemporaryDirectory() as temporary, ExitStack() as stack:
            path = Path(temporary) / "report.json"
            args = benchmark.arguments(["--run-id", "diagreport", "--output", str(path)])
            args.rows = args.batch = args.writers = args.query_samples = args.mixed_seconds = args.rate = 1
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
            self.assertEqual(saved["manifest"]["mixed_acknowledged_rows"], 0)
            self.assertEqual(saved["manifest"]["total_committed_watermark_rows"], 1)
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
