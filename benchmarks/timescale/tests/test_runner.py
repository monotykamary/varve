import argparse
import asyncio
import json
import shutil
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

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
            with self.assertRaises(benchmark.MixedWorkloadError):
                await asyncio.wait_for(benchmark.mixed_workload(
                    SimpleNamespace(request=reject), SimpleNamespace(writers=[object()]),
                    benchmark.names_for("test"), args, 0, Stats(), Oracle(), Deadline(10),
                ), timeout=0.5)

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
