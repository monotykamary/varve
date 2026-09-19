import asyncio
import contextlib
import hashlib
import io
import json
import math
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import verify_exact
from benchmark import names_for


NAMES = names_for("exact")


def plan(initial=1, late=0, state="passed"):
    return verify_exact.SourcePlan(
        source={"run_id": "exact", "artifact": {}},
        names=NAMES,
        base_us=1_800_000_000,
        initial_rows=initial,
        late_rows=late,
        source_state=state,
        source_report_sha256="a" * 64,
        dependency_hashes={},
    )


def raw(row):
    return {
        "timestamp_us": row.timestamp_us,
        "tenant": row.tenant,
        "series": row.series,
        "value": row.value_q / 4,
        "tags": {},
    }


def aggregate(row):
    bucket, tenant, series, _, count, sum_q, min_q, max_q = row
    return {
        "bucket_us": bucket,
        "tenant": tenant,
        "series": series,
        "tags": {},
        "count": count,
        "sum": sum_q / 4,
        "min": min_q / 4,
        "max": max_q / 4,
    }


def fetch(rows):
    async def page(offset, limit):
        return rows[offset:offset + limit]
    return page


def backend_pages(good, bad, bad_backend):
    return tuple(
        (backend, fetch(bad if backend == bad_backend else good))
        for backend in ("Varve", "Timescale")
    )


def artifact():
    root = Path(verify_exact.__file__).resolve().parent
    files = {name: verify_exact.file_sha256(root / name) for name in verify_exact.ARTIFACT_FILES}
    combined = hashlib.sha256(
        "".join(files[name] for name in verify_exact.ARTIFACT_FILES).encode("ascii")
    ).hexdigest()
    return {"format_version": 1, "files": files, "sha256": combined}


def complete_report(state="overloaded"):
    initial = 1
    late = 1
    report = {
        "mode": "benchmark",
        "state": state,
        "run_id": "exact",
        "artifact": artifact(),
        "namespaces": NAMES.json(),
        "configuration": {
            "rows": initial,
            "mixed_seconds": 1,
            "offered_rate_rows_per_second": 1.0,
        },
        "manifest": {
            "seed": verify_exact.DATASET_SEED,
            "base_timestamp_us": 1_800_000_000,
            "initial_rows": initial,
            "series": verify_exact.SERIES_COUNT,
            "tenants": verify_exact.TENANT_COUNT,
            "value_rule": "((i * 17) % 10000) / 4",
            "timestamp_rule": "base + (i//1024)*1 second + (i%1024) microseconds",
            "tags": {},
            "aggregate_width_us": verify_exact.BUCKET_US,
            "mixed_paired_acknowledged_rows": late,
            "mixed_varve_acknowledged_rows": late,
            "mixed_timescale_acknowledged_rows": late,
            "mixed_failed_or_ambiguous_rows": 0,
            "mixed_pending_rows": 0,
            "total_reconciled_watermark_rows": initial + late,
        },
        "initial_ingest": {
            backend: {
                "state": "passed",
                "offered_rows": initial,
                "assigned_rows": initial,
                "rows": initial,
                "never_submitted_rows": 0,
                "failed_or_ambiguous_rows": 0,
            }
            for backend in ("varve", "timescale")
        },
        "mixed_workload": {
            "state": state,
            "target_rows": late,
            "offered_rows": late,
            "dropped_rows": 0,
            "acknowledged_rows": late,
            "varve_acknowledged_rows": late,
            "timescale_acknowledged_rows": late,
            "failed_or_ambiguous_rows": 0,
            "pending_rows": 0,
            "never_submitted_rows": 0,
            "reads": {"offered": 0, "completed": 0, "failed": 0, "pending": 0},
            "data_complete": True,
            "schedule_met": state == "passed",
            "clean": state == "passed",
        },
        "mixed_freshness_barrier": {
            "same_reconciled_watermark_rows": initial + late,
            "timescale_explicit_refresh_seconds": 0.1,
            "varve_eager_aggregate": True,
        },
        "phases": [{
            "name": "mixed_barrier_retry_drills_and_final_correctness",
            "state": "passed",
        }],
    }
    if state == "overloaded":
        report["overloaded"] = {
            "data_complete": True,
            "schedule_met": False,
            "never_submitted_rows": 0,
            "pending_rows": 0,
        }
    return report


class ExactCleanupTests(unittest.IsolatedAsyncioTestCase):
    async def test_stalled_cleanup_is_bounded_and_preserves_the_primary_error(self):
        class Client:
            def __init__(self):
                self.close_started = False
                self.close_cancelled = False

            async def open(self, *_):
                return None

            async def close(self):
                self.close_started = True
                try:
                    await asyncio.Event().wait()
                finally:
                    self.close_cancelled = True

        async def stalled_identity(*_):
            await asyncio.Event().wait()

        async def failed_identity(*_):
            raise RuntimeError("primary identity failure")

        credentials = {"VARVE_URL": "unused", "VARVE_API_TOKEN": "unused"}
        for identity, error, message in (
            (stalled_identity, TimeoutError, None),
            (failed_identity, RuntimeError, "primary identity failure"),
        ):
            varve, pg = Client(), Client()
            with contextlib.ExitStack() as stack:
                for name, replacement in (
                    ("VarveClient", lambda *_: varve),
                    ("TimescaleClient", lambda *_: pg),
                    ("build_aggregates", lambda *_: ([], verify_exact.QuarterStats(), verify_exact.IterationBounds())),
                    ("verify_report_fingerprints", lambda *_: None),
                    ("current_identities", identity),
                    ("CLEANUP_TIMEOUT", 0.02),
                ):
                    stack.enter_context(patch.object(verify_exact, name, replacement))
                started = asyncio.get_running_loop().time()
                with self.assertRaises(error) as caught:
                    await asyncio.wait_for(
                        asyncio.wait_for(verify_exact.verify_remote(plan(), credentials, verify_exact.Deadline(1)), 0.02 if message is None else 0.5),
                        2,
                    )
                if message:
                    self.assertIn(message, str(caught.exception))
                self.assertLess(asyncio.get_running_loop().time() - started, 0.5)
                self.assertTrue(varve.close_started and pg.close_started)
                self.assertTrue(varve.close_cancelled and pg.close_cancelled)


class ExactRawTests(unittest.IsolatedAsyncioTestCase):
    async def test_middle_substitution_old_edges_count_and_sum_could_miss(self):
        expected = list(verify_exact.expected_rows(plan(initial=24)))
        observed = list(expected)
        observed[10] = verify_exact.ExactRow(
            observed[10].timestamp_us,
            observed[10].tenant,
            observed[10].series,
            observed[10].value_q + 4,
        )
        observed[11] = verify_exact.ExactRow(
            observed[11].timestamp_us,
            observed[11].tenant,
            observed[11].series,
            observed[11].value_q - 4,
        )
        self.assertEqual(expected[:8], observed[:8])
        self.assertEqual(expected[-8:], observed[-8:])
        self.assertEqual(len(expected), len(observed))
        self.assertEqual(sum(row.value_q for row in expected), sum(row.value_q for row in observed))
        self.assertEqual(min(row.value_q for row in expected), min(row.value_q for row in observed))
        self.assertEqual(max(row.value_q for row in expected), max(row.value_q for row in observed))
        good = [raw(row) for row in expected]
        bad = [raw(row) for row in observed]
        for backend in ("Varve", "Timescale"):
            with self.subTest(backend=backend), self.assertRaisesRegex(RuntimeError, "differs at sorted row"):
                await verify_exact.verify_paged(
                    "raw identity/multiplicity",
                    expected,
                    backend_pages(good, bad, backend),
                    verify_exact.normalize_raw,
                    page_size=5,
                )

    async def test_missing_extra_and_duplicate_rows_fail(self):
        expected = list(verify_exact.expected_rows(plan(initial=8)))
        cases = {
            "missing": expected[:3] + expected[4:],
            "extra": expected[:4] + [verify_exact.ExactRow(1_800_000_000, "tenant_0", "series_0000", 999)] + expected[4:],
            "duplicate": expected[:4] + [expected[3]] + expected[4:],
        }
        good = [raw(row) for row in expected]
        for name, changed in cases.items():
            bad = [raw(row) for row in sorted(changed)]
            for backend in ("Varve", "Timescale"):
                with self.subTest(name=name, backend=backend), self.assertRaisesRegex(RuntimeError, "differs at sorted row"):
                    await verify_exact.verify_paged(
                        "raw identity/multiplicity",
                        expected,
                        backend_pages(good, bad, backend),
                        verify_exact.normalize_raw,
                        page_size=3,
                    )

    async def test_wrong_aggregate_group_key_and_value_fail(self):
        expected = [
            (1_800_000_000, "tenant_0", "series_0000", "{}", 2, 8, 0, 8),
            (1_800_000_000, "tenant_0", "series_0001", "{}", 1, 4, 4, 4),
        ]
        changes = {
            "group": (1_800_000_000, "tenant_1", "series_0000", "{}", 2, 8, 0, 8),
            "count": (1_800_000_000, "tenant_0", "series_0000", "{}", 3, 8, 0, 8),
            "value": (1_800_000_000, "tenant_0", "series_0000", "{}", 2, 12, 0, 8),
        }
        good = [aggregate(row) for row in expected]
        for name, replacement in changes.items():
            bad = [aggregate(row) for row in sorted([replacement, expected[1]])]
            for backend in ("Varve", "Timescale"):
                with self.subTest(name=name, backend=backend), self.assertRaisesRegex(RuntimeError, "minute aggregate group differs"):
                    await verify_exact.verify_paged(
                        "minute aggregate group",
                        expected,
                        backend_pages(good, bad, backend),
                        verify_exact.normalize_aggregate,
                        page_size=2,
                    )


class ExactContractTests(unittest.TestCase):
    def test_empty_tags_accept_only_the_same_map_across_sql_representations(self):
        for value in ({}, "{}", " { } "):
            self.assertEqual(verify_exact._empty_tags(value, "raw"), "{}")
        for value in (None, False, [], "[]", '"{}"', "{bad", '{"extra": 1}', "{} trailing", "", "NaN"):
            with self.subTest(value=value), self.assertRaisesRegex(RuntimeError, "tags"):
                verify_exact._empty_tags(value, "raw")

    def test_expected_iteration_only_buffers_one_late_remainder_class(self):
        source = plan(initial=3, late=4097)
        bounds = verify_exact.IterationBounds()
        previous = None
        count = 0
        for row in verify_exact.expected_rows(source, bounds):
            if previous is not None:
                self.assertLessEqual(previous, row)
            previous = row
            count += 1
        self.assertEqual(count, source.total_rows)
        self.assertEqual(bounds.max_late_class_values, math.ceil(source.late_rows / verify_exact.SERIES_COUNT))

    def test_output_path_must_not_exist(self):
        with tempfile.TemporaryDirectory() as temporary:
            report = Path(temporary) / "report.json"
            output = Path(temporary) / "already.json"
            report.write_text("{}")
            output.write_text("occupied")
            with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                verify_exact.arguments(["--report", str(report), "--output", str(output)])
            self.assertEqual(output.read_text(), "occupied")

    def test_complete_overloaded_report_is_diagnostic_and_never_promoted(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "source.json"
            path.write_text(json.dumps(complete_report("overloaded")))
            source = verify_exact.load_source(path)
        result = verify_exact.result_base(source)
        self.assertEqual(result["verdict"]["source_benchmark"], "overloaded")
        self.assertTrue(result["verdict"]["source_benchmark_preserved"])
        self.assertFalse(result["verdict"]["performance_approval"])
        self.assertTrue(result["claim_boundary"]["supplemental_not_replacement"])
        self.assertIn("does not promote", result["verdict"]["statement"])

    def test_malformed_conservation_and_verdict_fields_are_refused(self):
        for state in ("passed", "overloaded"):
            mutations = {
                "offered_rows": 2,
                "dropped_rows": 1,
                "state": "failed",
                "schedule_met": state != "passed",
                "clean": state != "passed",
            }
            with tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary) / "source.json"
                path.write_text(json.dumps(complete_report(state)))
                self.assertEqual(verify_exact.load_source(path).source_state, state)
                for field, value in mutations.items():
                    report = complete_report(state)
                    report["mixed_workload"][field] = value
                    path.write_text(json.dumps(report))
                    with self.subTest(state=state, field=field), self.assertRaises(RuntimeError):
                        verify_exact.load_source(path)

    def test_run_identity_namespace_and_minute_alignment_are_bound(self):
        mutations = (
            ("run_id", None),
            ("run_id", "unsafe-id"),
            ("run_id", "other"),
            ("namespaces", names_for("other").json()),
            ("base_timestamp_us", 1_800_000_001),
        )
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "source.json"
            for field, value in mutations:
                report = complete_report()
                target = report["manifest"] if field == "base_timestamp_us" else report
                target[field] = value
                path.write_text(json.dumps(report))
                with self.subTest(field=field, value=value), self.assertRaises(RuntimeError):
                    verify_exact.load_source(path)

    def test_incomplete_report_is_refused(self):
        report = complete_report("overloaded")
        report["mixed_workload"]["pending_rows"] = 1
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "source.json"
            path.write_text(json.dumps(report))
            with self.assertRaisesRegex(RuntimeError, "failed, ambiguous, pending"):
                verify_exact.load_source(path)


if __name__ == "__main__":
    unittest.main()
