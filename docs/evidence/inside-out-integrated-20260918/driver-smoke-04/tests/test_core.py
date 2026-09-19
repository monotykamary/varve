import unittest

from core import (
    BUCKET_US,
    Deadline,
    Oracle,
    aligned_base_us,
    canonical_json_sha256,
    canonical_payload_digest,
    checked_identifier,
    checked_run_id,
    late_measurement,
    latency_summary,
    measurement,
    parse_bounded_float,
    parse_bounded_int,
    percentile,
)


class FakeClock:
    def __init__(self):
        self.now = 10.0

    def __call__(self):
        return self.now


class DatasetTests(unittest.TestCase):
    def test_formula_and_schema_payloads(self):
        base = aligned_base_us(1_800_000_000_123_456)
        self.assertEqual(base % BUCKET_US, 0)
        row = measurement(1025, base)
        self.assertEqual(row.timestamp_us, base + 1_000_001)
        self.assertEqual(row.tenant, "tenant_1")
        self.assertEqual(row.series, "series_0001")
        self.assertEqual(row.value, ((1025 * 17) % 10000) / 4)
        varve = row.varve()
        self.assertEqual(set(varve), {"timestamp_us", "tenant", "series", "value", "tags"})
        self.assertIsInstance(varve["timestamp_us"], int)
        self.assertIsInstance(varve["value"], float)
        self.assertEqual(varve["tags"], {})
        postgres = row.postgres()
        self.assertEqual(len(postgres), 5)
        self.assertEqual(postgres[1:4], (row.tenant, row.series, row.value))
        self.assertEqual(postgres[4], {})

    def test_late_rows_are_fixed_and_before_base(self):
        base = 1_000_000_000
        rows = [late_measurement(i, base) for i in range(20)]
        self.assertTrue(all(row.timestamp_us < base for row in rows))
        self.assertNotEqual(rows[0].timestamp_us, rows[1].timestamp_us)
        self.assertEqual(rows, [late_measurement(i, base) for i in range(20)])

    def test_canonical_payload_digest_is_ordered_and_exact(self):
        rows = [measurement(0, 0), measurement(1, 0)]
        digest = canonical_payload_digest(rows)
        self.assertEqual(digest, canonical_payload_digest(list(rows)))
        self.assertNotEqual(digest, canonical_payload_digest(reversed(rows)))
        changed = [rows[0], type(rows[1])(rows[1].timestamp_us, rows[1].tenant, rows[1].series, rows[1].value_q + 1)]
        self.assertNotEqual(digest, canonical_payload_digest(changed))
        self.assertRegex(digest, r"^[0-9a-f]{64}$")
        self.assertEqual(
            canonical_json_sha256({"b": 2, "a": 1}),
            canonical_json_sha256({"a": 1, "b": 2}),
        )

    def test_integer_oracles_cover_groups_buckets_and_window(self):
        base = 1_020_000_000
        oracle = Oracle()
        oracle.extend(measurement(i, base) for i in range(8192))
        snapshot = oracle.snapshot()
        self.assertEqual(snapshot["global"]["count"], 8192)
        self.assertEqual(len(snapshot["tenants"]), 4)
        self.assertEqual(len(oracle.groups), 4096)
        self.assertGreaterEqual(len(snapshot["selected_buckets"]), 1)
        self.assertLessEqual(len(snapshot["selected_window"]), 5)
        expected_q = sum((i * 17) % 10000 for i in range(8192))
        self.assertEqual(snapshot["global"]["sum"], expected_q / 4)


class UtilityTests(unittest.TestCase):
    def test_nearest_rank_percentiles(self):
        self.assertIsNone(percentile([], 0.5))
        self.assertEqual(percentile([4, 1, 3, 2], 0.5), 2)
        self.assertEqual(percentile([4, 1, 3, 2], 0.99), 4)
        summary = latency_summary([1.0, 2.0, 3.0])
        self.assertEqual(summary["samples"], 3)
        self.assertEqual(summary["p95_ms"], 3.0)

    def test_deadline_never_extends_outer_ceiling(self):
        clock = FakeClock()
        deadline = Deadline(20, clock)
        self.assertEqual(deadline.timeout(3), 3)
        clock.now = 29
        self.assertEqual(deadline.timeout(3), 1)
        clock.now = 30
        with self.assertRaises(TimeoutError):
            deadline.timeout(1)


    def test_cli_numeric_bounds_reject_unsafe_values(self):
        self.assertEqual(parse_bounded_int("1000000", "rows", 1, 1_000_000), 1_000_000)
        self.assertEqual(parse_bounded_float("20000", "rate", 1, 20_000), 20_000.0)
        for value in ("0", "1000001", "nan", "inf"):
            with self.assertRaises(ValueError):
                if value in ("nan", "inf"):
                    parse_bounded_float(value, "rate", 1, 20_000)
                else:
                    parse_bounded_int(value, "rows", 1, 1_000_000)

    def test_identifier_and_run_id_safety(self):
        self.assertEqual(checked_identifier("vb_run_123"), "vb_run_123")
        self.assertEqual(checked_run_id("run_123"), "run_123")
        for value in ("UPPER", "a-b", "../x", "1bad", "a" * 64):
            with self.assertRaises(ValueError):
                checked_identifier(value)
        for value in ("Bad", "a-b", "x" * 25):
            with self.assertRaises(ValueError):
                checked_run_id(value)


if __name__ == "__main__":
    unittest.main()
