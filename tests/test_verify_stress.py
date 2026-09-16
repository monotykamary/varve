import importlib.util
import pathlib
import sys
import unittest
from unittest import mock

SCRIPTS = pathlib.Path(__file__).parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))
SPEC = importlib.util.spec_from_file_location("verify_stress", SCRIPTS / "verify-stress.py")
verify_stress = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(verify_stress)
sys.path.pop(0)


class VerifyStressTests(unittest.TestCase):
    def report(self, expired=False):
        return {"table": "stress_test", "aggregate": "agg_test", "completed_rows": 8, "expected_sum": 3.5, "retained_rollup_after_raw_expiration": [{"n": 8}] if expired else None}

    def client(self, expired=False):
        client = mock.Mock()
        client.sql.side_effect = [[{"n": 0, "total": None} if expired else {"n": 8, "total": 3.5}], [{"n": 8, "total": 3.5}]]
        client.request.return_value = {"fenced": None}
        return client

    def test_raw_and_independently_expired_reports(self):
        for expired in (False, True):
            result = verify_stress.verify(self.report(expired), self.client(expired))
            self.assertFalse(result["fenced"])
            self.assertEqual(result["rollup"]["n"], 8)

    def test_bad_identifiers_never_reach_sql(self):
        report = self.report()
        report["table"] = "metrics; DROP TABLE metrics"
        client = self.client()
        with self.assertRaises(ValueError):
            verify_stress.verify(report, client)
        client.sql.assert_not_called()

    def test_incorrect_count_and_fencing_are_failures(self):
        report = self.report()
        report["completed_rows"] = 9
        with self.assertRaisesRegex(RuntimeError, "oracle mismatch"):
            verify_stress.verify(report, self.client())
        client = self.client()
        client.request.return_value = {"fenced": "failure"}
        with self.assertRaisesRegex(RuntimeError, "fenced"):
            verify_stress.verify(self.report(), client)
