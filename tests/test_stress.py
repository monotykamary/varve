import importlib.util
import io
import pathlib
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location("stress", pathlib.Path(__file__).parents[1] / "scripts" / "stress.py")
stress = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(stress)


class StressProbeTests(unittest.TestCase):
    def test_rejects_cleartext_remote_and_embedded_credentials(self):
        for url in ["http://example.com", "https://user:pass@example.com", "https://example.com/?token=secret", "file:///etc/passwd"]:
            with self.assertRaises(ValueError):
                stress.validate_target(url)
        for url in ["https://example.com", "http://127.0.0.1:8080", "http://[::1]:8080"]:
            stress.validate_target(url)

    def test_rows_are_exact_binary_fractions_and_out_of_order(self):
        rows = stress.rows_for(0, 1000, 1000000)
        self.assertEqual(len(rows), 1000)
        self.assertGreater(rows[0]["timestamp_us"], rows[-1]["timestamp_us"])
        self.assertEqual(sum(row["value"] for row in rows), 999 * 1000 / 16)
        self.assertEqual(rows, stress.rows_for(0, 1000, 1000000))

    def test_redirects_and_invalid_header_tokens_are_rejected(self):
        with self.assertRaisesRegex(RuntimeError, "redirect refused"):
            stress.NoRedirect().redirect_request(None, None, 302, "redirect", {}, "https://another.example")
        for token in ["x" * 31, "x" * 32 + "\n", "x" * 32 + "\u00e9"]:
            with self.assertRaises(ValueError):
                stress.Client("http://127.0.0.1:8080", token)

    def test_retries_are_bounded_and_do_not_echo_transport_errors(self):
        client = stress.Client("http://127.0.0.1:8080", "x" * 32)
        with mock.patch.object(client.opener, "open", side_effect=stress.urllib.error.URLError("private transport details")) as opened, mock.patch.object(stress.time, "sleep") as slept:
            with self.assertRaisesRegex(RuntimeError, "ambiguous") as error:
                client.request("/v1/write", {"request_id": "unchanged"}, retry=True)
            self.assertEqual(opened.call_count, 4)
            self.assertEqual(client.retries, 3)
            self.assertEqual(slept.call_count, 3)
            self.assertNotIn("private", str(error.exception))
            self.assertEqual(len({call.args[0].data for call in opened.call_args_list}), 1)

    def test_response_size_and_nonretryable_status_are_bounded(self):
        client = stress.Client("http://127.0.0.1:8080", "x" * 32)
        response = mock.MagicMock()
        response.__enter__.return_value = response
        response.read.return_value = b"x" * (2 * 1024 * 1024 + 1)
        with mock.patch.object(client.opener, "open", return_value=response):
            with self.assertRaisesRegex(RuntimeError, "response exceeds"):
                client.request("/v1/status")
        response.read.assert_called_once_with(2 * 1024 * 1024 + 1)
        error = stress.urllib.error.HTTPError("http://127.0.0.1:8080", 409, "conflict", {}, None)
        with mock.patch.object(client.opener, "open", side_effect=error) as opened:
            with self.assertRaisesRegex(RuntimeError, "status 409"):
                client.request("/v1/write", {}, retry=True)
            self.assertEqual(opened.call_count, 1)

    def test_timed_id_and_error_details_remain_out_of_printed_errors(self):
        self.assertEqual(stress.timed_request_id(123, "nonce"), "v1:123:nonce")
        client = stress.Client("http://127.0.0.1:8080", "x" * 32)
        error = stress.urllib.error.HTTPError("http://127.0.0.1:8080", 400, "bad", {}, io.BytesIO(b'{"error":"private idempotency window detail"}'))
        with mock.patch.object(client.opener, "open", side_effect=error):
            with self.assertRaises(stress.HTTPStatusError) as caught:
                client.request("/v1/write", {})
        self.assertEqual(caught.exception.status, 400)
        self.assertIn("idempotency window", caught.exception.server_message)
        self.assertNotIn("private", str(caught.exception))

    def test_percentiles_and_safe_literals(self):
        self.assertEqual(stress.percentile([4, 1, 2, 3], .95), 4)
        self.assertIsNone(stress.percentile([], .50))
        self.assertEqual(stress.literal("a'b"), "'a''b'")
