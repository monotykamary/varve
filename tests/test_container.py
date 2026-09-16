import pathlib
import subprocess
import tempfile
import unittest

ENTRYPOINT = pathlib.Path(__file__).parents[1] / "scripts" / "container-entrypoint.sh"


class ContainerEntrypointTests(unittest.TestCase):
    def invoke(self, *, mounted=True, token=True, uid="0", command=()):
        with tempfile.TemporaryDirectory() as temporary:
            directory = pathlib.Path(temporary)
            programs = {
                "mountpoint": "exit " + ("0" if mounted else "1"),
                "mkdir": "exit 0",
                "chown": "printf 'CHOWN\\n'",
                "id": "printf '%s\\n' " + uid,
                "gosu": "printf 'DROP_UID:%s\\n' \"$1\"; shift; exec \"$@\"",
                "varve": "printf 'ARG:%s\\n' \"$@\"",
            }
            for name, body in programs.items():
                executable = directory / name
                executable.write_text("#!/bin/sh\n" + body + "\n")
                executable.chmod(0o755)
            environment = {
                "PATH": str(directory),
                "VARVE_REQUIRE_VOLUME": "1",
                "VARVE_USE_S3": "true",
                "VARVE_CONFIG": "/app/config.json",
                "VARVE_DATA_DIR": "/data/varve",
                "PORT": "8080",
            }
            if token:
                environment["VARVE_API_TOKEN"] = "x" * 64
            return subprocess.run(["/bin/sh", str(ENTRYPOINT), *command], env=environment, text=True, capture_output=True, timeout=3)

    def test_missing_volume_refuses_before_creating_directories(self):
        result = self.invoke(mounted=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Refusing ephemeral", result.stderr)
        self.assertNotIn("CHOWN", result.stdout)
        self.assertNotIn("ARG:", result.stdout)

    def test_public_default_requires_token(self):
        result = self.invoke(token=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("VARVE_API_TOKEN must be configured", result.stderr)
        self.assertNotIn("ARG:", result.stdout)

    def test_default_forwards_exact_args_and_drops_root(self):
        result = self.invoke()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("DROP_UID:varve", result.stdout)
        arguments = [line[4:] for line in result.stdout.splitlines() if line.startswith("ARG:")]
        self.assertEqual(arguments, ["--data", "/data/varve", "--config", "/app/config.json", "--s3", "serve", "--bind", "0.0.0.0", "--port", "8080", "--allow-remote"])
        self.assertNotIn("x" * 64, result.stdout + result.stderr)

    def test_existing_nonroot_identity_and_explicit_command(self):
        result = self.invoke(uid="10001", token=False, command=("varve", "--help"))
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("DROP_UID", result.stdout)
        self.assertNotIn("CHOWN", result.stdout)
        self.assertEqual(result.stdout, "ARG:--help\n")
