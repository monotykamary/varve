"""Bounded filesystem diagnostic on disposable files, not a DB durability test."""
import contextlib
import hashlib
import json
import os
from pathlib import Path
import statistics
import tempfile
import time

SAMPLES = 64
BLOCK = 1024
MODES = ("append_fsync", "append_fdatasync", "preallocated_fsync", "preallocated_fdatasync")


def write_all(fd, payload, offset=None):
    view = memoryview(payload)
    done = 0
    while done < len(view):
        count = os.write(fd, view[done:]) if offset is None else os.pwrite(fd, view[done:], offset + done)
        if count <= 0:
            raise RuntimeError("write made no progress")
        done += count


def summary(values):
    ordered = sorted(values)
    return {"samples": len(values), "mean_ms": statistics.mean(values), "p50_ms": statistics.median(values), "p95_ms": ordered[(95 * len(values) + 99) // 100 - 1], "max_ms": max(values)}


def main():
    if not hasattr(os, "fdatasync") or not hasattr(os, "pwrite"):
        raise RuntimeError("Linux fdatasync/pwrite required; do not substitute")
    root = Path("/data/probes")
    if root.resolve(strict=True) != root or not root.is_dir():
        raise RuntimeError("expected the existing owned /data/probes directory")
    result = {"kind": "filesystem_sync_diagnostic_not_database_qualification", "samples_per_mode": SAMPLES, "bytes_per_write": BLOCK, "maximum_fixture_bytes": len(MODES) * SAMPLES * BLOCK, "deployment_id": os.environ.get("RAILWAY_DEPLOYMENT_ID"), "device": root.stat().st_dev, "block_size": os.statvfs(root).f_bsize, "cpu_max": Path("/sys/fs/cgroup/cpu.max").read_text().strip(), "memory_max": Path("/sys/fs/cgroup/memory.max").read_text().strip(), "modes": {}}
    expected = hashlib.sha256()
    for i in range(SAMPLES):
        expected.update(bytes([i % 251]) * BLOCK)
    with tempfile.TemporaryDirectory(prefix="varve-sync-diag-", dir=root) as temporary:
        with contextlib.ExitStack() as owners:
            fds = {}
            timings = {mode: {"write": [], "sync": []} for mode in MODES}
            for mode in MODES:
                fd = os.open(Path(temporary) / mode, os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_CLOEXEC, 0o600)
                owners.callback(os.close, fd)
                fds[mode] = fd
                if mode.startswith("preallocated"):
                    for _ in range(SAMPLES):
                        write_all(fd, bytes(BLOCK))
                os.fsync(fd)
            directory = os.open(temporary, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            for i in range(SAMPLES):
                payload = bytes([i % 251]) * BLOCK
                # Rotate order to avoid giving one mode every first observation.
                for j in range(len(MODES)):
                    mode = MODES[(i + j) % len(MODES)]
                    fd = fds[mode]
                    start = time.perf_counter_ns()
                    write_all(fd, payload, i * BLOCK if mode.startswith("preallocated") else None)
                    written = time.perf_counter_ns()
                    (os.fdatasync if mode.endswith("fdatasync") else os.fsync)(fd)
                    synced = time.perf_counter_ns()
                    timings[mode]["write"].append((written - start) / 1e6)
                    timings[mode]["sync"].append((synced - written) / 1e6)
            for mode, fd in fds.items():
                os.lseek(fd, 0, os.SEEK_SET)
                digest = hashlib.sha256()
                size = 0
                while data := os.read(fd, 65536):
                    digest.update(data)
                    size += len(data)
                if size != SAMPLES * BLOCK or digest.hexdigest() != expected.hexdigest():
                    raise RuntimeError("probe readback mismatch: " + mode)
                result["modes"][mode] = {"write": summary(timings[mode]["write"]), "sync": summary(timings[mode]["sync"]), "bytes": size, "readback_sha256": digest.hexdigest()}
        result["open_files_closed"] = True
    result["fixture_removed"] = not Path(temporary).exists()
    if not result["fixture_removed"]:
        raise RuntimeError("probe fixture was not removed")
    result["state"] = "passed"
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
