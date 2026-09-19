"""Disposable fixed-extent I/O diagnostic; no database/recovery-format prototype."""
import hashlib
import json
import os
from pathlib import Path
import statistics
import tempfile
import time

N = 64
BLOCK = 1024
DATA = 12288
SIZE = DATA + N * BLOCK


def put(fd, payload, offset):
    view = memoryview(payload)
    written = 0
    while written < len(view):
        count = os.pwrite(fd, view[written:], offset + written)
        if count <= 0:
            raise RuntimeError("write made no progress")
        written += count


def stats(values):
    ordered = sorted(values)
    return {"samples": len(values), "mean_ms": statistics.mean(values), "p50_ms": statistics.median(values), "p95_ms": ordered[(95 * len(values) + 99) // 100 - 1], "max_ms": max(values)}


def main():
    root = Path("/data/probes")
    if root.resolve(strict=True) != root or not root.is_dir():
        raise RuntimeError("expected existing owned probe directory")
    expected = bytearray(SIZE)
    phases = {name: [] for name in ("frame_write", "frame_sync", "marker_write", "marker_sync", "total")}
    with tempfile.TemporaryDirectory(prefix="varve-two-sync-diag-", dir=root) as temporary:
        fd = os.open(Path(temporary) / "fixed", os.O_CREAT | os.O_EXCL | os.O_RDWR | os.O_CLOEXEC, 0o600)
        try:
            for offset in range(0, SIZE, BLOCK):
                put(fd, bytes(BLOCK), offset)
            os.fsync(fd)
            directory = os.open(temporary, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
            for i in range(N):
                frame = bytes([i + 1]) * BLOCK
                marker = bytes([255 - i]) * 128  # Opaque stub, NOT a commit-marker encoding.
                frame_offset = DATA + i * BLOCK
                marker_offset = 4096 + ((i + 1) % 2) * 4096
                t0 = time.perf_counter_ns()
                put(fd, frame, frame_offset)
                t1 = time.perf_counter_ns()
                os.fdatasync(fd)
                t2 = time.perf_counter_ns()
                put(fd, marker, marker_offset)
                t3 = time.perf_counter_ns()
                os.fdatasync(fd)
                t4 = time.perf_counter_ns()
                for name, start, end in (("frame_write", t0, t1), ("frame_sync", t1, t2), ("marker_write", t2, t3), ("marker_sync", t3, t4), ("total", t0, t4)):
                    phases[name].append((end - start) / 1e6)
                expected[frame_offset:frame_offset + BLOCK] = frame
                expected[marker_offset:marker_offset + 128] = marker
            os.lseek(fd, 0, os.SEEK_SET)
            actual = bytearray()
            while block := os.read(fd, 65536):
                actual.extend(block)
            if actual != expected or os.fstat(fd).st_size != SIZE:
                raise RuntimeError("exact full-file readback or extent mismatch")
        finally:
            os.close(fd)
    if Path(temporary).exists():
        raise RuntimeError("fixture was not removed")
    print(json.dumps({"state": "passed", "kind": "filesystem_two_ordered_syncs_not_database_qualification", "deployment_id": os.environ.get("RAILWAY_DEPLOYMENT_ID"), "uid": os.getuid(), "device": root.stat().st_dev, "fixture_bytes": SIZE, "fixture_removed": True, "initialization_excluded": True, "marker_is_opaque_stub": True, "sync_calls": N * 2, "exact_readback_sha256": hashlib.sha256(actual).hexdigest(), "phases": {name: stats(values) for name, values in phases.items()}}, sort_keys=True))


if __name__ == "__main__":
    main()
