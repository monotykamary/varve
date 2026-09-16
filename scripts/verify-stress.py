#!/usr/bin/env python3
"""Read-only count/sum verification from a prior synthetic stress report, including after restart."""
import argparse
import json
import math
import os
import pathlib
import re

from stress import Client


def verify(report, client):
    table, aggregate = report["table"], report["aggregate"]
    for name in (table, aggregate):
        if not isinstance(name, str) or re.fullmatch(r"[a-z][a-z0-9_]{0,62}", name) is None:
            raise ValueError("report contains an invalid relation name")
    count, total = report["completed_rows"], report["expected_sum"]
    if type(count) is not int or not 1 <= count <= 500000 or not isinstance(total, (int, float)) or not math.isfinite(total):
        raise ValueError("invalid report oracle")
    expired = report.get("retained_rollup_after_raw_expiration") is not None
    raw = client.sql("SELECT count(*) AS n, sum(value) AS total FROM " + table)[0]
    derived = client.sql("SELECT CAST(sum(count) AS BIGINT) AS n, sum(sum) AS total FROM " + aggregate)[0]
    expected_raw = {"n": 0, "total": None} if expired else {"n": count, "total": total}
    if raw != expected_raw or derived != {"n": count, "total": total}:
        raise RuntimeError("persisted raw/aggregate oracle mismatch")
    status = client.request("/v1/status")
    if status.get("fenced") is not None:
        raise RuntimeError("reopened database is fenced")
    return {"table": table, "aggregate": aggregate, "raw": raw, "rollup": derived, "fenced": False, "verified": ["persisted_raw_count_sum", "persisted_named_aggregate_count_sum"], "note": "Read-only persisted-data check; invoke after observing an actual service restart to claim restart recovery."}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--report", required=True, type=pathlib.Path)
    args = parser.parse_args()
    with args.report.open("rb") as source:
        data = source.read(2 * 1024 * 1024 + 1)
    if len(data) > 2 * 1024 * 1024:
        raise ValueError("report exceeds size limit")
    report = json.loads(data)
    client = Client(args.url, os.environ.get("VARVE_API_TOKEN", ""))
    print(json.dumps(verify(report, client), indent=2, allow_nan=False))


if __name__ == "__main__":
    main()
