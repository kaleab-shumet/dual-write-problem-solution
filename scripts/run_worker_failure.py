#!/usr/bin/env python3
"""Exercise a real worker outage in two phases.

Run ``prepare`` while the worker is stopped, then restart the worker and run
``finish``. The queued BullMQ job should repair the dirty cache.
"""

from __future__ import annotations

import json
import sys
from urllib.error import HTTPError
from urllib.request import Request, urlopen


BASE_URL = "http://127.0.0.1:8000"


def request(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    payload = None if body is None else json.dumps(body).encode()
    req = Request(
        f"{BASE_URL}{path}",
        data=payload,
        method=method,
        headers={"content-type": "application/json"},
    )
    try:
        with urlopen(req, timeout=20) as response:
            return response.status, json.loads(response.read().decode())
    except HTTPError as error:
        return error.code, json.loads(error.read().decode())


def prepare() -> None:
    request("POST", "/demo/reset")
    request("GET", "/users/42")
    status, writer = request(
        "POST",
        "/demo/race/writer/42?writer_id=w1&skip_after=true",
        {"name": "Grace", "phone_number": "+1-555-0143"},
    )
    if status != 200 or writer.get("outcome") != "simulated_crash_after_postgres_commit":
        raise AssertionError(f"failed to create dirty cache: {status} {writer}")

    status, pending = request("GET", "/users/42")
    _, debug = request("GET", "/debug/users/42")
    print(json.dumps({"read_while_worker_down": pending, "debug": debug}, indent=2))
    if status != 202 or pending.get("served_from") != "repair_pending":
        raise AssertionError(f"expected a pending repair while worker is down: {status} {pending}")
    if debug["redis"].get("trusted"):
        raise AssertionError("Redis became trusted while the worker was down")


def finish() -> None:
    status, repaired = request("GET", "/users/42")
    _, debug = request("GET", "/debug/users/42")
    print(json.dumps({"read_after_worker_restart": repaired, "debug": debug}, indent=2))
    if status != 200:
        raise AssertionError(f"read failed after worker restart: {status} {repaired}")
    if repaired.get("served_from") not in {"redis", "redis_after_worker_repair"}:
        raise AssertionError(f"worker did not repair the cache: {repaired}")
    if repaired.get("user", {}).get("name") != "Grace":
        raise AssertionError("worker returned the wrong repaired value")
    if not debug["redis"].get("trusted"):
        raise AssertionError("Redis is still untrusted after worker repair")


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in {"prepare", "finish"}:
        raise SystemExit("usage: run_worker_failure.py prepare|finish")
    if sys.argv[1] == "prepare":
        prepare()
    else:
        finish()
