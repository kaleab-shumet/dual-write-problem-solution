#!/usr/bin/env python3
"""Exercise Redis failures around the BEFORE and AFTER boundaries."""

from __future__ import annotations

import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError, URLError
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
        raw = error.read().decode()
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {"error": raw}
        return error.code, payload
    except (URLError, TimeoutError) as error:
        return 599, {"error": str(getattr(error, "reason", error))}


def reset_and_warm() -> None:
    request("POST", "/demo/reset")
    request("GET", "/users/42")


def before_failure() -> None:
    status, payload = request(
        "POST",
        "/demo/race/writer/42?writer_id=redis-before-failure",
        {"name": "ShouldNotCommit", "phone_number": "+1-555-0191"},
    )
    print(json.dumps({"http_status": status, "response": payload}, indent=2))


def after_failure() -> None:
    def send() -> tuple[int, dict]:
        return request(
            "POST",
            "/demo/race/writer/42?writer_id=redis-after-failure&after_delay_ms=15000",
            {"name": "CommittedInPostgres", "phone_number": "+1-555-0192"},
        )

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(send)
        time.sleep(1)
        print("READY_TO_STOP_REDIS", flush=True)
        status, payload = future.result()
    print(json.dumps({"http_status": status, "response": payload}, indent=2))


def verify_after_recovery() -> None:
    status, payload = request("GET", "/users/42")
    _, debug = request("GET", "/debug/users/42")
    print(json.dumps({"http_status": status, "read": payload, "debug": debug}, indent=2))
    if status != 200:
        raise AssertionError(f"recovery read failed: {status} {payload}")
    if payload.get("user", {}).get("name") != "CommittedInPostgres":
        raise AssertionError("recovery returned the wrong database value")
    if not debug["redis"].get("trusted"):
        raise AssertionError("Redis did not become trusted after recovery")
    if debug["redis"]["value"]["name"] != "CommittedInPostgres":
        raise AssertionError("Redis did not converge to PostgreSQL")


if __name__ == "__main__":
    commands = {
        "reset": reset_and_warm,
        "before": before_failure,
        "after": after_failure,
        "verify": verify_after_recovery,
    }
    if len(sys.argv) != 2 or sys.argv[1] not in commands:
        raise SystemExit("usage: run_redis_failure.py reset|before|after|verify")
    commands[sys.argv[1]]()
