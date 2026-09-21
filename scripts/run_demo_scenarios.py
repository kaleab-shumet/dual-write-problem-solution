#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


BASE_URL = os.getenv("DEMO_BASE_URL", "http://localhost:8000").rstrip("/")
USER_ID = "42"


def print_title(text: str) -> None:
    print("\n" + "=" * 88)
    print(text)
    print("=" * 88)


def print_step(number: int, text: str) -> None:
    print(f"\n{number}. {text}")


def print_json(label: str, value: Any) -> None:
    print(f"\n{label}:")
    print(json.dumps(value, indent=2, sort_keys=True))


def request(method: str, path: str, body: dict[str, Any] | None = None) -> tuple[int, dict[str, Any]]:
    payload = None if body is None else json.dumps(body).encode("utf-8")
    req = Request(
        f"{BASE_URL}{path}",
        data=payload,
        method=method,
        headers={"content-type": "application/json"},
    )
    try:
        with urlopen(req, timeout=5) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        data = error.read().decode("utf-8")
        try:
            payload = json.loads(data)
        except json.JSONDecodeError:
            payload = {"error": data}
        return error.code, payload
    except URLError as error:
        raise SystemExit(
            f"Could not reach {BASE_URL}. Start the demo first with: docker compose up -d"
        ) from error


def get_user() -> dict[str, Any]:
    _, payload = request("GET", f"/users/{USER_ID}")
    return payload


def debug_state() -> dict[str, Any]:
    _, payload = request("GET", f"/debug/users/{USER_ID}")
    return payload


def wait_for_trusted(timeout_seconds: float = 3.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    latest = get_user()
    while time.monotonic() < deadline:
        if latest.get("trusted_cache"):
            return latest
        time.sleep(0.1)
        latest = get_user()
    return latest


def summarize_read(payload: dict[str, Any]) -> dict[str, Any]:
    user = payload.get("user") or {}
    return {
        "served_from": payload.get("served_from"),
        "trusted_cache": payload.get("trusted_cache"),
        "name": user.get("name"),
        "phone_number": user.get("phone_number"),
        "version": user.get("version"),
        "repair_job_id": payload.get("repair_job_id"),
    }


def summarize_debug(debug: dict[str, Any]) -> dict[str, Any]:
    redis = debug.get("redis") or {}
    postgres = debug.get("postgres") or {}
    return {
        "postgres": {
            "name": postgres.get("name"),
            "phone_number": postgres.get("phone_number"),
            "version": postgres.get("version"),
        },
        "redis": {
            "trusted": redis.get("trusted"),
            "before_uuid": redis.get("before_uuid"),
            "after_uuid": redis.get("after_uuid"),
            "value_name": (redis.get("value") or {}).get("name"),
            "value_version": (redis.get("value") or {}).get("version"),
        },
        "dirty_keys": debug.get("dirty_keys"),
    }


def reset_and_bootstrap() -> None:
    print_title("Scenario 1: Empty Redis, First Read Boots Cache")

    print_step(1, "Reset the demo. Postgres has Ada, Redis is empty.")
    _, reset = request("POST", "/demo/reset")
    print_json("State after reset", summarize_debug(reset))

    print_step(2, "Read the user. Redis has no trusted value, so the API reads Postgres and seeds Redis.")
    first = get_user()
    print_json("First read", summarize_read(first))

    print_step(3, "Read again. This time Redis is trusted, so the API serves from Redis.")
    second = get_user()
    print_json("Second read", summarize_read(second))


def normal_update() -> None:
    print_title("Scenario 2: Normal Writer Confirms Redis")

    print_step(1, "Send a normal profile update.")
    _, update = request(
        "PATCH",
        f"/users/{USER_ID}",
        {"name": "Margaret Hamilton", "phone_number": "+1-555-0201"},
    )
    print_json(
        "Writer result",
        {
            "outcome": update.get("outcome"),
            "attempt_uuid": update.get("attempt_uuid"),
            "confirm_result": update.get("confirm_result"),
            "user": update.get("user"),
        },
    )

    print_step(2, "The writer advanced the expected attempt, committed DB + cache state, then wrote AFTER with the same UUID.")
    latest = get_user()
    print_json("Read after normal update", summarize_read(latest))


def crash_after_commit() -> None:
    print_title("Scenario 3: DB Commit Succeeds, Redis AFTER Is Skipped")

    print_step(1, "Simulate a crash after the DB commit. The endpoint commits Postgres but intentionally skips Redis AFTER.")
    _, crash = request(
        "POST",
        "/demo/crash-after-db-commit",
        {"name": "Grace Hopper", "phone_number": "+1-555-0202"},
    )
    print_json(
        "Immediately after simulated crash",
        {
            "outcome": crash.get("outcome"),
            "attempt_uuid": crash.get("attempt_uuid"),
            "postgres_user": crash.get("user"),
            "debug": summarize_debug(crash.get("debug") or {}),
        },
    )

    print_step(
        2,
        "A BullMQ repair job reconciles the committed database attempt and writes a matching AFTER value.",
    )
    repaired = wait_for_trusted()
    print_json("Read after worker repair", summarize_read(repaired))
    print_json("Final state", summarize_debug(debug_state()))


def rejected_write() -> None:
    print_title("Scenario 4: Rejected Business Operation, Worker Repairs")

    print_step(1, "Simulate a rejected business operation. The DB transaction rolls back its cache-attempt transition.")
    _, rejected = request(
        "POST",
        "/demo/rejected-write",
        {"name": "Should Not Commit", "phone_number": "+1-555-0999"},
    )
    print_json(
        "Rejected write response",
        {
            "outcome": rejected.get("outcome"),
            "attempt_uuid": rejected.get("attempt_uuid"),
            "postgres_returned_row": rejected.get("postgres_returned_row"),
            "debug": summarize_debug(rejected.get("debug") or {}),
        },
    )

    print_step(
        2,
        "The worker observes the committed database attempt and confirms the still-current DB value.",
    )
    repaired = wait_for_trusted()
    print_json("Read after worker confirms rejected attempt", summarize_read(repaired))
    print_json("Final state", summarize_debug(debug_state()))


def delayed_after_race() -> None:
    print_title("Scenario 5: Late Old AFTER Cannot Clobber Newer Cache")

    print_step(1, "Run two committed updates, confirm the newer one first, then try the older AFTER late.")
    _, race = request(
        "POST",
        "/demo/delayed-after-race",
        {"name": "Katherine Johnson", "phone_number": "+1-555-0203"},
    )
    print_json(
        "Race response",
        {
            "outcome": race.get("outcome"),
            "first_update": race.get("first_update"),
            "second_update": race.get("second_update"),
            "confirm_second_result": race.get("confirm_second_result"),
            "delayed_confirm_first_result": race.get("delayed_confirm_first_result"),
        },
    )

    print_step(2, "The old AFTER returns 0, so Redis keeps the newer value.")
    latest = get_user()
    print_json("Read after delayed race", summarize_read(latest))


def concurrent_dirty_reads() -> None:
    print_title("Scenario 6: Many Readers See Dirty Redis")

    print_step(1, "Create a dirty cache by simulating crash-after-commit.")
    _, crash = request(
        "POST",
        "/demo/crash-after-db-commit",
        {"name": "Mary Jackson", "phone_number": "+1-555-0204"},
    )
    before_uuid = crash.get("attempt_uuid")
    print_json(
        "Dirty state",
        {
            "attempt_uuid": before_uuid,
            "debug": summarize_debug(crash.get("debug") or {}),
        },
    )

    print_step(
        2,
        "Send 10 concurrent reads. They all see the same dirty BEFORE UUID, enqueue the same BullMQ job id, then wait on Redis instead of each hammering Postgres.",
    )
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(get_user) for _ in range(10)]
        results = [future.result() for future in as_completed(futures)]

    summarized = [summarize_read(result) for result in results]
    print_json("Concurrent read results", summarized)

    print_step(3, "After the worker finishes, normal reads are Redis hits again.")
    repaired = wait_for_trusted()
    print_json("Final read", summarize_read(repaired))


def main() -> None:
    print_title("Redis BEFORE/AFTER Demo Scenario Runner")
    print(f"Base URL: {BASE_URL}")
    print("This script explains each scenario and prints the important state transitions.")

    reset_and_bootstrap()
    normal_update()
    crash_after_commit()
    rejected_write()
    delayed_after_race()
    concurrent_dirty_reads()

    print_title("Done")
    print("The key guarantee: a writer can advance a cache key only when its expected AFTER attempt is still current in the database.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
