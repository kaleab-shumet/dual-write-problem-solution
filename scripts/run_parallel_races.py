#!/usr/bin/env python3
from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor
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
        with urlopen(req, timeout=40) as response:
            return response.status, json.loads(response.read().decode())
    except HTTPError as error:
        return error.code, json.loads(error.read().decode())


def run_case(title: str, writers: list[tuple[str, str, dict]]) -> None:
    print(f"\n=== {title} ===")
    request("POST", "/demo/reset")

    def send(writer_id: str, query: str, body: dict) -> tuple[str, int, dict]:
        status, payload = request(
            "POST",
            f"/demo/race/writer/42?writer_id={writer_id}&{query}",
            body,
        )
        return writer_id, status, payload

    with ThreadPoolExecutor(max_workers=len(writers)) as executor:
        futures = [executor.submit(send, *writer) for writer in writers]
        results = [future.result() for future in futures]

    for writer_id, status, payload in results:
        print(
            json.dumps(
                {
                    "writer_id": writer_id,
                    "http_status": status,
                    "outcome": payload.get("outcome"),
                    "attempt_uuid": payload.get("attempt_uuid"),
                    "confirm_result": payload.get("confirm_result"),
                    "user": payload.get("user"),
                },
                indent=2,
            )
        )

    _, final_state = request("GET", "/debug/users/42")
    print("Final state:")
    print(json.dumps(final_state, indent=2))


def run_sequential_handoff_case() -> None:
    print("\n=== Sequential handoff: W3 confirms, then W2 starts from W3 ===")
    request("POST", "/demo/reset")
    request("GET", "/users/42")

    results = []
    for writer_id, name, phone in [
        ("w3", "Maya", "+1-555-0163"),
        ("w2", "Luna", "+1-555-0162"),
    ]:
        status, payload = request(
            "POST",
            f"/demo/race/writer/42?writer_id={writer_id}",
            {"name": name, "phone_number": phone},
        )
        results.append((writer_id, status, payload))

    for writer_id, status, payload in results:
        print(
            json.dumps(
                {
                    "writer_id": writer_id,
                    "http_status": status,
                    "outcome": payload.get("outcome"),
                    "attempt_uuid": payload.get("attempt_uuid"),
                    "confirm_result": payload.get("confirm_result"),
                    "user": payload.get("user"),
                },
                indent=2,
            )
        )

    _, final_state = request("GET", "/debug/users/42")
    print("Final state:")
    print(json.dumps(final_state, indent=2))

    outcomes = [payload.get("outcome") for _, _, payload in results]
    if outcomes != ["committed_and_confirmed", "committed_and_confirmed"]:
        raise AssertionError(f"sequential handoff did not commit twice: {outcomes}")
    if final_state["postgres"]["name"] != "Luna":
        raise AssertionError("second sequential writer did not become the final DB value")
    if not final_state["redis"]["trusted"]:
        raise AssertionError("sequential handoff left Redis untrusted")
    if final_state["redis"]["value"]["name"] != "Luna":
        raise AssertionError("Redis does not contain the second writer's confirmed value")


def run_reader_case() -> None:
    print("\n=== Reader arrives while W1 is paused before the database ===")
    request("POST", "/demo/reset")
    request("GET", "/users/42")

    def send_writer() -> tuple[str, int, dict]:
        return (
            "w1",
            *request(
                "POST",
                "/demo/race/writer/42?writer_id=w1&before_delay_ms=2000",
                {"name": "Grace", "phone_number": "+1-555-0131"},
            ),
        )

    def send_reader() -> tuple[str, int, dict]:
        return (
            "r1",
            *request(
                "GET",
                "/demo/race/reader/42?reader_id=r1&before_read_delay_ms=100&repair=false",
            ),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        writer, reader = executor.submit(send_writer), executor.submit(send_reader)
        for result in (reader.result(), writer.result()):
            label, status, payload = result
            print(
                json.dumps(
                    {
                        "client_id": label,
                        "http_status": status,
                        "served_from": payload.get("served_from"),
                        "trusted_cache": payload.get("trusted_cache"),
                        "user": payload.get("user"),
                        "observed_before_uuid": payload.get("observed_before_uuid"),
                        "observed_after_uuid": payload.get("observed_after_uuid"),
                    },
                    indent=2,
                )
            )

    _, final_state = request("GET", "/debug/users/42")
    print("Final state:")
    print(json.dumps(final_state, indent=2))


def run_repair_writer_overlap_case() -> None:
    print("\n=== Repair overlaps a writer already inside its DB transaction ===")
    request("POST", "/demo/reset")
    request("GET", "/users/42")

    def send_writer() -> tuple[str, int, dict]:
        return (
            "w1",
            *request(
                "POST",
                "/demo/race/writer/42?writer_id=w1&transaction_delay_ms=2000",
                {"name": "Grace", "phone_number": "+1-555-0141"},
            ),
        )

    def send_reader() -> tuple[str, int, dict]:
        return (
            "r1",
            *request(
                "GET",
                "/demo/race/reader/42?reader_id=r1&singleflight=true",
            ),
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        writer_future = executor.submit(send_writer)
        time.sleep(0.2)
        reader_future = executor.submit(send_reader)
        writer = writer_future.result()
        reader = reader_future.result()

    for label, status, payload in (writer, reader):
        print(
            json.dumps(
                {
                    "client_id": label,
                    "http_status": status,
                    "outcome": payload.get("outcome"),
                    "served_from": payload.get("served_from"),
                    "singleflight_role": payload.get("singleflight_role"),
                    "trusted_cache": payload.get("trusted_cache"),
                    "user": payload.get("user"),
                    "observed_before_uuid": payload.get("observed_before_uuid"),
                    "observed_after_uuid": payload.get("observed_after_uuid"),
                },
                indent=2,
            )
        )

    _, final_state = request("GET", "/debug/users/42")
    print("Final state:")
    print(json.dumps(final_state, indent=2))

    if writer[2].get("outcome") != "committed_and_confirmed":
        raise AssertionError(f"writer did not commit and confirm: {writer[2]}")
    if reader[2].get("user", {}).get("name") != "Grace":
        raise AssertionError("repair reader did not observe the committed writer value")
    if reader[2].get("trusted_cache") is not True:
        raise AssertionError("repair reader did not finish with a trusted cache")
    if final_state["postgres"]["name"] != "Grace":
        raise AssertionError("database disagreed with the writer result")
    if not final_state["redis"].get("trusted"):
        raise AssertionError("Redis remained untrusted after repair completed")
    if final_state["redis"]["value"]["name"] != "Grace":
        raise AssertionError("Redis disagreed with the committed database value")


def run_concurrent_singleflight_readers_case() -> None:
    print("\n=== Ten readers share one repair for a dirty cache ===")
    request("POST", "/demo/reset")
    request("GET", "/users/42")
    status, writer = request(
        "POST",
        "/demo/race/writer/42?writer_id=w1&skip_after=true",
        {"name": "Grace", "phone_number": "+1-555-0142"},
    )
    if status != 200 or writer.get("outcome") != "simulated_crash_after_postgres_commit":
        raise AssertionError(f"failed to create dirty cache: {status} {writer}")

    def send(reader_id: str) -> tuple[str, int, dict]:
        return (
            reader_id,
            *request(
                "GET",
                f"/demo/race/reader/42?reader_id={reader_id}&singleflight=true",
            ),
        )

    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(send, [f"r{i}" for i in range(1, 11)]))

    summary = [
        {
            "reader_id": reader_id,
            "http_status": status,
            "served_from": payload.get("served_from"),
            "singleflight_role": payload.get("singleflight_role"),
            "trusted_cache": payload.get("trusted_cache"),
            "name": (payload.get("user") or {}).get("name"),
        }
        for reader_id, status, payload in results
    ]
    print(json.dumps(summary, indent=2))

    winner_count = sum(
        payload.get("singleflight_role") == "winner"
        for _, _, payload in results
    )
    database_repair_count = sum(
        payload.get("served_from") == "postgres_singleflight_repair"
        for _, _, payload in results
    )
    if winner_count != 1 or database_repair_count != 1:
        raise AssertionError(
            f"expected one singleflight repair winner, got roles={winner_count}, "
            f"database_repairs={database_repair_count}"
        )
    if any(status != 200 for _, status, _ in results):
        raise AssertionError("at least one reader request failed")
    if any(payload.get("user", {}).get("name") != "Grace" for _, _, payload in results):
        raise AssertionError("at least one reader received the wrong value")
    if any(payload.get("trusted_cache") is not True for _, _, payload in results):
        raise AssertionError("at least one reader did not receive a trusted result")

    _, final_state = request("GET", "/debug/users/42")
    print("Final state:")
    print(json.dumps(final_state, indent=2))
    if not final_state["redis"].get("trusted"):
        raise AssertionError("singleflight readers did not repair Redis")


def run_three_writer_permutations() -> None:
    print("\n=== Three-writer arrival/claim-order permutations ===")
    profiles = {
        "w1": ("Grace", "+1-555-0151"),
        "w2": ("Luna", "+1-555-0152"),
        "w3": ("Maya", "+1-555-0153"),
    }
    permutations = [
        ("w1", "w2", "w3"),
        ("w1", "w3", "w2"),
        ("w2", "w1", "w3"),
        ("w2", "w3", "w1"),
        ("w3", "w1", "w2"),
        ("w3", "w2", "w1"),
    ]

    for completion_order in permutations:
        title = " -> ".join(completion_order)
        print(f"\nOrder: {title}")
        request("POST", "/demo/reset")
        request("GET", "/users/42")

        def send(writer_id: str) -> tuple[str, int, dict]:
            name, phone = profiles[writer_id]
            return (
                writer_id,
                *request(
                    "POST",
                    "/demo/race/writer/42"
                    f"?writer_id={writer_id}&transaction_delay_ms=1000",
                    {"name": name, "phone_number": phone},
                ),
            )

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = []
            for writer_id in completion_order:
                futures.append(executor.submit(send, writer_id))
                time.sleep(0.2)
            results = [future.result() for future in futures]

        for writer_id, status, payload in results:
            print(
                json.dumps(
                    {
                        "writer_id": writer_id,
                        "http_status": status,
                        "outcome": payload.get("outcome"),
                        "attempt_uuid": payload.get("attempt_uuid"),
                        "confirm_result": payload.get("confirm_result"),
                        "user": payload.get("user"),
                    },
                    indent=2,
                )
            )

        committed = [
            writer_id
            for writer_id, _, payload in results
            if payload.get("user") is not None
        ]
        request("POST", "/repair/run-once")
        _, final_state = request("GET", "/debug/users/42")
        final_name = final_state["postgres"]["name"]
        expected_name = profiles[completion_order[0]][0]
        print(
            json.dumps(
                {
                    "committed_writers": committed,
                    "expected_winner": completion_order[0],
                    "final_postgres_name": final_name,
                    "redis_trusted": final_state["redis"].get("trusted", False),
                    "redis_name": (final_state["redis"].get("value") or {}).get("name"),
                },
                indent=2,
            )
        )

        if committed != [completion_order[0]]:
            raise AssertionError(f"expected only {completion_order[0]} to commit, got {committed}")
        if final_name != expected_name:
            raise AssertionError(f"expected final DB value {expected_name}, got {final_name}")
        if not final_state["redis"].get("trusted"):
            raise AssertionError(f"Redis remained untrusted for order {title}")
        if final_state["redis"]["value"]["name"] != expected_name:
            raise AssertionError(f"Redis disagreed with DB for order {title}")


def main() -> None:
    run_sequential_handoff_case()
    run_case(
        "W1 pauses before DB; W2 reaches DB first",
        [
            ("w1", "before_delay_ms=2000", {"name": "Grace", "phone_number": "+1-555-0101"}),
            ("w2", "", {"name": "Luna", "phone_number": "+1-555-0102"}),
        ],
    )
    run_case(
        "W1 holds the DB transaction; W2 waits and loses the stale expectation",
        [
            ("w1", "transaction_delay_ms=1500", {"name": "Grace", "phone_number": "+1-555-0111"}),
            ("w2", "", {"name": "Luna", "phone_number": "+1-555-0112"}),
        ],
    )
    run_case(
        "W1 commits but skips AFTER while W2 races",
        [
            ("w1", "skip_after=true", {"name": "Grace", "phone_number": "+1-555-0121"}),
            ("w2", "before_delay_ms=300", {"name": "Luna", "phone_number": "+1-555-0122"}),
        ],
    )
    run_reader_case()
    run_repair_writer_overlap_case()
    run_concurrent_singleflight_readers_case()
    run_three_writer_permutations()


if __name__ == "__main__":
    main()
