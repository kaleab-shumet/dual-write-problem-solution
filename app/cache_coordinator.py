from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

import asyncpg
from bullmq import Queue
from redis.asyncio import Redis

from app import cache, db
from app.settings import REPAIR_WAIT_MS


RowDict = dict[str, Any]
FetchFn = Callable[[], Awaitable[asyncpg.Record | None]]
FetchInTxFn = Callable[[asyncpg.Connection], Awaitable[asyncpg.Record | None]]
WriteFn = Callable[[asyncpg.Connection], Awaitable[asyncpg.Record | None]]


class CacheCoordinator:
    def __init__(
        self,
        db_pool: asyncpg.Pool,
        redis: Redis,
        repair_queue: Queue | None = None,
    ) -> None:
        self.db_pool = db_pool
        self.redis = redis
        self.repair_queue = repair_queue

    def repair_job_id(self, cache_key: str, before_uuid: str) -> str:
        return f"repair-{cache_key}-{before_uuid}"

    async def enqueue_repair(
        self,
        cache_key: str,
        before_uuid: str,
    ) -> str:
        if self.repair_queue is None:
            raise RuntimeError("repair queue is not configured")

        job_id = self.repair_job_id(cache_key, before_uuid)
        await self.repair_queue.add(
            "repair-cache",
            {
                "cache_key": cache_key,
                "before_uuid": before_uuid,
            },
            {
                "jobId": job_id,
                "deduplication": {"id": job_id},
                "attempts": 5,
                "backoff": {"type": "exponential", "delay": 100},
                "removeOnComplete": {"age": 60},
                "removeOnFail": {"age": 300},
            },
        )
        return job_id

    async def wait_for_trusted(self, cache_key: str) -> dict[str, Any] | None:
        attempts = max(1, REPAIR_WAIT_MS // 30)
        for _ in range(attempts):
            cached = await cache.get_cache(self.redis, cache_key)
            if cached.get("trusted"):
                return cached
            await asyncio.sleep(0.03)
        return None

    async def read_with_worker(
        self,
        cache_key: str,
        fetch_db: FetchFn,
    ) -> dict[str, Any]:
        cached = await cache.get_cache(self.redis, cache_key)
        if cached.get("trusted"):
            return {
                "served_from": "redis",
                "trusted_cache": True,
                "value": cached["value"],
            }

        before_uuid = cached.get("before_uuid")
        if before_uuid:
            job_id = await self.enqueue_repair(cache_key, before_uuid)
            repaired_cache = await self.wait_for_trusted(cache_key)
            if repaired_cache:
                return {
                    "served_from": "redis_after_worker_repair",
                    "trusted_cache": True,
                    "repair_job_id": job_id,
                    "value": repaired_cache["value"],
                }
            return {
                "served_from": "repair_pending",
                "trusted_cache": False,
                "repair_job_id": job_id,
                "value": None,
                "status_code": 202,
            }

        row = await fetch_db()
        if row is None:
            return {"served_from": "missing", "trusted_cache": False, "value": None}

        attempt_uuid = await db.bootstrap_cache_attempt(
            self.db_pool,
            cache_key,
            cache.new_uuid(),
        )
        seeded = await cache.set_trusted(self.redis, cache_key, dict(row), attempt_uuid)
        if not seeded:
            return await self.read_with_worker(cache_key, fetch_db)
        return {
            "served_from": "postgres",
            "trusted_cache": True,
            "value": dict(row),
        }

    async def read_with_singleflight(
        self,
        cache_key: str,
        fetch_db: FetchFn,
        fetch_db_in_tx: FetchInTxFn | None = None,
    ) -> dict[str, Any]:
        cached = await cache.get_cache(self.redis, cache_key)
        if cached.get("trusted"):
            return {
                "served_from": "redis",
                "trusted_cache": True,
                "singleflight_role": "not_needed",
                "value": cached["value"],
            }

        before_uuid = cached.get("before_uuid")
        if not before_uuid:
            row = await fetch_db()
            if row is None:
                return {"served_from": "missing", "trusted_cache": False, "value": None}
            attempt_uuid = await db.bootstrap_cache_attempt(
                self.db_pool,
                cache_key,
                cache.new_uuid(),
            )
            seeded = await cache.set_trusted(self.redis, cache_key, dict(row), attempt_uuid)
            if not seeded:
                return await self.read_with_singleflight(cache_key, fetch_db)
            return {
                "served_from": "postgres_bootstrap",
                "trusted_cache": True,
                "singleflight_role": "not_needed_empty_cache",
                "value": dict(row),
            }

        owner_token = cache.new_uuid()
        if await cache.acquire_singleflight(self.redis, cache_key, before_uuid, owner_token):
            try:
                latest = await cache.get_cache(self.redis, cache_key)
                if latest.get("trusted"):
                    return {
                        "served_from": "redis",
                        "trusted_cache": True,
                        "singleflight_role": "winner_found_already_repaired",
                        "value": latest["value"],
                    }
                if latest.get("before_uuid") != before_uuid:
                    return await self._wait_for_singleflight_follower(
                        "winner_saw_newer_attempt",
                        cache_key,
                    )

                repair_fetch = fetch_db_in_tx or (lambda conn: fetch_db())
                result = await self.repair_attempt(cache_key, before_uuid, repair_fetch)
                result["served_from"] = "postgres_singleflight_repair"
                result["singleflight_role"] = "winner"
                return result
            finally:
                await cache.release_singleflight(self.redis, cache_key, before_uuid, owner_token)

        return await self._wait_for_singleflight_follower("follower", cache_key)

    async def _wait_for_singleflight_follower(
        self,
        role: str,
        cache_key: str,
    ) -> dict[str, Any]:
        waited = await self.wait_for_trusted(cache_key)
        if waited:
            return {
                "served_from": "redis_after_singleflight_wait",
                "trusted_cache": True,
                "singleflight_role": role,
                "value": waited["value"],
            }
        return {
            "served_from": "repair_pending",
            "trusted_cache": False,
            "singleflight_role": f"{role}_timeout",
            "value": None,
            "status_code": 202,
        }

    async def cached_write(
        self,
        cache_key: str,
        write_db: WriteFn,
        *,
        confirm_after: bool = True,
        enqueue_repair_after_commit: bool = False,
        before_delay_ms: int = 0,
        transaction_delay_ms: int = 0,
        after_delay_ms: int = 0,
        reject_business: bool = False,
    ) -> dict[str, Any]:
        attempt_uuid = cache.new_uuid()
        await cache.write_before(self.redis, cache_key, attempt_uuid)
        current_cache = await cache.get_cache(self.redis, cache_key)
        expected_attempt_uuid = current_cache.get("after_uuid")

        # Capture the expected AFTER before any demo delay. A writer must keep
        # the expectation it observed when it raised BEFORE; adopting a newer
        # AFTER after sleeping would let a stale operation commit.
        if before_delay_ms:
            await asyncio.sleep(before_delay_ms / 1000)

        async def write_with_demo_controls(conn: asyncpg.Connection) -> asyncpg.Record | None:
            if transaction_delay_ms:
                await asyncio.sleep(transaction_delay_ms / 1000)
            if reject_business:
                return None
            return await write_db(conn)

        row = await db.run_with_cache_attempt(
            self.db_pool,
            cache_key,
            expected_attempt_uuid,
            attempt_uuid,
            write_with_demo_controls,
        )
        if row is None:
            await self.enqueue_repair(cache_key, attempt_uuid)
            return {
                "outcome": "postgres_rejected_write_or_attempt_lost",
                "attempt_uuid": attempt_uuid,
                "value": None,
            }

        if enqueue_repair_after_commit:
            await self.enqueue_repair(cache_key, attempt_uuid)

        if not confirm_after:
            return {
                "outcome": "simulated_crash_after_postgres_commit",
                "attempt_uuid": attempt_uuid,
                "value": dict(row),
            }

        if after_delay_ms:
            await asyncio.sleep(after_delay_ms / 1000)

        confirm_result = await cache.confirm_after(
            self.redis,
            cache_key,
            attempt_uuid,
            dict(row),
        )
        return {
            "outcome": "committed_and_confirmed" if confirm_result else "committed_confirmation_rejected",
            "attempt_uuid": attempt_uuid,
            "confirm_result": confirm_result,
            "value": dict(row),
        }

    async def repair_attempt(
        self,
        cache_key: str,
        before_uuid: str,
        fetch_db: FetchInTxFn,
    ) -> dict[str, Any]:
        cached = await cache.get_cache(self.redis, cache_key)
        expected_attempt_uuid = cached.get("after_uuid")
        row, current_attempt, db_action = await db.repair_entity_for_attempt(
            self.db_pool,
            cache_key,
            expected_attempt_uuid,
            before_uuid,
            fetch_db,
        )
        if row is None:
            return {
                "served_from": "missing",
                "trusted_cache": False,
                "value": None,
                "db_action": db_action,
            }

        confirm_result = await cache.repair_from_db(
            self.redis,
            cache_key,
            before_uuid,
            current_attempt or before_uuid,
            dict(row),
        )
        repaired = await cache.get_cache(self.redis, cache_key)
        return {
            "served_from": "postgres_repair",
            "trusted_cache": bool(repaired.get("trusted")),
            "db_action": db_action,
            "confirm_result": confirm_result,
            "value": dict(row),
        }
