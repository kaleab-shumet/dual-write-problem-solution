from __future__ import annotations

import asyncio
import logging
import signal
from typing import Any

from bullmq import Worker

from app import cache, db
from app.cache_coordinator import CacheCoordinator
from app.settings import DATABASE_URL, REDIS_URL, REPAIR_QUEUE_NAME


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("repair-worker")


async def repair_user(
    coordinator: CacheCoordinator,
    redis,
    cache_key: str,
    before_uuid: str,
) -> dict[str, Any]:
    user_id = cache.user_id_from_cache_key(cache_key)

    cached = await cache.get_cache(redis, cache_key)
    if cached.get("trusted") or cached.get("before_uuid") != before_uuid:
        return {
            "action": "skipped_cache_already_moved",
            "trusted": cached.get("trusted", False),
            "current_before_uuid": cached.get("before_uuid"),
        }

    repaired = await coordinator.repair_attempt(
        cache_key,
        before_uuid,
        lambda: db.get_user(coordinator.db_pool, user_id),
    )
    if repaired.get("value") is None:
        return {"action": "missing_postgres_row", "db_action": repaired.get("db_action")}

    return {
        "action": "repaired_after_from_db",
        "db_action": repaired.get("db_action"),
        "confirm_result": repaired.get("confirm_result"),
        "version": repaired["value"]["version"],
    }


async def main() -> None:
    db_pool = await db.create_pool(DATABASE_URL)
    redis = await cache.create_redis(REDIS_URL)
    coordinator = CacheCoordinator(db_pool, redis)
    await db.init_db(db_pool)

    async def process(job, job_token):
        data = job.data
        result = await repair_user(
            coordinator,
            redis,
            data["cache_key"],
            data["before_uuid"],
        )
        logger.info("processed repair job %s: %s", job.id, result)
        return result

    shutdown_event = asyncio.Event()

    def handle_shutdown(signum, frame):
        logger.info("signal %s received, shutting down", signum)
        shutdown_event.set()

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    worker = Worker(REPAIR_QUEUE_NAME, process, {"connection": REDIS_URL})
    try:
        await shutdown_event.wait()
    finally:
        await worker.close()
        await redis.aclose()
        await db_pool.close()


if __name__ == "__main__":
    asyncio.run(main())
