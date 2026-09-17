from __future__ import annotations

import asyncio
import logging
import signal
from typing import Any

from bullmq import Worker

from app import cache, db
from app.settings import DATABASE_URL, REDIS_URL, REPAIR_QUEUE_NAME


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("repair-worker")


async def repair_user(
    db_pool,
    redis,
    entity_type: str,
    entity_id: str,
    before_uuid: str,
) -> dict[str, Any]:
    if entity_type != "user":
        raise ValueError(f"Unsupported entity_type: {entity_type}")

    cached = await cache.get_cache(redis, entity_id)
    if cached.get("trusted") or cached.get("before_uuid") != before_uuid:
        return {
            "action": "skipped_cache_already_moved",
            "trusted": cached.get("trusted", False),
            "current_before_uuid": cached.get("before_uuid"),
        }

    row, db_action = await db.repair_user_for_attempt(db_pool, entity_id, before_uuid)
    if row is None:
        return {"action": "missing_postgres_row", "db_action": db_action}

    confirm_result = await cache.repair_from_db(
        redis,
        entity_id,
        before_uuid,
        dict(row),
        row["version"],
    )
    return {
        "action": "repaired_after_from_db",
        "db_action": db_action,
        "confirm_result": confirm_result,
        "version": row["version"],
    }


async def main() -> None:
    db_pool = await db.create_pool(DATABASE_URL)
    redis = await cache.create_redis(REDIS_URL)
    await db.init_db(db_pool)

    async def process(job, job_token):
        data = job.data
        result = await repair_user(
            db_pool,
            redis,
            data["entity_type"],
            data["entity_id"],
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
