from __future__ import annotations

import asyncio
import logging

from app import cache, db
from app.settings import DATABASE_URL, REDIS_URL, WORKER_INTERVAL_SECONDS


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("repair-worker")


async def repair_loop() -> None:
    db_pool = await db.create_pool(DATABASE_URL)
    redis = await cache.create_redis(REDIS_URL)
    await db.init_db(db_pool)

    try:
        while True:
            keys = await cache.dirty_keys(redis)
            for key in keys:
                try:
                    user_id = cache.user_id_from_cache_key(key)
                    cached = await cache.get_cache(redis, user_id)
                    if cached.get("trusted"):
                        await cache.remove_dirty(redis, key)
                        logger.info("removed already trusted key %s", key)
                        continue

                    row = await db.get_user(db_pool, user_id)
                    if row is None:
                        logger.warning("dirty key %s has no Postgres row", key)
                        continue

                    repaired = await cache.repair_from_db(
                        redis,
                        user_id,
                        dict(row),
                        row["version"],
                    )
                    if repaired:
                        logger.info("repaired %s from Postgres version %s", key, row["version"])
                    else:
                        logger.info("skipped %s because cache has newer confirmed state", key)
                except Exception:
                    logger.exception("failed to repair %s", key)

            await asyncio.sleep(WORKER_INTERVAL_SECONDS)
    finally:
        await redis.aclose()
        await db_pool.close()


if __name__ == "__main__":
    asyncio.run(repair_loop())
