#!/usr/bin/env python3
"""Run the slow-first-writer/fast-second-writer race deterministically."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import cache, db
from app.settings import DATABASE_URL, REDIS_URL


async def main() -> None:
    pool = await db.create_pool(DATABASE_URL)
    redis = await cache.create_redis(REDIS_URL)
    try:
        await db.init_db(pool)
        await redis.flushdb()
        await db.reset_user(pool, "42")

        key = cache.user_cache_key("42")
        initial = cache.new_uuid()
        await db.bootstrap_cache_attempt(pool, key, initial)
        await cache.set_trusted(
            redis,
            key,
            {
                "id": "42",
                "name": "Ada Lovelace",
                "phone_number": "+44-20-7946-0100",
                "email": "ada.lovelace@example.com",
                "version": 1,
            },
            initial,
        )

        print("Initial: BEFORE=AFTER=b0, database name=Ada")

        w1_attempt = cache.new_uuid()
        await cache.write_before(redis, key, w1_attempt)
        w1_expected = (await cache.get_cache(redis, key))["after_uuid"]
        print(f"W1: BEFORE=b1, expected AFTER={w1_expected}; pause before DB")

        w2_attempt = cache.new_uuid()
        await cache.write_before(redis, key, w2_attempt)
        w2_expected = (await cache.get_cache(redis, key))["after_uuid"]
        print(f"W2: BEFORE=b2, expected AFTER={w2_expected}; reaches DB first")

        w2_row = await db.run_with_cache_attempt(
            pool,
            key,
            w2_expected,
            w2_attempt,
            lambda conn: db.update_user_in_tx(conn, "42", "Luna", None),
        )
        print(f"W2: database claim and business update committed: {w2_row['name']}")
        await cache.confirm_after(redis, key, w2_attempt, dict(w2_row))
        print("W2: AFTER=b2, Redis is trusted with Luna")

        w1_row = await db.run_with_cache_attempt(
            pool,
            key,
            w1_expected,
            w1_attempt,
            lambda conn: db.update_user_in_tx(conn, "42", "Grace", None),
        )
        print(f"W1: late database result = {w1_row}; its stale attempt was rejected")

        print(f"Final database row: {dict(await db.get_user(pool, '42'))}")
        print(f"Final Redis state: {await cache.get_cache(redis, key)}")
    finally:
        await redis.aclose()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
