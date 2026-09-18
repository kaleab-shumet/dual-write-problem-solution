from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import TypeVar

import asyncpg


T = TypeVar("T")


CREATE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL DEFAULT 'Ada Lovelace',
  phone_number TEXT NOT NULL,
  email TEXT NOT NULL DEFAULT 'ada.lovelace@example.com',
  version BIGINT NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS cache_attempts (
  cache_key TEXT NOT NULL,
  attempt_uuid UUID NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (cache_key, attempt_uuid)
);

ALTER TABLE users
  ADD COLUMN IF NOT EXISTS name TEXT NOT NULL DEFAULT 'Ada Lovelace';

ALTER TABLE users
  ADD COLUMN IF NOT EXISTS email TEXT NOT NULL DEFAULT 'ada.lovelace@example.com';
"""


async def create_pool(database_url: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(database_url, min_size=1, max_size=10)


async def init_db(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(CREATE_SCHEMA_SQL)


async def reset_user(pool: asyncpg.Pool, user_id: str = "42") -> asyncpg.Record:
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM cache_attempts WHERE cache_key = $1",
            f"user:{user_id}",
        )
        return await conn.fetchrow(
            """
            INSERT INTO users (id, name, phone_number, email, version)
            VALUES ($1, $2, $3, $4, 1)
            ON CONFLICT (id) DO UPDATE
            SET name = EXCLUDED.name,
                phone_number = EXCLUDED.phone_number,
                email = EXCLUDED.email,
                version = 1
            RETURNING id, name, phone_number, email, version
            """,
            user_id,
            "Ada Lovelace",
            "+44-20-7946-0100",
            "ada.lovelace@example.com",
        )


async def get_user(pool: asyncpg.Pool, user_id: str) -> asyncpg.Record | None:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            "SELECT id, name, phone_number, email, version FROM users WHERE id = $1",
            user_id,
        )


async def update_user(
    pool: asyncpg.Pool,
    user_id: str,
    name: str | None,
    phone_number: str | None,
    expected_version: int,
) -> asyncpg.Record | None:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            UPDATE users
            SET name = COALESCE($1, name),
                phone_number = COALESCE($2, phone_number),
                version = version + 1
            WHERE id = $3
              AND version = $4
            RETURNING id, name, phone_number, email, version
            """,
            name,
            phone_number,
            user_id,
            expected_version,
        )


async def insert_cache_attempt(
    conn: asyncpg.Connection,
    cache_key: str,
    attempt_uuid: str,
) -> None:
    await conn.execute(
        """
        INSERT INTO cache_attempts (cache_key, attempt_uuid)
        VALUES ($1, $2::uuid)
        """,
        cache_key,
        attempt_uuid,
    )


async def run_with_cache_attempt(
    pool: asyncpg.Pool,
    cache_key: str,
    attempt_uuid: str,
    write_fn: Callable[[asyncpg.Connection], Awaitable[T | None]],
) -> T | None:
    async with pool.acquire() as conn:
        tx = conn.transaction()
        await tx.start()
        try:
            await insert_cache_attempt(conn, cache_key, attempt_uuid)
            result = await write_fn(conn)
            if result is None:
                await tx.rollback()
                return None
            await tx.commit()
            return result
        except asyncpg.UniqueViolationError:
            await tx.rollback()
            return None
        except Exception:
            await tx.rollback()
            raise


async def repair_entity_for_attempt(
    pool: asyncpg.Pool,
    cache_key: str,
    attempt_uuid: str,
    fetch_fn: Callable[[], Awaitable[T | None]],
) -> tuple[T | None, str]:
    async with pool.acquire() as conn:
        tx = conn.transaction()
        await tx.start()
        try:
            await insert_cache_attempt(conn, cache_key, attempt_uuid)
            await tx.commit()
            return await fetch_fn(), "repair_claimed_attempt"
        except asyncpg.UniqueViolationError:
            await tx.rollback()

    return await fetch_fn(), "attempt_already_committed"


async def update_user_in_tx(
    conn: asyncpg.Connection,
    user_id: str,
    name: str | None,
    phone_number: str | None,
    expected_version: int,
) -> asyncpg.Record | None:
    return await conn.fetchrow(
        """
        UPDATE users
        SET name = COALESCE($1, name),
            phone_number = COALESCE($2, phone_number),
            version = version + 1
        WHERE id = $3
          AND version = $4
        RETURNING id, name, phone_number, email, version
        """,
        name,
        phone_number,
        user_id,
        expected_version,
    )


async def update_user_with_attempt(
    pool: asyncpg.Pool,
    user_id: str,
    attempt_uuid: str,
    name: str | None,
    phone_number: str | None,
    expected_version: int,
) -> asyncpg.Record | None:
    return await run_with_cache_attempt(
        pool,
        f"user:{user_id}",
        attempt_uuid,
        lambda conn: update_user_in_tx(conn, user_id, name, phone_number, expected_version),
    )


async def repair_user_for_attempt(
    pool: asyncpg.Pool,
    user_id: str,
    attempt_uuid: str,
) -> tuple[asyncpg.Record | None, str]:
    return await repair_entity_for_attempt(
        pool,
        f"user:{user_id}",
        attempt_uuid,
        lambda: get_user(pool, user_id),
    )
