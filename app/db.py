from __future__ import annotations

import asyncpg


CREATE_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL DEFAULT 'Ada Lovelace',
  phone_number TEXT NOT NULL,
  email TEXT NOT NULL DEFAULT 'ada.lovelace@example.com',
  version BIGINT NOT NULL DEFAULT 1
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
