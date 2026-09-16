from __future__ import annotations

import json
import time
import uuid
from typing import Any

from redis.asyncio import Redis

from app.settings import DIRTY_KEYS_SET


WRITE_BEFORE_SCRIPT = """
redis.call(
  'HSET', KEYS[1],
  'before_uuid', ARGV[1],
  'before_written_at_ms', ARGV[2]
)
redis.call('SADD', KEYS[2], KEYS[1])
return 1
"""


CONFIRM_AFTER_SCRIPT = """
local current = tonumber(redis.call('HGET', KEYS[1], 'confirmed_version') or '0')
local incoming = tonumber(ARGV[3])

if incoming > current then
  redis.call(
    'HSET', KEYS[1],
    'after_uuid', ARGV[1],
    'value', ARGV[2],
    'confirmed_version', ARGV[3]
  )

  if redis.call('HGET', KEYS[1], 'before_uuid') == ARGV[1] then
    redis.call('SREM', KEYS[2], KEYS[1])
    return 1
  end

  return 2
end

return 0
"""


REPAIR_FROM_DB_SCRIPT = """
local current_confirmed = tonumber(redis.call('HGET', KEYS[1], 'confirmed_version') or '0')
local db_version = tonumber(ARGV[3])
local before_uuid = redis.call('HGET', KEYS[1], 'before_uuid')
local after_uuid = redis.call('HGET', KEYS[1], 'after_uuid')
local repair_uuid = before_uuid or ARGV[2]

if db_version > current_confirmed then
  redis.call(
    'HSET', KEYS[1],
    'value', ARGV[1],
    'after_uuid', repair_uuid,
    'confirmed_version', ARGV[3],
    'before_written_at_ms', ARGV[4]
  )

  if before_uuid == repair_uuid then
    redis.call('SREM', KEYS[2], KEYS[1])
    return 1
  end

  return 2
end

if db_version == current_confirmed and after_uuid then
  redis.call('HSET', KEYS[1], 'before_uuid', after_uuid, 'before_written_at_ms', ARGV[4])
  redis.call('SREM', KEYS[2], KEYS[1])
  return 3
end

return 0
"""


def cache_key(user_id: str) -> str:
    return f"user:{user_id}"


def user_id_from_cache_key(key: str) -> str:
    if not key.startswith("user:"):
        raise ValueError(f"Unknown cache key format: {key}")
    return key.removeprefix("user:")


def new_uuid() -> str:
    return str(uuid.uuid4())


def now_ms() -> int:
    return int(time.time() * 1000)


async def create_redis(redis_url: str) -> Redis:
    return Redis.from_url(redis_url, decode_responses=True)


async def write_before(
    redis: Redis,
    user_id: str,
    attempt_uuid: str,
) -> int:
    return int(
        await redis.eval(
            WRITE_BEFORE_SCRIPT,
            2,
            cache_key(user_id),
            DIRTY_KEYS_SET,
            attempt_uuid,
            str(now_ms()),
        )
    )


async def confirm_after(
    redis: Redis,
    user_id: str,
    attempt_uuid: str,
    user: dict[str, Any],
    postgres_version: int,
) -> int:
    return int(
        await redis.eval(
            CONFIRM_AFTER_SCRIPT,
            2,
            cache_key(user_id),
            DIRTY_KEYS_SET,
            attempt_uuid,
            encode_user(user),
            str(postgres_version),
        )
    )


async def repair_from_db(
    redis: Redis,
    user_id: str,
    user: dict[str, Any],
    postgres_version: int,
) -> int:
    return int(
        await redis.eval(
            REPAIR_FROM_DB_SCRIPT,
            2,
            cache_key(user_id),
            DIRTY_KEYS_SET,
            encode_user(user),
            new_uuid(),
            str(postgres_version),
            str(now_ms()),
        )
    )


async def set_trusted(
    redis: Redis,
    user_id: str,
    user: dict[str, Any],
    postgres_version: int,
) -> None:
    attempt_uuid = new_uuid()
    await redis.hset(
        cache_key(user_id),
        mapping={
            "value": encode_user(user),
            "before_uuid": attempt_uuid,
            "after_uuid": attempt_uuid,
            "confirmed_version": postgres_version,
            "before_written_at_ms": now_ms(),
        },
    )
    await redis.srem(DIRTY_KEYS_SET, cache_key(user_id))


async def get_cache(redis: Redis, user_id: str) -> dict[str, Any]:
    data = await redis.hgetall(cache_key(user_id))
    return normalize_cache(data)


def encode_user(user: dict[str, Any]) -> str:
    return json.dumps(user, separators=(",", ":"), sort_keys=True)


def decode_user(value: str) -> dict[str, Any]:
    return json.loads(value)


def normalize_cache(data: dict[str, str]) -> dict[str, Any]:
    normalized: dict[str, Any] = dict(data)
    if "value" in normalized:
        normalized["value"] = decode_user(normalized["value"])
    for field in ("confirmed_version", "before_written_at_ms"):
        if field in normalized:
            normalized[field] = int(normalized[field])
    normalized["trusted"] = bool(
        normalized.get("before_uuid")
        and normalized.get("before_uuid") == normalized.get("after_uuid")
    )
    return normalized


async def dirty_keys(redis: Redis) -> list[str]:
    return sorted(await redis.smembers(DIRTY_KEYS_SET))


async def remove_dirty(redis: Redis, key: str) -> None:
    await redis.srem(DIRTY_KEYS_SET, key)
