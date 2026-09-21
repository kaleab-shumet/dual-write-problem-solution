from __future__ import annotations

import json
import time
import uuid
from typing import Any

from redis.asyncio import Redis

from app.settings import DIRTY_KEYS_SET
from app.settings import SINGLEFLIGHT_TTL_MS


LOCK_RELEASE_SCRIPT = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""


WRITE_BEFORE_SCRIPT = """
redis.call(
  'HSET', KEYS[1],
  'before_uuid', ARGV[1],
  'before_written_at_ms', ARGV[2]
)
redis.call('SADD', KEYS[2], KEYS[1])
return 1
"""


BOOTSTRAP_CACHE_SCRIPT = """
if redis.call('HEXISTS', KEYS[1], 'before_uuid') == 1 then
  return 0
end

redis.call(
  'HSET', KEYS[1],
  'before_uuid', ARGV[1],
  'after_uuid', ARGV[1],
  'value', ARGV[2],
  'before_written_at_ms', ARGV[3]
)
redis.call('SREM', KEYS[2], KEYS[1])
return 1
"""


CONFIRM_AFTER_SCRIPT = """
if redis.call('HGET', KEYS[1], 'before_uuid') ~= ARGV[1] then
  return 0
end

redis.call(
  'HSET', KEYS[1],
  'after_uuid', ARGV[1],
  'value', ARGV[2]
)
redis.call('SREM', KEYS[2], KEYS[1])
return 1
"""


REPAIR_CACHE_SCRIPT = """
if redis.call('HGET', KEYS[1], 'before_uuid') ~= ARGV[1] then
  return 0
end

redis.call(
  'HSET', KEYS[1],
  'before_uuid', ARGV[2],
  'after_uuid', ARGV[2],
  'value', ARGV[3]
)
redis.call('SREM', KEYS[2], KEYS[1])
return 1
"""


def user_cache_key(user_id: str) -> str:
    return f"user:{user_id}"


def singleflight_key(cache_key_value: str, attempt_uuid: str) -> str:
    return f"repair:singleflight:{cache_key_value}:{attempt_uuid}"


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


async def acquire_singleflight(
    redis: Redis,
    cache_key_value: str,
    attempt_uuid: str,
    owner_token: str,
    ttl_ms: int = SINGLEFLIGHT_TTL_MS,
) -> bool:
    return bool(
        await redis.set(
            singleflight_key(cache_key_value, attempt_uuid),
            owner_token,
            nx=True,
            px=ttl_ms,
        )
    )


async def release_singleflight(
    redis: Redis,
    cache_key_value: str,
    attempt_uuid: str,
    owner_token: str,
) -> bool:
    released = await redis.eval(
        LOCK_RELEASE_SCRIPT,
        1,
        singleflight_key(cache_key_value, attempt_uuid),
        owner_token,
    )
    return bool(released)


async def write_before(
    redis: Redis,
    cache_key_value: str,
    attempt_uuid: str,
) -> int:
    return int(
        await redis.eval(
            WRITE_BEFORE_SCRIPT,
            2,
            cache_key_value,
            DIRTY_KEYS_SET,
            attempt_uuid,
            str(now_ms()),
        )
    )


async def confirm_after(
    redis: Redis,
    cache_key_value: str,
    attempt_uuid: str,
    value: dict[str, Any],
) -> int:
    return int(
        await redis.eval(
            CONFIRM_AFTER_SCRIPT,
            2,
            cache_key_value,
            DIRTY_KEYS_SET,
            attempt_uuid,
            encode_value(value),
        )
    )


async def repair_from_db(
    redis: Redis,
    cache_key_value: str,
    observed_before_uuid: str,
    confirmed_attempt_uuid: str,
    value: dict[str, Any],
) -> int:
    return int(
        await redis.eval(
            REPAIR_CACHE_SCRIPT,
            2,
            cache_key_value,
            DIRTY_KEYS_SET,
            observed_before_uuid,
            confirmed_attempt_uuid,
            encode_value(value),
        )
    )


async def set_trusted(
    redis: Redis,
    cache_key_value: str,
    value: dict[str, Any],
    attempt_uuid: str,
) -> bool:
    return bool(
        await redis.eval(
            BOOTSTRAP_CACHE_SCRIPT,
            2,
            cache_key_value,
            DIRTY_KEYS_SET,
            attempt_uuid,
            encode_value(value),
            str(now_ms()),
        )
    )
    await redis.srem(DIRTY_KEYS_SET, cache_key_value)


async def get_cache(redis: Redis, cache_key_value: str) -> dict[str, Any]:
    data = await redis.hgetall(cache_key_value)
    return normalize_cache(data)


def encode_value(value: dict[str, Any]) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def decode_value(value: str) -> dict[str, Any]:
    return json.loads(value)


def normalize_cache(data: dict[str, str]) -> dict[str, Any]:
    normalized: dict[str, Any] = dict(data)
    if "value" in normalized:
        normalized["value"] = decode_value(normalized["value"])
    for field in ("before_written_at_ms",):
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
