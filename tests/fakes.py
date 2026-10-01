import asyncio

from async_q.utils import (
    get_dead_letter_key, get_redis_q_backup_key, get_redis_q_key, get_retry_zset_key,
)


class FakeAsyncRedis:
    def __init__(self):
        self.lists = {}
        self.kv = {}
        self.zsets = {}
        self.sets = {}
        self.ttl = {}

    # list ops
    async def lpush(self, key, value):
        self.lists.setdefault(key, [])
        self.lists[key].insert(0, value)
        return len(self.lists[key])

    async def lrem(self, key, count, value):
        lst = self.lists.get(key, [])
        removed = 0
        if count == 0:
            # remove all occurrences
            new = [v for v in lst if v != value]
            removed = len(lst) - len(new)
            self.lists[key] = new
        else:
            new = []
            c = count
            for v in lst:
                if v == value and c > 0:
                    removed += 1
                    c -= 1
                else:
                    new.append(v)
            self.lists[key] = new
        return removed

    async def rpoplpush(self, src, dst):
        src_list = self.lists.get(src, [])
        if not src_list:
            return None
        val = src_list.pop()
        self.lists[src] = src_list
        await self.lpush(dst, val)
        return val

    # kv ops
    async def set(self, key, value, ex=None, px=None):
        self.kv[key] = value
        self.ttl[key] = ex if ex is not None else (px / 1000 if px is not None else None)
        return True

    async def exists(self, key):
        return int(key in self.kv or bool(self.lists.get(key)) or bool(self.sets.get(key)))

    async def delete(self, key):
        self.kv.pop(key, None)
        self.lists.pop(key, None)
        self.sets.pop(key, None)

    async def rpush(self, key, value):
        self.lists.setdefault(key, []).append(value)
        return len(self.lists[key])

    async def rpop(self, key):
        lst = self.lists.get(key, [])
        return lst.pop() if lst else None

    async def brpoplpush(self, src, dst, timeout=0):
        val = await self.rpoplpush(src, dst)
        if val is None:
            await asyncio.sleep(0.01)  # ponytail: don't really block; just yield
        return val

    async def sadd(self, key, member):
        self.sets.setdefault(key, set()).add(member)

    async def srem(self, key, member):
        self.sets.get(key, set()).discard(member)

    async def smembers(self, key):
        return {m.encode() if isinstance(m, str) else m for m in self.sets.get(key, set())}

    async def zrange(self, key, start, end, withscores=False):
        items = sorted(self.zsets.get(key, {}).items(), key=lambda x: x[1])
        items = items[start:] if end == -1 else items[start:end + 1]
        return items if withscores else [m for m, _ in items]

    # zset ops
    async def zadd(self, key, mapping):
        z = self.zsets.setdefault(key, {})
        for member, score in mapping.items():
            z[member] = score
        return len(mapping)

    async def zrangebyscore(self, key, min, max, start=0, num=None):
        z = self.zsets.get(key, {})
        items = [(m, s) for m, s in z.items() if s <= max]
        items.sort(key=lambda x: x[1])
        members = [m for m, _ in items]
        if num is None:
            return members[start:]
        return members[start:start + num]

    async def zrem(self, key, member):
        z = self.zsets.get(key, {})
        if member in z:
            del z[member]
            return 1
        return 0


class FakeTask:
    def __init__(self, exc=None, result=None):
        self._exc = exc
        self._result = result

    def result(self):
        if self._exc:
            raise self._exc
        return self._result


class NoopLogger:
    def debug(self, *a, **k):
        pass

    def info(self, *a, **k):
        pass

    def warning(self, *a, **k):
        pass

    def error(self, *a, **k):
        pass


# Lua stand-ins with the same LREM-guarded semantics (FakeAsyncRedis has no eval).
async def fake_requeue(redis, q, wid, value):
    n = await redis.lrem(get_redis_q_backup_key(q, wid), 1, value)
    if n:
        await redis.rpush(get_redis_q_key(q), value)
    return n


async def fake_fail_ack(redis, q, wid, original, new_payload, mode, score=0):
    n = await redis.lrem(get_redis_q_backup_key(q, wid), 1, original)
    if n:
        if mode == 'retry':
            await redis.zadd(get_retry_zset_key(q), {new_payload: score})
        else:
            await redis.lpush(get_dead_letter_key(q), new_payload)
    return n
