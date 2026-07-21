# Redis — The Complete Guide (Basics to Internals)

Redis (**RE**mote **DI**ctionary **S**erver) is an open-source, in-memory data structure store used as a database, cache, message broker, and streaming engine. It's one of the most widely deployed pieces of infrastructure on the internet, powering everything from session stores to real-time leaderboards to job queues.

This document walks through Redis from first principles to the internal engineering that makes it fast, covering data types, persistence, replication, clustering, and real production use cases.

---

## Table of Contents

1. [What Redis Actually Is](#what-redis-actually-is)
2. [Why Redis Is Fast — The Architecture](#why-redis-is-fast--the-architecture)
3. [Installing & Running Redis](#installing--running-redis)
4. [Core Data Types](#core-data-types)
5. [Keyspace & Expiration](#keyspace--expiration)
6. [Persistence: RDB vs AOF](#persistence-rdb-vs-aof)
7. [Replication](#replication)
8. [High Availability with Sentinel](#high-availability-with-sentinel)
9. [Scaling with Redis Cluster](#scaling-with-redis-cluster)
10. [Transactions](#transactions)
11. [Pub/Sub Messaging](#pubsub-messaging)
12. [Redis Streams](#redis-streams)
13. [Lua Scripting](#lua-scripting)
14. [Memory Management & Eviction](#memory-management--eviction)
15. [Security](#security)
16. [Common Real-World Use Cases](#common-real-world-use-cases)
17. [Redis Modules & Extensions](#redis-modules--extensions)
18. [Monitoring & Observability](#monitoring--observability)
19. [Redis vs Other Databases](#redis-vs-other-databases)
20. [Best Practices](#best-practices)
21. [Common Pitfalls](#common-pitfalls)

---

## What Redis Actually Is

Redis stores data as key-value pairs, but unlike a simple cache, the *values* can be rich data structures — strings, hashes, lists, sets, sorted sets, bitmaps, hyperloglogs, geospatial indexes, and streams. Everything lives primarily in RAM, which is why reads and writes complete in sub-millisecond time.

Redis was created by **Salvatore Sanfilippo (antirez)** in 2009 while trying to scale a real-time analytics product. It's written in C, released under a mix of open-source licenses depending on version (core Redis under BSD historically; some newer editions use the RSALv2/SSPLv1 dual license, with **Valkey** — a Linux Foundation fork — continuing under BSD for those who need it).

At its heart, Redis answers one question extremely well: *"How do I get and set data as fast as physically possible?"*

---

## Why Redis Is Fast — The Architecture

This is the part most tutorials skip. Understanding *why* Redis is fast matters as much as knowing the commands.

### 1. Everything lives in memory
No disk seek, no page cache misses for reads — data structures sit directly in RAM as C structs, and Redis's own encodings (like `ziplist`/`listpack` for small collections) are memory-compact.

### 2. Single-threaded execution model (historically)
For most of its life, Redis processed all client commands on a **single thread**. This sounds like a bottleneck, but it's actually the opposite:

- No locks, no mutexes, no context switching between threads for command execution.
- Every command runs atomically to completion — no race conditions between two clients modifying the same key.
- CPU cache stays hot because one core does the actual data manipulation.

Redis multiplexes thousands of client connections using an **event loop** built on `epoll` (Linux), `kqueue` (BSD/macOS), or `select` as fallback — this is largely powered by Redis's own event library, `ae` (originally inspired by libevent/libev design patterns).

The loop looks conceptually like this:

```
while (true) {
    events = epoll_wait(socket_fds);
    for (event in events) {
        read_command(event.socket);
        execute_command();       // atomic, single-threaded
        write_response(event.socket);
    }
    process_timers();            // expired keys, cron jobs
}
```

### 3. Since Redis 6: I/O threading
Redis 6+ introduced **optional multi-threaded I/O** — reading raw bytes off the socket and parsing the protocol can happen on multiple threads, but **command execution itself is still single-threaded**. This gives you better throughput on multi-core machines without sacrificing the atomicity guarantees that make Redis predictable.

### 4. Efficient internal encodings
Redis doesn't always store a data type the "obvious" way. Small lists, hashes, and sets are stored as compact `listpack` encodings (flat, contiguous memory blocks) instead of full-blown hash tables or linked lists — and Redis only "upgrades" to the heavier encoding once a size threshold is crossed. This is a huge, underappreciated reason Redis is memory-efficient for small objects.

### 5. Lazy freeing (UNLINK)
Deleting a huge key can block the main thread while memory is freed. Redis solves this with lazy freeing (`UNLINK` command, or `lazyfree-lazy-*` config options) — the actual memory deallocation happens on a background thread so the main event loop isn't stalled.

---

## Installing & Running Redis

```bash
# Debian/Ubuntu
sudo apt-get install redis-server

# macOS
brew install redis

# Docker (most common in production-like setups)
docker run -d --name redis -p 6379:6379 redis:latest

# Start the CLI
redis-cli
127.0.0.1:6379> PING
PONG
```

Config lives in `redis.conf`. Key settings you'll touch early: `bind`, `port`, `requirepass`, `maxmemory`, `maxmemory-policy`, `appendonly`.

---

## Core Data Types

### String
The simplest type — binary-safe, up to 512MB.
```
SET user:1:name "Alice"
GET user:1:name
INCR page:views          # atomic counter
EXPIRE user:1:name 3600  # TTL in seconds
```
Used for: caching values, counters, feature flags, distributed locks.

### Hash
A field-value map inside a single key — like a mini object/record.
```
HSET user:1 name "Alice" age 30 city "NYC"
HGET user:1 name
HGETALL user:1
```
Used for: storing structured objects (a user profile, a product) without needing to serialize/deserialize JSON on every access.

### List
An ordered, doubly-linked list of strings — push/pop from either end in O(1).
```
LPUSH queue:jobs "job1"
RPUSH queue:jobs "job2"
LPOP queue:jobs
LRANGE queue:jobs 0 -1
```
Used for: queues, activity feeds, recent-items lists.

### Set
An unordered collection of unique strings.
```
SADD tags:post1 "redis" "database" "cache"
SISMEMBER tags:post1 "redis"
SINTER tags:post1 tags:post2   # set intersection
```
Used for: tagging, unique visitor tracking, relationship graphs (followers/following).

### Sorted Set (ZSet)
Like a set, but every member has a **score**, kept in sorted order — backed internally by a **skip list** plus a hash table for O(1) lookups.
```
ZADD leaderboard 100 "alice" 95 "bob"
ZRANGE leaderboard 0 -1 WITHSCORES
ZRANK leaderboard "alice"
ZINCRBY leaderboard 10 "bob"
```
Used for: leaderboards, priority queues, rate limiting windows, time-ordered feeds.

### Bitmap
Not a distinct type, just strings treated as bit arrays.
```
SETBIT online:2026-07-21 123 1
BITCOUNT online:2026-07-21
```
Used for: extremely memory-efficient boolean flags at scale (e.g., "did user X log in today" across millions of users).

### HyperLogLog
A probabilistic structure estimating the cardinality (unique count) of a set using **only ~12KB regardless of how many elements you add**, with ~0.81% standard error.
```
PFADD visitors "user1" "user2"
PFCOUNT visitors
```
Used for: approximate unique visitor counts, unique search-query counts — anywhere exact counting is too memory-expensive.

### Geospatial
Built on sorted sets, using **geohash encoding** to store lat/long.
```
GEOADD locations -122.4194 37.7749 "San Francisco"
GEODIST locations "San Francisco" "Oakland" km
GEOSEARCH locations FROMMEMBER "San Francisco" BYRADIUS 50 km
```
Used for: "find nearby drivers/stores" type features.

### Streams
An append-only log data type (added in Redis 5), similar in spirit to Kafka topics but lightweight.
```
XADD mystream '*' sensor "temp1" value "22.5"
XRANGE mystream - +
XREAD COUNT 10 STREAMS mystream 0
```
Covered in more depth below.

---

## Keyspace & Expiration

Keys can carry a TTL (`EXPIRE`, `PEXPIRE`, `EXPIREAT`). Internally, Redis handles expiration two ways:

1. **Passive expiration**: when a client tries to access a key, Redis checks its TTL and deletes it on the spot if expired.
2. **Active expiration**: a background cron job runs several times per second, sampling a random set of keys with TTLs and removing the expired ones — this keeps expired keys from silently piling up in memory even if nobody ever reads them again.

Redis also emits **keyspace notifications** (pub/sub events) when keys expire or are modified, if enabled via `notify-keyspace-events`.

---

## Persistence: RDB vs AOF

Redis is in-memory, but it's not purely ephemeral — it offers two persistence mechanisms, often used together.

### RDB (Redis Database file)
Point-in-time binary snapshots of the whole dataset, taken at configured intervals (`save 900 1` means "snapshot if at least 1 key changed in 900 seconds").

- Uses `fork()` to create a child process which writes the snapshot while the parent keeps serving traffic (copy-on-write memory pages keep this cheap).
- Compact, fast to load on restart.
- Risk: you can lose whatever changed since the last snapshot.

### AOF (Append Only File)
Every write command is logged to a file, replayed on startup to rebuild state.

- `appendfsync everysec` (default, good balance), `always` (safest, slowest), or `no` (fastest, riskiest).
- AOF files are periodically **rewritten/compacted** in the background so they don't grow forever — Redis replays the current dataset into a fresh, minimal command log rather than keeping every historical write.
- Since Redis 7, AOF uses a multi-part format (base file + incremental files + manifest) making rewrites safer and less disruptive.

### Which to use?
Most production setups run **both**: RDB for fast restarts and portable backups, AOF for durability. Redis will prefer AOF on restart if both are enabled, since it's more complete.

---

## Replication

Redis supports **asynchronous master-replica replication**. A replica connects to the master, receives an initial RDB snapshot (full sync), then streams subsequent write commands as they happen (partial sync via a replication backlog buffer).

```
# On the replica
REPLICAOF <master-ip> <master-port>
```

Key characteristics:
- Replication is asynchronous by default — a write can succeed on the master before all replicas confirm it (there's a `WAIT` command to enforce stronger guarantees when needed).
- Replicas are read-only by default, great for scaling read traffic.
- If the connection drops, Redis attempts a **partial resynchronization** using the replication backlog instead of a full resync, saving bandwidth.

---

## High Availability with Sentinel

Replication alone doesn't give you automatic failover. **Redis Sentinel** is a separate distributed system (a set of Sentinel processes) that:

1. Continuously monitors master and replica health.
2. Uses a quorum-based vote among Sentinels to agree a master is actually down (avoiding a single Sentinel's network hiccup triggering false failover).
3. Elects a replica to promote to master.
4. Reconfigures the other replicas to follow the new master.
5. Notifies clients of the new topology.

Sentinel requires at least 3 Sentinel nodes in production to avoid split-brain scenarios.

---

## Scaling with Redis Cluster

Sentinel gives you *availability*; **Redis Cluster** gives you *horizontal scale* by sharding data across multiple nodes.

- The keyspace is divided into **16,384 hash slots**.
- Every key is mapped to a slot via `CRC16(key) mod 16384`.
- Each master node owns a subset of slots; replicas back up each master for failover.
- Clients (with cluster-aware drivers) compute the slot locally and talk directly to the right node — no proxy hop needed.
- Multi-key operations only work if all involved keys hash to the same slot — this is why Redis supports **hash tags** (`{user123}.profile` and `{user123}.settings` both hash on `user123`, forcing them onto the same slot so they can be used together in a transaction or multi-key command).

Cluster handles node failure similarly to Sentinel — internally, but built into the cluster bus protocol (a gossip protocol between nodes on a separate port, usually `+10000` from the client port).

---

## Transactions

Redis transactions batch commands using `MULTI` / `EXEC`, guaranteeing they run back-to-back with no other client's commands interleaved (since execution is single-threaded, this is naturally atomic once queued).

```
MULTI
INCR balance:alice
DECR balance:bob
EXEC
```

- `DISCARD` cancels a queued transaction.
- `WATCH` implements **optimistic locking**: if a watched key changes before `EXEC`, the whole transaction aborts, letting the client retry. This is the standard pattern for check-and-set logic in Redis.

Note: Redis transactions do **not** support rollback on runtime errors (e.g., wrong data type) — commands that fail still execute; only queue-time errors (bad syntax) abort the whole batch.

---

## Pub/Sub Messaging

Redis has built-in publish/subscribe messaging.

```
SUBSCRIBE news.tech
PUBLISH news.tech "Redis 8 released!"
```

- Fire-and-forget — if no subscriber is listening, the message is lost (unlike Streams, which persist).
- `PSUBSCRIBE news.*` allows pattern-based subscriptions.
- Redis Cluster supports **sharded pub/sub** (`SPUBLISH`/`SSUBSCRIBE`) so messages stay local to the shard owning the channel, avoiding cluster-wide broadcast overhead.

Good for: real-time notifications, chat fan-out, cache invalidation broadcasts — bad for anything needing guaranteed delivery or replay.

---

## Redis Streams

Streams (since Redis 5.0) are Redis's answer to Kafka-style log processing, but simpler to operate.

```
XADD orders '*' order_id 1001 status "created"

# Consumer groups let multiple workers split up the stream
XGROUP CREATE orders workers '$'
XREADGROUP GROUP workers worker1 COUNT 5 STREAMS orders '>'
XACK orders workers <message-id>
```

Key concepts:
- Each entry gets an auto-generated ID (`<timestamp>-<sequence>`), so entries are naturally ordered.
- **Consumer groups** let multiple workers cooperatively process a stream, with each message going to exactly one consumer in the group (similar to Kafka partitions/consumer groups, minus the partition concept — a single stream handles it internally).
- `XPENDING` and `XCLAIM` let you detect and recover messages a crashed consumer never acknowledged.
- Streams can persist alongside RDB/AOF, unlike Pub/Sub.

Good for: event sourcing, activity logs, job queues with at-least-once delivery guarantees.

---

## Lua Scripting

Redis embeds a Lua interpreter, letting you run custom logic **atomically, server-side** — no round trips, no race conditions between the read and the write.

```
EVAL "return redis.call('GET', KEYS[1])" 1 mykey

# Better: preload with SCRIPT LOAD, then call by SHA
SCRIPT LOAD "return redis.call('SET', KEYS[1], ARGV[1])"
EVALSHA <sha1> 1 mykey "value"
```

Redis 7 introduced **Functions** (`FUNCTION LOAD`) as a more manageable, versioned alternative to raw `EVAL` scripts, letting you register named, reusable Lua libraries.

Classic use case: atomic "check rate limit and increment counter" logic that would otherwise require a transaction with `WATCH`/retry.

---

## Memory Management & Eviction

Since RAM is finite, `maxmemory` caps how much Redis can use, and `maxmemory-policy` decides what happens when the limit is hit:

| Policy | Behavior |
|---|---|
| `noeviction` | Return errors on writes once full (default) |
| `allkeys-lru` | Evict least-recently-used key across all keys |
| `volatile-lru` | Evict LRU among keys that have a TTL set |
| `allkeys-lfu` | Evict least-frequently-used key |
| `volatile-lfu` | LFU, but only among keys with TTL |
| `allkeys-random` | Evict a random key |
| `volatile-ttl` | Evict the key with the nearest expiration |

Redis doesn't track *true* global LRU (too expensive) — it uses an **approximated LRU**, sampling a small pool of random keys and evicting the "most stale" one from that sample. This is a deliberate trade-off: near-perfect eviction accuracy at a fraction of the CPU cost.

---

## Security

- `requirepass` sets a password; Redis 6+ supports full **ACLs** — you can create named users with fine-grained command and key-pattern permissions:
```
ACL SETUSER app_readonly on >password ~cache:* +get +exists
```
- `rename-command` can disable or rename dangerous commands (`FLUSHALL`, `CONFIG`) in production.
- TLS support since Redis 6 for encrypted client-server and replication traffic.
- Never expose Redis directly to the public internet without auth — it's a common target for cryptomining botnets when left open.

---

## Common Real-World Use Cases

- **Caching**: the #1 use case — cache expensive DB query results or computed values with a TTL.
- **Session store**: web apps store login sessions in Redis so any server in a fleet can read them.
- **Rate limiting**: sliding window counters using `INCR` + `EXPIRE`, or sorted sets with timestamps as scores.
- **Leaderboards**: sorted sets are a near-perfect fit — `ZADD`, `ZREVRANGE`, `ZRANK` give you ranking for free.
- **Job queues**: `LPUSH`/`BRPOP` for simple queues; Streams for queues needing consumer groups and replay.
- **Real-time analytics**: HyperLogLog for unique counts, bitmaps for daily-active-user tracking.
- **Distributed locks**: `SET key value NX PX 30000` (set-if-not-exists with expiry) is the basis of simple distributed locking; the **Redlock** algorithm extends this across multiple independent Redis instances for stronger guarantees.
- **Geospatial search**: "find the 5 nearest stores" type features.
- **Chat/notifications**: Pub/Sub or Streams for fan-out messaging.

---

## Redis Modules & Extensions

Redis's module API lets you extend it with new data types and commands, loaded as shared libraries. Notable ones:

- **RedisJSON**: native JSON document storage and querying inside Redis.
- **RediSearch**: full-text search and secondary indexing on top of Redis data.
- **RedisTimeSeries**: purpose-built time-series data type with downsampling and retention policies.
- **RedisGraph** (deprecated as of 2025 in favor of other graph solutions, but historically notable): graph database on Redis.
- **RedisBloom**: probabilistic data structures — Bloom filters, Cuckoo filters, Top-K, Count-Min Sketch.

These modules are commonly bundled together as **"Redis Stack"**, giving you document, search, time-series, and probabilistic capabilities on top of core Redis.

---

## Monitoring & Observability

- `INFO` — the single most useful command; dumps memory usage, replication state, stats, CPU, keyspace info.
- `MONITOR` — streams every command Redis processes in real time (debugging only — heavy performance cost, never use in production long-term).
- `SLOWLOG GET` — inspect commands that exceeded a configurable execution time threshold.
- `LATENCY HISTORY` / `LATENCY DOCTOR` — built-in latency spike diagnostics.
- `redis-cli --stat` — a running summary of ops/sec, memory, and connected clients.
- `MEMORY USAGE <key>` — see exactly how much RAM a specific key consumes.
- Prometheus + `redis_exporter`, Datadog, and Grafana dashboards are the common production monitoring stack.

---

## Redis vs Other Databases

| | Redis | Memcached | PostgreSQL/MySQL | MongoDB |
|---|---|---|---|---|
| Data model | Rich structures (lists, sets, streams...) | Simple key-value only | Relational, structured | Document (JSON-like) |
| Persistence | Optional (RDB/AOF) | None (pure cache) | Full ACID durability | Durable by default |
| Typical role | Cache + fast primary store + broker | Pure cache | System of record | System of record |
| Speed | Sub-millisecond | Sub-millisecond | Millisecond-ish | Millisecond-ish |
| Scaling | Cluster (sharded) | Client-side sharding | Vertical + read replicas | Native sharding |

Redis is rarely a *replacement* for your primary relational/document database — it's most often deployed **alongside** one, absorbing the read-heavy, latency-sensitive parts of your workload.

---

## Best Practices

- Set a `maxmemory` limit and an eviction policy explicitly — don't let Redis grow unbounded.
- Avoid huge keys (giant lists/hashes/sets on a single key) — they cause latency spikes on read/write/delete and complicate cluster resharding.
- Use pipelining to batch multiple commands into one round trip when you don't need each result immediately.
- Prefer `SCAN` over `KEYS *` in production — `KEYS` is O(N) and blocks the single event loop; `SCAN` is cursor-based and incremental.
- Namespace your keys clearly (`user:1001:profile`, `order:2026:5023`) for readability and easier pattern-based operations.
- Use hash tags in Cluster mode when you need multi-key atomic operations.
- Monitor `SLOWLOG` and `INFO latencystats` regularly, not just when something breaks.

---

## Common Pitfalls

- Treating Redis as a guaranteed-durable primary database without enabling AOF — a crash before an RDB snapshot means data loss.
- Running expensive O(N) commands (`KEYS`, `SMEMBERS` on a huge set, `SORT` on a huge list) on a production instance — it blocks every other client because of the single-threaded model.
- Forgetting TTLs on cache keys, leading to unbounded memory growth.
- Assuming Pub/Sub messages persist — they don't; use Streams if you need durability or replay.
- Under-provisioning Sentinel (fewer than 3 nodes) risking split-brain during failover.
- Ignoring `CLUSTER` hash-slot rules and trying to run multi-key commands across differently-hashed keys, which Redis will reject with a `CROSSSLOT` error.

---

## Closing Notes

Redis earns its ubiquity by doing a small number of things — storing structured data in memory and serving it back almost instantly — with an unusual level of engineering discipline: a deliberately simple concurrency model, compact internal encodings, and operational tooling (replication, Sentinel, Cluster, Streams) that scales that simplicity to production-grade systems. Understanding both *what* commands to run and *why* the engine behaves the way it does is what separates "I used Redis as a cache" from actually operating it well at scale.
