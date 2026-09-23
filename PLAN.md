# marchiq — Kafka-inspired queue MVP

Educational message queue in Go. Same spirit as [marchilogs](../marchilogs):
**one learning goal per slice**, append-only disk layout, HTTP-first API, Docker
runnable, tests that prove the core loop.

**Not a Kafka clone.** We borrow the durable **partitioned append log** model and
consumer offset semantics — not KRaft, ZooKeeper, ISR, or wire-protocol compatibility.

---

## Learning goal

After the MVP you can explain, with a running binary and on-disk files:

1. Why Kafka is a **log**, not an LSM (no per-key merge; retention deletes segments).
2. How **topic → partition → offset** gives ordering and replay.
3. How **consumer groups** shard partitions and commit offsets.
4. What **durability** costs (page cache vs `fsync`, single broker vs replicas).

---

## Kafka concepts we keep (and what we drop)

| Kafka idea | marchiq v0 | Deferred |
|---|---|---|
| Topic | named stream of records | auto-create policies, quotas |
| Partition | ordered shard; unit of parallelism | cross-broker placement |
| Offset | monotonic int64 per partition | transactional offsets |
| Log segment | immutable file + sparse offset index | compaction topic (key→latest) |
| Producer | append with optional ack wait | idempotent producer, txn |
| Consumer | fetch by offset | isolation levels |
| Consumer group | one active reader per partition | static membership, rebalance protocols |
| Retention | time and/or size per topic | tiered storage |
| Broker | single process in v0 | cluster, replication, ISR |

**Explicit non-goals (v0):**

- KRaft / ZooKeeper / controller election
- Multi-broker replication and min ISR
- Kafka binary protocol compatibility
- Schema registry, Connect, Streams
- Exactly-once semantics (at-least-once + idempotent consumer is enough for MVP)
- Log compaction (keyed retention) — slice later if needed

---

## Core loop (the whole system in one picture)

```text
Producer                         Broker (single node)                    Consumer group G
   |                                    |                                      |
   |  POST /produce                     |                                      |
   |  topic=T, partition=P, body      |                                      |
   +----------------------------------->|  append to active segment            |
   |                                    |  return {offset, timestamp}          |
   |                                    |                                      |
   |                                    |  GET /fetch?group=G&topic=T          |
   |                                    |<-------------------------------------+
   |                                    |  assign partitions, read from        |
   |                                    |  committed_offset+1 .. high-water    |
   |                                    +------------------------------------->|
   |                                    |                                      |
   |                                    |  POST /commit {group,topic,part,off}|
   |                                    |<-------------------------------------+
```

**High-water mark (HWM):** last offset **written and visible** to readers (after
produce ack policy). **Log end offset (LEO):** same in single-broker v0.

---

## Storage model (log, not LSM)

Kafka’s broker storage is:

```text
append-only segment files + offset index + time index (optional)
```

There is **no** MemTable/SSTable/compaction for the main path. Old data disappears
when **retention** deletes whole segments.

### On-disk layout (v0)

```text
{dataDir}/
  meta/
    topics.json              # topic configs (partitions, retention)
    groups.json              # consumer group committed offsets
  topics/
    {topic}/
      {partition}/
        00000000000000000000.log    # segment: [length][crc?][key][value][timestamp]*
        00000000000000000000.index  # sparse map: relative offset → file position
        00000000000000000000.timeindex  # optional slice 3+: timestamp → offset
        active.segment              # symlink or meta pointer to active base offset
        partition.meta              # next offset to assign, active segment base
```

**Record batch (v0, simple):**

```text
magic(1) | length(4) | timestamp_ns(8) | key_len(4) | key | value_len(4) | value
```

- Empty key is allowed (Kafka allows null key).
- Offset = implicit index in log (base_offset + record_index) or stored per record;
  prefer **implicit** in v0: segment file name = base offset, index every N records.

**Segment roll** when `segment.bytes` or `segment.ms` exceeded → seal segment,
open new file at next offset.

**Atomicity:** produce appends to active segment with `O_APPEND`; on crash, trailing
partial record is truncated on broker start (length check).

---

## API (HTTP first, like marchilogs)

Binary protocol can be slice 6+; HTTP keeps demos and tests simple.

| Method | Path | Purpose |
|---|---|---|
| GET | `/healthz` | liveness |
| POST | `/topics` | create topic `{name, partitions, retention_ms?, retention_bytes?}` |
| GET | `/topics` | list topics |
| GET | `/topics/{topic}/offsets` | earliest / latest per partition |
| POST | `/produce` | `topic`, `partition` (or `-1` for round-robin), `key`, `value` (body) |
| GET | `/fetch` | `group`, `topic`, `max_bytes`, `timeout_ms` |
| POST | `/commit` | `{group, topic, partition, offset}` |
| POST | `/groups/{group}/join` | register member; returns partition assignment (v0 static) |
| GET | `/` | help text |

**Produce acks (v0):**

| `acks` | Behavior |
|---|---|
| `0` | append to page cache, return immediately (may lose on crash) |
| `1` | `fsync` active segment before ack (broker durable) |

Default `acks=1` for demos; document the tradeoff in `DURABILITY.md`.

**Fetch semantics:**

- Return records with `offset >= committed_offset + 1` for assigned partitions.
- Empty poll → 200 with `records: []` (long-poll optional slice 4).
- Include `next_offset` per partition in response.

---

## Consumer groups (v0 — intentionally small)

Kafka’s group coordinator is heavy. MVP rules (implemented in slice 4):

1. **Static assignment:** the first join declares the group size; members get an
   ordinal by join order; partition `p` belongs to the member with
   `ordinal == p % size`. No partition ever changes hands.
2. **One committed offset per (group, topic, partition)** in `meta/groups.json`,
   published with the same atomic protocol as the topic catalog
   (tmp → sync → rename → sync dir).
3. **No rebalance protocol** — a member beyond the declared size gets 409;
   full rebalance is slice 5+.
4. **At-least-once:** commit **after** processing; crash before commit → replay.

This is enough to demo “two consumers, two partitions, no duplicate partition”.

---

## MVP slices (build order)

Each slice = branch + tests + `docker compose` still works.

### Slice 0 — skeleton

- `go mod`, `cmd/marchiq/main.go`, `storage/` package
- `-dataDir`, `-addr`, `GET /healthz`, `GET /`
- Empty `topics/` tree

**Done when:** `go test ./...` green, Docker image builds.

### Slice 1 — topic + partition log

- Create topic with N partitions
- `Produce` appends to partition log (single segment)
- `GetOffsets` → `{earliest: 0, latest: N}`

**Tests:** append 3 records, latest offset = 3, records readable by offset scan.

### Slice 2 — segments + index + roll

- Sparse `.index` every `index.interval.bytes` (e.g. 4 KiB)
- Roll segment at `segment.bytes` (e.g. 1 MiB for tests)
- Broker restart truncates torn tail, reloads `partition.meta`

**Tests:** roll produces `0000...000.log` + `0000...001.log`; read spans segments.

### Slice 3 — fetch + manual offset

- `GET /fetch?topic=T&partition=P&offset=O` (no group yet)
- `max_bytes` limit

**Tests:** produce 100 messages, fetch from offset 50, get 50 records.

### Slice 4 — consumer group + commit

- `join` + `fetch?group=G` + `commit`
- `meta/groups.json` persisted; survives restart

**Tests:** consume 10, commit 9, restart, next fetch starts at 10.

### Slice 5 — retention

- Per-topic `retention_ms` and/or `retention_bytes` (catalog stays v1; zero =
  unlimited; catalogs written before this slice keep loading)
- Background ticker (`-retentionCheckInterval`, default 30s) deletes **sealed**
  segments; the active segment is never a candidate, even if it alone exceeds
  the size budget. `Broker.EnforceRetention()` is the deterministic single pass
  the ticker and the tests both drive.
- **Clamp:** never delete offsets still needed by any group joined to the
  topic. Per partition the deletable boundary is min over joined groups of
  (committed+1); a joined group that never committed contributes 0 (its next
  read is offset 0, so nothing is deletable); no joined group = no clamp.
- Age = sealed `.log` mtime (≈ seal time; index rebuilds never touch it, so it
  survives restarts). Size = delete oldest-first until the partition is back
  under budget. Deletion unlinks `.log` + `.index` and syncs the partition
  directory — durable deletes, same discipline as the atomic-publish protocol.
- `earliest` advances to the first surviving segment's base. Explicit `Fetch`
  below earliest → `ErrOffsetOutOfRange` (Kafka behavior); `FetchGroup` clamps
  its start up to earliest (auto.offset.reset=earliest) — only reachable by a
  group that joined after the deletion with no commits.

**Tests:** aged sealed segments deleted and earliest advances (no sleep —
`os.Chtimes` backdates mtimes); group clamp holds the segment containing
committed+1; min across groups; uncommitted group blocks all; byte budget
deletes oldest-first and keeps the active segment; restart preserves earliest
and late-joining groups start at earliest; fetch below earliest rejected.

### Slice 6 — polish (optional before “MVP done”)

- `acks=0` path
- Produce partition `-1` round-robin
- Basic metrics: bytes in/out, lag per group (`GET /debug/lag`)
- `cmd/demo/` producer + consumer script

---

## Durability decisions (document early)

Write `storage/DURABILITY.md` in slice 1:

| Mode | Crash behavior | Use |
|---|---|---|
| `acks=0` | lose recent produces | max throughput demos |
| `acks=1` + `fsync` | single broker durable | default MVP |
| Replicas | not in v0 | explain why Kafka needs ISR |

Contrast with marchilogs: logs batch flush on interval; marchiq acks=1 is closer to
**Kafka `acks=all` on a 1-replica cluster**.

Page cache: even with `fsync`, teach that **replica fsync on multiple brokers**
is what production Kafka relies on for AZ failure.

---

## Observability (minimal)

| Signal | v0 |
|---|---|
| Log | produce/fetch/commit with topic, partition, offset |
| HTTP headers | `X-Marchiq-Records-Returned`, `X-Marchiq-High-Watermark` |
| Debug | `GET /debug/segments?topic=&partition=` lists segment files + sizes |

No JMX. Metrics slice can add Prometheus later.

---

## Project layout (target)

```text
marchiq/
  PLAN.md                 # this file
  go.mod
  Dockerfile
  docker-compose.yml
  cmd/
    marchiq/main.go       # HTTP server, flags
    demo/                 # produce/consume demo
  storage/
    topic.go
    partition_log.go
    segment.go
    index.go
    group.go
    retention.go
    DURABILITY.md
  storage/*_test.go
```

Module path: `github.com/marchi/marchiq` (mirror marchilogs).

---

## Demo script (graduation bar)

```bash
# terminal 1 — broker
docker compose up

# terminal 2 — create topic
curl -X POST localhost:9092/topics -d '{"name":"events","partitions":2}'

# terminal 3 — producer
for i in $(seq 1 20); do
  curl -X POST "localhost:9092/produce?topic=events&key=k$i" -d "payload-$i"
done

# terminal 4 — consumer group (two members, static assignment)
curl -X POST "localhost:9092/groups/workers/join?topic=events&member=w1&members=2"
curl -X POST "localhost:9092/groups/workers/join?topic=events&member=w2&members=2"
curl "localhost:9092/fetch?group=workers&topic=events&member=w1&max_bytes=65536"
curl -X POST localhost:9092/commit -d '{"group":"workers","topic":"events","partition":0,"offset":9}'
```

Pass: two partitions show roughly balanced keys; after commit, restart broker,
fetch does not replay committed offsets.

---

## Relation to sibling projects

| Project | Overlap | Difference |
|---|---|---|
| **marchilogs** | append-only parts, retention, atomic publish | query by time/stream; many readers scan |
| **marchiq** | append-only files, retention | ordered partition log; consumers track offset |
| **marchimetrics** (planned) | time partitioning | numeric samples, not byte records |

marchiq is the right second system after marchilogs if the goal is **Kafka
durability narrative** (log, segments, fsync, replicas story) without building
another LSM.

---

## Open decisions (defaults for v0)

| Question | Default | Revisit when |
|---|---|---|
| HTTP vs Kafka protocol | HTTP | slice 6+ if wire compat needed |
| Key hashing to partition | `hash(key) % N` if partition omitted | sticky partitioner |
| Segment index interval | every 4 KiB | large messages |
| Group rebalance | static / single member | slice 5 multi-member |
| CRC per record | optional magic byte | corruption detection slice |

---

## Success criteria (“MVP done”)

- [x] Single broker, multiple topics, multiple partitions
- [x] Produce → fetch → commit → restart → no duplicate past commit
- [x] Segment roll + retention reclaim disk
- [ ] `go test ./...` + Docker demo script documented
- [ ] `DURABILITY.md` explains acks and why this is not LSM

---

## After MVP (not now)

1. Log compaction topic (`key` → latest value, tombstone delete)
2. Multi-broker + leader/follower replication (mini-ISR)
3. Kafka protocol frontend (or franz-go client against marchiq)
4. Transactional produce (out of scope for learning MVP)

Start implementation at **slice 0** on branch `feat/skeleton`.
