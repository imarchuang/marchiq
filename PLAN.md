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
3. How **consumer groups** shard partitions, commit offsets, and fence zombie
   members (heartbeat + generation).
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
| Consumer group | one active reader per partition; heartbeat + generation fencing | static membership (instance ids), cooperative/incremental rebalance |
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
| POST | `/produce` | `topic`, `partition` (`-1` = key hash / round-robin), `key`, `acks` (`0`\|`1`), `value` (body) |
| GET | `/fetch` | `group`, `topic`, `max_bytes`, `timeout_ms` |
| POST | `/commit` | `{group, topic, partition, offset, generation}` — generation required, fenced (409) |
| POST | `/groups/{group}/join` | join; returns `{generation, partitions}` over the live member list |
| POST | `/groups/{group}/heartbeat` | `topic`, `member`, `generation`; refreshes the session, returns current assignment; 409 = rebalance happened |
| POST | `/groups/{group}/leave` | `topic`, `member`; removes the member, bumps the generation |
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

## Consumer groups (dynamic membership + fencing)

Kafka’s group coordinator is heavy; marchiq keeps the protocol small but real
(slice 7 replaced slice 4's static declared-size assignment):

1. **Dynamic membership over a live list:** members join in order; the member
   at index `i` owns partition `p` iff `i == p % len(members)`. Joins append,
   leaves/evictions remove, and assignments redistribute automatically. More
   members than partitions → trailing members idle with an empty assignment.
2. **Heartbeat + session timeout:** members heartbeat (`-sessionTimeout`,
   default 10s); a background reaper (`-groupReapInterval`, default 1s)
   evicts expired members. Sessions are in-memory only — a broker restart
   grants every loaded member a fresh timeout's grace — while membership and
   generation persist in `meta/groups.json`.
3. **Generation fencing:** every membership change bumps the generation.
   Commits (and heartbeats) carry a generation; stale ones are rejected
   (409 / `ErrGenerationFence`). Fencing guards the offset WRITE path only —
   fetches stay unfenced (docs/kafka-notes.md §3). Rejoining after eviction
   is a NEW join at the end of the order (no static-membership reclaim).
4. **One committed offset per (group, topic, partition)**, published with the
   same atomic protocol as the topic catalog (tmp → sync → rename → sync
   dir); offsets survive the last member leaving (the retention clamp depends
   on them).
5. **At-least-once:** commit **after** processing; crash before commit → replay.

This demos “two consumers, two partitions, no duplicate partition” — and a
paused consumer being fenced out of the group.

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

### Slice 6 — polish (done)

- `acks=0` produce path: `POST /produce?...&acks=0` writes to the page cache
  and returns without fsync; `acks=1` (default) keeps write→sync→publish;
  unknown acks → 400. Storage takes an `Acks` parameter
  (`AcksNone`/`AcksLeader`) on `Produce`/`Append`; the offset is still
  assigned and published under the partition lock. Crash interaction: an OS
  crash can rewind LEO **below** a group's committed offset — `FetchGroup`
  clamps to an empty poll at the new LEO (no error, no skipped data, commit
  untouched), and `lag` goes negative. Full story in `storage/DURABILITY.md`.
- Produce `partition=-1`: the broker picks — key present → `fnv32a(key) % N`
  (same key, same partition); no key → per-topic round-robin (atomic counter,
  in-memory only). The thin-HTTP-client counterpart of Kafka's client-side
  partitioner (docs/kafka-notes.md §2).
- `GET /debug/lag[?group=G]`: per joined (group, topic, partition)
  `{committed, next_fetch, latest, lag}` with `committed: null` when never
  committed; group members included. `GET /debug/stats`: process-lifetime
  produce/fetch record + payload-byte counters (reset at Open).
- `cmd/demo/`: stdlib-only producer/consumer (`-mode produce|consume`);
  producer prints the per-partition histogram and key→partition placement,
  consumer joins/fetches/commits with a `-noCommit` replay mode.

**Tests:** acks=0 skips sync (failSync fault injection) and stays fenced after
an acks=1 failure; unknown acks rejected without fencing; same key → same
partition, keyless spreads evenly, explicit partition wins, -1 on unknown
topic → 404; lag after produce 10 / commit 6 is 3, uncommitted partition lag =
LEO; LEO-rewind-below-committed clamps to an empty poll, never rewrites the
commit, and resumes at the first offset past the cursor; stats counters move
and reset at restart; HTTP coverage for acks/-1/400s, /debug/lag schema,
/debug/stats.

### Slice 7 — group protocol (heartbeat + fencing + rebalance)

- Dynamic membership replaces slice 4's declared-size static assignment:
  `groupTopic` is `{members (join order), generation, offsets}` plus an
  in-memory-only `lastSeen`; assignment is recomputed over the live list on
  every change (`i == p % len(members)`), and the generation bumps on every
  join/leave/eviction. The legacy `members=N` join param is accepted and
  ignored.
- `POST /groups/{g}/heartbeat?topic=T&member=M&generation=N` refreshes the
  session and returns the current `{generation, partitions}` — the client's
  rebalance sensor (409 = generation moved, resync; 404 = evicted, rejoin).
  `POST /groups/{g}/leave?topic=T&member=M` removes a member; the group and
  its committed offsets survive the last member leaving.
- Generation fencing guards the offset write path: `POST /commit` requires
  `"generation"` (400 if missing, 409 if stale). Fetches stay unfenced but
  resolve members over the CURRENT live list, so an evicted zombie's fetch
  fails member-not-found.
- Reaper: `Broker.ReapExpiredMembers(now)` is the deterministic single pass
  the tests drive; the ticker (`-groupReapInterval`, default 1s) mirrors the
  retention janitor. `-sessionTimeout` default 10s. Sessions are in-memory:
  Open grants every loaded member a full timeout's grace.
- `meta/groups.json` envelope v2 (`generation` per binding, `size` dropped);
  v1 files migrate on load (join order kept, generation := 1).
- `cmd/demo` consume is a Kafka-client-shaped loop: heartbeat goroutine at
  sessionTimeout/3, commits carry the generation, 409/404 → rejoin → resume,
  LeaveGroup on shutdown.

**Tests:** the kafka-notes §3 zombie timeline end-to-end (evict → old
generation fenced → new generation commits → rejoin appends at the end);
heartbeat refresh / not-found / fence names the current generation; leave
redistributes and the last member keeps offsets (retention clamp still
enforced); restart keeps members + generation + offsets with sessions reset
(no mass-eviction, heartbeats work); more members than partitions idle;
commit without generation rejected (HTTP 400, storage fence on stale);
groups.json v1→v2 migration and unknown-version refusal; slice-4 tests
ported to live-member semantics.

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
| Debug | `GET /debug/segments?topic=&partition=` segment files + sizes; `GET /debug/lag` per-group lag; `GET /debug/stats` produce/fetch counters |

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

# terminal 2 — create topic, then produce 100 records over 10 keys with
# partition=-1 (broker-side partitioner): histogram is balanced and every
# key is pinned to exactly one partition
curl -X POST localhost:9092/topics -d '{"name":"events","partitions":2}'
go run ./cmd/demo -mode produce -topic events -n 100 -keys 10

# terminal 3+4 — two-member consumer group (dynamic assignment, one partition
# each; both heartbeat in the background and commit with the generation)
go run ./cmd/demo -mode consume -topic events -group workers -member w1
go run ./cmd/demo -mode consume -topic events -group workers -member w2

# lag drains to zero; counters moved; the generation is visible per group
curl localhost:9092/debug/lag?group=workers
curl localhost:9092/debug/stats

# zombie fencing: kill -STOP the w1 consumer (heartbeats stop); within one
# session timeout the reaper evicts it (broker log line), w2's heartbeat picks
# up BOTH partitions, and the generation bumps. kill -CONT wakes w1: its
# in-flight commit is fenced (409), so it rejoins at the end of the order and
# resumes — watch the join lines in both terminals.

# at-least-once replay: a -noCommit consumer restarted re-reads everything
go run ./cmd/demo -mode consume -topic events -group workers -member w1 -noCommit

# acks=0 vs crash: produce with -acks 0, then simulate an OS crash (kill -9
# alone keeps the kernel page cache — see storage/DURABILITY.md); LEO can
# rewind below a committed offset, /debug/lag shows negative lag, and the
# group keeps polling sanely at the new LEO
```

Pass: two partitions show roughly balanced keys and same-key records share a
partition; after commit, restarting the broker does not replay committed
offsets; a `-noCommit` consumer restarted replays; `/debug/lag` reaches 0
after a drain.

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
| Group rebalance | eager rebalance over the live member list (slice 7) | cooperative/incremental protocols, static membership |
| CRC per record | optional magic byte | corruption detection slice |

---

## Success criteria (“MVP done”)

- [x] Single broker, multiple topics, multiple partitions
- [x] Produce → fetch → commit → restart → no duplicate past commit
- [x] Segment roll + retention reclaim disk
- [x] `go test ./...` + Docker demo script documented
- [x] `DURABILITY.md` explains acks and why this is not LSM

---

## After MVP (not now)

1. Log compaction topic (`key` → latest value, tombstone delete)
2. Multi-broker + leader/follower replication (mini-ISR)
3. Kafka protocol frontend (or franz-go client against marchiq)
4. Transactional produce (out of scope for learning MVP)

Start implementation at **slice 0** on branch `feat/skeleton`.
