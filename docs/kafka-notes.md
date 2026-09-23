# Kafka 机制笔记：micro-batching 与 producer 侧 metadata

这份文档记录两个理解 Kafka 设计的关键机制：**micro-batching**（攒批）和
**producer 侧 metadata**（客户端路由）。它们解释了 Kafka 客户端那些"奇怪"
配置项的存在理由，也标出了 marchiq 刻意简化掉的复杂度在哪里。

---

## 1. Micro-batching

### 核心思想：摊薄固定成本

逐条操作的代价里有一大块是**固定开销**（与批内条数无关）：一次系统调用、
一次网络往返、一次 fsync、一次锁竞争。攒 N 条一起处理，固定成本摊薄到 1/N：

```text
逐条:  [syscall][syscall][syscall][syscall]...   ← N 次固定开销
攒批:  [_______syscall_______]                    ← 1 次固定开销 ÷ N 条
```

### Kafka 里的四个出现位置

**Producer 端（最经典）。** Producer 不是来一条发一条，而是按分区攒 batch：

| 参数 | 含义 |
|---|---|
| `batch.size` | 攒满这么多字节就发（默认 16 KB） |
| `linger.ms` | 没攒满也最多等这么久（默认 0，调优常设 5–100ms） |

`linger.ms` 是 micro-batching 的精髓：**主动等几毫秒**让同分区记录凑成一批。
附带一个巨大红利 —— **压缩**：逐条压缩几乎压不动（重复结构太少），整批一起
压（gzip/lz4/zstd）压缩率差 5–10 倍，省的都是网络带宽。

**Broker 磁盘端。** 顺序追加 + page cache 批量刷盘：broker 收到的是 batch，
写盘是顺序写。这是"为什么是 log 结构"的延伸 —— 顺序批写逼近磁盘理论带宽。

**Consumer 端。** 一次 fetch 拉一批（`max.bytes` 类参数控制批量），处理完一整
批再 commit 一次 offset —— commit 也按批摊薄。逐条 commit 会把 broker 打爆；
逐批 commit 把崩溃重放窗口控制在一批以内（at-least-once 语义下）。

**流处理框架（另一个语境）。** 说 Spark Streaming / Structured Streaming 是
micro-batch 架构，指它把无限数据流切成一串小批次作业（如每 1 秒一批），每批
走一次完整批处理调度。对比 record-at-a-time（Flink、Kafka Streams）：

| | micro-batch（Spark） | record-at-a-time（Flink/KStreams） |
|---|---|---|
| 延迟 | 秒级（批间隔） | 毫秒级 |
| 吞吐 | 高（批优化） | 相对低 |
| 状态管理 | 批次间快照 | 持续增量 |

### 代价永远一样：延迟

所有 micro-batching 都是同一笔交易：**用 X 毫秒的额外等待换 N 倍吞吐**。
判断标准不是"要不要攒批"，而是"业务能容忍多少延迟" —— 支付风控可能只容忍
10ms，日志聚合等 1 秒毫无压力。

### marchiq 对照

marchiq 当前是极端的反 micro-batch 设计：每次 `POST /produce` = 一次 HTTP
往返 = 一条记录 = 一次 fsync。这是刻意的 —— 先把"单条 durable"的语义钉死，
才能看清攒批到底优化什么：

- `acks=1` 每条 fsync ≈ 吞吐天花板几百条/秒
- `acks=0`（page cache 即返回）是 producer 侧攒批的 broker 对应物
- 若将来加批量 produce API（一次请求多条记录、一次 fsync 落整批），就是完整
  的 micro-batching 闭环 —— 也是 Kafka record batch 的对应物（marchiq 一帧 =
  一条记录，刻意不是 Kafka 的 record batch，差别就在这）

---

## 2. Producer 侧 metadata（客户端路由）

### 分区决策在 producer 端，不在 broker 端

直觉上容易以为"消息发给 broker，broker 决定进哪个分区"。Kafka 恰恰相反：
**先定分区，再攒批，再发送**，全部发生在客户端：

```text
send(record)
   │
   ▼
序列化 key/value
   │
   ▼
分区器（partitioner）           ← 在这里就定了分区
   │  key 存在:  hash(key) % N
   │  key 为空:  sticky / round-robin
   ▼
RecordAccumulator              ← 按 (topic, partition) 分桶攒批
   │  Map<TopicPartition, Deque<Batch>>
   ▼
Sender 线程                     ← 整批直发该分区的 leader broker
```

producer 通过 metadata 请求拿到 topic 的分区数 N 和每个分区的 leader 位置
（启动时 + 定期刷新），之后 `hash(key) % N` 在客户端本地计算。broker 收到的
是已打好分区标签的 batch，只管追加，不做路由。

为什么非要客户端做分区：

1. **攒批的前提就是知道分区** —— 两件事互为因果
2. **省掉 broker 的路由开销** —— batch 直发 leader，broker 是纯存储
3. **顺序性由客户端保证** —— 同 key 永远 hash 到同分区，这个承诺只能在发送方做

佐证：无 key 消息若严格 round-robin（每条换分区），每个分区的 batch 只有一两
条，攒批直接失效。所以 Kafka 2.4+ 默认 **sticky partitioner**：无 key 时粘住
一个分区直到攒满或 linger 到期再换 —— 这个优化的存在恰恰证明"分区已知"是
攒批的前提。

### Leader 变更时怎么办：最终一致 + 乐观执行

metadata 是缓存，leader 切换时客户端手里的就是旧地图。Kafka 的解法不是保持
强一致（太贵），而是**允许拿旧地图走路，撞墙就取新地图重试**：

```text
t0  producer 缓存：partition-0 的 leader = broker1
t1  broker1 宕机；controller（KRaft）从 ISR 选出 broker2 当新 leader
t2  producer 不知情，batch 仍发往 broker1
    → 连接被拒（broker 挂了）
    → 或收到 NOT_LEADER_OR_FOLLOWER（broker 活着但已不是 leader）
t3  producer 把该 topic 的 metadata 标记为 stale，重新拉取
t4  学到 leader = broker2，重试同一个 batch → 成功
```

三个机制：

1. **错误驱动刷新（主机制）**：broker 收到不属于自己的 partition 写入时明确
   拒绝（`NOT_LEADER_OR_FOLLOWER`，可重试错误）。producer 收到后：标记
   metadata 过期 → 重新拉取 → 重试 batch（batch 还在 accumulator 里，没丢）。
   这就是 `retries` 必须 > 0 的原因 —— 重试不仅防网络抖动，更是 leader 切换
   的正常恢复路径。
2. **定时刷新（兜底）**：`metadata.max.age.ms`（默认 5 分钟），无错误也定期
   过期重拉，防止冷分区握着陈旧路由。
3. **metadata 请求可发给任意 broker**：每个 broker 都从 controller 同步了全量
   集群元数据，旧 leader 死了也能从 `bootstrap.servers` 里任意一台问到新拓扑。

### 重试的顺序性陷阱

leader 切换触发重试时，若 `max.in.flight.requests.per.connection > 1`，先发后
到的 batch 可能乱序。这正是**幂等 producer** 要解决的另一个问题：PID + 序列号
让 broker 检测并拒绝乱序/重复 batch。所以幂等 producer 要求
`max.in.flight ≤ 5` —— 在序列号窗口内保序。

### 模式总结

Kafka 客户端- broker 元数据协议是一个**最终一致 + 乐观执行**的协议：

- **乐观执行**：拿缓存直接发，大概率是对的
- **悲观检测**：错了 broker 一定拒绝，不会静默写错
- **自动修复**：刷新 + 重试，对上层透明

同构的模式在分布式系统里很通用（DNS、HTTP 缓存、Raft 的 leader hint）：
**缓存的正确性不靠同步维护，靠使用时的校验和失效恢复**。

### marchiq 对照

单 broker = 没有 leader 概念 = 没有元数据一致性问题。PLAN 把
cross-broker placement 明确推迟，砍掉的就是这一整块：controller 选举、ISR、
元数据传播、错误驱动刷新。

另一个差异：marchiq 的瘦 HTTP 客户端把分区决策交给了 broker（请求里显式带
`partition=`，或 Slice 6 的 `-1` round-robin 由 broker 选）。代价是客户端侧无
法做 per-partition 攒批。若将来写"胖客户端"（先 `GET /topics` 拿分区数，本地
hash，按分区缓冲，批量发送），就把 Kafka 的客户端分区 + micro-batching 完整
复刻了 —— 而 broker 的 HTTP API 一个字符都不用改。
