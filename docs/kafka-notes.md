# Kafka 机制笔记：micro-batching、producer metadata 与 group fencing

这份文档记录三个理解 Kafka 设计的关键机制：**micro-batching**（攒批）、
**producer 侧 metadata**（客户端路由）和 **consumer group 协调**（membership
与 fencing）。它们解释了 Kafka 客户端/协议那些"奇怪"设计的存在理由，也标出
了 marchiq 刻意简化掉的复杂度在哪里。

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

---

## 3. Consumer group 协调：fencing 防的是僵尸，不是恶意者

### 分配是协调协议，不是访问控制

直觉上容易以为"group 的分配结果是一种权限：member 只能读分给自己的分区"。
Kafka 里恰恰相反 —— **group 协议管的是正确性协调，安全在另一层**：

- **membership 协议**：consumer 用 `JoinGroup` 加入，member.id 由 broker
  （group coordinator）下发；之后靠周期心跳维持会话（`session.timeout.ms`，
  默认 45s），停止心跳即被判定死亡并触发 rebalance。
- **generation fencing**：每次 rebalance 把 group 的 generation +1。
  `OffsetCommit` 等写路径请求携带 generation；旧 generation 的请求被明确拒绝
  （`UNKNOWN_MEMBER_ID` / `REBALANCE_IN_PROGRESS`；static membership 下是
  `FENCED_INSTANCE_ID`）。
- **但 Fetch 不校验 membership**：普通拉取请求不检查"你是不是这个 group 的
  member、这个分区是不是分给了你"。读任何分区都不需要先有分配。

为什么 fencing 只守写路径不守读路径？因为**重复读最多造成重复处理**（
at-least-once 语义本来就允许，下游幂等可解），而 **commit 污染会让整个 group
的消费进度错乱** —— 旧 member 把 offset 往回拨或往前跳，其他 member 的光标
全废。fencing 守住的是 offset 的写入口。

### 僵尸场景：fencing 到底在防什么

协议要防的现实威胁不是恶意攻击者，而是**僵尸** —— 一个 GC 卡顿或网络分区后
还以为自己活着的 consumer：

```text
t0  member m1 持有 partition-0（generation 5）
t1  m1 GC 卡顿 30s，心跳全部错过（session.timeout.ms = 10s）
t2  coordinator 判定 m1 死亡 → rebalance → generation 6，partition-0 分给 m2
t3  m1 从 GC 中醒来，以为自己还是 owner，继续处理并 commit offset
    → commit 携带 generation 5 → broker 拒绝（UNKNOWN_MEMBER_ID）
t4  只有 m2 的 commit（generation 6）能成功 → partition-0 的进度不被污染
```

没有 fencing 的话，t3 的 commit 会覆盖 m2 的进度：已处理的消息被跳过、或已
跳过的消息被重放 —— 而且静默发生，没有任何报错。

### 为什么这套东西不防恶意者

member.id 虽由 broker 下发，但**没有不可伪造性**：无认证环境下任何人都能
`JoinGroup` 领一个合法 member.id，也能直接发 Fetch 读任何分区。整个 group 协
议假设参与者是可信的，它只解决"**过期的**可信者不能捣乱"。

真正的安全边界是另外两层，与 group 协议正交：

| 层 | 机制 | 防什么 |
|---|---|---|
| 认证 | SASL / mTLS | 你是谁（防冒名） |
| 授权 | topic 级 ACL（read/write/describe） | 你能碰哪个 topic（防越权） |

### marchiq 对照

v0 把两层都明确 defer 了，而且比 Kafka 更"裸"：

- **member id 是自报的**（`member=m1` 只是个 query param），无 broker 下发、
  无心跳、无会话、无 generation。冒名没有任何成本。
- **显式 offset 路径完全敞开**：`GET /fetch?topic=T&partition=P&offset=O` 不
  经过 group 机制 —— group 是协调便利，不是访问控制。
- **静态分配下僵尸的表现**值得记住：member 真死 → 它名下的分区停滞（lag 增
  长）但**不会重复**；假死（网络分区后旧进程还活着）→ 两个进程读同一分区 →
  重复处理。at-least-once 容忍重复，但由于 commit 无 fencing，僵尸可以污染
  group 的 committed offset —— 这正是 slice 5+ 若做 rebalance/动态 membership
  时必须引入 generation 的原因。

安全（认证 + ACL）与教育 MVP 的定位无关，继续 defer；但"协调 ≠ 安全"这个区
分本身就是要学的一课。
