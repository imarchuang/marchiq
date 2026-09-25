# Durability：承诺与非承诺

marchiq 是单 broker 教学系统。本文档把"返回成功"到底意味着什么钉死：哪些
故障下数据一定在，哪些故障下数据可能丢，以及丢了之后系统如何表现。

## acks：返回成功之前等什么

| 模式 | 返回成功前 | 崩溃行为 | 用途 |
|---|---|---|---|
| `acks=0` | frame 经 write(2) 进入 page cache 即返回，不 fsync | **OS 崩溃/断电**可能丢近期写入；**进程 kill -9 不丢**（page cache 属于内核，不属于进程） | 吞吐演示 |
| `acks=1`（默认） | write + segment 与 index 各一次 fsync 之后才返回 | 在文件系统与硬件兑现 fsync 的前提下，本机重启可恢复 | 默认 |
| 副本 | v0 不提供 | 磁盘损坏、主机丢失、AZ 故障仍可丢全部数据 | — |

几个精确区分：

- **page cache ≠ 进程内存。** acks=0 的写入在 write(2) 返回时就在内核手里；
  kill -9 只杀进程，内核持有的 page cache 不受影响，重启 broker 后数据还在。
  acks=0 真正的丢失窗口是 **OS 崩溃、断电、内核 panic** —— 未刷盘的页随内核
  一起消失。acks=0 丢数据是"整机故障"的故事，不是"进程退出"的故事。
- **fsync 买的是"本机重启可恢复"，不是"不丢"。** 它把数据推到存储设备（并
  要求设备刷自己的易失缓存）。磁盘坏、主机丢、机房断电，单机 fsync 都救不
  了 —— 那是副本要解决的问题（见下）。
- **acks=0 的崩溃恢复不需要新代码。** 未刷盘的页丢失后，磁盘上的日志表现为
  一个更早的干净 EOF（LEO 回退），或一个被截断的半帧 —— 后者就是 active 段
  torn tail，Open 时截断并同步（FORMAT.md §5）。index 文件同理：它是派生
  缓存，Open 时从扫描重建，acks=0 跳过它的 fsync 不引入任何新状态。
- `O_APPEND` 不是 record 原子提交；acks=1 下 sync 失败或 HTTP 响应丢失时，
  调用者无法知道记录是否已持久化：重试可能重复。不承诺 exactly-once，也不
  承诺"失败返回意味着没写入"（`ErrResultUncertain` + partition fenced）。
- 文件 fsync 不自动确保新文件的目录项落盘；catalog/groups 的原子 rename 也
  不等于持久化。完整协议（tmp → sync → rename → sync dir）见 FORMAT.md。
  metadata 发布失败必须处理为失败或结果不确定，不能照常 ack。

## acks=0 ↔ committed offset：LEO 回退到已提交位点之下

groups.json 的每次 commit 都走完整原子发布协议（含 fsync），所以
**committed offset 可能比数据本身更耐久**：一组记录用 acks=0 写入、被 group
消费并提交，随后 OS 崩溃丢掉未刷盘的尾部 —— 重启后 LEO 回退到
committed+1 之下。

marchiq 的行为（与 Kafka unclean 恢复后的方向一致：offset 丢了就是丢了，
不伪造、不篡改游标）：

- `FetchGroup` 把越界的起点 clamp 到 LEO：在 LEO 处空轮询，不报错；
  **不跳过任何数据**，也**不改写 groups.json 里的 committed offset**。
- clamp 是安全阀，不是恢复手段：游标仍然是权威。之后新写入复用了丢失的
  offset 时，**已经提交过这些 offset 的 group 不会再收到替身记录** —— 它的
  commit 声明了"已处理到 9"，于是 refill 后轮询保持为空，直到 LEO 越过
  committed+1，从第一个真正新的 offset 继续投递。丢失窗口不会自动愈合；
  下游若在意，只能靠自己的幂等/对账机制。
- 反过来，若 group 的 committed 仍在回退后的 LEO 之下（没提交到丢失区），
  它会从 committed+1 正常重读 —— 占据这些 offset 的新记录会被再送一次，
  at-least-once 语义不变。
- `GET /debug/lag` 把回退摆到明处：`lag = latest - (committed+1)` 变成
  **负数**，就是 LEO 回退到 committed 之下的直接证据。
- 对高于当前 LEO 的 commit，`CommitOffset` 仍然 fail-closed 拒绝
  （`ErrOffsetOutOfRange`）：不允许把游标写到数据不存在的地方。

教训一句话：**commit 的耐久性 ≥ 数据的耐久性才有意义。** 用 acks=0 生产、
又按批 commit 的 group，等于主动签署"崩溃时我的游标可能指向虚空"的协议。

## 为什么是 log，不是 LSM

数据按 partition offset 追加，读取按 offset 顺序扫描；无按 key 合并、无
MemTable、无 SSTable 层级 compaction。旧数据由 retention 整段删除；`.index`
是可重建的派生缓存（Open 时从扫描原子重写）。写路径因此只是一次顺序追加
—— 这正是 Kafka 用磁盘顺序写逼近理论带宽的原因，也是"攒批"要优化的对象
（docs/kafka-notes.md §1）。CRC 尚未加入：格式校验能发现边界不完整与部分
结构损坏，**不能保证检测 bit rot**。

## 单 broker vs 副本：为什么生产 Kafka 需要 ISR

本机 fsync 只覆盖"进程死、机器活"和"机器死、盘活"两类故障。生产 Kafka 的
durability 单位是**跨故障域的副本**，不是单块盘：

- leader 收到写入后由 follower 复制；`acks=all` + `min.insync.replicas` 要求
  ISR 中足够多副本确认才返回成功。
- 每个副本通常也只写 page cache（Kafka 默认不逐条 fsync，靠后台批量刷盘）
  —— 因为**多台机器同时断电**的概率远低于一台。多副本 page cache 是用冗余
  换掉逐条 fsync 的延迟。
- marchiq 是单 broker：acks=1 的逐条 fsync 是教学上把"本地持久化成本"钉死
  的刻意选择，不能替代跨故障域复制。

## 与 marchilogs 的对照

marchilogs 按 interval 批量 flush（攒批摊薄 fsync）；marchiq acks=1 逐条
fsync。语义强度上 marchiq acks=1 **接近**"单副本 Kafka 集群的 acks=all"
—— 但只是接近：Kafka 的 acks=all 是 ISR 副本确认，不意味着每个副本逐条
fsync；marchiq 没有副本，它的 acks=1 强调的只是本地落盘。两个项目共享同一
套原子发布协议（tmp → sync → rename → sync dir）与"log 是真相、索引是缓存"
的恢复纪律。
