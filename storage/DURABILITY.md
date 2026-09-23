# Durability：承诺与非承诺

这是 Slice 1 的目标语义，当前只有 codec 和 skeleton，尚无 Produce 实现。

## 本项目的 acks

| 模式 | 返回成功前 | 保证边界 |
|---|---|---|
| Slice 1 默认 `acks=1` | 完整 write + 文件 fsync；新文件/目录的目录项也已持久化 | 在文件系统和硬件正确兑现 fsync 的假设下，本机重启可恢复 |
| Slice 6 `acks=0` | 写入 page cache，不等待 fsync | 主机崩溃/断电可能丢近期写入；进程退出不等于 page cache 丢失 |
| 副本 | v0 不提供 | 磁盘损坏、主机丢失、AZ 故障仍可丢全部数据 |

`O_APPEND` 不是 record 原子提交；一次 Write 也可能只写了一部分。
Sync 失败或 HTTP 响应丢失时，调用者不知道记录是否持久化：重试可能重复。
因此不承诺 exactly-once，也不承诺“失败返回意味着没写入”。

文件 fsync 不自动确保新文件的目录项落盘；catalog 的原子 rename 也不等于持久化。
完整创建顺序见 FORMAT.md。metadata 发布失败必须处理为失败或结果不确定，不能照常 ack。

## 与 Kafka 的区别

**marchiq 的 acks 名称不能直接套用 Kafka 语义。**

- Kafka `acks=1` 是 leader 确认，不表示每条消息都执行 fsync。
- Kafka `acks=all` 是 ISR 副本确认，结合 `min.insync.replicas` 等配置提供复制保障，
  也不表示每个副本每条消息都执行 fsync。
- Kafka 通常依赖多副本和 page cache，而不是强制每条消息在多个副本逐条 fsync。
- 本项目“单机逐条 fsync”强调的是本地持久化成本，不能替代跨故障域复制，
  也不能简单称为等同于“单副本 Kafka acks=all”。

## 为什么没有 LSM

数据按 partition offset 追加，读取按 offset 顺序扫描；无按 key 合并、无 SSTable
层级 compaction。未来 retention 删除完整旧 segment，index 是可重建加速结构。
CRC 尚未加入：格式校验不能证明内容未损坏。
