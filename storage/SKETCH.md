# Slice 0/1 代码草图

## 已实现 vs 尚未实现

| 部分 | 状态 |
|---|---|
| go module、HTTP 入口、flags、空目录、优雅关闭 | 已实现 |
| `GET /`、`GET /healthz` | 已实现 |
| Record v1 编解码、长度上限、EOF 分类 | 已实现，有单测及 fuzz target |
| TopicConfig 校验、catalog JSON envelope | 类型及校验已实现 |
| Broker → Topic → PartitionLog → Segment | 字段草图，没有存储方法实现 |
| CreateTopic / Produce / GetOffsets / Close | 只有 BrokerAPI 接口契约 |
| topic HTTP、catalog 原子保存、重启扫描 | 待实现 |
| torn-tail 修复、index、roll、fetch HTTP、groups | 后续 slice |

不会用返回 nil 的占位方法伪装功能完成。当前 `/produce`、`/topics` 返回 404。

## 数据所有权

```text
Broker
  mu: 保护 topic catalog
  topics[name] → Topic
                  config
                  partitions[id] → PartitionLog
                                     mu: 同一 partition 串行化
                                     nextOffset
                                     failed
                                     active → Segment
                                                file
                                                baseOffset=0
                                                records
                                                sizeBytes
```

不同 partition 可并行写；同一 partition 的 offset 分配、append、sync、发布必须
在同一把锁内。`O_APPEND` 只保证定位到文件末尾，不提供事务/断电原子性。
Broker 锁用于短暂查找和 catalog 更新，不要覆盖所有 partition 的 fsync。
Close 与并发操作的生命周期协议还需实现，不能直接在活跃请求中关闭文件。

## Slice 1 下一步的方法草图

以下是实施顺序，不是现有可调用 API：

```go
// topic.go
func Open(dataDir string) (*Broker, error)
func (b *Broker) CreateTopic(config TopicConfig) error
func (b *Broker) ListTopics() []TopicConfig
func (b *Broker) Produce(topic string, partition int, key, value []byte) (Record, error)
func (b *Broker) GetOffsets(topic string, partition int) (Offsets, error)
func (b *Broker) Close() error

// partition_log.go
func openPartition(dir string) (*PartitionLog, error)
func (p *PartitionLog) Append(key, value []byte) (Record, error)
func (p *PartitionLog) Offsets() (Offsets, error)
// 先做内部线性扫描用于测试，Slice 3 才接 HTTP fetch。
func (p *PartitionLog) ReadFrom(offset Offset, maxRecords int) ([]Record, error)
```

### Append：先 durable，后 visible

```text
lock partition
  拒绝 closed/failed 状态；校验 frame 大小、offset overflow
  r = {Offset: nextOffset, TimestampNS: clock.Now().UnixNano(), Key, Value}
  frame = EncodeRecord(r)
  n, err = active.file.Write(frame)
  若 err != nil 或 n != len(frame): 标记 failed；返回结果不确定错误
  若 active.file.Sync() 失败: 标记 failed；返回结果不确定错误
  active.sizeBytes += len(frame)
  active.records++
  nextOffset++
  发布 r；返回成功
unlock
```

失败后不得继续 append，否则 partial record 后接新记录会破坏扫描。
不轻率回滚 sync 失败的记录：数据可能已经落盘。重启扫描后它可能出现，重试可能重复。
返回的 key/value 应使用独立副本；调用者不得在 Produce 执行期间修改输入。

### Open：只信日志，不信计数缓存

```text
读取并校验 catalog
逐一打开已登记的 partition 文件（不自动补建缺失文件）
从 base=0 顺序 DecodeRecord
  成功：累计字节数、record 数
  clean EOF：恢复 nextOffset、sizeBytes
  任何坏帧/短尾：关闭所有已打开文件并报错（Slice 1）
全部成功后才发布 broker
```

### ReadFrom：先正确，再索引

- 检查 `0 <= offset <= nextOffset`、`maxRecords > 0`；等于 LEO 返回空集。
- Slice 1 可持 partition 读锁完成扫描，阻塞写但易于理解。
- 用 `io.SectionReader` / `ReadAt` 避免共享 file seek cursor；边界为 sizeBytes。
- offset 从 0 递增，跳过目标之前的 record；不得按 timestamp 排序。

## 验收顺序

1. CreateTopic：N 个空 partition，catalog 原子发布，非法名称/重复创建拒绝。
2. Produce 三条：返回 offset 0/1/2，GetOffsets 返回 `{earliest:0, latest:3}`。
3. ReadFrom(1)：返回 1/2；ReadFrom(3) 返回空；负数和越界拒绝。
4. 两个 partition 各自产生 offset 0；同 partition 并发写不重复、不交叉 frame。
5. Close → Open：offset 从 3 接续，不重新从 0 分配。
6. 注入 short write / Sync 错误，确认不会成功 ack，后续操作被 fenced。
7. 短尾/坏帧使 Open 报错，原文件保持不变；Slice 2 再做尾部修复。
8. HTTP topic/produce 接口和 Docker 端到端 smoke test。

设计自检：当前草图约 8/10（按学习型单机 MVP 范围）。格式、offset、并发边界和
失败策略已明确；要达到此范围的 10/10，还需真正实现 catalog/append/reopen，
补齐故障注入、生命周期并发测试和跨进程目录锁。此评分不是生产可靠性认证。
