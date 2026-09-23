# Slice 0/1：磁盘格式 v1

状态：record codec 已实现并测试；catalog 持久化和 partition append 仍是设计草图。
这是 marchiq 自己的格式，不兼容 Kafka wire/disk format。

## 1. 最小目录

Slice 0 只创建 `meta/`、`topics/`。Slice 1 创建 topic 后的目标布局：

```text
data/
  meta/
    topics.json
  topics/
    events/
      0/00000000000000000000.log
      1/00000000000000000000.log
```

- partition 是十进制整数；segment 文件名是 20 位十进制 base offset。
- Slice 1 每个 partition 只有 base=0 的单段；暂不创建 index、timeindex、
  active.segment、partition.meta、groups.json，避免同时维护多份真相。
- log 是数据源；`nextOffset` 和文件长度从顺序扫描重建。
- 一个 dataDir 只允许一个进程使用。草图还没有跨进程锁，不支持共享目录。
- dataDir 为可信本地目录；topic 名校验不是对恶意符号链接的安全沙箱。

## 2. Topic catalog

目标 `meta/topics.json`：

```json
{
  "version": 1,
  "topics": [
    {"name": "events", "partitions": 2}
  ]
}
```

Topic 名：`[a-zA-Z0-9][a-zA-Z0-9._-]{0,248}`；partition 数 1–1024，创建后不变。
Slice 1 不接受 retention 配置，避免保存了却悄悄不执行。

计划中的创建顺序（尚未实现）：

1. 持 broker catalog 写锁，校验名称、数量、重复 topic；重复创建返回冲突。
2. 创建目录和空 log；同步文件及新目录的父目录项。
3. 在 `meta/` 同目录写完整 `topics.json.tmp`，`Sync`，关闭，rename 覆盖正式文件，
   然后同步 `meta/` 目录。首次建 `meta/`、`topics/` 时也需要同步父目录。
4. 发布内存 Topic，返回成功。catalog 发布失败不能把 topic 暴露给生产者。

恢复只加载正式 catalog；未知版本/非法配置/重复 topic 必须报错。
已登记 topic 缺失 log 必须报错，不能静默创建空日志造成“数据没丢”的假象。
创建中断留下的未登记目录不自动采用或删除；启动/再次创建时报告冲突，人工处理。
rename 后目录 sync 失败属于结果不确定，须停止 catalog 变更，重启检查。
原子 rename 不是目录项落盘保证。

## 3. Record frame

全部整数用 **big-endian**，不把 Go struct 原样写盘；无 padding。
一条 frame 对应一条 record，并不是 Kafka record batch。

| 字节位置 | 大小 | 字段 | 含义 |
|---|---:|---|---|
| 0 | 1 | magic/version | 固定 `0x01`，不是 CRC |
| 1 | 4 | body_len | uint32，不含前面的 5 bytes |
| 5 | 8 | timestamp_ns | int64，broker 分配的 Unix 纳秒时间 |
| 13 | 4 | key_len | uint32 |
| 17 | K | key | 任意 bytes |
| 17+K | 4 | value_len | uint32 |
| 21+K | V | value | 任意 bytes |

```text
body_len  = 16 + K + V
frame_len = 21 + K + V
```

- frame 总长度上限 **1 MiB**，分配内存前校验 body_len。
- nil 与空 key/value 在磁盘上都编码为零长度，不区分 null，不表示 tombstone。
- offset 不写入 frame：`offset = segment.baseOffset + ordinal`，ordinal 从 0 开始。
- timestamp 不决定顺序，可能因时钟校正倒退；只有 offset 决定 partition 内顺序。
- 无 checksum：能查出边界不完整及部分结构损坏，**不能保证检测 bit rot**。
  将来加入 CRC 必须引入新格式版本，不可偷偷改变 v1。

### Golden bytes

`timestamp=1, key="k", value="v"`，共 23 bytes：

```text
01 | 00000012 | 0000000000000001 | 00000001 | 6b | 00000001 | 76
```

`storage/record_test.go` 固定了这个编码，防止无意中改格式。

## 4. Offset 的唯一约定

```text
写入条数             0       1       3
实际 record offsets  []      [0]     [0,1,2]
earliest             0       0       0
latest / LEO         0       1       3
最后一条 offset      无      0       2
```

`GetOffsets` 返回半开区间 `[earliest, latest)`。
本项目 HWM 同样使用 exclusive boundary：在当前仅同步写入的单 broker 中，
成功发布后 HWM=LEO；不是“最后一条 offset”。
commit 后续记录的是最后处理完成的 offset，下一次读 `commit + 1`。
Slice 1 无 retention，因此 earliest 恒为 0。

## 5. 顺序扫描与错误分类

从文件位置 0 开始，`DecodeRecord(reader, nextOffset)` 成功一次就递增 offset。
读取 offset=O 需要从头扫描，O(N)；稀疏 index 到 Slice 2 再加入。

| 结果 | 含义 | Slice 1 计划 | Slice 2 计划 |
|---|---|---|---|
| `io.EOF` | frame 边界，0 bytes 消耗 | 正常结束 | 正常结束 |
| `io.ErrUnexpectedEOF` | header/body 不完整 | 拒绝打开，不改文件 | active 尾部截断到 lastGood 并 Sync |
| `ErrCorruptRecord` | 版本/长度结构不合法 | 拒绝打开 | 拒绝打开 |
| `ErrRecordTooLarge` | 声明长度超过上限 | 拒绝打开 | 拒绝打开 |
| 其他 I/O error | 设备/文件读取失败 | 报错 | 报错 |

scanner 必须单独保存 `lastGood`，不能用失败时已经消耗的字节位置作为截断点。
不能跳过坏 record：implicit offset 会让后续所有 record 的身份发生偏移。
即使是短尾也可能来自长度字段损坏；没有 CRC 时无法完全区分损坏和 torn write。
sealed segment 将来出现短尾应报错，而不是按 active tail 自动修复。
