# 运维手册

quick-memory 没有常驻服务：运维对象是**一个桶**和**每台机器上的本地缓存**。
所有状态都在桶里，因此"备份"就是桶本身的持久性与版本控制。

## 常规节奏

| 频率 | 动作 | 命令 |
|---|---|---|
| 每次会话结束 | 编译并发布 | `qm consolidate --session <id>` 然后 `qm publish` |
| 每天/每周 | 压缩索引（可选） | `qm compact` |
| 每周 | 回收不可达对象（先 dry-run） | `qm gc` → 确认后 `qm gc --apply` |
| 随时 | 查看状态 | `qm status --json` |

这些都是**可放弃作业**：拿不到租约就跳过，没有它们系统照常工作，只是分片或孤儿对象变多。

## 回收（GC）

- 可达性分析决定生死：manifest、每条 live page 的**整条 supersession 链**、tombstone 删掉的那一版、
  当前 catalog 及其引用的分片、每个会话 head 与其可达段、以及 **commit log**（时间线）都算"活"。
- 其余对象（输掉 CAS 的孤儿段、发布失败的孤儿分片、被新 generation 取代的旧 catalog）
  在**宽限期**之后才回收。
- `--grace-ms` 默认 1 小时，保护"还在上传的机器"和"刚钉住某个 catalog 的读者"。
- **默认 dry run**：只报告 `scanned/live/collectable/kept_recent/deleted`。`--apply` 才真删。

```bash
qm gc --json
qm gc --apply --grace-ms 3600000
```

## 观测保留

原始观测会一直躺在会话链里。要收敛它们：

```bash
qm compact-session --session <id> --keep-ms 2592000000 --keep-last 50            # dry run
qm compact-session --session <id> --keep-ms 2592000000 --keep-last 50 --apply
```

- 两条规则取**并集**：`keep_ms`（默认 30 天）+ `keep_last`（默认 50 条，防时钟错误清空）。
- 实现是**重写链**（新段 `prev: None` + CAS 换 head），旧段变成不可达 → 交给 `qm gc` 回收。
  所以流程是：先 `compact-session --apply`，再择机 `qm gc --apply`。
- 默认 dry run，只报告会保留/退休多少条。

## 压缩（compaction）

从**权威页面**重建索引：被 supersede 的旧版本和被删除的页面由构造天然消失，不靠过滤。

```bash
qm compact          # 拿不到租约会返回 "another machine holds the compaction lease"
```

压缩完成时只新增一个分片并 CAS 换 `head.json`；旧分片仍被更早的 catalog 引用，
直到 GC 把它们回收（读者通常只钉住 catalog 几秒，所以默认宽限期足够）。

## 发布与缓存

- `qm publish` 只发布**本机上次发布之后**变化的页面（watermark 存在 `QM_CACHE_DIR`）。
  清掉缓存 = 下次全量重发一次，代价是冗余，不是错误。
- 检索把分片材料化到 `QM_CACHE_DIR`（默认 `$TMPDIR/qm-cache-<writer>`），
  按分片内容哈希命名；命中缓存就不再访问桶。缓存可以随时删除。
- 多机共用一台机器时**不要**让不同 `QM_WRITER` 共用一个缓存目录（watermark 会互相覆盖）。

## 失败模式

| 失败 | 影响 | 处理 |
|---|---|---|
| 桶不可达 | 写入失败 / hook 落 spool | 恢复后 `qm hook-drain` |
| catalog head CAS 冲突 | 发布重试，最坏留下孤儿分片 | 由 GC 回收 |
| 压缩者中途消失 | 租约到期后由别的机器接管 | 无需人工干预；过期租约可被抢 |
| commit log 少一条（提交与日志之间崩溃） | 历史少一个时间戳 | 权威状态不受影响；无需修复 |
| 索引损坏或格式升级 | 检索异常 | 从权威页面重建：`qm compact`（或清空 index 前缀后重建） |
| 凭据泄露 | 全桶可读写（若用每机全桶 token） | 轮换 token；考虑前置 Worker 网关做前缀级鉴权（见 design.md §9） |

## 成本与容量

- R2 出口免费，读分片不产生出口费；写入按 Class A 操作计费。
- 一次 `qm capture` = 1 段 + 1 个 head CAS；一次 `qm publish` = 分片文件数 + 1 个 catalog + 1 次 CAS。
- 分片随发布次数增长，压缩把 N 个分片并回 1 个；检索成本 ≈ 分片数 × 流数（3）。
- 桶内对象布局见 `design.md` §4；用 `qm status --json` 看当前规模。

## 观测

目前没有内置指标端点（无服务可暴露）。日常判断依据：

```bash
qm status --json        # pages / tombstones / splits / sessions
qm gc --json            # 孤儿对象规模（collectable 长期增长说明有机器发布失败）
qm log --limit 50       # 最近提交，按机器看谁在写
```
