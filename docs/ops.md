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

## 嵌入向量与换模型

向量是**派生的**，跟着分片走，而且分片会记录**是谁生成了这些向量**：
每个用 provider 建出的分片都带一份 `embedding-identity.json`，内容是
`{provider, model, dim}`。它写在分片目录里，因此和向量一起上传、一起哈希、一起被读者
材料化——读者不可能只拿到向量而拿不到它的来源。读路径据此比对：

- **`(provider, model)` 不一致 → 硬失败并点名两边身份**。这是宽度看不出的一半：
  换模型但宽度不变（很常见）时，两种模型的坐标本无关，余弦照样算得出 -1..1 的数字，
  排序看起来完全正常却毫无意义。报错形如
  `the split <prefix> was indexed with provider 'openai-compatible', model 'model-a', dim 768, but this machine would query with provider 'openai-compatible', model 'model-b', dim 768`。
- **宽度不一致 → 仍然由余弦的宽度守卫硬失败**，文案是它自己那句更窄的
  `cannot compare embeddings of different widths: query has 8, stored value has 4`。
  两者都是 fail-closed，只是各自报最具体的原因。
- **旧分片（没有 `embedding-identity.json`）不判定**：向量出现之前发布的分片没有这份记录，
  "没记录"绝不等于"不是同一个模型"，否则升级会直接把既有桶搜挂。未配置 provider 时这一路
  根本不跑，比对也就无从发生。

- 症状（宽度）：换了不同维度的 `QM_EMBEDDING_MODEL`（或改了 `QM_EMBEDDING_DIM`）却没有重新嵌入，
  **每一次搜索都会 fail-closed 报错**（上面那句宽度文案）。这是刻意的：拿两种模型的距离做比较，
  会给出看起来正常、实则无意义的排序。
- 症状（同宽度换模型）：报错改成点名两边 `provider/model/dim`，同样每次搜索都失败。
- **`qm publish` 修不了这个问题**：发布是**追加**（`publish_split` → `catalog.push_split`），
  它只把新世代的向量作为**新分片**加进 catalog，旧世代分片仍留在目录里，
  于是"8 维查询 vs 4 维存量"照样硬失败；同宽度换模型同理，旧分片依旧带着旧身份留在目录里。
  （watermark 只记 `{manifest_seq, embedded}`，没有 model/dim，换模型时不会归零。）
  `publish` 只在**首次启用 provider**（既有分片根本没有向量列）时是无害且有效的。
- **换模型/换维度后的必需动作是 `qm compact`**：它从权威页面整库重建，
  并用 `replace_catalog` **整表替换**目录，旧世代分片被换出，之后搜索恢复
  （实测 `splits 2 -> 1`，同一次搜索从报错变为正常返回）。

建议顺序：

1. 设好新的 `QM_EMBEDDING_MODEL` 与 `QM_EMBEDDING_DIM`（二者必须一致）。
2. 应急绕过：查询加 `--no-vector`（MCP: `memory_search { no_vector: true }`）维持可用，
   它只关掉向量流，正文/实体/链接不受影响。
3. **`qm compact`** —— 唯一的修复动作（从权威页面重嵌入并整表替换目录）。
4. `qm status` 确认 `splits` 已收敛（多次发布后可能 >1；compact 后应回到 1）。
5. 确认后才去掉 `--no-vector`。

多机注意：每台机器都要改自己的 `QM_EMBEDDING_*`，但 `qm compact` 是**全局**动作
（有租约保护，一台机器跑一次即可）。**仍带旧配置的机器一旦继续 `qm publish`，
会把旧世代向量再次追加进目录**，需要重新 compact。

`QM_EMBEDDING_DIM` 默认 1536（按 OpenAI `text-embedding-3-small` 取），
这只是一个**假设**；换服务商时必须显式设置，不要依赖默认值。

## 发布与缓存

- `qm publish` 只发布**本机上次发布之后**变化的页面。发布 watermark 存在
  `QM_CACHE_DIR`，并按 `(endpoint, bucket)` 分键；不同桶不会互相抑制发布。
- **升级或换桶后的第一次 `qm publish` 会做一次全量发布**：旧版 watermark 没有桶名，
  换桶后第一次也会从空 watermark 开始。这是安全退化——多写一个分片，
  代价是冗余，不会漏数据。
- **指向第二个桶请使用独立 cache dir**（例如每个桶一个 `QM_CACHE_DIR`）：
  这样既避免旧版二进制读错 watermark，也让缓存的清理和迁移边界清晰。
- 清掉缓存 = 下次全量重发一次，代价是冗余，不是错误。
- 检索把分片材料化到 `QM_CACHE_DIR`（默认 `$TMPDIR/qm-cache-<writer>`），
  按分片内容哈希命名；命中缓存就不再访问桶。缓存可以随时删除。
- 多机共用一台机器时，不同 `QM_WRITER` 的 watermark 本来就不会互相覆盖；仍建议按
  `(writer, bucket)` 拆开缓存目录，便于排查、清理和迁移。

## 备份与迁移

桶本身就是备份（对象不可变 + 版本控制）。除桶之外，还可以导出一份**人工可读**的副本：

```bash
qm export --to ./backup-$(date +%F)     # 页面 markdown + 会话 JSONL + _export.json
qm import --from ./backup-2026-09-15    # 迁到另一个桶/项目；内容相同的页面会跳过
```

导出不含历史链与 commit log —— 它给的是内容，不是考古现场。要完整的历史请依赖桶的持久性与版本控制。

## 验证状态

代码已在**真实 S3 实现**（本地 MinIO）上验证过完整链路：条件写契约、多机并发提交（含真实 CAS 冲突重试）、
多机检索与过期过滤、采集→编译→检索、跨机器读写与 handoff、export/import、`verify --strict`。
细节与原始数字见 `design.md` §10.5。

仍未验证的是 **R2 特有行为**（PUT 返回 version、GET 不返回）与 **Quickwit 二进制产出的真实分片**。
有 R2 凭据时先跑 `cargo run -p qm-probe --bin cas-conformance`，它专门盯这两类后端差异。

## 完整性自检

```bash
qm verify                 # 当前项目：后端契约 + manifest/链/catalog 自洽性
qm verify --global        # 整个 workspace
qm verify --global --strict   # 有问题就非零退出（可用于定时巡检）
```

只读、不修。报告会逐个列出问题（类型 + 对象键 + 细节），而不是只报第一个。

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
- `qm digest` / `qm log` / `qm history`（`memory_digest` / `memory_log`）读**整条 commit log**：
  读的对象数随提交数**线性**增长（N 条提交 = N 次对象读），并发上限 16，所以延迟约 `⌈N/16⌉ × RTT`，
  而不是 `N × RTT`。这是**有界并发**，不是 O(1)：`--limit` 只封顶返回几条，不封顶读几条，
  窗口里没有变化时也要付这个代价。代价换来的是「刚启动的机器和有缓存的机器看到同一个答案」。
- 桶内对象布局见 `design.md` §4；用 `qm status --json` 看当前规模。

## 空输出：`--limit 0` 的两种口径

"你要了零条"在两个输出面上**故意表现不同**，不必去读源码才能知道：

| 命令 | `--limit 0` 的人类可读输出 |
|---|---|
| `qm recent --limit 0`（列表类） | **空串**（零数据行） |
| `qm digest --limit 0`（报告类） | `no entries shown: --limit 0 caps every digest section` |

判据是输出的**形状**，不是输出的**内容**：

- **列表类**（`qm recent`）输出的是"每行一条记录"的结果集。
  要零条就是零**数据行**，不必先剥掉一句散文。
  它**不会**因此说 `no pages`——"你要了零条"和"这个项目是空的"是两回事。
  注意命令本身仍会输出一个换行（`main` 无条件 `println!`），所以
  `qm recent --limit 0 | wc -l` 得的是 **1**，不是 0；要拿可以整段消费的空集，
  用 `qm recent --limit 0 --json`（输出 `[]`）。
- **报告类**（`qm digest`）输出的是**固定三段**报告，段标题（`pages` / `sessions` /
  `handoffs`）本身就是信息。`--limit` 是**每段**的封顶，`--limit 0` 会把三段一起清空；
  此时若打 `no recent activity`，就把"你要了零条"谎报成"窗口里什么都没有"。
  所以它打的是截断说明，而不是空窗口说明。
- `--limit` 一致地**只封顶输出、不封顶读取**：窗口由 `--since-ms` / `--hours` 决定，
  `--limit 0` 不会让命令跳过该读的对象（读代价见上一节）。
- **JSON 面不受影响**：`qm recent --json` / `qm digest --json` 以及 MCP 的
  `memory_recent` / `memory_digest` 都原样返回空数组/空窗口，形状不变——
  上面这两句人类可读文案只在非 JSON 输出里出现。

## 观测

目前没有内置指标端点（无服务可暴露）。日常判断依据：

```bash
qm status --json        # pages / tombstones / splits / sessions
qm gc --json            # 孤儿对象规模（collectable 长期增长说明有机器发布失败）
qm log --limit 50       # 最近提交，按机器看谁在写
```
