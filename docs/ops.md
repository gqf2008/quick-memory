# 运维手册

quick-memory 没有常驻服务：运维对象是**一个桶**和**每台机器上的本地缓存**。
所有状态都在桶里，因此"备份"就是桶本身的持久性与版本控制。

## 常规节奏

| 频率 | 动作 | 命令 |
|---|---|---|
| 每次会话结束 / 定时 | 编译并发布 | `qm maintain --json`；也可手动 `qm consolidate --session <id>` 后 `qm publish` |
| 每天/每周 | 压缩索引（可选） | `qm compact` |
| 每周 | 回收不可达对象（先 dry-run） | `qm gc` → 确认后 `qm gc --apply` |
| 随时 | 查看状态 | `qm status --json` |

这些都是**可放弃作业**：拿不到租约就跳过，没有它们系统照常工作，只是分片或孤儿对象变多。

## 一次性维护闭环

`qm maintain` 把本地 spool drain、当前 scope 全部会话的 consolidate、以及需要时的
publish 收成一次可定时的幂等命令。它适合 cron、launchd 或 CI 调用，**不是常驻服务**；
hook 仍保持原有的 fire-and-forget 契约，绝不在 agent 生命周期钩子里等待维护完成。

```bash
qm maintain --json
qm maintain --compiler rules --drain-limit 100   # 默认 auto；每次最多处理 100 条当前 scope 条目
```

- 先 drain 当前 scope 的 spool；`--drain-limit` 只约束当前 scope，其他 scope 的条目既不
  消耗预算也不被删除。
- 再逐个编译当前 scope 的会话。单个会话失败会记录错误后继续处理其余会话，最后非零退出。
- consolidate 后调用同一条 typed publish 路径；没有新页面时 publish 是 no-op。
- 会话租约被其他机器占用时计入 `skipped_locked`，不算失败。
- 空 session（没有可编译观测）计入 `skipped_empty`，不计入 `skipped_locked`。
- 第二次运行不会新增 manifest `seq`、重复页面版本或重复分片；未变化会话计入
  `already_up_to_date`。

`--json` 的稳定字段包括 `drained / spool_kept / sessions / consolidated /
already_up_to_date / skipped_locked / skipped_empty / failed / failures / published /
published_pages / publish_already_present / publish_error / manifest_seq /
generation / splits`。`spool_kept` 是所有未成功处理的剩余条目总数（其他 scope、当前
scope 超过 limit 的条目和坏条目）；`published` 只表示本次是否真的新增了 split。
`publish_error` 非空时 `published` 必须为 `false`，即使失败发生在 catalog 提交前的分片上传阶段。
即使存在会话失败或 publish 失败，完整报告仍写入 stdout，
进程以非零状态退出，便于调度器报警。

## 回收（GC）

- 可达性分析决定生死：manifest、每条 live page 的**整条 supersession 链**、tombstone 删掉的那一版、
  当前 catalog 及其引用的分片、每个会话 head 与其可达段、以及 **commit log**（时间线）都算"活"。
  分片形态下，提交点命名的**每一个分片**、以及它 `predecessor` 指向的 archive 也算"活"
  （漏掉前者会回收掉 manifest 还在指的对象；漏掉后者会让回滚失去备份）。这一条**只在该提交点
  仍是分片形态时成立**：回滚成整份之后它们不再被任何东西引用，会在宽限期之后被回收（见「怎么回滚」）。
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

`qm migrate-manifest` 是**形态**迁移（整份 <-> 分片），不是数据搬迁：见「manifest 的存储形态」。
跨桶搬家的边界同样要注意：**换桶请配独立 `QM_CACHE_DIR`**（见「发布与缓存」），
并且新桶要从 `--to 2` 还是 `--to 1` 开始，由那边第一次提交的开关决定。

## 验证状态

代码已在**真实 S3-compatible MinIO HTTP 后端**上做 current-head 复验（2026-09-16，`main @ c9006a0`）：
`cas-conformance` 7/7、`manifest-probe --machines 3 --writes 5`、`search-probe project`、
`session-probe`、跨进程 `digest-probe seed/read`、向量 publish/query 协议闭环、`qm maintain` 两次幂等运行，
以及 `qm verify --strict`。细节与原始数字见 `design.md` §10.5。向量行的 embedding 是本地 deterministic
HTTP stub，不是真 provider；MinIO 也不是 R2，不能把这组结果扩展成 R2 验证。

Quickwit 官方 0.9.0 容器产出的真分片读取验证已完成（见 `design.md` §6.4），这不是真 R2 证据。
仍未验证的是 **R2 特有行为**（PUT 返回 version、GET 不返回）、**ACL/签名/区域/一致性/配额/延迟/错误 XML 变体**，
以及**在真 R2 上跑一遍向量闭环与跨进程 digest**（`digest-probe seed` 然后 `read`；`--workspace` / `--project`
必填，两者必须传同一组唯一值）；此外，**向量链的真 provider 仍未验证**（当前 vector 证据使用本地
deterministic HTTP stub，不是真 provider）。有 R2 凭据时先跑 `cargo run -p qm-probe --bin cas-conformance`，
它专门盯这组后端差异；向量链另有 `search-probe vector-publish` / `vector-query` 的真桶验收点，digest 另有
`digest-probe` 的跨进程闭环。

没有凭据时能走多远：`qm-probe` 里有一个进程内的最小 S3 stub（`s3-stub` 二进制 / `qm_probe::s3_stub`），
探针可以**真打 socket** 走完条件写、`ListObjectsV2` 分页、跨进程检索、跨进程 digest，以及“独立进程 A 经真实 HTTP
embedding stub 发布向量、独立进程 B 只靠 `vector` 流召回”的闭环（见 `design.md` §5.1 / §6.20）。
但它只是「协议层」，不是真桶，有两条读法要记住：

- 它**不建模条件读**：带 `If-Match`/`If-None-Match` 的 `GET`/`HEAD` 一律回 `501 NotImplemented`。
  真实 S3/R2 命中时本该是 `304`（带 ETag），这层语义**没有建模，也没有被验证**——`501` 是「我们还没做」，
  不要当成后端行为。
- 它**不校验签名**：请求里的 `AWS4-HMAC-SHA256` 只被记录、不被验算，签名对不对只有真后端能拒。
- 向量闭环的“真 HTTP”指 embedding stub 与 S3 stub 都走本地 socket；它仍没有证明 R2 的
  latency/quota/一致性，也没有把真 provider 的网络故障形态纳入。

跨进程 digest 这条链的具体形状：`digest-probe` 的 `seed` 与 `read` 是两个独立进程，第二个只有桶坐标，
必须自己把 pages（含删除）/ sessions / handoffs 三段从对象里重组出来。交接棒那一段走完了**全部三个阶段**
（开 → 认领 → 收尾），而且收尾时间落在断言的窗口之内、开与认领都在窗口之外：所以「窗口与排序取
created/claimed/finished 里最新的那个」是被断言的，不是靠一根只开不认领的棒凑出来的。`read` 同时报告
manifest 仍认账的 live pages，所以「manifest 已不再返回那条 path、digest 仍然报这条删除」也是被断言的
事实，而不是对代码的转述（见 `design.md` §6.22、`crates/qm-probe/tests/digest_probe_stub.rs`）。
`seed` 的 scope 参数是必填项，会进入键布局；写入前的 listing 预检会拒绝非空目标 scope，但它是
防复用的 best-effort 检查，不是并发锁。`read` 只读。当前 probe 不自动清理，
所以真桶应使用专用 bucket/prefix，并为每次运行显式传唯一 scope。`QM_S3_PREFIX` 只被
`cas-conformance` 与 `search-probe build/query` 采用，不是所有 probe 的全局隔离。
这些 stub 路径本身仍只证明协议层；其中 digest 的同一命令形状也已在 MinIO current-head 跑过（§10.5），
但**真 R2 仍未验证**。

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
| `qm maintain` 中单个会话链损坏 | 其他会话仍继续；维护报告 `failed` 并非零退出 | 从 `failures[].session` 定位并修复该会话；其他会话通常已发布 |
| `qm maintain` 的 publish 失败 | 会话页可能已提交但未进入 catalog；报告 `publish_error` 并非零退出 | 修复桶/CAS 后重跑；已提交页面不会被重复版本覆盖 |
| catalog head CAS 冲突 | 发布重试，最坏留下孤儿分片 | 由 GC 回收 |
| 压缩者中途消失 | 租约到期后由别的机器接管 | 无需人工干预；过期租约可被抢 |
| commit log 少一条（提交与日志之间崩溃） | 历史少一个时间戳 | 权威状态不受影响；无需修复 |
| 索引损坏或格式升级 | 检索异常 | 从权威页面重建：`qm compact`（或清空 index 前缀后重建） |
| 凭据泄露 | 全桶可读写（若用每机全桶 token） | 轮换 token；考虑前置 Worker 网关做前缀级鉴权（见 design.md §9） |

## 成本与容量

- R2 出口免费，读分片不产生出口费；写入按 Class A 操作计费。
- 一次 `qm capture` = 1 段 + 1 个 head CAS；一次 `qm publish` = 分片文件数 + 1 个 catalog + 1 次 CAS。
- 一次 `qm maintain` 的对象访问随会话数线性增长：每个会话至少读 head/链（变化时再加一次
  page commit），最后做一次 publish 扫描；没有未发布页面时不写分片或 catalog。它是单次
  CLI 过程，不会为定时执行保留后台任务或连接池。
- 分片随发布次数增长，压缩把 N 个分片并回 1 个；检索成本 ≈ 分片数 × 流数（3）。
- 一次 `qm digest` 的**三段都读整份对象**，读的对象数各自随对象数**线性**增长，并发上限统一是
  **16**，所以每段延迟约 `⌈N/16⌉ × RTT`，而不是 `N × RTT`：
  - **pages**：整条 commit log（N 条提交 = N 次对象读）。`qm log` / `qm history` /
    `qm read-page --as-of`（`memory_log` / `memory_history` / `memory_read_page`）走的是同一段；
  - **sessions**：每个会话一个 head（N 个会话 = N 次对象读）；
  - **handoffs**：每个交接棒一个对象（N 个交接棒 = N 次对象读）。`qm handoff list`
    （`memory_handoff_list`）走的是同一段，而且没有窗口，列多少就读多少；
  这是**有界并发**，不是 O(1)：`--limit` 只封顶**返回**几条，不封顶**读取**几条，窗口里没有
  变化时也要付这个代价。封顶的到底是哪一头，逐命令看：

  - `qm digest --limit k`（`memory_digest`）：先把窗口里的对象读完，再**每段**截断到 k
    （`read_commit_log(..., usize::MAX)` 之后 `truncate`）；
  - `qm log --limit k`（`memory_log`）：`k` 直接交给 `read_commit_log`，而它的契约是
    **列全 prefix、读完全部对象、最后才 `truncate(k)`**（见该函数自己的文档注释）。所以
    listing 与对象读都是全量，封顶的只有返回条数——**`k` 小不代表读得少**，这是最容易读错的
    一条；
  - `qm history`（`memory_history`）与 `qm read-page --as-of`（`memory_read_page`）**没有**
    `--limit`：读的就是整条 commit log；
  - `qm recent --limit k`（`memory_recent`）：只读一份 manifest 再截断到 k，读代价随 manifest
    大小走，与 k 无关。

  代价换来的是「刚启动的机器和有缓存的机器看到同一个答案」。
  同一个上限也覆盖 `ProjectStore::read_wal`（`cargo run -p qm-probe --bin manifest-probe`
  收尾时读整份 WAL）：每个 WAL 记录一个对象，记录数是**不同的 page id 数**——key 是内容寻址的
  `wal/<page_id>.json`，同样的内容重提交落同一个 key。它与上面三段**共用同一处实现**，所以没有
  第二个数字要记。另外三点值得写下来，因为它们容易被想反：

  - **它返回的是「桶里有的」，不是「已提交的」**。WAL 记录写在 manifest CAS **之前**，重试时若
    前驱变了就会换一个 page id（`derive_page_id` 把 `supersedes` 算进哈希），于是**输掉那次 CAS
    的对象留在桶里**、不被 manifest 引用，而 `read_wal` 照样返回它。所以「已提交」只由 manifest
    决定；`read_wal` 的条数可能**多于**已提交版本数，多的就是这类从未赢过的尝试。要清理它们得靠
    `qm gc`，不是靠读。
  - **它不做 key 形状过滤**（commit log 与 handoffs 只看 `.json`，sessions 只看 `/head.json`）：
    `wal/` 前缀下每个对象都是一条记录，按形状过滤等于给自己开一个少读一条的口子，所以它读列出来的
    每一个 key。
  - **缺失是硬失败，不是跳过**（这点与 commit log、handoffs 同口径，只有 sessions 段把缺失当
    竞争跳过）。提交路径从不删 WAL 对象——输掉 CAS 只**多**一个对象，没有任何路径原地改写 key
    ——所以「列了又没了」只可能来自写路径之外：`qm gc --apply` 回收（它也是上面那些无引用对象的
    最终回收者，过了 grace 才动手），或探针收尾时的整段删除。与这类操作并发的读拿到报错而不是一份
    短答案，因为静默跳过会把「桶在我眼皮底下被回收了」变成一份看着完整、实际缺条的结果。
- 上面这些读**任一失败就整次失败**，不会返回部分结果；报出的是**listing 序里最小的那个 key**
  的失败，而不是先失败的那条，所以同一个桶坏掉几条对象时，每次报错都说同一个 key；`read_wal`
  复用同一个助手，错误身份同样由 listing 序而不是完成先后决定。
  唯独**会话被删**是容忍的：head 在 listing 之后被删除是快照读的正常竞争，digest 跳过它而不是
  整次失败。交接棒对象被删仍是失败——它从来不是「列了又没了也照样算」的那种读。
- 桶内对象布局见 `design.md` §4；用 `qm status --json` 看当前规模。

## manifest 的规模上限

一个 project 的提交点是**单个对象** `manifest.json`：它装下该 scope 全部 path 的当前版本，
所以**每次提交都要重写整个对象**，规模随 project 增长而不是随本次提交增长。这个上限以前只是
`design.md` 里的一句提醒，现在是实测数字加一条会拒绝提交的守卫。

### 实测

    cargo test -p qm-store --lib manifest_size_is_linear_in_paths -- --nocapture

形状（`manifest_with_paths` 的夹具）：path 31 B、title 40 B（含 CJK）、`page_id` 64 位 hex、
`writer_id` 5 B、`created_at_ms` 13 位、`supersedes: null`。

| path 数 | manifest 字节 | 边际 B/path | 平均 B/path |
|---:|---:|---:|---:|
| 100 | 24 217 | — | 242 |
| 1 000 | 242 019 | 242 | 242 |
| 10 000 | 2 429 021 | 243 | 242 |
| 100 000 | 24 389 023 | 244 | 243 |
| 10 000（每条都带 `supersedes`） | 3 049 021 | 305 | 304 |

边际成本随 `seq` 位数每涨一个数量级只加 1 B（242 → 243 → 244），所以**这是这个形状的数字，
不是通用常数**：title 每多一字节就多一字节，`supersedes` 从 `null` 变成 64 位 hex 是 +62 B/path。

改写行的 **305** 是**同一个形状**在 9 000 → 10 000 之间的边际（和上面几行一样是跨尺度的边际），
复跑命令会把它打印在 `marginal_b_per_path` 那一列，并由
`manifest_size_is_linear_in_paths` 用字面量断言钉住。它与"同样的 path 数下和新鲜行的差"**不是**
同一个数——后者是 `supersedes` 的单价（62 B/path，由 `manifest_bytes_are_accounted_for_field_by_field`
钉住）；一张表里只有一个列名，这两者混起来会让数字不可复跑。

单条构成（`manifest_bytes_are_accounted_for_field_by_field` 逐字段实测；entry JSON 各字段相加
**恰好**等于实测的 205 B，多于或少于都会让该测试红）：

| 构成 | 字节 |
|---|---:|
| `"<path>"`（31 + 2 引号） | 33 |
| 键与值的冒号 | 1 |
| JSON 骨架（字段名、引号、括号、冒号） | 78 |
| `page_id`（64 hex） | 64 |
| `title` | 40 |
| `created_at_ms`（13 位） | 13 |
| `writer_id` | 5 |
| `seq`（本行按 1 位算） | 1 |
| `supersedes` | 4（`null`；`Some(<64 hex>)` 是 66） |
| 分隔逗号 | 1 |
| **合计** | **240**（seq 为 1 位时；1 000–10 000 条量级的实测边际是 242–243，差别来自 seq 位数） |

### 两个口径

| 口径 | 字节 | 能装多少 path |
|---|---:|---:|
| 提交点上限 `qm_store::MANIFEST_MAX_BYTES` | 1 048 576（1 MiB） | **4 319**（每条都重写过则 **3 441**） |
| R2/S3 单对象上限 | 5 GiB | ~2.2×10⁷（重写 ~1.8×10⁷） |

第二行是**实测外推，不是承诺**：没有真的构建过 5 GiB 的 manifest，而且单对象上限根本不是这里的
约束——约束是"每次提交重写整个对象"，所以提交点上限按 1 MiB 定，而不是按桶能收多大定。

### 超限会发生什么

提交被**拒绝**：不截断，也不静默继续。报文同时给出条数与阈值——4 320 条、1 048 779 B 时是：

    refusing to commit: manifest for acme/ai-memory holds 4320 paths and encodes to 1048779 bytes,
    over the 1048576-byte limit of the single-object commit point

拒绝发生在**写出任何对象之前**，所以被拒的提交是干净的空操作：不留孤立 page 对象，也不留 WAL
记录（`a_manifest_at_the_ceiling_commits_and_one_path_past_it_is_refused` 用桶内对象列表钉住这一点）。

**删除路径不受这条上限约束**：删除是唯一**可能**缩减 manifest 的操作——删掉一个已经写过的 path
会去掉它的 entry——卡住它会让一个已经超限的 scope 再也修不回来。但"可能"不是"一定"：删一个
**从未写入过**的 path 只会**新增**一条 tombstone，manifest 反而变大。而这条路径没有守卫，
所以这种增长是**无声的**：每条 delete 最多加一条 tombstone，但没有任何东西限制能加多少条。
这是刻意选的代价——守卫见 `encode_manifest`，豁免见 `encode_manifest_for_delete`，下一个走守卫的
提交才会把它拒掉。

### 离上限还有多远

现在没有直接报 manifest 字节数的命令；`qm status --json` 给出 `pages` 与 `tombstones`，
把两者之和乘以上表的 B/path 即可估算。**达到上限的出路**有两条：把一个 project 的内容拆成两个
（scope 是 project 级的，拆开立即可用），或者切到分片形态（下一节），把这条上限变成**每片**的。

## manifest 的存储形态（format 1 / format 2）

一个 scope 的提交点有两种形态，由 `QM_MANIFEST_FORMAT` 决定**写**哪一种，默认 `1`：

| | format 1（默认） | format 2（opt-in） |
|---|---|---|
| 提交点 | 一个对象 `manifest.json`，装下全部 path | `manifest.json` 只装**布局**（一组指向分片的引用） |
| 每次提交移动多少 | 整个 project | 一条 path 所在的那**一片** |
| `MANIFEST_MAX_BYTES`（1 MiB）约束谁 | 整份对象 | **单片** |
| `status` 里的 `manifest_format` | `1`，`manifest_shards: 0` | `2`，`manifest_shards: N` |

**读不看这个开关**：读路径按对象自己的形态判别分派，所以一台从不设置它的机器照样能正确读一个
format 2 的 scope。`qm status --json` 报的是**存储形态**（不是开关的值），迁移前后要看它。

### 规模实测点

`format 2` 的上限口径不是从"每条 path 约 243 B"直接乘出来的，而是有一个**跑过**的点：

    cargo test -p qm-store --lib sharded_manifest_scale_probe -- --nocapture

夹具就是上面单对象那节用的那个形状（path 31 B、title 40 B、`page_id` 64 hex），
这一次把它按 format 2 分片：

| path 数 | 非空分片 | 每片 path（最小/最大） | 最大单片字节 | 根指针字节 | 同样内容整份要多少字节 |
|---:|---:|---:|---:|---:|---:|
| 1 000 | 251 | 1 / 10 | 2 526 | 58 014 | 242 019 |
| 10 000 | 256 | 25 / 61 | 14 928 | 59 424 | 2 429 021 |
| 100 000 | 256 | 335 / 442 | 107 911 | 59 681 | 24 389 023 |

三件事可以从这张表直接读出来：

- **分片是均匀的**：100 000 条 path 落在 256 片里，最挤的一片 442 条、最松的 335 条，
  均值 391。哈希分片不会把 path 堆到某一片上去（注意 1 000 条时只有 251 片非空——
  **只物化非空分片**，不是一开始就写 256 个对象）。
- **根指针不随 path 增长**：path 从 10 000 到 100 000（十倍）根指针只从 59 424 涨到
  59 681 字节（+257），因为它的体积由**分片引用**和 `path_count`/`seq` 的位数决定，
  与分片里装了多少 path 无关。
- **最大单片离上限还很远**：100 000 条 path 时最大的那片是 107 911 B，
  `MANIFEST_MAX_BYTES / 107911 == 9`，即还不到单片上限的九分之一。

**单条 path 的成本**（同一次运行里的第二段）：在这个 100 000 条 path 的 scope 上
——按 key 集合断言，不是比条数：

| 操作 | 读哪些对象 | 写哪些对象 |
|---|---|---|
| `commit_page` | 根指针 + 命中那一片（**2** 个） | 页面版本 + WAL + **一片** + 根指针 + 提交记录（5 个） |
| `read_page` | 根指针 + 命中那一片 + 指向的版本（**3** 个） | — |

和 60 条 path 时的数字**一样**：分片 bounds 的是"一次提交搬多少"，不是"project 有多大"。

**单 scope 能装多少 path：仍是外推，但现在是"从这个实测点外推"。** 算法是：
100 000 条 path 时最大的一片用了上限的 1/9.7，所以照这个分布，一片能装约
`442 × 9.7 ≈ 4 300` 条 path，256 片约 **1.1×10⁶** —— 与旧口径数字巧合地接近，
但依据完全不同：旧的是"243 B/path × 单片 1 MiB"，现在是**实测的每片字节**乘出来的。
仍然**没有实测到 1.1×10⁶**，也没有分层结构；这个数字依赖两个假设：

1. 分片分布保持这个均匀度（哈希分片对均匀 path 集合成立，对极端对抗性的 path 集合未验证）；
2. 每片每条 path 的边际字节不变（实测 242–244 B/path，随 `seq` 位数缓慢变化）。

要真正确认上限，需要在真桶上造到那个量级；本仓库没有做过这件事。

### 持续写入下的迁移（压测点）

    cargo test -p qm-store --lib sustained_contention -- --nocapture

8 个写者各提交 50 轮（`RetryPolicy { max_attempts: 500, base_delay: 0 }`），
转换在跑到一半时由独立 actor 触发，写者不停。以下是这一轮的**实测数字**
（`InMemory`、单线程确定性调度，所以每次跑都一样）：

| 指标 | 实测 |
|---|---:|
| 成功提交 | 400 |
| 提交点被写入的次数（每次尝试一次） | 1 581 |
| 其中被 CAS 拒绝 | 1 180 |
| 平均每次成功提交花的写入次数 | 3.95 |
| 单次提交最多尝试次数 | 295 |
| 转换自身的尝试次数 | 5 |
| 转换提交时的 `seq` | 400 |

不变量每条都是精确断言：每个返回过成功的提交都能读回、body 与当时一致；`seq` 恰好是
`1..=400` 各一次（无空洞无重复）；提交点命名的 `path → seq` 与写者被告知赢得的集合逐一相等；
`shard.path_count` 之和等于 path 数；`qm verify` ok。

**结论（限于上述 `InMemory` + 单线程调度）：持续写入会把迁移饿住。** 转换的每一次尝试都要先把整份
状态重新写成不可变分片（本例 256 片，各一次对象写），再拿这些分片去 CAS 提交点；在这个 schedule 下，
写者在那个窗口里总会先落一次提交，于是转换在写入停下之前赢不了 CAS。本次转换在 200 次提交时开始，
到第 400 次提交之后才提交（重试 5 次）。它不是“慢一点”，而是**需要一个安静窗口**：运维上应当在
写入低谷执行 `qm migrate-manifest`，或者接受它自己一直重试到安静为止。

这条结论的适用范围要说清楚：数字来自 `InMemory` 上的单线程公平调度，转换的 256 次分片写在
真桶上是与写者**并行**推进的，竞争窗口与这里不同，真桶上的收敛时间**未验证**。

### 什么时候切

- 撞到「manifest 的规模上限」（上面那节）：4 319/3 441 条 path 量级，或者提交开始变得很重
  （每次提交都要重写整个对象，写放大随 project 线性增长）。
- 还没撞上限时**不要切**：两种形态的读语义相同，但分片多了一次对象读（根指针 + 1 片），
  迁移也是一次真实提交。

### 怎么迁移

```bash
qm status --json | grep manifest_format   # 先看现在是什么形态
qm migrate-manifest --json                # format 1 -> format 2
qm status --json | grep manifest_format   # 确认已经切了
```

- **幂等**：重复跑是 no-op（`already_there: true`），不写任何对象。
- **可中断/可续跑**：中断只会留下孤儿分片（可复用），提交点要么还是旧的、要么已经指向新的一代；
  再跑一次补齐。
- **可回滚**：`qm migrate-manifest --to 1`。
- **迁移不删除旧对象**：旧 body 被复制到 `manifest/archive/<hash>.json` 并记进根指针的
  `predecessor`。`qm verify` 会检查这个 archive 还在、且 hash 对得上。

### 怎么回滚

```bash
qm migrate-manifest --to 1 --json
```

回滚是**再写一次整份 manifest**：把当前分片 reassemble 成整份形态再 CAS，**不是**把指针换回
archive。差的这一点是有意的：从 archive 恢复会静默丢掉迁移之后的所有提交。

回滚本身**不删除**任何东西（archive 与分片都还在桶里），但它把提交点变回整份形态，于是
**live 的条件不再成立**：分片与 archive 不再被任何东西引用，`qm gc` 会在宽限期之后把它们回收。
所以有两条实操结论：

- 回滚后**马上**再切回分片是安全的：分片按当前状态重新算出**同样的键**（内容寻址，不会堆出新的
  分片对象）。archive 则按**它当时归档的那份 body** 落键：回滚后的状态若与迁移前不同（迁移之后
  有过提交），第二次 archive 就是另一个对象；**若两者逐字节相同（迁移与回滚之间没有写入），它会
  复用同一个键，桶里仍然只有一个 archive**。上一轮那份 archive 在回滚之后已经无人引用；
- 想长期保留旧的 archive 做字节级考古，就别在回滚后的 scope 上跑 `gc --apply`——回收之前
  `qm verify` 都能读到它，之后就不能了。

回滚会被整份形态的上限拒绝——如果 scope 已经长到超过 1 MiB，它只能留在分片形态（或者拆 project）。

### 切形态时会发生什么

两个方向的形态错配都**被拒绝**，报文指向 `qm migrate-manifest`：

- 用 format 2 写一个已有内容的 format 1 scope：拒绝（新根指针只会命名这一次写的片，
  已有 path 会全部消失）。
- 用 format 1 写一个 format 2 scope：拒绝（会静默把 scope 的形态翻回去）。**只能读**。

拒绝发生在写出任何对象之前，所以被拒的写是干净的空操作。一台机器的默认设置不会让 scope 换形态：
换形态只有 `qm migrate-manifest` 一条路。

### 仍然没做到的

- 分片数固定 256、单片 1 MiB ⇒ 单 scope 的 path 上限约 **1.1×10⁶**：这是从上面那个
  100 000 条 path 的**实测点**外推的（最大单片用了上限的 1/9.7），**没有实测到 1.1×10⁶**，
  也没有分层结构。再往上要分片分层或按 path 范围再切一层。
- `search` 的可见性校验按候选命中 path 读片（根指针 + 命中所在的那几片），但它**只对自己读到的片
  负责**：没被候选碰到的片即使损坏，搜索也不会报错，`qm verify` 才是全量检查那一层。
- 迁移的最后一次 CAS 会与并发写者互相冲突，靠重试收敛，没有"迁移中"状态位。这条**有测试**：
  一个写者在迁移的读与 CAS 之间落地时，迁移重读重算（`attempts == 2`），赢的那次提交仍在最终
  状态里、`seq` 无空洞；反方向的时序（写在 CAS 之后落地）按形态分别落进新形态或被响亮拒绝。
  **已有压测点，但边界仍窄**：上述 8 写者 × 50 轮持续竞争已量到 400 次成功提交、1 581 次提交点写入、
  1 180 次 CAS 拒绝和迁移 5 次尝试；它仍只跑在 `InMemory` + 单线程确定性调度上，真桶多核 RTT、
  不同写者数、多 scope 与持续 supersession 未覆盖。
- 该持续竞争压测没有在真桶上验证：这套断言全部跑在 `InMemory` 上；MinIO current-head 复验未覆盖它。

详见 `design.md` §6.21。

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
- `--limit 0` 不会让命令少读一个对象：`digest` 与 `log` 都是**先读完再截断**，
  窗口由 `--since-ms` / `--hours` 决定（逐命令的封顶口径见上一节「成本与容量」）。
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
