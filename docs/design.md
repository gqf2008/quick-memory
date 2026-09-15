# quick-memory 设计 v1（多机形态）

> 状态：S0 进行中。本文记录已经过实证的结论与仍待验证的假设；未验证的部分显式标注。
> 借鉴来源：ai-memory（领域模型与"编译而非检索"）、moltlink（对象存储 CAS/WAL/租约/GC 协议）。

## 1. 目标

给多台机器上跑的 agent 提供**共享的长期记忆**：

- 每台机器都能写：hook 即发即忘、有界、不阻塞 agent。
- **每台机器都能独立搜全量**：不需要任何常驻服务、协调者或对等发现。
- 权威数据在对象存储；搜索索引是可重建的派生物。
- 机器会休眠、换网、消失——这是正常态，不是故障。

非目标（v1）：多租户 SaaS、向量检索、图形化前端、跨桶联邦。

## 2. 硬约束（决定了整个架构）

1. **没有常驻服务**：任何"单 indexer / 单 coordinator / 单 metastore writer"的假设都不成立。
2. **没有稳定拓扑**：不能靠 peer 列表或固定主机名发现彼此；只能靠桶里的对象。
3. **没有分布式事务**：唯一的原子原语是单对象的条件写（ETag CAS）。
4. **索引必须可重建**：索引损坏、格式升级、逻辑变更都能从权威数据重跑出来。
5. **搜索允许有界滞后**，但写入不能丢，且必须能证明"覆盖到哪个时间点"。

与 moltlink 的差别：moltlink 跑在服务器上，可以强约束一个 indexer、常驻 coordinator、单一 metastore writer；
quick-memory 跑在多机上，这三条全部不可用，因此**索引发布单元从"共享索引的一次 ingest"改成"每个写入方自己的分片"**。

## 3. 架构

```
机器 A / B / C（各自是完整单元）
  ├─ hooks / MCP 客户端
  ├─ 本机搜索：本地分片 + 已缓存远端分片（进程内 tantivy）
  └─ 本机写入
        │  1) 不可变对象（观测/页面版本/分片文件）
        │  2) CAS 提交点（manifest / catalog head）
        ▼
   S3 / R2（唯一共享状态）
     · 权威层：manifest、不可变页面版本、WAL 段、观测
     · 派生层：目录（catalog）、分片文件（split）
```

- **权威层**：唯一提交点是每个 project 的 `manifest.pb`；页面版本与观测都是不可变对象。
- **派生层**：分片按写入方命名空间发布；目录（catalog）用 CAS 追加；压缩是可放弃作业。
- **读**：拿目录 → 材料化分片到本地缓存 → 进程内打开查询 → 每条命中再过权威可见性校验。

## 4. 对象布局

```
v1/
└── ws/<ws>/proj/<proj>/
    ├── manifest.json                              # 页面提交点（CAS）
    ├── wal/<page_id>.json                         # 不可变提交记录
    ├── sessions/<session_id>/
    │   ├── head.json                              # 会话提交点（CAS，每会话独立）
    │   └── segments/<sha256>.json                 # 不可变观测段（prev 链）
    ├── pages/<path>/versions/<page_id>.md         # 不可变页面版本
    ├── observations/<obs_id>.json                 # 不可变原始观测
    ├── index/
    │   ├── head.json                              # CAS 指针 → 当前 catalog
    │   ├── catalog/<generation>.json              # 不可变目录版本
    │   └── splits/<writer_id>/<seq>/…             # 该写入方发布的分片文件
    └── leases/{compact,gc}.json                   # 可放弃作业的租约
v1/leases/<scope>.json
```

v1 的编码是 JSON（可读、可调试），扩展名即编码；换编码 = 换根前缀 `v2/`，不做就地格式迁移。

键由 `qm-core` 的 `KeyLayout` 统一派生，标识符与路径在构造时校验一次（拒绝空、`..`、绝对路径、反斜杠、控制字符）。

## 5. CAS 契约（已在代码中实现并测试）

- **ETag 是对象身份**：R2 的 `PUT` 可能返回只在 PUT 出现的 `x-amz-version-id`，后续 `GET`/`HEAD` 不返回；
  因此 CAS 一律以 ETag 为准，version 仅作无 ETag 后端的 fallback，且 `UpdateVersion.version` 永不自 PUT 回填。
- **缺 ETag fail closed**：拿不到 ETag 时拒绝条件写，绝不降级成无条件覆盖。
- **create-if-absent**：`If-None-Match: *`。
- **update-if-match**：`If-Match: <etag>`，对象被移动/删除即失败。

不同后端对"create 被拒"的报错形状不同（S3 412 → Precondition；`object_store` 的 InMemory → AlreadyExists），
CAS 层把两者都归一为"条件未满足"，但**不接受**泛化的后端错误——那会掩盖真实故障。

`qm-store::verify_conditional_writes` 是这套契约的守门检查，包含两个反向步骤（陈旧 ETag 必须被拒、
已消费的 ETag 必须被拒），并有阳性对照测试（一个"接受一切"的后端必须被该检查报错）。

## 6. 索引与检索

**分片发布（每个写入方独立）**

1. 本机把"自己写过的页面版本"构建成本地 tantivy 索引目录。
2. 上传到 `index/splits/<writer_id>/<seq>/…`（含 `.lock` 之类进程本地文件被跳过）。
3. 读 `head.json` → 载入它钉住的那版 catalog → 追加条目 → 写**内容寻址**的 `catalog/<sha256>.json` → `If-Match` 换 `head.json`。
4. CAS 冲突即重读重试；分片已上传但目录未更新时，该分片是不可见的孤儿，由 GC 回收。
5. 同一 `content_hash` 重复发布是 **no-op**（目录按内容去重），所以整条发布链路可安全重放。
6. **发布是增量的**：每台机器在本地记录自己发布到的 manifest `seq`（watermark），只发布 `seq` 之后变化的页面。
   没有变化时 `qm publish` 直接返回"nothing to publish"，不产生新分片。watermark 是**机器本地**的——
   因为每台机器只发布自己的分片；缓存被清掉最多导致一次冗余全量发布，不会错。

**读（任何一台机器）**

1. 读 `head.json` 得到被钉住的 catalog 与分片列表（读到 head 的那一刻就是一致快照，别的机器之后发布不影响本次查询）。
2. 把每个分片材料化到**跨命令复用**的本地缓存（目录按分片内容哈希命名，先写 staging 再 rename，
   留下完成标记才认为可用）。重复检索不再下载任何东西——测试里把桶里的分片对象全删掉，第二次检索仍能出结果。
3. **多路检索**：每个分片跑三条流——正文（title+body）BM25、**实体**（页面"声明"的标识符：wiki 链接目标、
   反引号 token、路径、`#tag`）、**链接**（页面指向的目标）。所有 (分片 × 流) 的结果在**读方**统一做 RRF 融合
   （`k=60`，按 `page_id` 去重，平票按 page id 稳定排序）。命中会带上它来自哪些流（`streams`），
   结果里给出 `streams_active` 与每路候选数，便于诊断召回。
   实体/链接是**词法抽取、无模型**，所以同一正文永远产出同样的字段、重建可复现。
4. **每条命中再过权威可见性校验**：只有 manifest 当前正好指向这个 `page_id` 才算数。
   于是旧分片可以永久保留过期副本，却永远不会答出过期内容——索引只提供候选，权威给结论。
5. 多路（正文 / entity / link）同样走 RRF，融合层必须在读方，不能放在某个服务里。

**read-your-writes**：本机刚写入但尚未发布分片的内容，由本地尾部（权威对象 + 本地索引）覆盖；
跨机可见性允许有界滞后，但必须在结果里给出 `covered_until`。

## 6.4 与 Quickwit 的格式兼容（S3 实证）

Quickwit 的 split 容器格式是**公开且可解析**的（上游 `docs/internals/split-format.md`）：

```
[ 所有文件首尾相接 ][ FileMetadata ][ metadata 长度 u64 LE ][ hotcache ][ hotcache 长度 u64 LE ]
FileMetadata = 8 字节版本头（magic = 403881646，version = 1）+ JSON {"files": {name: {start, end}}}
```

因此读方**不需要 Quickwit 集群**就能打开一个 `.split`：解析 footer、把文件切出来、用 tantivy 打开
（`qm-search::quickwit_split`）。`materialize` 会自动识别前缀下的 `.split` 并就地解包，于是
"Quickwit 构建器产出的分片"与"tantivy 构建器产出的目录"在读方是同一种东西。

已实现的边界：footer 的所有长度都按**不可信输入**做 checked 运算（损坏/恶意 split 必须报错，不得 panic 或越界），
元数据里的文件名拒绝路径穿越，越界偏移一律拒绝。

**兼容性证据（2026-09 核对源码）**：Quickwit v0.9.0 依赖的是 tantivy 的 fork（`quickwit-oss/tantivy`，
rev `057458b`）。核对结果是该 fork 的 `Cargo.toml` 版本为 **0.26.0**，且 `lib.rs` 里
`INDEX_FORMAT_VERSION = 7`、`INDEX_FORMAT_OLDEST_SUPPORTED_VERSION = 4`，与本仓库使用的 crates.io
**tantivy 0.26.2 完全相同**。也就是说读方与 Quickwit 属于同一索引格式代次，"解包后能不能打开"这一层的
风险已经很小区间。

仍待闭环的一步是**真实字节验证**：用 Quickwit v0.9.0 产出一个 split，让本仓的 tantivy 打开它。
GitHub 资产在本机被限速（73.9MB 只稳定拿到约 1MB），因此这一步需要外部条件（可用的 `QW_BIN`
或能换网的环境）。无论结果如何，两条路都已经铺好：兼容 → Quickwit 当构建器；不兼容 → 把工作区切到
同一个 fork（构建器与读方统一），或让 Quickwit 只做服务化加速层。

无论哪种结局，"任何一台机器独立搜全量"都不受影响：它由我们自己的 split 目录与上面的解包路径保证。

## 6.5 提交协议（S1 已实现）

一次页面写入：

1. 读 `manifest.json`（含版本号）。空 scope 视为 `seq=0` 的空 manifest。
2. 由 `(path, title, body, supersedes)` 派生 `page_id`（SHA-256），并写入不可变页面对象 `pages/<path>/versions/<page_id>.md`。
3. 写入不可变 WAL 记录 `wal/<page_id>.json`。
4. 把新页面写进 manifest，用 `If-Match`（或首次 `If-None-Match: *`）提交。
5. 冲突 → 重读 manifest、重算 `supersedes` 与 `page_id`、重试。冲突次数上限后返回 `Conflict`。

**WAL 记录只含内容，不含 `seq` 与时间戳**（S1 中被并发测试逼出来的修正）：`seq` 由赢得 CAS 的那次提交分配，
如果写进 WAL 内容，重试时同一 key 就会写出不同字节，触发"同键不同内容"的 fail-closed。
代价是 WAL 是**集合而非全序**：全局顺序看 manifest 的 `seq`，单页顺序看 `supersedes` 链。
将来若需要 as-of 全局时间线，应新增按写入方分区的有序日志，而不是把顺序塞回 WAL 内容。

其它已确定的选择：

- 页面对象与 WAL 记录都用内容寻址的 key：崩溃在"对象已上传、CAS 未提交"之间时，重试**复用**同一对象；
  如果同 key 已存在但内容不同，直接判 `Corrupt` 失败关闭（内容寻址被破坏比崩溃更值得警惕）。
- manifest 目前是单对象（含全部 path 的当前版本），因此有规模上限。按 path 前缀分片 manifest 是后续工作，
  不在 S1 范围；当前实现对该上限没有静默降级。
- **本机文件系统不是可用后端**：`object_store` 的 local 后端不支持条件写，探针会在预检阶段直接拒绝
  （这正是"本地文件不能当对象存储语义模型"的可执行证据）。

## 6.6 采集与编译（S4 已实现）

**采集**（`ingest_observations`）：

- `Observation` 只含内容（`session_id / actor / kind / text / created_at_ms`）＋由内容派生的 `observation_id`。
  捕获路径**不分配序号**，因此 hook 重试写的是同一个对象。
- **脱敏在唯一的入口做**：`Observation::sanitized()` 在 `ingest_observations` 内被强制调用，scrub 掉常见凭据形状
  （Bearer、`password=`、`sk-…`、`AKIA…`、URL 内嵌密码），把 `text` 限到 16 KiB、`actor`/`kind` 限长，
  并**重算 id** 使之与落库字节一致。调用方忘了脱敏也不会写进秘密。
- **会话是独立提交域**：`sessions/<sid>/head.json` 是 CAS 提交点，段是内容寻址的不可变对象并带 `prev` 链。
  两台机器捕获**不同**会话零竞争；捕获**同一**会话只在该会话 head 上竞争。可见性 = 从 head 沿 `prev` 可达，
  因此输掉 CAS 的段是孤儿，不会变成幽灵事件。
- 重放同一批（head 末尾就是这批）是 no-op；读侧再按 `observation_id` 去重，所以"重复段"只是浪费，不会出错。
- 会话发现靠 LIST `sessions/` 下的 `head.json`：没有 head 的会话不可见。

**编译**（`consolidate_session`）：

- 编译器**可插拔**：`CompilerChoice::Rules`（确定性渲染，永远是地板）或 `CompilerChoice::Llm`
  （OpenAI 兼容 `/chat/completions`，`QM_LLM_BASE_URL/API_KEY/MODEL`）。**LLM 失败一律回落到 rules 并标记
  `used_fallback`**——丢掉页面比丢掉文采严重得多。
- **幂等由链指纹决定，不由正文决定**：页面里嵌 `<!-- qm:compiled <sha256> -->`，指纹覆盖观测 id 序列。
  链没变就不再写版本——否则一个每次措辞都不同的 LLM 会产生无限版本。链变了才重编译。
- 链没变时**不追加版本**（`already_up_to_date`）。
- 用租约保护（`consolidate/<ws>/<proj>/<sid>`），拿不到就 `skipped`——和压缩一样是可放弃作业。
- 输出走的是普通页面提交路径（`commit_page`），因此自动获得 supersession 链、索引发布与权威过滤；
  将来接 LLM 重写时，也走同一条路径，不需要新机制。

## 6.7 agent 可用面（S5 进行中）

`qm` 是 agent 直接可用的入口，每条命令都是库调用的薄封装——CLI 不引入自己的协议，
所以 MCP 服务器将来暴露的能力与它完全一致。

```bash
qm capture --session sess-1 --kind tool_use --text "switched to tantivy splits"
qm consolidate --session sess-1        # 编译成 sessions/sess-1.md
qm publish                             # 把当前页面发布成一个分片
qm search "tantivy" --json
qm write-page --path notes/raft.md --body "leader election"
qm read-page  --path notes/raft.md
qm delete-page --path notes/raft.md
qm compact                             # 租约保护的全量重建
qm status                              # pages / tombstones / splits / sessions
qm sessions
```

- 作用域：`--workspace` / `--project` / `--writer`（或 `QM_*` 环境变量），默认 `default/default/machine`。
- 凭据：`QM_S3_*`（或 `R2_*`）必须在环境里；拿不到就**报错**，不猜、不降级成本地存储。
- 每条命令都重新读取权威状态：命令之间不缓存，这是多机共享的前提。
- `--json` 输出机器可读结果，便于被 agent 或脚本直接消费。
- 命令逻辑（而非仅参数解析）在内存桶上做了端到端测试：capture → consolidate → publish → search、
  页面的写/读/删、status/sessions、作用域解析。

**MCP 服务器**（`qm-mcp`，stdio）：

- 19 个 `memory_*` 工具：`capture / consolidate / search / write_page / read_page / delete_page /
  publish / compact / sessions / status / history / restore / log /
  handoff_open / handoff_list / handoff_claim / handoff_done / verify /
  compact_session`，沿用 ai-memory 的命名习惯。
- **每个工具都走 `qm_cli::execute` 这条同一个分发**，因此两个面不可能漂移：工具 = 类型化参数 + 一次调用。
- 协议层有真实回环测试：拉起 `qm-mcp` 二进制，走 `initialize → tools/list → tools/call`，
  断言工具齐全且都有描述与 inputSchema，跑通 capture → consolidate → publish → search，
  并确认非法路径返回**错误而不是伪成功**。
- 该测试用 `--synthetic-bucket`（进程内、非持久、**不是后端**，启动时向 stderr 打警告）。
  真后端仍由三个探针在带凭据时验证。

## 6.11 历史与回滚：回滚本身也是一次提交

不可变版本让"历史"不需要额外机制：链就是历史。

```bash
qm history --path notes/raft.md                       # 从最早到最新列出各版本（含提交时间）
qm read-page --path notes/raft.md --as-of 1735000000000
qm log --limit 20                                     # 最近提交
qm restore --path notes/raft.md --version <page_id>
```

- `history` 沿 `supersedes` 链逐版取出正文（**不写任何东西**）。
- `restore` **不是覆盖，而是一次普通写入**：以旧版正文提交一个新版本、supersede 当前版本。
  因此回滚本身可被再次回滚，任何后续版本都不会被销毁——这正是"发散写只 supersede、绝不销毁"的延伸。
- MCP 侧对应 `memory_history` / `memory_restore`。
- **时间线来自 commit log（已实现）**：提交时间/序号写在**提交成功之后**的
  `commits/<seq>.json`（`CommitRecord`）。为什么必须后写：这些字段只在提交点确定，塞进内容寻址的不可变版本会破坏
  重试幂等。因此该日志是**建议性元数据**——提交与日志之间崩溃会少一条记录（历史少一个时间戳），
  但不会让任何权威判断出错。
- 由此得到两个查询：`qm log`（最近提交，最新在前）与 `qm read-page --as-of <ms>`（那一刻这一页是什么）。
  删除之后再按更早时间读，仍能读到删除前的正文——历史是只读的。
- 测试：写三个版本 → history 按序返回 3 条 → restore 最早那版 → 当前正文等于旧版、
  链长变 4（而不是 3）、被回滚掉的第三版仍可读；未知版本 id 报 not found。

## 6.10 交接棒（handoff）：只能被认领一次

ai-memory 里最值得搬的一条并发语义是"页面共享、接力棒自有"。在对象存储上它反而比 SQL 更简单：
**认领就是一次 CAS**，不需要第二道守卫。

```bash
qm handoff open --title "finish the rebuild" --body "compaction is pending"
qm handoff list [--state open|claimed|done|all]     # 默认只看 open
qm handoff claim --id <id>                          # 恰好一台机器能赢
qm handoff done  --id <id>                          # 只有认领者能收尾
```

- 对象：`handoffs/<id>.json`，`id` 由 `(title, body, created_at_ms)` 派生 → 重复 open 同一张便条是 **no-op**，
  不会留下两个棒。
- 状态从字段推导（`claimed_by` / `finished_at_ms`），不存冗余 state 字段，避免自相矛盾。
- **认领 = 对同一对象的 `If-Match` CAS**：两台机器同时 claim 只有一个成功，输家收到
  `handoff ... is not open`；已认领/已完成的棒不能再被认领（有测试盯着）。
- 收尾要求 `claimed_by == 调用者`，别人来收尾会被拒——"棒是自有的"这条不变量的落点。
- 测试：open（含重复 open 幂等）→ 两台机器并发 claim（恰好一个赢、输家报 not open）→ 第三方想收尾被拒
  → 认领者收尾成功 → 已完成的棒不能再次被认领；CLI 与 MCP 两条面各有一条端到端用例。

## 6.9 自动采集（hook）与 fire-and-forget 契约

记忆不应该依赖 agent"记得去记"。`qm hook` 从 stdin 读一个事件并落进会话链：

```bash
# harness 的 lifecycle hook 直接指向它，事件 JSON 走 stdin
qm hook --event PostToolUse --session "$SESSION_ID" --actor codex < payload.json
# 也可以用管道喂纯文本
echo "rolled back the index change" | qm hook --session sess-1
```

- **永不阻塞、永不失败**：`--timeout-ms`（默认 200ms）内拿到桶就写入，否则**落本地 spool**并返回成功。
  只有"连本地 spool 都写不了"才会报错。这条契约是针对 agent 生命周期钩子的：记忆不可用不能拖垮 agent。
- **载荷容错**：JSON 里认 `session_id/session/conversation_id`、`hook_event_name/event/kind`、
  `text/message/prompt/summary/tool_response/tool_input`；认不出来就整段当文本。会话 id 会被规范化成合法 key
  （非法字符转 `-`，空则 `unattributed`），不让奇怪的 harness id 把事件丢掉。
- **输入有界**：stdin 最多读 256 KiB（解析前的 DoS 闸门），随后仍要过入口的 16 KiB + 脱敏。
- `qm hook-drain` 在桶恢复后重投 spool；**spool 条目带 scope**，绝不会写进别的项目；投递成功才删除本地文件。
- 测试：不可达的桶（指向关闭端口）→ 事件落 spool；换成可用桶后 drain 成功、spool 清空、事件可在会话链里读到。

## 6.16 迁出与迁入（export / import）

桶是权威，但"只能通过这个桶访问自己的记忆"是不可接受的。迁出是**人工可读**的：

```bash
qm export --to ./exported      # 每个 live 页面写成同路径 .md，外加 _export.json / _sessions/
# …换桶、换机器、发给别人…
qm import --from ./exported
```

- 页面写成**原样的 markdown**（不发明 frontmatter），任何编辑器都能看；标题等元数据放在 `_export.json`。
- 会话以**原始观测 JSONL** 迁出，而不是渲染后的页面——迁入方可以重新编译，而不是被迫接受别人的渲染结果。
- 迁入对**内容相同**的页面直接跳过，所以重复迁入不会堆版本（有测试断言 manifest `seq` 不变）。
- 与 ai-memory 的 export/import 定位一致：这是迁移与人工备份路径，**不是**权威状态的一部分；
  历史（supersession 链）与 commit log 不随迁出，迁出方得到的是内容而不是考古现场。
- 只暴露在 CLI 上：这是运维动作，不是 agent 工具，所以没有对应的 MCP tool。

## 6.15 完整性自检（`qm verify`）

没有服务器可以问"这个桶健康吗"，所以任何一台机器都必须能从对象自己回答。`qm verify` 做三件事：

1. **先验后端契约**：跑一次条件写探针（create-if-absent / 陈旧 ETag 必须被拒）。不支持条件写的后端不是"不健康"，
   而是**不能用**——越早知道越好。
2. **再验权威状态**：manifest 点到的每个页面版本必须存在且自洽、每条 supersession 链必须走通、
   每个会话 head 的段链必须走通、catalog 引用的每个分片必须有对象。
3. **报告全部问题而不是第一个**：运维要的是损伤的形状，不是一个症状。`--strict` 才把问题变成非零退出。

只读，不修任何东西。zero problems 的含义是"manifest 与 catalog 点到的都存在且自洽"，
不是"数据就是你想写的内容"。MCP 对应 `memory_verify`。

## 6.14 召回评测：把"检索好不好"变成数字

`qm-search` 里有一条合成语料的召回评测（`recall_eval_separates_declared_matches_from_prose`）：

- 语料构造刻意让"声明的页"和"正文反复提及的干扰页"竞争：答案页只提一次但**声明**该标识符
  （反引号 → entity），每个干扰页在同一条短正文里重复四次（BM25 偏爱高频短文档）。
- **所有竞争页都真的提交过**——否则它们会被权威过滤掉，评测就变成自欺（这是第一版评测踩到的坑）。
- 同一套查询跑两种配置：保留声明字段 vs 把声明字段清空。当前结果：

| 配置 | recall@5 | MRR |
|---|---|---|
| 带实体/链接流 | 1.000 | **1.000** |
| 清空声明字段 | 1.000 | **0.250** |

- 断言是双向的：带流时 recall@5 ≥ 0.95 且 MRR ≥ 0.80；同时**必须**比清空配置更好，
  否则说明这个指标分辨不出它声称在衡量的东西（这正是第一次跑时的情况，评测自己被"卡"了一次）。
- 想复现数字：`cargo test -p qm-search recall_eval -- --nocapture`。

## 6.13 全局检索（跨项目）

agent 常常只知道"我以前做过类似的事"，不知道在哪个项目。`--global` 解决这个：

```bash
qm search "tantivy" --global          # 当前 workspace 下所有项目
```

- 项目发现靠提交点：列出 `v1/ws/<ws>/proj/*/manifest.json`——没有 manifest 的目录还不是项目。
- 每个项目**各自检索**（自己的 catalog、自己的权威过滤），然后把"每项目一份结果列表"
  再用同一套 RRF 融合一次。这样排序靠的是"跨项目的共识"，而不是不可比的原始分数。
- 命中带 `workspace_id` / `project_id` 出处；范围检索与全局检索共用同一条过滤与缓存路径。
- MCP：`memory_search { global: true }`。

## 6.12 观测保留：压缩会话链（而不是删段）

原始观测会无限增长，但**会话链是 `prev` 链接起来的**——删中间一段会直接断链。所以保留策略是
**重写整条链**：

```bash
qm compact-session --session <id> --keep-ms 2592000000 --keep-last 50        # dry run
qm compact-session --session <id> --keep-last 50 --apply                     # 真做
```

- 保留规则是两条的**并集**：`keep_ms` 限时间、`keep_last` 限数量。只用时间会被错误的时钟清空，
  只用数量会让安静的项目永远不收敛。
- 重写 = 写一个 `prev: None` 的新段（只含存活观测）+ CAS 换 head。**旧段立刻不可达**，
  于是被常规可达性 GC 回收——没有"会话专用清理路径"（测试里断言了这一点）。
- 幂等：同样的存活集合 → 同样的内容寻址段 id，重放是 no-op。
- 默认 **dry run**；`--apply` 才写。MCP 对应 `memory_compact_session`。
- 注意：观测时间戳是**客户端时钟**，所以 `keep_last` 是防错时钟的兜底。

## 6.8 回收与保留（S5 已实现）

对象都是不可变的，所以"删除"实际是"不再引用"，回收唯一安全的做法是**可达性分析**：

可达 = manifest ＋ 每个 live page 的**整条 supersession 链**（历史是特性，`read_page`/checkpoint 靠它）
＋ 每个 tombstone 删掉的那一版（删除可逆）＋ head 指向的 catalog 及其引用的分片 ＋ 每个会话 head 及其可达段。

其余一切——输掉 CAS 的孤儿段、发布失败的孤儿分片、被新 generation 取代的旧 catalog——都可回收，
但**只在那之后过了宽限期**才回收：宽限期正是为了保护"还在上传的机器"和"刚钉住某个 catalog 的读者"。

- `qm gc` 默认是 **dry run**（只报告），`--apply` 才真删；默认宽限期 1 小时。
- 测试：孤儿（段与 catalog）在 `--grace-ms 0` 下被回收、dry run 不删任何东西；
  live 页面、整条历史链、会话链在回收后仍完整可读；宽限期内的孤儿必须保留。

## 7. 删除与压缩（S3 已实现）

- **不用 Quickwit delete-tasks**。那是集群形态的产物（只对 mature split 生效、需要协调者与可见性探针）。
- 删除 = 权威 manifest 里写 tombstone（`delete_page`，走与写入同一条 CAS 提交路径）→ 查询期过滤
  （`Manifest::is_current` 是唯一的可见性规则）→ 物理清除发生在压缩时。
- 删除是幂等的：重复删除不消耗 `seq`；对同一路径再写一次即为"更新的真相"，会清掉 tombstone。
- 压缩 = 从权威页面重建分片（被 supersede 的旧版本与被删除的页面**由构造天然消失**，不靠过滤）；
  任何机器拿到租约就能做，做完用一次 CAS 换 `head.json`，旧分片仍被更早的 catalog 引用。
- 租约：`v1/leases/<scope>.json`，含 owner + 单调 epoch + 到期时间；释放 = CAS 写成"立即过期"（不做条件删除），
  接管 = 对过期租约的 CAS；原持有者的续租因版本过期而失败。
- **没有机器跑压缩时系统不降级**，只是分片变多：压缩是可放弃作业。

## 8. 故障模型

| 故障 | 行为 |
|---|---|
| 机器离线/休眠 | 其分片已在桶里，别人照搜；租约到期后被接管 |
| 时钟漂移 | 权威顺序只认 CAS 时的 seq；排序用 `(updated_at, writer_id, seq)` |
| 重复发布 | 分片名含 writer+seq；查询期按 `page_id` 去重取最新 |
| 目录滞后 | 本机写走本地档；跨机结果显式带 `covered_until` |
| CAS 竞争 | 重读重试；不同 project 之间零竞争 |
| 凭据泄露 | 见第 9 节 |

## 9. 凭据（待决策）

| 方案 | 优点 | 缺点 |
|---|---|---|
| 每机全桶 token | 零新组件、一次往返 | token 只能按桶授权，无前缀级 ACL：一台机器被入侵 = 全库可读可写可删 |
| Worker 网关 | 最小权限、可审计、可吊销、可强制 CAS 协议 | 多一跳与一个依赖；大对象不应中转（用短时签名直连） |

倾向：**控制面走 Worker（鉴权 + CAS 提交点 + 短时签名），数据面直连 R2**。
落地前必须实测（S0）：Workers 的 R2 binding 条件写语义、presigned URL 与 `If-Match` 的组合是否可靠。
当前 S0 先用全桶 token 打通协议，凭据方案不阻塞协议验证。

## 10. 从 ai-memory 借的思想

保留（均已实现）：`(workspace, project, path)` 三元身份、观测→页面的"编译而非检索"、supersession 链、handoff 只被认领一次、FTS+entity+link 多路 RRF、历史/revert（restore-page）、
sanitize 作为唯一入口边界、hook 即发即忘 202/429、读路径 fail-closed 的 scope 解析、MCP 工具面。

替换：SQLite（→ CAS 对象 + 派生索引）、git 工作树（→ 不可变版本 + manifest 链）、fs watcher（→ 发布/重建作业）、
单写者事务（→ 每 scope 一个提交点 + at-least-once 索引 + ack 游标）。

## 10.5 真 S3 协议验证（2026-09-15）

同一套代码在**真实 S3 实现**（MinIO 2026版，本地 127.0.0.1:9100，非内存后端）上跑过一遍，全部通过：

| 验证 | 原始结果 |
|---|---|
| `cas-conformance`（条件写契约） | 7/7 PASS：create 返回 ETag、重复 create 被拒、ETag 跨读稳定、陈旧 ETag 被拒、匹配 ETag 被接受且 ETag 改变、已消费 ETag 被拒、最终内容正确 |
| `manifest-probe --machines 3 --writes 5` | `commits=15 attempts=21 pages=3 wal=15 all checks passed`（21 次尝试 = **6 次真实 CAS 冲突**被正确重试，15 次提交全部落地） |
| `search-probe project` | `splits=3 hits=1 filtered_out=1 all checks passed`（过期副本被权威过滤） |
| `session-probe` | `observations=3 segments=2 splits=1 hits=1 all checks passed` |
| CLI 端到端（quickstart 路径） | capture → consolidate → publish → search 命中 → write-page → history（含提交时间）→ log → **`verify --strict`：no problems** → `gc`：scanned 19 / live 19 / collectable 0 |
| 跨机器 | B（新 writer + 新缓存）读到 A 的页面与正文；B 写入并 publish 后 A 能搜到；handoff 由 B 认领后 A 再认领被拒（`is not open (state: Claimed)`） |
| export / import | 导出 3 页 + 1 会话 → 导入到同桶另一项目 → 再导入 `0 page(s) (3 unchanged)` → publish → search 命中 → `verify --strict` 通过 |

**这次验证覆盖了什么**：真实 HTTP + S3 协议路径（签名、endpoint、path-style、条件头、412 语义、列目录）、真实网络下的 CAS 冲突与重试、
mTLS 之外的完整读写链路、多机协作语义。

**这次没有覆盖什么**（保持诚实）：

- **R2 特有行为**：MinIO 的 PUT 不返回 `x-amz-version-id`（探针输出 `version=None`），
  所以"PUT 带 version、GET 不带"那条 R2 教训没有被这次运行触发。CAS 层按 ETag 判定并拒绝无 ETag 的后端，
  逻辑上已经对这种情况免疫，但仍建议在真 R2 上再跑一次 `cas-conformance`。
- **Quickwit 二进制**：仍是格式级验证（解析器 + `INDEX_FORMAT_VERSION = 7` 一致性），没有用真分片跑过字节级往返。

## 11. 实证结论（截至本次提交）

已在本仓库验证（离线，`cargo test`）：

**S0**

- ETag CAS 契约与探针（含阳性对照：接受一切的后端会被探针报错）。
- **任何一台机器独立搜全量**：A 构建索引上传对象存储，B 只有桶访问权，材料化后进程内查询命中（`qm-search` 集成测试）。
- 探针在缺少凭据时**报错而非跳过**。

**S5（agent 可用面、自动采集、交接棒、多路检索与回收，进行中）**

- 多路检索：正文 + 实体 + 链接三路 RRF；声明式匹配优先于正文顺带提及（有测试断言排序与 `streams` 归因）。
- 工程化：分片本地缓存按内容哈希复用（删掉桶里的分片后仍可检索）；发布按机器本地 watermark 增量进行。
- 质量证据：合成语料召回评测（带流 MRR 1.000 vs 清空声明字段 0.250），且评测自身有"分辨力"断言。

- 交接棒：`handoff open/list/claim/done`，CAS 保证恰好一次认领、只有认领者能收尾。

- 自动采集：`qm hook` + `qm hook-drain`，fire-and-forget 契约（超时即 spool，永不阻塞 agent）。

- 回收：可达性分析 + 宽限期 + dry-run 默认；live 页面、历史链、会话链在回收后仍可读。
- 鉴权/凭据方案仍未定（Worker 网关 vs 每机全桶 token），是**决策项**而非实现项。

- `qm` CLI 11 条命令可用；命令逻辑在内存桶上做了端到端测试（无需凭据）。
- MCP stdio 服务器已实现并通过协议级回环测试（19 个工具，与 CLI 同一分发）。

**S4（采集与编译）**

- 捕获：3 批观测 → head `generation=3`、`count=6`；重复提交最后一批被识别为 no-op（不新增段）。
- 两台机器并发捕获同一会话：两批都落地（`count=2`/`generation=2`），读侧按 id 去重后正好两条。
- **脱敏在入口生效**：调用方直接塞 `api_key=abcd1234` 也存不进秘密，且落库的 `observation_id` 与落库字节一致。
- 会话互相独立：三会话并存可按 LIST 发现；只有段没有 head 的会话**不可见**。
- 编译：3 条观测 → 一页 `sessions/<sid>.md`（正文含全部文本）→ 发布分片后**可被检索**；
  链没变时重编译 `already_up_to_date` 且不消耗 manifest `seq`；另一台从未见过该状态的机器可编译；租约被占则 `skipped`。

**S3（删除与压缩）**

- 删除后：页面从 manifest 移除、tombstone 就位；重复删除不推进 `seq`；再写入会清掉 tombstone。
- 压缩：3 个分片 → 1 个分片，**检索结果逐条不变**，且被删除的页面没有复活（它的词条查不到）。
- 压缩期间租约被占：第二个压缩者返回 `skipped`，不做任何写入（可放弃作业必须能放弃）。
- 租约语义：TTL 内不可抢占；过期后可接管且 epoch 递增；原持有者续租失败；释放后立即可被接管。
- `.split` 容器：可从原文解包成 tantivy 目录并被检索（跨进程往返测试），损坏/恶意 footer 不 panic。

**S2（分片发布与检索）**

- 目录追加：8 个并发发布全部保留（generation=8），且测试断言本轮确实发生过 head CAS 冲突；重复发布同一 `content_hash` 不推进 generation。
- 三台机器交替写、其中一台发布后**永不再出现**：新读者仍能搜到它的内容，说明"离线机器的数据可搜"成立。
- 被封面的旧版本不会答出：查询只命中旧版本时返回 0 条并记 `filtered_out=1`；命中新旧两版时只返回当前版本。
  两个独立读者（各自全新句柄、各自缓存目录）结果完全一致。

**S1（权威层）**

- 4 台机器 × 4 条路径 × 5 个版本 = 80 次提交：`manifest.seq == 80`（没有提交被覆盖），
  每条链长度 5 且 `supersedes` 逐环相连，WAL 含 80 条互不重复的记录。
  该测试同时断言**本轮确实发生过 CAS 冲突与重试**（否则它证明不了重试路径）。
- 一台"从未见过当前状态"的机器（全新句柄、只共享桶）能读到最新版本并继续提交第 4 个版本。
- 模拟"上传后崩溃"：重试复用同一页面对象，不报错、不产生重复版本。
- 同 key 已存在但内容不同 → `Corrupt` 失败关闭。
- 多机场景探针 `manifest-probe`（先做 CAS 预检，再跑场景并全量复核）；local 后端在预检阶段被明确拒绝。

已确认的事实（影响选型）：

- `quickwit-*` 系列 crate 在 crates.io 上最后发布是 **0.3.0（2022-06）**，与线上运行的 v0.9.0 差一大截；
  因此不能把 Quickwit 作为库依赖，只能作为**外部服务/构建器**。
- tantivy 0.26 的 `TopDocs` 必须先 `order_by_score()` 才能作为 collector 使用。

待验证（需要真 R2 凭据 / Quickwit 二进制）：

1. 真 R2 上跑 `qm-probe cas-conformance`（ETag 稳定性、陈旧 ETag 拒绝）。
2. 真 R2 上跑 `qm-probe search-probe build/query`（跨进程/跨机器检索）。
3. ~~Quickwit 产出的 `.split` 能否解包成 tantivy 目录被进程内直读~~ —— **格式已实现并测试**（见 §6.4）；
   ~~格式版本是否兼容~~ —— **源码核对一致**（fork 0.26.0，`INDEX_FORMAT_VERSION = 7`，与本仓 tantivy 0.26.2 相同）；
   仅剩**真实字节**验证，需要 `QW_BIN`（本机下载被限速，见 §6.4）；
4. 真 R2 上的 S2 场景：`cargo run -p qm-probe --bin search-probe -- project`。
