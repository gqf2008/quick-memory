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
├── ws/<ws>/proj/<proj>/
│   ├── manifest.pb                                  # 唯一提交点（CAS）
│   ├── pages/<path>/versions/<page_id>.md           # 不可变页面版本
│   ├── observations/<obs_id>.json                   # 不可变原始观测
│   ├── index/
│   │   ├── head.pb                                  # CAS 指针 → 当前 catalog
│   │   ├── catalog/<generation>.pb                  # 不可变目录版本
│   │   └── splits/<writer_id>/<seq>/…               # 该写入方发布的分片文件
│   └── leases/{compact,gc}.pb                       # 可放弃作业的租约
└── v1/leases/<scope>.pb
```

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
3. 读 `head.pb` → 追加条目 → 写新 `catalog/<n+1>.pb` → `If-Match` 换 `head.pb`。
4. CAS 冲突即重读重试；分片已上传但目录未更新时，该分片是不可见的孤儿，由 GC 回收。

**读（任何一台机器）**

1. 读 `head.pb` 得到分片列表。
2. 把每个分片材料化到本地缓存目录（下载到本地再打开，字节精确、与格式版本无关）。
3. 进程内打开并用 BM25 查询。
4. 每条命中再过权威可见性校验（是否被 supersede / 删除 / 越出 scope）。
5. 多路（正文 / entity / link）结果在**读方**做 RRF 融合。

**read-your-writes**：本机刚写入但尚未发布分片的内容，由本地尾部（权威对象 + 本地索引）覆盖；
跨机可见性允许有界滞后，但必须在结果里给出 `covered_until`。

## 7. 删除与压缩

- **不用 Quickwit delete-tasks**。那是集群形态的产物（只对 mature split 生效、需要协调者与可见性探针）。
- 删除 = 权威 manifest 里写 tombstone → 查询期过滤 → 物理清除发生在压缩时。
- 压缩 = 从权威页面重建分片；任何机器拿到租约就能做，做完只**新增**分片并 CAS 换 `head.pb`。
- **没有机器跑压缩时系统不降级**，只是分片变多。

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

保留：`(workspace, project, path)` 三元身份、观测→页面的"编译而非检索"、supersession 链、handoff 只被认领一次、
sanitize 作为唯一入口边界、hook 即发即忘 202/429、读路径 fail-closed 的 scope 解析、MCP 工具面。

替换：SQLite（→ CAS 对象 + 派生索引）、git 工作树（→ 不可变版本 + manifest 链）、fs watcher（→ 发布/重建作业）、
单写者事务（→ 每 scope 一个提交点 + at-least-once 索引 + ack 游标）。

## 11. S0 实证结论（截至本次提交）

已在本仓库验证（离线，`cargo test`）：

- ETag CAS 契约与探针（含阳性对照：接受一切的后端会被探针报错）。
- **任何一台机器独立搜全量**：A 构建索引上传对象存储，B 只有桶访问权，材料化后进程内查询命中（`qm-search` 集成测试）。
- 探针在缺少凭据时**报错而非跳过**。

已确认的事实（影响选型）：

- `quickwit-*` 系列 crate 在 crates.io 上最后发布是 **0.3.0（2022-06）**，与线上运行的 v0.9.0 差一大截；
  因此不能把 Quickwit 作为库依赖，只能作为**外部服务/构建器**。
- tantivy 0.26 的 `TopDocs` 必须先 `order_by_score()` 才能作为 collector 使用。

待验证（需要真 R2 凭据 / Quickwit 二进制）：

1. 真 R2 上跑 `qm-probe cas-conformance`（ETag 稳定性、陈旧 ETag 拒绝）。
2. 真 R2 上跑 `qm-probe search-probe build/query`（跨进程/跨机器检索）。
3. Quickwit 产出的 `.split` 能否解包成 tantivy 目录被进程内直读——决定"Quickwit 当构建器"是否可行；
   若不可行，则由 tantivy 直接承担构建器角色，Quickwit 退化为可选的服务化加速层。
