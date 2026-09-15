# quick-memory 设计 v1（多机形态）

> 状态：S0 进行中。本文记录已经过实证的结论与仍待验证的假设；未验证的部分显式标注。
> 借鉴来源：ai-memory（领域模型与"编译而非检索"）、moltlink（对象存储 CAS/WAL/租约/GC 协议）。

## 1. 目标

给多台机器上跑的 agent 提供**共享的长期记忆**：

- 每台机器都能写：hook 即发即忘、有界、不阻塞 agent。
- **每台机器都能独立搜全量**：不需要任何常驻服务、协调者或对等发现。
- 权威数据在对象存储；搜索索引是可重建的派生物。
- 机器会休眠、换网、消失——这是正常态，不是故障。

非目标（v1）：多租户 SaaS、图形化前端、跨桶联邦、近似最近邻索引（向量检索本身见 §6.20，先用暴力余弦）。

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

- **权威层**：唯一提交点是每个 project 的 `manifest.json`；页面版本与观测都是不可变对象。
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

### 5.1 R2 特有行为：按构造免疫，而不是按经验祈祷

R2 的 PUT 可能返回只在 PUT 出现的 `x-amz-version-id`，后续 GET/HEAD 不返回。CAS 层对此的处理是**规则**而非巧合：

- 对象身份一律取 ETag；`UpdateVersion.version` **永不**由 PUT 响应回填（见 `ObjectVersion::as_update_version`）。
- 因此"PUT 带 version、GET 不带"不会造成版本比较失败——这正是最初差点写错的地方。
- 两条回归把它钉住（无需真桶即可运行）：
  - `an_r2_style_put_only_version_still_commits`：构造一个"PUT 形状"的版本（ETag + 只此一次的 version），
    用它做条件写必须成功，且后续读回的版本用于下一次更新时只带 ETag；
  - `a_version_without_etag_fails_closed`：只有 version 没有 ETag 的后端会被拒绝（`MissingEtag`），
    而不是被当成"版本没变"。
- 探针会把这一特征直接印出来：`cas-conformance` 第一步同时报告 PUT 返回的 ETag 与 version，
  在 R2 上会看到 `version=Some(...)`，在 MinIO 上是 `version=None`。

**仍然没有做的**：在真 R2 上跑一次 `cas-conformance`。上面是"按构造 + 回归"级别的证据，
不是"在 R2 上观测到的证据"，两者不应混为一谈。

#### 协议层验证（stub）

上面两条回归跑在 `object_store` 的 InMemory 后端上——它没有 HTTP 层，所以 ETag 头、条件请求头、
404/412 错误体、ListObjectsV2 XML 从未被端到端走过。`qm_probe::s3_stub::S3Stub` 补上了这一层：
一个进程内的最小 S3 兼容服务（`std::net::TcpListener` + 手写 HTTP/1.1，无新依赖，仅在测试与探针辅助代码里），
探针通过**真 socket** 驱动 `object_store` 的 S3 客户端。

这层现在**测到了**：

- **条件头真的上了线**：`If-None-Match: *`（create-if-absent）与 `If-Match: <etag>`（update-if-match）
  由测试断言 stub **收到**的值，而不是断言客户端"打算"发什么；陈旧 ETag 与已消费 ETag 各被拒一次，
  两次都是 stub 回 `412`。
- **S3 的名字语义**：重复 create 由 bucket（412）而不是客户端拒绝；`If-Match` 打在不存在的对象上回 `404 NoSuchKey`，
  再由客户端按契约翻译成 precondition failure（`cas-conformance` 与 `qm-store` 的错误映射都覆盖）；
  `DELETE` 幂等（删不存在的 key 也是成功）。
- **错误体是 S3 错误 XML**：`404 NoSuchKey` / `404 NoSuchBucket` / `412 PreconditionFailed` 都以
  `<Error><Code>…` 文档返回，不是空体。
- **list 与分页**：`ListObjectsV2` XML、`prefix`、`continuation-token` 走通——5 个对象按每页 2 个
  返回时客户端恰好发 3 次请求、每次带同一个 prefix，且 ETag/size 逐条对上（stub 分页是刻意开的，
  否则这层只会被单页覆盖）。
- **HEAD 元数据**：`Content-Length`、RFC2822 `Last-Modified`（解析回的时间就是写入时刻）、ETag 都能被客户端解出。
- **签名代码路径**：请求确实带 `AWS4-HMAC-SHA256` 的 `Authorization` 头（`SignedHeaders` 里能看见
  `if-match`/`if-none-match`），因此走的不是 `skip_signature` 捷径；这一条现在有回归守着——
  测试解析每条请求的 `SignedHeaders`，断言凡是带条件头的请求都把它签进了签名头列表，
  并断言这样的条件写恰好 5 次（两次 create + 陈旧/匹配/已消费三次 update），免得断言在空集合上空转。
- **409 冲突与重试**：stub 可注入「前 N 次**通过前置条件**的条件写回 `409 Conflict`」——真实 S3 在并发
  `If-Match` 写重叠时就是这么答的，而 `object_store` 只对这类写打开 `retry_on_conflict`。注入 1 次时探针
  必须仍然全绿，且测试断言线上真的是「同一个 `If-Match` 的 409 紧跟一个 200」，不是因为恰好没注入；
  把注入次数调到超过客户端的重试预算时，探针必须以重试耗尽的错误失败。两条合起来才说明"探针在冲突下
  通过"不是因为 409 被吞掉了。
- **R2 的 PUT-only version 形状**：stub 打开该形状后，测试同时断言 PUT **有** `x-amz-version-id`、
  随后的 GET **没有**，探针仍全绿——§5.1 的"按构造免疫"于是在真实 HTTP 形状下被观测到，
  而不再只是类型层面构造出来的。
- **并发条件写**：两条针对同一 ETag 的并发 `update` 恰好一条成功、另一条拿到 412，落盘内容等于胜者的字节。
- **两个故障注入的对照**：stub 忽略 `If-Match` → `cas-conformance` 必须在 `stale-etag-rejected` /
  `consumed-etag-rejected` 上报 FAIL 并退出非零（测试同时跑一遍正常 stub 确认这不是环境问题）；
  stub 回一个从未写过的读 ETag → 探针也必须失败。
- **检索路径也走真实 HTTP，而且是跨进程的**：`search-probe build` 与 `search-probe query` 是**两个独立
  进程**打同一个 stub——第一个进程发布分片，第二个进程只有桶访问权、自己新建的缓存目录，必须把分片对象
  全部取回并命中全量页面。测试断言的是 stub **收到**的 PUT/GET（每个 key 都被第二个进程取过），而不是
  探针自称的数字；stub 刻意按每页 1 个对象分页，所以"读到全量"必须真的跟着 `continuation-token` 走。
  多机目录场景（`search-probe project`：三写一读、旧版本被目录过滤掉）也在同一 socket 上跑通。
- **向量链也是真 HTTP、跨进程的**：`search-probe vector-publish` 在一个进程里经真实 HTTP
  embedding stub 生成向量、提交权威页面并把分片 PUT 到 S3 stub；进程退出后，
  `search-probe vector-query` 只带桶坐标、自己的缓存和查询文本启动，从同一个 S3 stub 取回目录、权威
  与分片，并把一个**与页面无词法重合**的查询召回为目标页。测试断言命中的 `streams` 和
  `streams_active` 都只有 `vector`，且无 provider 时命令硬失败、同宽换模型在查询向量生成前被身份守卫拒绝；
  它不是产品 CLI，而是探针的协议层证据。见 `crates/qm-probe/tests/search_probe_stub.rs`。
- **digest 也有一条跨进程证据**：`digest-probe seed` 与 `digest-probe read` 是**两个独立进程**打同一个
  stub；两个命令都必须显式传 `--workspace` / `--project`，不能裸跑。第一个提交两页、删掉其中一页、提交两个会话 head，并留下三根交接棒（一根开→认领→**收尾**、
  一根只开不认领、一根全部阶段都在窗口之前），第二个只有桶坐标，
  必须自己把 pages（含删除）/ sessions / handoffs 重组出来，并报告 manifest 仍认账的 live pages
  （所以「manifest 已不再返回那条 path、digest 仍报这条删除」是被断言的）。测试对两个进程显式传入同一组
  唯一 scope；`seed` 写入前会用 listing 预检拒绝非空目标 scope（best-effort，不是并发锁），`read` 只读，当前 probe 不自动清理。故障对照：stub 把 listing
  第一页当成整份答案 → 同一个正向谓词（三段内容、`at_ms` 时钟序、per-section 窗口与 `limit`）变红。
  见 §6.22 与 `crates/qm-probe/tests/digest_probe_stub.rs`。
- **两个检索侧的故障注入对照**：stub 把 listing 的第一页当成完整答案（`--fault truncate-listing`）→
  读者只被告知 1 个对象、材料化出来的目录缺 `meta.json`，`query` 必须退出非零且不打印命中；
  stub 把每个 `GET` 的 body 截成 1 字节、`content-length` 仍然诚实（`--fault truncate-read-body`）→
  传输全部 200 成功，读者仍必须在打开索引时报 `Data corrupted`。前者证明"搜到全量"不是因为列表恰好够短，
  后者证明损坏的内容不会被静默当成有效索引。
- **条件读是拒绝，不是错答**：stub 不建模 `GET`/`HEAD` 上的 `If-Match`/`If-None-Match`（今天仓里没有任何
  调用点），带条件的读回 `501 NotImplemented` + S3 错误 XML，而不是按"无条件 200 + 正文"回答——**错答比缺答更坏**，
  一个建立在错答上的探针会为错误的理由变绿。这与 `delimiter` 的处置是同一条规则。`HEAD` 只回头且
  `content-length` 与同一请求的 `GET` 一致（声明它拒绝发送的那份正文）。
- **`If-Match: *`**：按 HTTP 的"存在即通过"处理（对不存在的对象回 `404 NoSuchKey`，与其它 `If-Match` 一致），
  而不是把它当 ETag 去比——那样会拒绝每一个**存在**的对象。
- **`<MaxKeys>` 回显请求值**：客户端请求 1000、stub 每页只发 2 条时，响应里的 `<MaxKeys>` 仍是 1000
  （`object_store` 今天不读这个字段，但"看起来对、其实不忠实"的字段迟早会咬人）；另有一条按请求的 1 条截断。

**一条读法陷阱（先于本层既有，不是 stub 引入的）**：把 409 注入开到超过客户端的重试预算时，
`cas-conformance` 只报 `matching update failed: object already exists`。它**不代表桶里有重复对象**：
HTTP `409 Conflict` 被 `object_store` 映射成 `Error::AlreadyExists`，`qm-store` 再映射成自己的
`AlreadyExists`，于是"并发写争用、重试耗尽"被渲染成了"重复对象"的诊断。真实桶上看到这句话时先查
并发写/同前缀争用，不要去找第二个对象。复现见下面的 stub 命令段（`--conflict-conditional-puts 100`；
注入 1 次时探针全绿且 `409` 紧跟同一个 `If-Match` 的 `200`，说明注入本身是有效的，只有**耗尽**预算才走这条报文）。

**这层仍然没有测到什么**（不要把它读成真 R2 验证）：

- **条件读的语义**：`If-None-Match` 命中时本该是 `304`（并带 ETag），stub 一律回 `501`——是"我们没建模"
  的诚实表达，不是"304 已被验证"。

- **真 R2/S3 账号**：没有凭据，仍然没有在真桶上跑过 `cas-conformance`；stub 是我们写的服务，
  它证明的是"客户端在真实 HTTP 往返下的行为"，不是"R2 真的这样回答"。
- **服务端签名校验**：stub 记录并忽略 `Authorization`；签名是否正确，只有真后端才能拒。
- **R2 的延迟、配额、区域行为、一致性、错误 XML 变体**：stub 一律立刻回答、无错误变体。
- **检索与向量路径同样只有 stub 级证据**：`search-probe` 的 build/query、project 以及
  vector-publish/vector-query 已经真打 socket，但**没有在真 R2 上跑过**（§11 第 2、4 项）；`project`
  里的“多台机器”是同一进程内的并发任务，只有桶访问是跨进程的，向量闭环则另有独立进程的 A/B 证据。
- **检索与 digest 路径同样只有 stub 级证据**：`search-probe` 的 build/query 与 project、以及
  `digest-probe` 的 seed/read 都已经真打 socket，但**没有在真 R2 上跑过**（§11 第 2、5 项）；
  `project` 里的"多台机器"是同一进程内的并发任务，只有桶访问是跨进程的（`digest-probe` 的两个命令
  才各是一个进程）。
- **multipart**：本仓的对象都远小于 5 MiB，客户端走单次 PUT；stub 不实现分片上传，
  所以"大对象"这条路径没有被覆盖。
- **真实网络故障**：重试/退避只被单元测试覆盖，stub 不制造超时、5xx 或连接断裂。

复现（stub 只在测试与探针辅助里，产品路径不可达）：

```bash
# 进程内：真实 S3 客户端 + 真 socket（约 1.5s）
cargo test -p qm-probe --test s3_protocol

# 跨进程：起一个独立 stub，再用现有探针二进制打它
cargo build -p qm-probe --bins
./target/debug/s3-stub --port 0 --port-file /tmp/s3-stub-port &
QM_S3_ENDPOINT="http://127.0.0.1:$(cat /tmp/s3-stub-port)" QM_S3_BUCKET=stub-bucket \
QM_S3_ACCESS_KEY_ID=stub-access QM_S3_SECRET_ACCESS_KEY=stub-secret QM_S3_FORCE_PATH_STYLE=true \
./target/debug/cas-conformance
# 对照：注入一次 409（客户端重试，探针仍全绿）；次数超过重试预算则必须失败
./target/debug/s3-stub --conflict-conditional-puts 1 --port-file /tmp/s3-stub-conflict-port &
QM_S3_ENDPOINT="http://127.0.0.1:$(cat /tmp/s3-stub-conflict-port)" QM_S3_BUCKET=stub-bucket \
QM_S3_ACCESS_KEY_ID=stub-access QM_S3_SECRET_ACCESS_KEY=stub-secret QM_S3_FORCE_PATH_STYLE=true \
./target/debug/cas-conformance

# 对照：同一个探针必须失败
./target/debug/s3-stub --fault ignore-if-match --port-file /tmp/s3-stub-fault-port &
QM_S3_ENDPOINT="http://127.0.0.1:$(cat /tmp/s3-stub-fault-port)" QM_S3_BUCKET=stub-bucket \
QM_S3_ACCESS_KEY_ID=stub-access QM_S3_SECRET_ACCESS_KEY=stub-secret QM_S3_FORCE_PATH_STYLE=true \
./target/debug/cas-conformance
```

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
5. 多路（正文 / entity / link，以及可选的 vector / neighbor）同样走 RRF，融合层必须在读方，
   不能放在某个服务里。

**read-your-writes**：本机刚写入但尚未发布分片的内容，由本地尾部（权威对象 + 本地索引）覆盖；
跨机可见性允许有界滞后，但必须在结果里给出 `covered_until`。

## 6.20 向量检索流（第 4 路，可选）

前三条流都是词法的：页面必须**含有**查询的词或标识符。语义召回要回答的是另一个问题——
"哪些页面在讲同一件事，尽管一个词都没重合"。这一路是可选的，因为它需要外部 embedding 服务：

- **分片里存的是向量**：每个文档一个 `embedding` 字节快速字段，值是 f32 小端拼接（不做
  f32→文本→f32 的往返，那是有损的）。`PageDoc.embedding` 为 `Option` 且带 `serde(default)`，
  所以向量出现之前发布的 JSONL 文档仍然可解析。
- **分片记录向量的身份**：每个用 provider 建出的分片都带一份
  `embedding-identity.json`（`{provider, model, dim}`，字段全部 `serde(default)`）。它写在
  分片目录里，所以和向量一起上传、哈希、材料化——读者拿不到向量而漏掉它的来源。搜索时先
  比对 `(provider, model)`：**同宽度换模型**以前只会静默给出无意义排序，现在 fail-closed 报错
  并点名两边的 provider/model/dim；**宽度变化**仍由余弦的宽度守卫报出那句更窄的
  `cannot compare embeddings of different widths`；**没有记录的老分片不判定**（"没记录"不等于
  "不是同一个模型"，否则升级会搜挂既有桶）。compact 同样重写这份记录，所以它是分片级事实，
  不是一次 publish 的临时状态。
- **查询向量来自同一个 provider**：`QM_EMBEDDING_BASE_URL` / `_API_KEY` / `_MODEL`（宽度由
  `QM_EMBEDDING_DIM` 给，默认 1536）。**未配置不是错误**：publish 不写向量、search 不跑这一路，
  其余行为完全不变。配置了但调用失败/宽度不符/条数不符则是**硬失败**——静默补零或截断会让
  之后的每一次距离比较都建立在错位的坐标上。
- **打分是暴力余弦**：打开分片、逐 doc 读列、算相似度、取 top-k。这是 v1 的选择：近似结构是
  优化，而会改变召回率的优化必须先被测量才能信任。
- **只收正相关**：相似度 `<= 0` 的页面不进候选。余弦不是置信度，也不是跨查询可比的分数；
  正交页面没有任何"它在讲这件事"的证据，把它放进融合等于给每次检索注入噪声。这是地板，不是调参阈值。
- **权重 0.6**（`SearchTuning.vector_weight`）：语义邻居是比"页面真的含有查询词"更弱的信号，
  所以它加召回但不挤掉直接命中。`--no-vector` / `no_vector` 可以显式关掉这一路。
- **重建必须保留向量**：`compact` 从权威页面重建索引，如果它丢掉了向量，第一次压缩之后语义召回
  就会**静默消失**（关键词那几路仍然正常）。所以压缩路径同样接入 provider，并有测试守着这一点。
- **换 provider 需要重新嵌入**：publish 的 watermark 记录了"这一版是否带向量"，所以
  配置 provider 之后第一次 publish 会重发全部页面（序号没动，但内容需要重算）。
- **请求有界**：`OpenAiCompatEmbedder` 对一次 HTTP embedding 请求设置 30 秒总超时。一个接受连接后
  永不回答的确定性 stall endpoint 会让调用返回超时错误，而不是让读/发布路径永久挂住；测试用短超时
  跑这条阳性对照，生产值只在一个常量里。

**协议层（stub）已经证明，真 R2 仍未验证**：上面的向量闭环跨两个独立 `search-probe` 进程、经由真实
HTTP embedding stub 与 `S3Stub` 完成，能证明客户端在真实 HTTP 往返上的组合行为；它不是 R2 观测，
没有覆盖 R2 的签名校验、延迟、配额、一致性或错误 XML 变体。真 R2 上仍应跑一次
`vector-publish` + `vector-query`，并保留无 provider / 换模型两条 fail-closed 对照。

已知取舍：embedding 服务是**读路径上的一个外部依赖**——配置了它，检索就可能等它，最多等到 30 秒超时。
这是 fail-closed 的代价，换来的是"索引里的向量一定与查询向量同源、同宽"。

## 6.4 与 Quickwit 的格式兼容（已用真分片验证）

Quickwit 的 split 容器格式是公开的，读方**不需要 Quickwit 集群**就能打开它：

```
[ 所有文件首尾相接 ][ FileMetadata ][ metadata 长度 ][ hotcache ][ hotcache 长度 ]
FileMetadata = 8 字节版本头（magic = 403881646 / 0x1812BEAE，version = 1）+ JSON {"files": {name: {start, end}}}
```

**长度字段是 u32**（不是 u64）——这一点由真实分片的字节决定，而不是由文档决定：Quickwit 0.9.0 产出的分片尾部
`hotcache_len = 3431`、`metadata_len = 485` 都是 4 字节小端；Quickwit 自己的 storage 侧 reader 用的是 8 字节，
属于它内部两处实现不一致。我们的解析器**两种宽度都接受**，由版本头 magic 判定实际是哪种，
所以两种写法都能读。

读 Quickwit 分片还需要 tantivy 的两个非默认特性，已在本工作区启用：

| 特性 | 为什么需要 |
|---|---|
| `zstd-compression` | Quickwit 的 docstore 用 zstd（`zstd(compression_level=8)`），不启用会报 `unsupported variant zstd` |
| `quickwit`（含 `sstable`） | Quickwit 的 term dictionary 是 SSTable，上游默认是 Fst，不启用会报 `Unsupported dictionary type` |

**已用真分片验证（2026-09-15）**：用官方 `quickwit/quickwit:0.9.0` 容器跑一个单节点、创建索引、
灌 3 条文档，产出 `/quickwit/qwdata/indexes/qm-split-test/<id>.split`（6893 字节）。把它拷出来交给
`split-probe --file <split> --query tantivy`：

```
split=... bytes=6893 recognised=true
extracted 8 file(s), hotcache 3431 bytes
  <segment>.fast/.fieldnorm/.idx/.pos/.store/.term, meta.json, split_fields
hits=1
```

也就是：**一个没有 Quickwit 的进程，读到了 Quickwit 产出的索引并检索出结果**。这正是"任何一台机器独立搜全量"
在跨引擎情形下的那一半。

顺带的效果：由于 `quickwit` 特性是编译期的，我们自己写出的分片也使用 SSTable 词表，与 Quickwit 同族；
`materialize` 会自动识别前缀下的 `.split` 并就地解包，于是"Quickwit 构建器产出的分片"与"tantivy 构建器产出的目录"
在读方是同一种东西。

边界仍然明确：抽取出来的文件名会经过校验（拒绝路径穿越），所有长度都做 checked 运算（损坏/恶意 footer 只报错不 panic），
外来 schema 的分片没有 `path`/`page_id` 字段时，命中会退化为"有正文、无身份"——这是读别人的索引时诚实的语义，
而不是伪造身份。

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
- manifest 目前是单对象（含全部 path 的当前版本），因此有规模上限。这个上限现在是**实测 + 守卫**：
  约 243 B/path、提交点上限 1 MiB（`MANIFEST_MAX_BYTES`）对应 4 319 条 path，越界时
  `commit_page` 返回 `StoreError::ManifestTooLarge` 并在**写出任何对象之前**拒绝（数字与复跑命令
  见 `ops.md`「manifest 的规模上限」）。按 path 哈希分片 manifest（**format 2**）见 §6.21：
  已经实现，`QM_MANIFEST_FORMAT=2` 打开，默认关（默认路径与上面这段逐字节相同）。
- **本机文件系统不是可用后端**：`object_store` 的 local 后端不支持条件写，探针会在预检阶段直接拒绝
  （这正是"本地文件不能当对象存储语义模型"的可执行证据）。

## 6.21 按 path 哈希分片 manifest（已实现：format 2，opt-in）

### 为什么需要

`manifest.json` 原来是**一个对象**：`Manifest::pages` 与 `Manifest::tombstones` 装下 scope 全部
path 的当前版本，所以每次提交都重写整个对象。实测（数字与复跑命令见 `ops.md`「manifest 的规模
上限」）：path 31 B / title 40 B 的形状下约 **243 B/path**，提交点上限 `MANIFEST_MAX_BYTES`（1 MiB）
对应 **4 319** 条 path，每条都重写过则 **3 441** 条。越界时 `ProjectStore::commit_page` 返回
`StoreError::ManifestTooLarge` 并**拒绝**提交，而不是继续长——拒绝是安全的，但它的解法过去只有
"把内容拆到第二个 project"。

format 2 是第二个解法：**把全部 path 换成一份布局**，于是提交点只随分片数增长（有界），而每次
提交只移动它要改的那一片。

### 开关：读按存储形态，写按配置

`QM_MANIFEST_FORMAT=1|2`，默认 **1**（整份对象，与 format 2 之前逐字节一致：`format` 字段在
整份形态下不落盘）。这个开关**只决定写**：

- **读**永远按对象自己的 `format` 判别分派，不看开关。所以一台从不设置它的机器照样能正确读一个
  format 2 的 scope；一台设置了 2 的机器也能读 format 1 的 scope。这是"向后兼容读"的实现方式，
  也是为什么没有"用开关回滚读"这条路——回滚是一次真实的写（见下）。
- **写**必须与 scope 已经存储的形态一致，两个方向都被拒绝（见「形态守卫」）。

`qm status` 报的是**存储形态**（`manifest_format` / `manifest_shards`），不是开关的值。

### 对象布局

```
manifest.json                        # 唯一提交点；可变对象，CAS；`format: 2`
  schema, format, workspace_id, project_id, seq, updated_at_ms
  shards: [ { shard, key, content_hash, path_count } ]
  predecessor: { format, seq, key, content_hash } | null

manifest/shards/<content_hash>.json  # 不可变分片，内容寻址
  schema, format, workspace_id, project_id, shard, pages, tombstones

manifest/archive/<content_hash>.json # 迁移前那一份整份对象；迁移只写不删（回收见"迁移与回滚"）
```

- **一次提交 = 一次 CAS**：写新分片（不可变、内容寻址），再 CAS 根指针。根指针的 `seq` 仍然
  "每次成功 CAS 恰好 +1"，`Manifest::next_seq` 的语义不变。
- **分片不可变**：输掉 CAS 的尝试留下孤儿分片，和今天的孤儿页面对象一样，不会留下半真。
- **根指针是唯一读到"当前状态"的地方**：拿到某一代 `shards` 就是拿到一致快照。
- **`format` 是判别字段**：一个 body 只有两种合法读法，判别读不出来就**拒绝**而不是猜。这条是
  必需的，不是装饰——根指针按整份 manifest 去读会解码失败，而"解码失败"远好于"读出一个空
  project"。

### 切分：path 的 SHA-256 首字节，固定 256 片

`manifest_shard_index(path) = sha256(path)[0]`，取值 `0..=255`（`MANIFEST_SHARD_COUNT`）。

- **只依赖 path**：分片是内容寻址且不可变的，如果索引依赖"scope 里有多少 path"或写入顺序，
  加一条 path 就会让别的 path 换片、连带重写每一片经过的分片。只依赖 path 就没有这个问题：
  一条 path 的分片在它的一生里不变。
- **固定片数而不是"每 N 条一片"**：根指针的大小由片数封顶（最坏 256 条引用），而不是随内容
  无限增长。代价是单 scope 的 path 上限变成"256 × 单片容量"，见「仍然没做到什么」。
- 只**物化非空分片**：几条 path 的 scope 只写几个对象，不是 256 个。

### 读路径怎么拼（实现状态）

| 操作 | 读什么 | 证据 |
|---|---|---|
| `commit_page` / `delete_page` | 根指针 + **目标那一片** | 插桩断言按 key 集合：一次按 path 的提交读 2 个对象（根 + 片），在 100 000 条 path 的 scope 上同样是 2 个 |
| `read_page` | 根指针 + 1 片 + 指向的版本对象 | 插桩断言按对象的 **key 集合**比较，不是只比条数 |
| `page_history` / `read_page_versions` | 根指针 + 1 片，然后沿 WAL 走链 | 同上 |
| `recent_pages` | 根指针 + **全部分片** | 全局按时间排序需要看全 |
| `digest` 的页面段 | 只读 commit log，不读 manifest | 与形态无关 |
| `verify` 的权威层检查 | 根指针 + 全部分片 + 每片的 archive 校验 | 报告 `shards` 与 predecessor 问题 |
| `gc` 的可达性 | 根指针 + 全部分片（一次读全，然后逐 path 走链时不再重读） | **根指针还在时**分片与 archive 进 live 集合；回滚成整份之后它们不再被引用 |
| `search` 的可见性校验 | 根指针 + **候选命中 path 所在的那几片**（同一片只读一次） | 插桩断言按对象的 **key 集合**比较：60 条 path 铺成 53 片时，一次 3 条候选的 search 只读 **4** 个 manifest 对象（根 + 3 片），而不是 54 个；整份形态仍恰好 1 个 |

读得少有一个直接后果，写下来免得被当成遗漏：**搜索不会发现它没读到的分片坏了**。可见性校验只碰
候选命中的片，所以别的片损坏（或键与内容哈希对不上）在搜索这里不报错——`qm verify` 是全量检查的
那一层，`read_page` / `page_history` 也仍然只保证它们各自读的那一片。这与按 path 的读
（`commit_page` / `read_page`）是同一条规则：读多少，就只对多少负责。

### 规模实测点

`sharded_manifest_scale_probe`（`cargo test -p qm-store --lib sharded_manifest_scale_probe -- --nocapture`）
把单对象那节用的同一份 100 000-path 夹具按 format 2 存了一遍，实测：

| path 数 | 非空分片 | 每片 path（最小/最大） | 最大单片字节 | 根指针字节 |
|---:|---:|---:|---:|---:|
| 1 000 | 251 | 1 / 10 | 2 526 | 58 014 |
| 10 000 | 256 | 25 / 61 | 14 928 | 59 424 |
| 100 000 | 256 | 335 / 442 | 107 911 | 59 681 |

- **分布是均匀的**：100 000 条 path 上最挤的一片 442 条、最松的 335 条（均值 391）。
  1 000 条时只有 251 片非空，因为只物化非空分片。
- **根指针不随 path 增长**：path 涨十倍（10 000 → 100 000）根指针只涨 257 字节，
  那是 `seq` 和 `path_count` 各多一位的代价；它的体积由**分片引用数**（上限 256）决定。
- **最大单片离上限还有 9.7 倍**：107 911 B 对 `MANIFEST_MAX_BYTES` 的整数商是 9。
- **单条 path 的读放大不随规模变化**：在同一个 100 000 条 path 的 scope 上，一次
  `commit_page` 读 2 个对象（根 + 片）、写 5 个（页面版本、WAL、**一片**、根、提交记录）；
  一次 `read_page` 读 3 个（根 + 片 + 版本）。与 60 条 path 时逐 key 相同。

这三条是**一个**实测点，不是一条曲线：100 000 条 path 是这里造到过的最大规模。

### 上限

- `MANIFEST_MAX_BYTES`（1 MiB）在 format 2 下按**单片**生效：约束的是"一次提交要移动多少字节"，
  而一次分片提交移动的是一片，不是整个 project。两条 path 各自 600 KB 时，整份 write 会被拒绝，
  分片 write 各自通过（`two_shards_can_hold_what_one_whole_manifest_cannot`）。
- 根指针有独立上限 `MANIFEST_ROOT_MAX_BYTES`（256 KiB）。最坏形状（256 片、**scope 名取布局允许的
  128 字符**、hash 取满长、`path_count` 取 `usize::MAX`）实测 **126 467 B**，由
  `a_root_with_every_shard_fits_under_its_ceiling` 钉住（断言"不超过上限的一半"，所以布局长一个字段
  会被测试抓到，而不是在迁移时才被拒）。这条上限不是拍出来的：最初按**短** scope 名量到 64 016 B 就
  定了 64 KiB，换成合法的最长名之后是 126 467 B——测试用的名字长度本身会把这个数字差掉一倍。
  正常 scope 名（`v1/ws/acme/proj/ai-memory/...`）下每片引用约 160 B，37 片时根指针约 6 KB。
- 两者都**在写出任何对象之前**判定：被拒绝的提交不留下页面对象、WAL 记录或孤儿分片
  （测试比较拒绝前后的桶内对象集合）。

### 迁移与回滚

```bash
qm migrate-manifest              # format 1 -> format 2（默认）
qm migrate-manifest --json       # from/to/shards/already_there/archive/manifest_seq
qm migrate-manifest --to 1       # format 2 -> format 1（回滚）
```

迁移的四个性质，以及它们各自靠什么成立：

1. **幂等**：分片键是内容哈希、根指针由它们确定，所以对**同一份状态**重复迁移写出同样的键与字节；提交点已是根
   指针时直接返回 `already_there`，连一片都不读。
2. **可中断/可续跑**：分片、archive、根指针都在那**一次** CAS 之前写完。在 CAS 之前任何时刻中断，
   提交点还是原来那份整份对象，留下的是孤儿分片；再跑一次复用它们（测试注入一次 CAS 失败，
   断言提交点仍是整份形态、scope 照常可读、续跑后对象数不再增长）。
3. **可回滚**：迁移**不删除**旧对象——它先把旧 body 复制到 `manifest/archive/<hash>.json`，再把
   `predecessor` 指向它。回滚是**再写一次**整份 manifest：把当前分片 materialize 成整份形态，
   走一次 CAS（不是"把指针换回旧对象"）。
   这一点是刻意的：从 archive 恢复会静默丢掉迁移之后的所有提交，materialize 不会。
   archive 与分片都留在桶里，但**live 的条件是根指针还在**：只要提交点还是分片形态，`gc` 就把每一片
   与 `predecessor` 指向的 archive 都算作"活"；一旦回滚成整份形态，它们不再被**任何东西**引用，
   `qm gc` 会在宽限期之后把它们回收——回滚本身**不删除**任何东西，`verify` 在那之前都能读到 archive。
   所以"回滚之后还能长期从 archive 做字节级考古"并不成立：要保留它，就别在回滚后的 scope 上跑
   `gc --apply`。
4. **迁移期间不阻塞读**：读按 `format` 分派，所以整份与分片两种形态同时可读。写竞争由 CAS 重试
   兜住（迁移与写者互不协调，见下）。

一个**从未提交过**的 scope 没有整份对象可转换：迁移会写一个空根指针，把形态**声明**下来
（`create`，不会覆盖期间落地的提交）。反之，一个 scope 也可以在 `QM_MANIFEST_FORMAT=2` 下由第一次
提交**出生即分片**：这种根指针没有 `predecessor`，也没有 archive。

### 形态守卫（两个方向都拒绝）

| 写入形态 | scope 存储形态 | 结果 |
|---|---|---|
| 1 | 1 | 正常，与 format 2 之前逐字节相同 |
| 2 | 2 | 正常 |
| 2 | 1（已有内容） | **拒绝** `ManifestFormMismatch`：新根指针只会命名这一次写的片，scope 里已有的 path 会全部从 project 消失 |
| 1 | 2 | **拒绝** `ManifestFormMismatch`：materialize 再写整份不会丢 path，但会**静默换掉** scope 的形态（一台用默认设置偶然写一次就会翻转），下一次分片写又要被拒 |
| 2 | 无提交点 | 允许：没有东西可丢，第一次提交可以出生即分片 |

报文直接给出出路（`qm migrate-manifest` 或"用存储形态写"），并且拒绝发生在写出任何对象之前。

### 仍然没做到什么

1. **单 scope 的 path 上限只是变成了另一个上限**：约 **1.1×10⁶** 条 path。这是**外推**，
   但外推的依据是一个实测点而不是一个常数：100 000 条 path 时最大的一片是 107 911 B，即上限的
   1/9.7，照此一片约 4 300 条 path、256 片约 1.1×10⁶。**没有实测到 1.1×10⁶**（那是 10 倍于
   已知夹具的量级），也没有分层结构。它依赖两个假设：分片分布的均匀度保持（哈希分片对均匀
   path 集合成立，对对抗性 path 集合未验证），以及每片每条 path 的边际字节不变（实测
   242–244 B，随 `seq` 位数缓慢变化）。再往上需要分片分层或按 path 范围再切一层。
2. **迁移不是在线免竞争的**：迁移的最后一次 CAS 会与并发写者互相冲突，靠双方重试收敛，没有
   "迁移期间暂停写"的协调机制，也没有"迁移中"的状态位。竞争的两个方向都有测试，且时序是夹具
   选定的而不是碰运气：写者在迁移的读与 CAS 之间落地时，迁移重读重算（`attempts == 2`）且赢的
   那次提交仍在最终状态里（`seq` 无空洞、`verify` ok）；写者在 CAS 之后才开始时，按形态分别
   落进新形态，或用 `ManifestFormMismatch` 被响亮拒绝、`seq` 不动。另一个测试在每个原子上采样
   一次完整读：迁移过程中**没有任何时刻**提交点指向一个还不存在的对象。
   **已有持续竞争压测，但仍不是在线结论**：`sustained_contention_keeps_every_commit_and_every_sequence`
   以 8 个写者 × 50 轮并发启动迁移，量到 400 次成功提交、提交点 1 581 次写入 / 1 180 次 CAS 拒绝、
   迁移 5 次尝试后提交；断言覆盖无丢写、`seq` 恰为 `1..=400`、分片 `path_count` 之和正确且 `verify` ok。
   这些数字来自 `InMemory` + 单线程确定性调度（复跑命令与边界见 `docs/ops.md`「持续写入下的迁移（压测点）」），
   不能替代真桶多核 RTT 下的持续冲突、收敛时间或重试分布测量。
3. **出生即分片的 scope 没有 archive**，因此没有"字节级回到迁移前"的东西——它本来就没有迁移前。
4. **format 1 的写者不能写 format 2 的 scope**（只能读）。混合部署时，升级顺序是先升读侧、
   要写新形态的人显式设置开关。
5. **真 S3/R2 未验证**：本工作项全部断言跑在 `InMemory` 上，桶的 listing/写入与真实 RTT 未端到端。
6. **跨平台未验证**：只在 macOS 本机跑过。

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
所以 MCP 服务器当前暴露的 25 个 `memory_*` 工具由同一套分发提供。

```bash
qm capture --session sess-1 --kind tool_use --text "switched to tantivy splits"
qm consolidate --session sess-1        # 编译成 sessions/sess-1.md
qm publish                             # 把当前页面发布成一个分片
qm maintain                            # 一次性 drain + 编译所有会话 + 按需 publish
qm search "tantivy" --json
qm write-page --path notes/raft.md --body "leader election"
qm read-page  --path notes/raft.md
qm delete-page --path notes/raft.md
qm compact                             # 租约保护的全量重建
qm status                              # pages / tombstones / manifest 形态 / splits / sessions
qm migrate-manifest                    # manifest 整份 <-> 分片（--to 1 回滚）
qm sessions
```

- 作用域：`--workspace` / `--project` / `--writer`（或 `QM_*` 环境变量），默认 `default/default/machine`。
- 凭据：`QM_S3_*`（或 `R2_*`）必须在环境里；拿不到就**报错**，不猜、不降级成本地存储。
- 每条命令都重新读取权威状态：命令之间不缓存，这是多机共享的前提。
- `--json` 输出机器可读结果，便于被 agent 或脚本直接消费。
- `maintain` 是 CLI 运维面的一次性闭环：drain spool → 当前 scope 的所有会话
  consolidate → publish。它不是 daemon，也不进入 hook 的 200ms 快速路径；单个会话失败会继续、
  汇总后非零退出，租约冲突按 `skipped_locked`、空 session 按 `skipped_empty` 计数。
  `--drain-limit` 先按当前 scope 过滤再应用；其他 scope 不消耗预算。`spool_kept` 是其他
  scope、超过 limit 的当前 scope 条目、坏条目和重放失败的总数；`published` 只表示本次真的
  新增 split，且 `publish_error` 必须为空。连续运行是幂等的，不重复版本或分片。
- 命令逻辑（而非仅参数解析）在内存桶上做了端到端测试：capture → consolidate → publish → search、
  页面的写/读/删、status/sessions、作用域解析。

**MCP 服务器**（`qm-mcp`，stdio）：

- **25 个 `memory_*` 工具**：`capture / consolidate / search / write_page / read_page / delete_page /
  publish / compact / sessions / status / history / restore / log / recent / digest /
  handoff_open / handoff_list / handoff_claim / handoff_done / verify / compact_session /
  propose / proposals / approve / reject`，沿用 ai-memory 的命名习惯。
- **每个工具都走 `qm_cli::execute` 这条同一个分发**，因此两个面不可能漂移：工具 = 类型化参数 + 一次调用。
- 协议层有真实回环测试：拉起 `qm-mcp` 二进制，走 `initialize → tools/list → tools/call`，
  断言工具齐全且都有描述与 inputSchema，跑通 capture → consolidate → publish → search，
  并确认非法路径返回**错误而不是伪成功**。
- 该测试用 `--synthetic-bucket`（进程内、非持久、**不是后端**，启动时向 stderr 打警告）。
  真后端仍由带凭据时运行的 `cas-conformance`、`manifest-probe`、`search-probe`、`session-probe` 等探针验证。

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

## 6.22 最近变化摘要（digest）：三张权威清单

`qm recent` 回答"哪些页面最近改过"，但一个刚接手的 agent 还要知道三件不同的事：哪些提交发生了
（**包括删除**）、哪些会话还在动、有没有留给自己的接力棒。`digest` 把这三件事一次问完：

```bash
qm digest [--since-ms N | --hours N] [--limit N] [--json]   # 默认 24 小时 / 每段 20 条
```

- `ProjectStore::digest(ws, proj, since_ms, limit) -> Digest { pages, sessions, handoffs }`，
  三部分都从**权威对象**读，不碰索引、不碰缓存：pages 来自 commit log（`commits/<seq>.json`）、
  sessions 来自各会话 head（`SessionSummary { session_id, observations, last_seen_ms }`，
  `observations` 就是 head 里的 `count`）、handoffs 来自 `handoffs/<id>.json`。
- **删除是提交，不是缺席**：`pages` 同时保留 `PageWritten` 与 `PageDeleted`。
  只留"还存在的页面"会把"这个被删掉了"洗成"什么都没发生"，而恰恰是前者更需要下一个会话知道——
  manifest 已不再知道那条路径，digest 仍然知道。
- **提交日志是建议性元数据**：提交与日志之间崩溃会少一条记录，digest 因此少报一条，
  但它**不会报错、也不会报出不存在的东西**——它只回答"什么时候"，从不回答"现在什么是真的"。
- **窗口按各自的时钟**：pages 按提交时间、sessions 按 head 的 `updated_at_ms`、
  handoffs 按 created/claimed/finished 三者中**最新**的那个——所以"窗口之前开、窗口之内被认领或收尾"的接力棒会出现，
  这正是它存在的意义。三部分各自降序，任何一部分为空都是正常答案（`Digest::is_empty()`），
  没有变化是**空 vec**，不是错误。
- **`limit` 是"每段"的上限**：`pages` / `sessions` / `handoffs` **三段各自**先排序、再各自截断，
  保留各自最新的那端。一个只有 `pages` 被封顶、另外两段可以无限增长的"摘要"不是摘要，
  所以 CLI 的 `--limit`、MCP 的 `limit` 与常量文档都写 "per section"——文案与实现必须一致。
  `pages` 的排序是 `at_ms` 降序、`seq` 破平局：`at_ms` 是**调用方提供的时钟**，时钟偏斜下"提交顺序"与"时间顺序"
  可能不一致，这里的契约是时间顺序，而 `seq` 作为 tie-breaker 让顺序成为**全序**——
  两台机器读同一份日志必须对顺序一致，而不只是对集合一致。
- **读代价是有界并发，不是 O(1)**：`digest`（以及共用 `read_commit_log` 的 `qm log`、
  `qm history`、`version_at`）读**整条** commit log，`limit` 只封顶返回、不封顶读取，
  所以读的对象数仍随提交数线性增长。`read_commit_log` 把这批读用 `buffer_unordered(16)`
  重叠起来，延迟从 `N × RTT` 降到约 `⌈N/16⌉ × RTT`。
  并发**不改变语义**：顺序来自上面的排序（`at_ms` 降序、`seq` 破平局），不来自读的完成先后；
  相应的测试故意让最旧的提交最先读回来，再断言答案不变。
- `--since-ms` 与 `--hours` **互斥**而不是按优先级静默取一个；人类输出分 `pages` / `sessions` / `handoffs`
  三段，整窗无活动时输出一句话而不是三个空标题。
- MCP 对应 `memory_digest { since_hours?, limit? }`（共 25 个工具）。MCP 的窗口在**调用时**按墙钟解析，
  而不是按 server 启动时钟——否则长驻 server 的回看窗口会冻在启动那一刻。
- **S3 协议层的跨进程证据**：`digest-probe` 的 `seed` / `read` 是两个独立进程，`--workspace` /
  `--project` 都是必填项；桩测试用唯一 scope 证明参数确实进入键布局，并证明裸命令会被 Clap 拒绝。`seed` 通过真实
  `object_store` S3 客户端提交两页、删掉其中一页、提交两个会话 head，并留下三根交接棒：一根
  **开 → 认领 → 收尾**（400/900/3200ms，即开与认领都在 2000ms 窗口之外、**收尾**在窗口之内）、
  一根只开不认领（2800ms）、一根全部阶段都在窗口之前（300ms）。`read` 只有桶坐标，必须把三段重新
  组装出来，并同时报告 manifest 仍认账的 live pages——所以「删除还在 digest 里、而 manifest 已经不返回
  那条 path」是被断言的事实，不是对代码的转述。
  `seed` 写入前先列出目标 scope，只要已有任何对象就 fail-loud，不覆盖也不自动清理；这是 best-effort
  预检，不是并发锁。测试另行证明
  非空 scope 的第二次 seed 被拒绝且对象集合逐字不变、不同 scope 的 read 互相不可见。
  `crates/qm-probe/tests/digest_probe_stub.rs` 还钉住：`at_ms` 时钟序与 commit log 的 `seq` 序
  **故意不一致**（只按日志顺序返回就会红）、交接棒的窗口与排序都取 created/claimed/finished 的**最新阶段**
  （收尾那根因此进窗并且排在只开不认领的那根**之前**，尽管它开得最早、认领得最早）、per-section 窗口与
  `limit`，以及一条「第一条 listing 当成整份 listing」的故障对照——同一个谓词在健康腿为空、在故障腿非空。
  这是协议层（`S3Stub`）证据，**真 R2 仍未验证**（§11）。

## 6.19 最近变化：会话开始时先看这里

agent 开场最常问的问题不是"搜点什么"，而是"上一个会话在做什么"。这不需要检索，
只需要**已经写在提交点里的顺序**：

- `ProjectStore::recent_pages(ws, proj, limit)` 读 manifest，按 `PageEntry::created_at_ms`
  **降序**返回 live 页面，同毫秒按 `path` **升序**打平。
  `created_at_ms` 是**该页最后一次提交**的时间，所以重写过的页面会浮到最前。
  tie-breaker 是纵深防御：manifest 是 `BTreeMap`、`sort_by` 又是稳定的，今天同毫秒本来就落在 path 升序上；
  把它写进比较器是为了让顺序在容器或排序实现变化之后**仍然是全序**——而"全序"正是两台机器必须达成一致的东西。
  比较器本身有直接单测（删掉 tie-breaker 就会红），集成测试只负责钉住方向。
- 被 tombstone 的路径**天然不在 `manifest.pages` 里**，所以这里不额外造"删除状态"去过滤它——
  删除的唯一权威仍然是提交点。
- `qm recent [--limit N]`（默认 10）/ `memory_recent { limit? }`：人类输出 `created_at_ms\tpath\ttitle`，
  `--json` 给结构化字段。两者共用同一次 `execute()` 分发。
- 它**不读任何页面对象**：标题已经在 manifest 里，所以这是"读一个对象即可回答"的问题，
  比跑一次检索便宜得多——这也是它在 agent 开场流程里的位置。

## 6.18 提案与审批：学习性修改不直接落盘

借来的设计里，自动化（curator/auto-improve）**只能提议**，不能直接改记忆；有权限的才好批准。
这一条在对象存储上同样便宜：

```bash
qm propose --path notes/raft.md --title Raft --body "..." --rationale "clearer"
qm proposals [--state pending|approved|rejected|all]     # 默认只看 pending
qm approve --id <id>      # 批准并应用
qm reject  --id <id> --note "not this time"
```

- 提案是 `proposals/<id>.json`，内容寻址（`(path,title,body,rationale,created_at)` 派生）→ 重复提议同一修改是 **no-op**。
- **在批准之前，目标页面一点没变**（有测试断言正文与 manifest `seq` 都没动）。
- 批准的顺序是刻意的：**先 CAS 抢占决定权**（`Pending → Approved`），**再**通过普通页面提交路径应用修改。
  所以两台机器同时批准只有一个成功，输家收到 `proposal ... is not pending`；应用后的页面 id 事后再写回提案——
  中途崩溃只会留下"已批准、id 未知"，**决定不会丢**。
- 应用走的是普通 `commit_page`：因此仍然 supersede 而非覆盖，历史与回滚照旧可用。
- 拒绝同样是一次 CAS，只写决定，不碰页面。
- MCP 对应 `memory_propose` / `memory_proposals` / `memory_approve` / `memory_reject`（共 25 个工具）。

## 6.17 新近度先验：它承诺什么、不承诺什么

借来的设计在融合之后还有一个**有界的权威调整**（新近度/来源权重）。我们的实现：

```bash
qm search "catalog"              # 默认带新近度先验
qm search "catalog" --no-recency # 关掉
```

- 形式：`multiplier = 1 + max_boost · 2^(−age/half_life)`，默认半衰期 30 天、`max_boost = 0.5`，
  即**最新最多 +50%**、越旧越接近 1.0；`now_ms = 0` 或半衰期 ≤ 0 表示关闭。
- 命中会同时给出 `fused_score`（调整前）与 `recency_multiplier`（调整倍数），排序依据是调整后的 `score`——
  先验是**可见的**，不是暗箱。
- **必须说清楚的边界**：RRF 的分数是压缩的（k=60 时 rank 1 与 rank 2 只差约 1.6%），
  所以任何正的加成都能翻转相邻名次。它因此是"**相关性相当的页面之间的破平局者**"，
  不是"强烈偏好新的"。实测语义（有测试断言）：
  - 两条同样相关的页面，新的在前；
  - **跨流一致**（正文 + 实体两路命中，分数约两倍）的旧页面仍然领先于只命中一路的新页面——
    更强的相关性信号不会被新近度推翻；
  - 关闭时所有 `recency_multiplier` 恒为 1.0。
- 工作区（`--global`）检索只在**全局融合之后**应用一次先验，避免同一先验被计两次。

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
| 凭据泄露 | 当前形态（每机全桶 token）下全桶可读写；轮换凭据，是否改用网关见 §9（**待拍板的决策项**） |

## 9. 凭据与授权（**待用户拍板**：决策就绪分析）

> **这一节是决策项，不是实现说明。** 三个选项都还没有被批准，代码里也没有任何一侧的实现
> ——没有网关、没有 token 发放流程、没有按前缀分权的鉴权层。S0 打协议时用的是「每机全桶
> token」（下文的选项 A），只因为那不引入新组件；**这不等于它已被选为最终方案**。
>
> 拍板之前，`quickstart.md` 与 `ops.md` 描述的行为（机器持 `QM_S3_*` 直连桶）就是当前实现。

### 9.1 先把边界钉住

这次决策被 §2 的三条硬约束夹住，三条都直接相关：

- **硬约束 #1「没有常驻服务」**：任何「单 indexer / 单 coordinator / 单 metastore writer」
  的假设都不成立。
- **硬约束 #2「没有稳定拓扑」**：只能靠桶里的对象发现彼此，不能靠固定主机名。
- **硬约束 #3「没有分布式事务」**：唯一的原子原语是**单对象的条件写**（§5 的 ETag CAS）。
  授权方案**不能**成为正确性的第 4 个前提——它只能决定「谁被允许发起这次 CAS」。

判据还来自 §1 目标里的两句，它们是这次决策的验收口径：

- 「**每台机器都能独立搜全量**：不需要任何常驻服务、协调者或对等发现」；
- 「每台机器都能写：hook 即发即忘、有界、不阻塞 agent」。

**现状（= 选项 A 的形态）**：`quickstart.md`「多台机器」明写「多台机器用**同一套桶 +
不同的 `QM_WRITER`** 即可，**无需任何服务器**」。凭据入口是四个环境变量名表
（`QM_S3_*`，回退 `R2_*`）：`S3_ENDPOINT_ENV_NAMES` / `S3_BUCKET_ENV_NAMES` /
`S3_ACCESS_KEY_ENV_NAMES` / `S3_SECRET_KEY_ENV_NAMES`，由 `build_bucket_from_env`（薄壳）
委托给 `build_bucket_from`（注入缝）解析。

### 9.2 三个选项

| | 形状 | 机器持有什么 |
|---|---|---|
| **A 每机全桶 token** | 现状：机器 → 桶 | 一把（或每机一把）能读写整个桶的 S3 凭据 |
| **B Worker 网关** | 机器 → 网关 → 桶 | 到网关的凭据/会话；桶凭据只在网关 |
| **C 混合** | 控制面经网关、数据面直连 | 网关凭据 + 网关签发的短时直连凭据 |

C 有两种子形态，**代价完全不同**，必须分开谈：

- **C1 读直连 / 写经网关**：网关只在**提交点**做准入；
- **C2 按前缀分权**：网关签发只覆盖某个 `KeyLayout::scope_prefix` 的直连凭据。

### 9.3 对照（每条给本仓库证据）

#### 9.3.1 与硬约束的关系

- **A 不新增架构角色**，与硬约束 #1 相容（`quickstart.md` 那句「无需任何服务器」）。
  它的代价全在密钥侧，不在结构侧。
- **B 把「读」也放进网关时与 §1 直接冲突**（见 9.4）。
- **C1 只把「写」的控制面放进网关**：网关不持有权威状态——权威状态仍是 §3「唯一提交点是
  每个 project 的 `manifest.json`」。若它把协议也接管（代写 manifest / 代持 catalog），
  它就变成了硬约束 #1 点名的 metastore writer，直接冲突。
- **C2 的粒度被 `KeyLayout::scope_prefix` 限死**：一个 scope 下同时住着 manifest、WAL、
  页面版本、会话链、catalog 与分片；而 `qm gc` 要跨整个 scope 删（§6.8 可达性分析），
  `qm verify --global` / `qm search --global` 要跨 project 读。所以「按前缀分权」能做的
  最细粒度是**一个 scope（workspace/project）**，不是「一个 path」，也不是「只读」。
  要更细，得先引入一个不是 S3 前缀 ACL 的授权层——那是新的架构决定，不在本项范围。

#### 9.3.2 可用性

- **A**：没有新的失败点。桶不可达时写路径由 §6.9 的 spool 兜住（`qm hook-drain` 重投，
  `ops.md`「失败模式」：桶不可达 → hook 落 spool）。检索侧要诚实：分片内容命中缓存就
  不再访问桶，但 catalog head 与 manifest 仍要读桶（§6.7「每条命令都重新读取权威状态」），
  **所以桶不可达时检索不可用**——A/B/C 在这一点上没有区别。
- **B**：网关成为**写的硬依赖**。降级路径已经存在且有测试（§6.9「永不阻塞、永不失败：
  200ms 内拿不到桶就落本地 spool」），所以写侧是「降级」而不是「丢数据」；
  但 §1 的「每台机器都能写」会退化成「每台机器都能 spool」。读侧若也经网关，
  则机器离线或分区时**整机不可搜**，与 §1 冲突。
- **C1**：读路径与 A 完全相同（不经过网关），写侧与 B 相同（可 spool 降级）。
- **C2**：与 A 相同（授予的是直连凭据，没有新增常驻依赖）。

#### 9.3.3 密钥生命周期

- **A**：轮换 = 换 `S3_ACCESS_KEY_ENV_NAMES` / `S3_SECRET_KEY_ENV_NAMES` 里的值，每台机器
  各自更新；吊销 = 在桶侧吊销该 key。**隔离**取决于后端能否做到「每机一把 bucket-scoped
  key」——本仓库对此**没有证据**，S3/R2 的 IAM 细节是外部事实。本节沿用既有口径
  「token 只能按桶授权」，于是**泄露半径 = 整个桶**：凭据覆盖的是整桶、不区分 scope，而机器
  可以被指向任意 scope——`qm gc` 每次只作用在**一个** workspace/project（`gc_orphans` 只在
  `KeyLayout::scope_prefix` 上 list），`qm verify --global` / `qm search --global` 的
  `--global` 是「这个 workspace 里的每个 project」。凭据里没有东西能把这台机器限制在
  「它真正需要的那几个 scope」上。
- **B / C1**：机器上**不再有桶凭据**，泄露一台机器不泄露桶；吊销是收回网关凭据，粒度可以
  做到「每机 / 每个 scope」。代价是**网关成为新的高价值目标**——这是把风险搬家，不是消灭。
- **C2**：介于两者之间，泄露半径收窄到某个 `scope_prefix`；但短时凭据需要签发与刷新流程
  （见 9.6），而 `qm publish` / `qm compact` 是长时间任务，运行中过期要有明确行为。

#### 9.3.4 审计与归因

现有能力（**是诚实记录，不是可验证归属**）：

- `WriterId` 已经写在权威层与派生物里：`PageEntry.writer_id`、`WalEntry.writer_id`、
  `Tombstone.writer_id`、`SplitEntry.writer_id`、`CommitRecord.writer_id`；
  协作面上还有 `Handoff.created_by` / `claimed_by`、`Proposal.created_by` / `decided_by`、
  `Lease.owner`。
- `qm history` / `qm log` 能由这些字段回答「哪台机器在什么时候写了什么」（§6.5、§6.11）。
- `WalEntry` 是内容寻址的不可变记录（§6.5），能证明「这份内容存在过」。
- **但 `QM_WRITER` 是一个环境变量**（`--writer`，默认 `machine`）：谁都能把 `writer_id`
  写成别人的名字。所以今天的归因是「**自述**」，不是「**认证**」。

网关能补的：把「谁在写」变成它**认证过**的 principal，并把 `writer_id` 与那个 principal 绑定，
于是 §6.11 的历史与 §6.10 的交接棒归属才有对抗性含义。

网关**补不了**的（C1 的固有缺口，必须写清）：数据面是直连的，桶看到的仍是那把直连凭据。
除非短时凭据本身带 per-machine 标识，否则页对象/WAL/分片这些**直连写入**的归因仍靠
`writer_id` 自述。C1 改善的是**提交点的准入**，不是全部写入的归属。

#### 9.3.5 代价

- **A**：零新组件、零新跳数。一次提交仍是 §6.5 的「读 manifest + 写页对象 + 写 WAL +
  一次 CAS」。运维成本 = 分发与轮换一把桶级凭据。
- **B / C1**：写路径每台机器多一跳（只在控制面）。运维成本 = 网关部署 + 凭据签发/刷新 +
  网关自身的可用性运维。对照 `ops.md`「成本与容量」：R2 出口免费、写入按 Class A 计费，
  **经网关中转大对象会改变这份成本结构**，所以数据面不应中转。
- **C2**：与 A 相同的跳数（直连），代价转移到签发流程。

#### 9.3.6 与既有实现的接缝

- **`build_bucket_from_env` / `build_bucket_from`**：目前签名的形状是「端点 + 桶 + 静态 key
  对」。换 B/C 时要么让网关**说 S3 协议**（机器上 `QM_S3_*` 的形状不变，只有端点变成网关
  URL），要么加一种新的鉴权模式。`build_bucket_from` 是**唯一构建桶客户端**的注入缝，改它
  不会碰到 CLI/MCP 的调用点；但环境解析不止这一处——`bucket_identity_from`（发布 watermark
  的身份）与 `manifest_format_from` 是同一形状的另两个缝，凭据形状一变要一起看。
- **`bucket_identity_from`（= `endpoint \0 bucket`）→ 发布 watermark**：端点是
  `QM_CACHE_DIR` 下 publish watermark 键的一部分（`ops.md`「发布与缓存」：换桶要配独立
  cache dir）。**如果网关换掉了机器看到的端点，bucket identity 就变了**，于是切换后每台机器
  第一次 `qm publish` 都做一次全量发布——一次性、安全（不会漏发），但会放大 Class A 写入。
  拍板时必须把这条算进切换成本。
- **`--synthetic-bucket`**（只在 `qm-mcp` 的 `main.rs` 上有这个开关）：它**完全绕开**环境
  变量，用进程内 `InMemory`，并在启动时向 stderr 打警告。所以它**不可能**用来验证授权
  ——它连真实 HTTP 都不走，也没有可被拦截的凭据。探针侧没有这个开关：`qm-probe` 一律经
  `S3Config::from_env` 打真 socket（没有凭据时报错而非跳过，§11）。授权验证因此需要
  一个**真 socket** 的形态：`qm-probe` 的最小 S3 stub（§5.1「协议层验证」）可以扩成
  「带鉴权的 stub」，或直接对真桶跑。
- **`qm-probe` 的 `S3Config::from_env`**：探针自己另有一份 `QM_S3_*` 解析（与 CLI 的常量
  分开），所以凭据形状一变，**两个地方都要改**（`ops.md`「验证状态」：有 R2 凭据时先跑
  `cas-conformance`）。

### 9.4 与硬约束直接冲突的形态（点名）

- **「读也经网关」与 §1 直接冲突。** §1 的目标句是「每台机器都能独立搜全量：
  **不需要任何常驻服务**、协调者或对等发现」。网关一旦落在读路径上，这个目标就不成立
  ——机器离线或分区时不可搜。**这条排除了 B 的全量形态，也排除了把读塞进网关的 C 变体。**
- **「网关代写 manifest / 代持提交点」与硬约束 #1 直接冲突**：那会让网关成为 metastore
  writer，而硬约束 #1 明确说这类假设不成立。网关只能做**准入**，不能做**提交**。
- **「假设网关常驻在某台机器上」与硬约束 #2 冲突**：不能靠固定主机名发现它；网关地址必须
  是配置（环境变量/静态配置）。这不阻塞，但意味着网关不可用时没有自动故障转移。

### 9.5 建议（**待用户拍板**）

**建议：C1 —— 读直连桶；写路径的提交点准入经网关；数据面用网关签发的短时凭据直连。**

一句话理由：**它只给「写」的控制面加了一个可选的把关人**，因此 §1 的「任何一台机器独立搜
全量」与 §2 三条硬约束都原样成立；而它换来的正是现有实现最缺的那样东西——把自述的
`QM_WRITER` 变成网关**认证过**的 principal。

被否决选项的代价：

- **否决 A（即选 C1/B）的代价**：多一个组件、写路径多一跳、网关成为新的高价值目标，
  还要一套凭据签发/刷新流程。换来的是「泄露一台机器 ≠ 泄露全桶」与提交点上的可验证归因。
- **否决 B（= 不把读放进网关；这么一改它就退化成 C1）的代价**：放弃「读也有统一鉴权与
  限流」，换来的是离线性——那是本项目最核心的生命线（§1）。
- **若最终选 A**，必须接受：`writer_id` **永远**只是自述（§9.3.4），并且 `ops.md`「失败模式」
  里那行「凭据泄露 → 全桶可读写」就是终局口径；唯一的缓解是轮换与桶侧最小权限，
  而「后端能否做到每机一把 bucket-scoped key」本仓库**没有证据**。

**本节不构成批准。** 用户拍板之前，9.6 的工作项都不立项。

### 9.6 拍板后需要的工作项

**若选 C1（建议方案）：**

1. **网关的最小接口**（只定接口，不实现协议）：机器侧只需要两个动作——(a) 为一次提交点 CAS
   申请准入，(b) 申请一次数据面直连的短时凭据。接口必须**无状态**且可降级：网关不持有
   manifest / catalog / WAL 的任何状态（硬约束 #1）。
2. **每机 token 的发放与轮换流程**：机器注册 → 签发机器凭据 → 轮换 / 吊销；明确
   「网关不可用时写路径降级到 §6.9 的 spool」，而不是报错失败。
3. **`build_bucket_from` 的接缝改造**：它是**唯一构建桶客户端**的注入缝（环境解析另有
   `bucket_identity_from` 与 `manifest_format_from` 两个同形状的缝，见第 4 条）；同一改动
   要覆盖 `qm-probe` 的 `S3Config::from_env`（今天两份解析不共享）。
4. **`bucket_identity_from` 的端点迁移决定**：网关 URL 是否参与 identity；若参与，
   切换那天要预期一次全量发布（§9.3.6）。
5. **探针怎么验证**：给 `qm-probe` 的 stub 加鉴权（401/403 + 带 `WWW-Authenticate` 的
   错误 XML），再跑 `cas-conformance`。**不能用 `--synthetic-bucket`**——它绕开网络。
   验收必须含反向对照：**拿掉凭据必须被拒**，而不是被跳过（§11「探针在缺少凭据时
   报错而非跳过」是既有口径）。

**若选 A：**

1. 把「每机一把 bucket-scoped key 是否可行」写成后端的**实测项**（本仓库没有证据）。
2. 在 `ops.md` 补轮换与吊销的操作手册。
3. 把「`writer_id` 是自述」写进 `docs/`，避免它被当审计依据。

**若选 C2：**

1. 先确认后端的前缀级 ACL 能力（同样是外部事实，本仓库无证据）。
2. 定短时凭据的时长与刷新：`qm publish` / `qm compact` 是长时间任务，中途过期要有明确行为。
3. `--global` 类命令（`qm verify --global`、`qm search --global`）要一次持有多个 scope 的
   凭据，所以网关的签发接口要能**一次给一组**，而不是一次一个。
4. 见 C1 的第 3、5 条（接缝与探针）。

### 9.7 落地前必须实测（S0）

无论选哪个：Workers 的 R2 binding 条件写语义、presigned URL 与 `If-Match` 的组合是否可靠，
都必须先测。当前 S0 用全桶 token 打通协议，**凭据方案不阻塞协议验证**。

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
| **Quickwit 真分片** | 官方 `quickwit/quickwit:0.9.0` 容器产出 6893 字节分片 → `split-probe` 解包 8 个文件 → 本仓 tantivy **检索命中 1 条**（无 Quickwit 集群参与） |

**这次验证覆盖了什么**：真实 HTTP + S3 协议路径（签名、endpoint、path-style、条件头、412 语义、列目录）、真实网络下的 CAS 冲突与重试、
mTLS 之外的完整读写链路、多机协作语义。

**这次没有覆盖什么**（保持诚实）：

- **R2 特有行为**：MinIO 的 PUT 不返回 `x-amz-version-id`（探针输出 `version=None`），
  所以"PUT 带 version、GET 不带"那条 R2 教训没有被这次运行触发。CAS 层按 ETag 判定并拒绝无 ETag 的后端，
  逻辑上已经对这种情况免疫，但仍建议在真 R2 上再跑一次 `cas-conformance`。
- **Quickwit 二进制**：~~仍是格式级验证~~ —— 已用官方 0.9.0 容器产出的真分片完成字节级验证（见上表）。

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
- 定时收口：`qm maintain` 复用同一 spool drain、consolidate 与 publish 路径；没有常驻进程，
  hook 的 200ms / 202 / 429 语义不变。

- 最近变化摘要（digest）：三张**权威**清单（commit log / session head / handoff）各按自己的时钟取窗口、
  各自降序、各自截断；交接棒的时间取 created/claimed/finished 三者中**最新**的那个（所以窗口之前开、
  窗口之内收尾的棒会出现）；删除是提交而非缺席，manifest 已不再返回的 path 仍以 `PageDeleted` 出现。
  跨进程 S3 协议层证据见 §6.22。

- 回收：可达性分析 + 宽限期 + dry-run 默认；live 页面、历史链、会话链在回收后仍可读。
- 鉴权/凭据方案仍未定（每机全桶 token vs Worker 网关 vs 混合），是**决策项**而非实现项：
  决策就绪的选项、对照与建议见 §9（**待用户拍板**，本文不构成批准）。

- `qm` CLI 29 个顶层子命令可用（含 `handoff` 的 4 个子动作）；命令逻辑在内存桶上做了端到端测试（无需凭据）。
- MCP stdio 服务器已实现并通过协议级回环测试（25 个工具，与 CLI 同一分发）。

**S4（采集与编译）**

- 捕获：3 批观测 → head `generation=3`、`count=6`；重复提交最后一批被识别为 no-op（不新增段）。
- 两台机器并发捕获同一会话：两批都落地（`count=2`/`generation=2`），读侧按 id 去重后正好两条。
- **脱敏在入口生效**：调用方直接塞 `api_key=abcd1234` 也存不进秘密，且落库的 `observation_id` 与落库字节一致。
- 会话互相独立：三会话并存可按 LIST 发现；只有段没有 head 的会话**不可见**。
- 编译：3 条观测 → 一页 `sessions/<sid>.md`（正文含全部文本）→ 发布分片后**可被检索**；
  链没变时重编译 `already_up_to_date` 且不消耗 manifest `seq`；另一台从未见过该状态的机器可编译；
  空 session 报 `nothing_to_compile`，租约被占报 `lease_held`，两者都不写页面。

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
- **format 2（分片 manifest，opt-in）**：同一条写入序列在整份与分片两种形态下，
  `read_page` / `page_history` / `version_at` / `recent_pages` / `digest` / `is_current` 逐条相同
  （表驱动比较，含被删 path 与从未存在的 path）。插桩断言按 path 的读只取"根指针 + 1 片"，
  而 recency listing 取"根指针 + 全部分片"。并发提交仍只一个 CAS 赢：输掉的那次留下孤儿分片，
  `seq` 不被消耗（三个成功 CAS 拿到 1/2/3，无空洞）。迁移幂等、可中断续跑、可回滚
  （回滚 materialize 当前状态，不丢迁移之后的写入）；两个方向的形态错配都在写出任何对象之前被拒。
  见 §6.21。
- 多机场景探针 `manifest-probe`（先做 CAS 预检，再跑场景并全量复核）；local 后端在预检阶段被明确拒绝。
  WAL 那一项按**覆盖**判：每个已提交版本都必须能在 WAL 里找到（按 page id 做集合包含），
  **不是**条数相等——`read_wal` 返回的是桶里有的集合，丢失 CAS 的尝试会留下 manifest 从不指向的记录
  （见 `docs/ops.md`），所以"多一条"是预期形状、"少一条已提交版本"才是失败。
  这一项特意放在链路复核**之前**并在缺记录时结束场景：supersession 链是通过 WAL 记录走的，
  缺记录会让链路复核只抛一句 `object not found`，而覆盖检查能点名缺的是哪个（或哪几个）版本。

已确认的事实（影响选型）：

- `quickwit-*` 系列 crate 在 crates.io 上最后发布是 **0.3.0（2022-06）**，与线上运行的 v0.9.0 差一大截；
  因此不能把 Quickwit 作为库依赖，只能作为**外部服务/构建器**。
- tantivy 0.26 的 `TopDocs` 必须先 `order_by_score()` 才能作为 collector 使用。

待验证（只剩真 R2 凭据；Quickwit 官方 0.9.0 容器产出的真分片读取验证已完成）：

Quickwit 官方 `quickwit/quickwit:0.9.0` 产出的真分片已于 2026-09-15 验证（6893 字节、解包 8 个文件、
检索命中 1 条；见 §6.4/§10.5），因此不再是待验证项。以下只剩真 R2：

1. 真 R2 上跑 `qm-probe cas-conformance`（ETag 稳定性、陈旧 ETag 拒绝）—— **协议层已验证（stub），真 R2 待验证**
   （无凭据）：`s3-stub` 起真 socket、`object_store` 的 S3 客户端打上去，条件头（`If-None-Match: *` /
   `If-Match`）、`404 NoSuchKey`/`412 PreconditionFailed` 错误 XML、`ListObjectsV2` 分页、409 冲突重试、
   两条故障注入对照都已被断言；见 §5.1 与 `crates/qm-probe/tests/s3_protocol.rs`。
2. 真 R2 上跑 `qm-probe search-probe build/query`（跨进程/跨机器检索）—— **协议层已验证（stub），真 R2 待验证**
   （无凭据）：两个独立 `search-probe` 进程在本地 S3 stub 上跑通 build → query，
   并有"第二个进程取回全部分片对象、命中全量页面"的线上（socket）证据；多机目录场景
   `search-probe project` 也在同一 socket 上跑通。见 §5.1 与 `crates/qm-probe/tests/search_probe_stub.rs`。
3. 真 R2 上的 S2 场景：`cargo run -p qm-probe --bin search-probe -- project`。
4. 真 R2 上的向量闭环：`search-probe vector-publish` 写入一个向量分片后，由另一个进程运行
   `search-probe vector-query`，并把无 provider / 同宽换模型两条 fail-closed 对照一起跑一遍——
   **协议层（stub）已验证，真 R2 待验证**（无凭据）。见 §5.1 与 §6.20。
5. 真 R2 上跑 `qm-probe digest-probe seed --workspace <unique> --project <unique>`，再用同一组参数跑 `read`（跨进程恢复最近变化摘要）—— **协议层已验证（stub），真 R2 待验证**
   （无凭据）：两个独立 `digest-probe` 进程在本地 S3 stub 上跑通 seed → read，
   删除在 manifest 已不再返回该 path 的条件下仍出现在 `pages` 里，交接棒的开/认领/**收尾**三个阶段
   都跨进程往返（窗口与排序按最新阶段判定），并有一条 listing 截断的故障对照使同一个正向谓词变红。
   见 §6.22 与 `crates/qm-probe/tests/digest_probe_stub.rs`。
