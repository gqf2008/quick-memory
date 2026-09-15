# quick-memory

多机共享的 agent 长期记忆：**权威数据在 S3/R2，搜索索引是可重建的派生层，任何一台机器都能独立搜全量**。

设计见 [`docs/design.md`](docs/design.md)。

## 现状

S0（架构验证）与 S1（权威层）已跑通**离线可验证的部分**：

- ETag CAS 契约 + 一致性探针（`qm-store`）
- "另一台机器只有桶访问权也能搜全量"的端到端往返（`qm-search`）
- 多机并发提交：无覆盖、supersession 链完整、WAL 完整、崩溃重试幂等（`qm-store`）
- 多机分片发布 + 跨分片 RRF 检索 + 权威可见性过滤：过期副本永不答出、离线机器的内容仍可搜（`qm-store`/`qm-search`）
- 删除（tombstone）+ 压缩（租约保护、结果不变、分片下降）+ 租约抢占语义（`qm-store`/`qm-search`）
- Quickwit `.split` 容器解包：读方无需 Quickwit 集群即可检索该格式的分片（`qm-search::quickwit_split`）
- 采集与编译：观测按会话链捕获（入口强制脱敏 + 限长），编译成页面并可被检索；重放与重编译幂等（`qm-store`/`qm-search`）

真 R2 与 Quickwit 的验证需要凭据与二进制，见下。

## 布局

```
crates/qm-core    纯领域类型与对象键布局（无 IO）
crates/qm-store   对象存储 CAS 原语 + 一致性探针
crates/qm-search  分片上传/材料化 + tantivy 进程内检索
crates/qm-probe   S0 探针二进制
```

## 跑测试

```bash
cargo test --workspace
```

## S0 探针

凭据缺失时探针**报错退出**，不会静默跳过。

```bash
export QM_S3_ENDPOINT="https://<account>.r2.cloudflarestorage.com"
export QM_S3_BUCKET="<bucket>"
export QM_S3_ACCESS_KEY_ID="..."
export QM_S3_SECRET_ACCESS_KEY="..."
export QM_S3_PREFIX="qm-probe"          # 可选，探针对象都放在这个前缀下
export QM_S3_FORCE_PATH_STYLE=true      # R2 需要 path-style

# 1) 条件写一致性：create-if-absent / 陈旧 ETag 必须被拒
cargo run -p qm-probe --bin cas-conformance

# 2) 多机并发提交（先做 CAS 预检，再跑场景并复核）
cargo run -p qm-probe --bin manifest-probe -- --machines 3 --writes 5

# 3) 多机发布 + 检索（含过期副本过滤与离线机器）
cargo run -p qm-probe --bin search-probe -- project

# 4) 采集 → 编译 → 检索一条会话
cargo run -p qm-probe --bin session-probe

# 5) 跨机器检索（单分片）：A 构建并上传分片
cargo run -p qm-probe --bin search-probe -- --split-prefix demo/0000000001 build docs.jsonl
#    B（另一台机器/另一次运行）只靠桶材料化并查询
cargo run -p qm-probe --bin search-probe -- --split-prefix demo/0000000001 query "consensus"
```

`--local <dir>` 只用于冒烟测试探针本身，**预期会失败**：本机文件系统不支持条件写，
探针会在预检阶段拒绝它。

`docs.jsonl` 每行一个 `PageDoc`：

```json
{"workspace_id":"acme","project_id":"ai-memory","path":"notes/raft.md","page_id":"p1","title":"Raft","body":"leader election","updated_at_ms":1}
```
