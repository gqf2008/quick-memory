# quick-memory

多机共享的 agent 长期记忆：**权威数据在 S3/R2，搜索索引是可重建的派生层，任何一台机器都能独立搜全量**。

设计见 [`docs/design.md`](docs/design.md)。

## 现状

S0（架构验证）进行中。已经跑通的是**离线可验证的部分**：

- ETag CAS 契约 + 一致性探针（`qm-store`）
- "另一台机器只有桶访问权也能搜全量"的端到端往返（`qm-search`）

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

# 2) 跨机器检索：A 构建并上传分片
cargo run -p qm-probe --bin search-probe -- --split-prefix demo/0000000001 build docs.jsonl
#    B（另一台机器/另一次运行）只靠桶材料化并查询
cargo run -p qm-probe --bin search-probe -- --split-prefix demo/0000000001 query "consensus"
```

`docs.jsonl` 每行一个 `PageDoc`：

```json
{"workspace_id":"acme","project_id":"ai-memory","path":"notes/raft.md","page_id":"p1","title":"Raft","body":"leader election","updated_at_ms":1}
```
