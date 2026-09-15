# quick-memory

多机共享的 agent 长期记忆：**权威数据在 S3/R2，搜索索引是可重建的派生层，任何一台机器都能独立搜全量**。

> 主仓在 **walgit**（`http://127.0.0.1:8081/gqf2008/quick-memory.git`），GitHub 仅作为发版镜像；
> 日常 push 走 `origin`，不要手动推 GitHub。

设计见 [`docs/design.md`](docs/design.md)；上手见 [`docs/quickstart.md`](docs/quickstart.md)；
运维见 [`docs/ops.md`](docs/ops.md)。

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
crates/qm-cli     `qm` 命令行（agent 入口）
crates/qm-mcp     `qm-mcp` MCP stdio 服务器（25 个 memory_* 工具）
```

## 命令行

共 **27** 个子命令（`qm --help`；下表按用途分组）：

| 子命令 | 作用 |
|---|---|
| `capture` / `consolidate` / `sessions` | 记观测 / 编译会话页 / 列会话 |
| `compact-session` | 收敛会话链里的旧观测（默认 dry run，`--apply` 才写） |
| `search` | 检索：`--global` 跨项目；`--no-recency` / `--no-neighbors` / `--no-vector` 各关一路信号 |
| `publish` / `compact` | 发布本机增量分片 / 从权威页面整体重建索引（租约保护） |
| `status` | 当前项目内容概览 |
| `recent` / `digest` / `log` | 最近改过的页面 / 最近变化摘要 / 最近提交 |
| `read-page` / `write-page` / `delete-page` | 读 / 提交 / 删除页面 |
| `history` / `restore` | 版本链 / 把旧版本恢复成新版本（回滚也是一次提交） |
| `hook` / `hook-drain` | 从 stdin 收生命周期事件 / 重投本地 spool |
| `handoff`（`open` / `list` / `claim` / `done`） | 交接棒：只能被认领一次 |
| `propose` / `proposals` / `approve` / `reject` | 提案与审批：批准之前目标页面不动 |
| `export` / `import` | 人工可读迁出 / 迁入 |
| `verify` / `gc` | 桶完整性自检（只读）/ 回收不可达对象（默认 dry run） |

```bash
export QM_S3_ENDPOINT=... QM_S3_BUCKET=... QM_S3_ACCESS_KEY_ID=... QM_S3_SECRET_ACCESS_KEY=...
export QM_WORKSPACE=acme QM_PROJECT=ai-memory QM_WRITER=mbp-1   # 可选

# 下面用安装后的 `qm`；没装的话把 `qm` 换成 `cargo run -p qm-cli --bin qm --`
qm capture --session sess-1 --text "ran the suite"
qm consolidate --session sess-1          # --compiler auto|rules|llm
# LLM 编译（可选）：设置 QM_LLM_BASE_URL / QM_LLM_API_KEY / QM_LLM_MODEL；失败自动回落 rules
qm publish                               # 增量：只发布本机上次发布后变化的页面
qm search "suite"
qm recent --limit 10                     # 开场先看：上次都在改什么
qm digest --hours 24                     # 最近变化摘要：提交（含删除）/ 会话 / 交接棒
# 可选语义检索：QM_EMBEDDING_BASE_URL / QM_EMBEDDING_API_KEY / QM_EMBEDDING_MODEL
# （宽度用可选的 QM_EMBEDDING_DIM，默认 1536）。未配置时这一路自动不跑，不是错误。
```

更完整的用法：

```bash
# 交接棒：只能被认领一次
qm handoff open --title "finish the rebuild" --body "compaction pending"
qm handoff list
qm handoff claim --id <id>          # 两台机器同时抢，只有一台成功
qm handoff done  --id <id>

# 学习性修改走审批：批准之前目标页面一点不动
qm propose --path notes/raft.md --title Raft --body "..." --rationale "clearer"
qm proposals
qm approve --id <id>

# 运维（默认只读 / dry run）
qm verify --strict                  # 有问题就非零退出，可挂定时巡检
qm gc                               # --apply 才真删
qm compact-session --session sess-1 --keep-last 50   # 观测保留
qm export --to ./exported && qm import --from ./exported
```

作用域来自 `--workspace/--project/--writer` 或同名环境变量；凭据缺失时命令直接报错，
不会退回本地存储。

## MCP 接入

```json
{
  "mcpServers": {
    "quick-memory": {
      "command": "qm-mcp",
      "env": {
        "QM_S3_ENDPOINT": "https://<account>.r2.cloudflarestorage.com",
        "QM_S3_BUCKET": "<bucket>",
        "QM_S3_ACCESS_KEY_ID": "...",
        "QM_S3_SECRET_ACCESS_KEY": "...",
        "QM_WORKSPACE": "acme",
        "QM_PROJECT": "my-project",
        "QM_WRITER": "mbp-1"
      }
    }
  }
}
```

工具共 **25** 个（`grep -c '#\[tool(' crates/qm-mcp/src/lib.rs`）：

| 分组 | 工具 |
|---|---|
| 写入 | `memory_capture` · `memory_consolidate` · `memory_write_page` · `memory_delete_page` |
| 检索与回顾 | `memory_search` · `memory_recent` · `memory_digest` · `memory_log` |
| 页面读取 | `memory_read_page` · `memory_history` · `memory_restore` |
| 索引 | `memory_publish` · `memory_compact` |
| 会话 | `memory_sessions` · `memory_compact_session` |
| 交接棒 | `memory_handoff_open` · `memory_handoff_list` · `memory_handoff_claim` · `memory_handoff_done` |
| 提案与审批 | `memory_propose` · `memory_proposals` · `memory_approve` · `memory_reject` |
| 运维 | `memory_status` · `memory_verify` |

它们与 `qm` 命令共用同一份实现，返回 JSON。

本地试协议用 `qm-mcp --synthetic-bucket`（内存、非持久，仅用于冒烟）。

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
