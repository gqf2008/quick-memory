# 快速上手

本文是端到端最小路径：装好 → 配好桶 → 记第一条记忆 → 检索 → 接进 agent。
设计背景见 [`design.md`](design.md)。

## 1. 准备

- 一个 S3 兼容桶（Cloudflare R2 / AWS S3 / MinIO 均可）。
  需要一个 **Object Read & Write** 的访问密钥。
- Rust 1.95（`rust-toolchain.toml` 已固定）。

```bash
cargo build --release -p qm-cli -p qm-mcp
install -m755 target/release/qm target/release/qm-mcp ~/.local/bin/
```

## 2. 配置

```bash
export QM_S3_ENDPOINT="https://<account>.r2.cloudflarestorage.com"   # 也接受 R2_ENDPOINT
export QM_S3_BUCKET="<bucket>"
export QM_S3_ACCESS_KEY_ID="<key>"
export QM_S3_SECRET_ACCESS_KEY="<secret>"
export QM_S3_REGION="auto"                # 可选，默认 auto
export QM_S3_FORCE_PATH_STYLE=true        # 可选，R2 需要 path-style（默认 true）

export QM_WORKSPACE="acme"                # 可选，默认 default
export QM_PROJECT="my-project"            # 可选，默认 default
export QM_WRITER="$(hostname -s)"         # 重要：多机时每台机器一个稳定名字
```

**凭据缺失时命令会直接报错**，不会退回本地存储——本地文件没有 CAS 语义，静默降级会悄悄丢写。

## 3. 记下第一条记忆

两条路径，用小步验证：

```bash
QM_WRITER=mbp-1 qm capture --session sess-1 --kind tool_use --text "switched the index to tantivy splits"
QM_WRITER=mbp-1 qm consolidate --session sess-1        # 编译成 sessions/sess-1.md
QM_WRITER=mbp-1 qm publish                             # 发布会话页，使其可检索
QM_WRITER=mbp-1 qm search "tantivy"                    # 换一台机器也能搜到
```

`qm search` 需要桶里已有分片；`qm publish` 之前检索不到任何东西是正常的。

手写页面走同一条路：

```bash
echo "leader election and snapshots" | qm write-page --path notes/raft.md --title Raft
qm publish
qm read-page --path notes/raft.md
qm history  --path notes/raft.md
qm log --limit 10
qm recent --limit 10        # 按最后修改时间列出页面：开场先看这里
```

## 4. 接进 agent

MCP（Codex / Claude Code 等）：

```json
{
  "mcpServers": {
    "quick-memory": {
      "command": "qm-mcp",
      "env": {
        "QM_S3_ENDPOINT": "https://<account>.r2.cloudflarestorage.com",
        "QM_S3_BUCKET": "<bucket>",
        "QM_S3_ACCESS_KEY_ID": "<key>",
        "QM_S3_SECRET_ACCESS_KEY": "<secret>",
        "QM_WORKSPACE": "acme",
        "QM_PROJECT": "my-project",
        "QM_WRITER": "mbp-1"
      }
    }
  }
}
```

工具：`memory_search` / `memory_capture` / `memory_consolidate` / `memory_publish` /
`memory_write_page` / `memory_read_page` / `memory_history` / `memory_restore` /
`memory_log` / `memory_recent` / `memory_delete_page` / `memory_compact` /
`memory_sessions` / `memory_status` / `memory_handoff_{open,list,claim,done}`。

自动采集（把 harness 的生命周期钩子指向它，事件 JSON 走 stdin）：

```bash
qm hook --event PostToolUse --session "$SESSION_ID" --actor codex < payload.json
qm hook-drain      # 桶恢复后重投本地 spool
```

`qm hook` 在 `--timeout-ms`（默认 200ms）内写不进桶就落本地 spool 并**返回成功**：
记忆不可用不该拖垮 agent。

## 5. 可选：让编译器用 LLM

```bash
export QM_LLM_BASE_URL="https://api.openai.com/v1"   # 或任何 OpenAI 兼容端点
export QM_LLM_API_KEY="..."
export QM_LLM_MODEL="gpt-4o-mini"
qm consolidate --session sess-1 --compiler llm       # 失败自动回落规则渲染
```

幂等由**链指纹**决定：页面里嵌 `<!-- qm:compiled <sha256> -->`，链没变就不会因为措辞不同而反复写版本。

## 6. 多机共享

多台机器用**同一套桶 + 不同的 `QM_WRITER`**即可，无需任何服务器：

- 页面共享、交接棒（handoff）自有：`qm handoff claim` 只有一台机器能成功。
- 每台机器只发布自己写的页面（本地 watermark 记录发布进度）。
- 检索会把所有已发布分片材料化到本地缓存；缓存按内容哈希命名，命中就不下载。

## 7. 排障

| 现象 | 原因 / 处理 |
|---|---|
| `missing S3 endpoint` / `missing access key id` | 环境变量没配全；这是刻意的硬失败 |
| `backend does not implement the CAS contract` | 用了不支持条件写的后端（例如本地文件系统）；换真桶 |
| `nothing to publish: this machine is up to date` | 正常：本机没有新变更，没有新分片 |
| 检索无结果但 `status` 显示有页面 | 忘了 `qm publish`，或另一台机器还没发布 |
| `handoff ... is not open` | 别的机器已经认领，或它已完成 |
| `spooled (...)` | 桶暂时不可达；恢复后跑 `qm hook-drain` |

## 8. 读 Quickwit 产出的分片

如果某个索引器用 Quickwit 而不是 quick-memory 构建分片，本仓库的读方仍能直接检索它：

```bash
cargo run -p qm-probe --bin split-probe -- --file /path/to/<split-id>.split --query "tantivy"
```

它会解包容器（u32/u64 两种 footer 都认）、列出内部文件、用 tantivy 查询并打印命中。
依赖两个编译期特性（`zstd-compression`、`quickwit`/`sstable`），仓库已经启用。

## 9. 自检探针（需要真桶）

```bash
cargo run -p qm-probe --bin cas-conformance            # 条件写契约
cargo run -p qm-probe --bin manifest-probe -- --machines 3 --writes 5   # 多机并发提交
cargo run -p qm-probe --bin search-probe -- project    # 多机发布 + 检索 + 过期过滤
cargo run -p qm-probe --bin session-probe              # 采集 → 编译 → 检索
```
