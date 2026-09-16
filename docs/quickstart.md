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

### 配置文件

上面的变量也可以写进一个本机凭据文件，`qm` 与 `qm-mcp` 会自己读：

```bash
install -d -m700 ~/.quick-memory
cat > ~/.quick-memory/env <<'EOF'
export QM_S3_ENDPOINT="https://<account>.r2.cloudflarestorage.com"
export QM_S3_BUCKET="<bucket>"
export QM_S3_ACCESS_KEY_ID="<key>"
export QM_S3_SECRET_ACCESS_KEY="<secret>"
export QM_WRITER="mbp-1"        # 换成这台机器稳定的名字：文件里的值不会被求值
EOF
chmod 600 ~/.quick-memory/env
```

- 路径：默认 `~/.quick-memory/env`；`QM_CONFIG_FILE=<path>` 指定别的文件（**指定了就必须存在**，
  找不到会报错而不是回落到环境变量）。
- 优先级：**命令行 > 环境变量 > 配置文件 > 内置默认**。`QM_S3_BUCKET=... qm status` 永远读你刚给的那个桶。
- 格式：**每行一个赋值**，`K=V` 或 `export K="V"`，支持单/双引号、`#` 整行注释或值后注释、
  `=` 两侧空白、CRLF。只采纳 `QM_*` / `R2_*` 开头的键；其余命名空间的赋值、以及 `set -a`、
  shell 函数等**非赋值行一律忽略**（所以这个文件仍然可以被 `source`）。
- **值不会被求值**：不展开 `$`、不执行命令替换、不做 glob。凡是 shell 会读成**另一个值**的写法
  都会**报错退出**而不是猜着用：未加引号的 `$`、反引号、引号、`\`、`;` `&` `|` `<` `>` `(` `)`，
  未加引号的第二个词（`K=v w`），引号拼接（`"a"b`、`"a"#b`），双引号内的 `$`/反引号/转义，
  以及 shell 会展开成家目录的 `~`（值开头，或**任意 `:` 之后**，如 `foo:~/bar`）与 zsh 的
  `=命令名` 展开（`K==ls`、`foo:=ls`）。
- **需要字面量就用单引号**：`QM_WRITER='$(hostname)'`、`QM_S3_SECRET_ACCESS_KEY='a$b*c'`
  —— 单引号内 shell 也按字面量读，两边一致，是唯一能同时满足两种读法的写法。
  `*`、`{a,b}`、`[abc]` 这类字符在赋值右值里不展开，未加引号也可以（`sh`/`bash`/`zsh` 实测一致）。
- 文件权限宽于 0600 时会在 stderr 给出一次告警（不阻断）。
- `QM_EMBEDDING_*` 与 `QM_LLM_*` 由 search/compile 两个 crate 直接从**进程环境**读取，
  **不由这个文件提供服务**——写在文件里会在 stderr 明确告知，请改用环境变量导出。

**凭据缺失时命令会直接报错**，不会退回本地存储——本地文件没有 CAS 语义，静默降级会悄悄丢写。

发布 watermark 按 `(endpoint, bucket)` 分键：**升级，或把环境切到另一个桶后，第一次
`qm publish` 会做一次全量发布**（安全退化，只是多写一个分片）。指向第二个桶请给
每个桶配独立的 `QM_CACHE_DIR`，避免旧版缓存和跨桶清理互相干扰。

## 3. 记下第一条记忆

两条路径，用小步验证：

```bash
QM_WRITER=mbp-1 qm capture --session sess-1 --kind tool_use --text "switched the index to tantivy splits"
QM_WRITER=mbp-1 qm consolidate --session sess-1        # 编译成 sessions/sess-1.md
QM_WRITER=mbp-1 qm publish                             # 发布会话页，使其可检索
QM_WRITER=mbp-1 qm search "tantivy"                    # 换一台机器也能搜到
```

`qm search` 需要桶里已有分片；`qm publish` 之前检索不到任何东西是正常的。

### 一键收口（可定时）

把上面的三步收成一次可重复执行的维护：

```bash
QM_WRITER=mbp-1 qm maintain --json
```

它先 drain 本地 hook spool，再编译当前 scope 的所有会话，最后复用普通
`publish` 路径发布变化。命令只跑一次，不是 daemon；连续跑第二次时，
未变化的会话计入 `already_up_to_date`，不会新增 manifest `seq`，不会生成重复页面版本，
也不会重复发布分片。`--json` 会给出 `drained / spool_kept / sessions / consolidated /
already_up_to_date / skipped_locked / skipped_empty / failed / published / manifest_seq /
splits`；`spool_kept` 统计所有未成功处理的剩余条目（其他 scope、超过
`--drain-limit` 的当前 scope 条目以及坏条目），而 limit 只约束当前 scope。
真实租约冲突只计入 `skipped_locked`，空 session 计入 `skipped_empty`。
`published` 只在本次确实新增 split 且 `publish_error` 为空时为 `true`。
某个会话坏掉时会继续处理其他会话，但仍以非零状态结束。

定时执行示例（环境文件就是上面那个 `~/.quick-memory/env`；`qm` 自己会读，不需要再 `.` 一次）：

```cron
*/15 * * * * /usr/local/bin/qm maintain --json >> "$HOME/.local/state/qm-maintain.log" 2>&1
# 想显式注入：. "$HOME/.quick-memory/env"; /usr/local/bin/qm maintain --json >> ...
# 只有在「LF 换行 + 只使用本文档语法」时，source 与 qm 才保证读法一致；
# qm 另外容忍 CRLF 与 `=` 两侧空白（shell 不认），交互式 `!` 历史展开不在保证内。
```

不要把 `qm maintain` 放进 agent 的 fire-and-forget hook 路径：`qm hook` 只负责
200ms 内接收或落 spool，维护命令交给 cron/launchd/CI 在会话外运行。

检索默认融合**正文 / 实体 / 链接**三路词法信号，再按"最近改过的排前面"做一次有界微调。
三个开关各关掉一路或那个先验：

```bash
qm search "tantivy" --no-recency      # 关掉新近度先验，纯按相关性排
qm search "tantivy" --no-neighbors    # 不再用"链接到命中页的页面"扩展召回
qm search "tantivy" --no-vector       # 即使配了 embedding，也不跑语义流（见第 7 节）
```

手写页面走同一条路：

```bash
echo "leader election and snapshots" | qm write-page --path notes/raft.md --title Raft
qm publish
qm read-page --path notes/raft.md
qm history  --path notes/raft.md
qm log --limit 10
```

## 4. 开场先看什么：`qm recent` 与 `qm digest`

新会话开始时，先问"上一个会话在做什么"，比上来就检索更快。这两条命令都只读权威对象：

```bash
qm recent --limit 10                 # 按最后修改时间列出页面（默认 10 条）
qm digest --hours 24 --limit 20      # 最近 24 小时的提交 / 会话 / 交接棒（默认 24 小时、每段 20 条）
```

- `qm recent`：**刚接手一个项目时先看它**——它只读 manifest 里的时间戳，回答"哪些页面最近改过"，
  不碰索引，比跑一次检索便宜得多。
- `qm digest`：**隔了几天回来、或要接手别人的工作前看它**——`recent` 只看还存在的页面，
  `digest` 还会告诉你哪些页面**被删了**、哪些会话还在动、有没有留给你的交接棒。
  三段各自排序、各自封顶 `--limit` 条；时间窗用 `--hours N` 或 `--since-ms N`（两者互斥，不能同时给）。

## 5. 接进 agent

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

工具共 **26** 个，完整清单见 [`README.md`](../README.md#mcp-接入)。与检索回顾有关的是
`memory_search` / `memory_recent` / `memory_digest` / `memory_log`，写入用
`memory_capture` / `memory_consolidate` / `memory_write_page` / `memory_delete_page` /
`memory_publish` / `memory_compact` / `memory_maintain`，其余包括 `memory_read_page` / `memory_history` /
`memory_restore` / `memory_sessions` / `memory_compact_session` / `memory_status` /
`memory_verify` / `memory_handoff_{open,list,claim,done}` /
`memory_propose` / `memory_proposals` / `memory_approve` / `memory_reject`。

自动采集（把 harness 的生命周期钩子指向它，事件 JSON 走 stdin）：

```bash
qm hook --event PostToolUse --session "$SESSION_ID" --actor codex < payload.json
qm hook-drain      # 桶恢复后重投本地 spool
```

`qm hook` 在 `--timeout-ms`（默认 200ms）内写不进桶就落本地 spool 并**返回成功**：
记忆不可用不该拖垮 agent。

## 6. 可选：让编译器用 LLM

```bash
export QM_LLM_BASE_URL="https://api.openai.com/v1"   # 或任何 OpenAI 兼容端点
export QM_LLM_API_KEY="..."
export QM_LLM_MODEL="gpt-4o-mini"
qm consolidate --session sess-1 --compiler llm       # 失败自动回落规则渲染
```

幂等由**链指纹**决定：页面里嵌 `<!-- qm:compiled <sha256> -->`，链没变就不会因为措辞不同而反复写版本。

## 7. 可选：开启语义检索（embedding）

前三路（正文 / 实体 / 链接）都是词法的：页面必须**含有**查询的词或标识符。想让"一个词都没重合、
但意思相近"的页面也能被召回，配一个 OpenAI 兼容的 embedding 端点：

```bash
export QM_EMBEDDING_BASE_URL="https://api.openai.com/v1"
export QM_EMBEDDING_API_KEY="<key>"
export QM_EMBEDDING_MODEL="text-embedding-3-small"
export QM_EMBEDDING_DIM="1536"        # 可选，默认 1536（text-embedding-3-small 的宽度）
```

- **未配置时这一路自动不跑，且不是错误**：不设 `QM_EMBEDDING_BASE_URL` 时 `publish` 不写向量、
  `search` 不跑语义流，其余行为完全不变。只配一半（有 URL 但缺 key 或 model）才是硬失败。
- **配好之后要重新 `qm publish`**：分片里的向量是发布时算出来的。
  **换模型（或换宽度）必须重新 `publish` 重嵌入**——否则查询向量与分片里存量向量的宽度对不上，
  整次搜索会 fail-closed 直接报错，而不是悄悄降级。
- `qm search --no-vector` 可以临时关掉这一路。

它只加召回、不挤掉直接命中。运维与容量细节见 [`ops.md`](ops.md)，
设计取舍见 [`design.md`](design.md) 的"向量检索流"一节。

## 8. 多机共享

多台机器用**同一套桶 + 不同的 `QM_WRITER`**即可，无需任何服务器：

- 页面共享、交接棒（handoff）自有：`qm handoff claim` 只有一台机器能成功。
- 每台机器只发布自己写的页面（本地 watermark 记录发布进度）。
- 检索会把所有已发布分片材料化到本地缓存；缓存按内容哈希命名，命中就不下载。

## 9. 排障

| 现象 | 原因 / 处理 |
|---|---|
| `missing S3 endpoint` / `missing access key id` | 环境变量没配全；这是刻意的硬失败 |
| `backend does not implement the CAS contract` | 用了不支持条件写的后端（例如本地文件系统）；换真桶 |
| `nothing to publish: this machine is up to date` | 正常：本机没有新变更，没有新分片 |
| 检索无结果但 `status` 显示有页面 | 忘了 `qm publish`，或另一台机器还没发布 |
| `handoff ... is not open` | 别的机器已经认领，或它已完成 |
| `spooled (...)` | 桶暂时不可达；恢复后跑 `qm hook-drain` |

## 10. 读 Quickwit 产出的分片

如果某个索引器用 Quickwit 而不是 quick-memory 构建分片，本仓库的读方仍能直接检索它：

```bash
cargo run -p qm-probe --bin split-probe -- --file /path/to/<split-id>.split --query "tantivy"
```

它会解包容器（u32/u64 两种 footer 都认）、列出内部文件、用 tantivy 查询并打印命中。
依赖两个编译期特性（`zstd-compression`、`quickwit`/`sstable`），仓库已经启用。
**已验证（2026-09-15）**：官方 `quickwit/quickwit:0.9.0` 产出的 6893 字节真分片已由
`split-probe` 解包 8 个文件并检索命中 1 条；见 `design.md` §6.4/§10.5。

## 11. 自检探针

### 真桶（需要 S3/R2 凭据）

```bash
cargo run -p qm-probe --bin cas-conformance            # 条件写契约
cargo run -p qm-probe --bin manifest-probe -- --machines 3 --writes 5   # 多机并发提交
cargo run -p qm-probe --bin search-probe -- project    # 多机发布 + 检索 + 过期过滤
cargo run -p qm-probe --bin session-probe              # 采集 → 编译 → 检索
# 向量闭环（需要 embedding 配置；pages.jsonl 的每行是 {path,title,body}）
cargo run -p qm-probe --bin search-probe -- vector-publish --workspace acme --project my-project ./pages.jsonl
cargo run -p qm-probe --bin search-probe -- vector-query --workspace acme --project my-project "语义查询"
# 最近变化摘要的跨进程真桶验收；两个进程必须使用同一组唯一 scope
DIGEST_WS="probe-digest-$(date +%s)-$$"
DIGEST_PROJECT="run-$(date +%s)-$$"
cargo run -p qm-probe --bin digest-probe -- seed --workspace "$DIGEST_WS" --project "$DIGEST_PROJECT"
cargo run -p qm-probe --bin digest-probe -- read --workspace "$DIGEST_WS" --project "$DIGEST_PROJECT" --since-ms 0 --limit 20
```

### 本地 S3 stub（协议层；不是真 R2）

无凭据时，下面的测试会启动进程内 `S3Stub`，并让探针以独立进程打本地 HTTP，实际运行跨进程检索、
`vector-publish` / `vector-query` 与 `digest-probe`：

```bash
cargo test -p qm-probe --test s3_protocol
cargo test -p qm-probe --test search_probe_stub
cargo test -p qm-probe --test digest_probe_stub
```

需要单独查看/启动这些探针时：

```bash
cargo run -p qm-probe --bin s3-stub -- --help
cargo run -p qm-probe --bin s3-stub -- --port 0 --port-file /tmp/qm-s3-stub-port
cargo run -p qm-probe --bin search-probe -- vector-publish --help
cargo run -p qm-probe --bin search-probe -- vector-query --help
cargo run -p qm-probe --bin digest-probe -- seed --help
cargo run -p qm-probe --bin digest-probe -- read --help
```

stub 证据只覆盖本地 HTTP 协议层；真 R2 的签名、ETag/版本行为、一致性、配额、延迟与错误 XML 变体仍未验证。

`digest-probe` 不自动清理；`seed` 写入前会列出目标 scope，发现任何既有对象就 fail-loud，`read` 只读。
`--workspace` / `--project` 是必填参数，必须由两个进程显式传入同一组唯一值；这个 listing 预检是
防复用的 best-effort 检查，不是并发锁。
真桶请优先使用专用 bucket 或专用 prefix。`QM_S3_PREFIX` 只被 `cas-conformance` 与
`search-probe build/query` 的分片前缀采用，**不能当所有 probe 的全局隔离**。
