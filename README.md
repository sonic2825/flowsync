# FlowSync

FlowSync 是一个基于 Rust + OpenDAL 的数据中转平台，用于在多种存储后端之间进行可视化、可调度、可审计的数据复制与同步，提供：
- CLI：`copy` / `sync` / `move` / `ls` / `config init`
- Web 管理端：任务管理、调度执行、运行历史、实时事件

## 功能概览

- 存储后端：`fs`、`s3`、`sftp`
- 差异检测：
  - 默认：`size + mtime`
  - `--checksum`：使用 SHA256
  - `--ignore-existing`：目标存在即跳过
- 传输控制：
  - `--transfers`：传输并发
  - `--checkers`：差异检查并发
  - `--chunk-size`：流式分块大小（默认 `8MB`）
  - `--bandwidth-limit`：限速
- 安全执行：`--dry-run` 仅输出计划动作，不执行写入/删除
- 失败记录：失败项会写入 `transfer_failures.log`
- Web 端能力：任务 CRUD、Cron 调度、运行日志、实时事件（WebSocket）

## 安装与构建

```bash
cargo build
```

## Docker 运行

1) 构建镜像：

```bash
docker build -t flowsync:latest .
```

2) 启动 Web 服务（推荐挂载数据目录，持久化 DB）：

```bash
docker run --rm -p 3030:3030 \
  -v $(pwd)/data:/data \
  flowsync:latest
```

默认启动命令等价于：

```bash
flowsync server --host 0.0.0.0 --port 3030 --db /data/flowsync.db
```

3) 可选：挂载配置文件并指定 `FLOWSYNC_CONFIG`：

```bash
docker run --rm -p 3030:3030 \
  -v $(pwd)/data:/data \
  -v $(pwd)/config.toml:/app/config.toml:ro \
  -e FLOWSYNC_CONFIG=/app/config.toml \
  flowsync:latest
```

4) 可选：为 Web 写操作配置管理员密码（推荐用环境变量）：

```bash
docker run --rm -p 3030:3030 \
  -v $(pwd)/data:/data \
  -e FLOWSYNC_ADMIN_PASSWORD='replace-with-a-strong-password' \
  flowsync:latest
```

说明：
- `config.toml` 不存在时，程序会使用空配置启动，不会因缺文件报错。
- 你可以直接在 Web 页面添加 remotes；这些 remotes 会写入 `--db` 指向的 SQLite 数据库。
- 若不挂载 `/data`，容器重建后通过 Web 添加的 remotes/tasks/runs 都会丢失。

### 使用 Docker Compose 持续后台运行

仓库根目录已提供 `docker-compose.yml`，默认：
- 映射端口 `3030:3030`
- 挂载 `./data:/data` 持久化数据库
- 设置 `restart: unless-stopped` 保持后台持续运行

启动（后台）：

```bash
docker compose up -d
```

查看日志：

```bash
docker compose logs -f
```

停止：

```bash
docker compose down
```

可选：先设置管理员密码环境变量再启动：

```bash
export FLOWSYNC_ADMIN_PASSWORD='replace-with-a-strong-password'
docker compose up -d
```

## 快速开始（CLI）

1) 初始化配置（交互式）：

```bash
cargo run -- config init
```

2) 复制（仅新增/覆盖，不删除目标多余文件）：

```bash
cargo run -- copy local:/data/src s3prod:backup
```

3) 同步（目标镜像源，包含删除）：

```bash
cargo run -- sync local:/data/src s3prod:backup --dry-run
```

4) 搬运（复制后删除源）：

```bash
cargo run -- move local:/data/src s3prod:archive
```

5) 列目录（用于连通性和路径验证）：

```bash
cargo run -- ls s3prod:backup
```

## 命令语义

- `copy`：复制新增/变更文件，不删除目标文件
- `sync`：让目标与源一致，会删除目标中源不存在的文件
- `move`：先复制，再删除源文件
- `ls`：列出 `remote:path` 下的对象
- `config init`：交互式写入配置文件

## 全局参数（适用于 `copy/sync/move`）

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `--log-level <LEVEL>` | `info` | 日志级别（`trace/debug/info/warn/error`） |
| `--transfers <N>` | `4` | 传输并发 |
| `--checkers <N>` | `8` | 差异检查并发 |
| `--dry-run` | `false` | 仅显示计划动作 |
| `--include <GLOB>` | 空 | 包含匹配路径（可重复） |
| `--exclude <GLOB>` | 空 | 排除匹配路径（可重复） |
| `--bandwidth-limit <RATE>` | 不限速 | 限速（如 `50MB`） |
| `--chunk-size <SIZE>` | `8MB` | 分块大小（支持 `KB/MB/GB`、`KiB/MiB/GiB`） |
| `--checksum` | `false` | 使用 SHA256 对比 |
| `--ignore-existing` | `false` | 目标已存在则跳过 |

示例：

```bash
# 高并发复制
cargo run -- copy local:/data/src s3prod:backup --transfers 16 --checkers 16

# 严格校验同步
cargo run -- sync local:/data/src s3prod:backup --checksum --checkers 16

# 限速同步
cargo run -- sync local:/data/src s3prod:backup --bandwidth-limit 100MB
```

## Remote 与路径格式

所有源/目标都使用 `remote:path` 格式，例如：
- `local:docs/a.txt`
- `s3prod:images/2026/`

### S3 路径规则

- 若 remote 已配置 `bucket`：`path` 仅表示 bucket 内前缀
- 若 remote 未配置 `bucket`：任务路径必须写成 `remote:bucket/prefix`

## 配置文件

默认路径：
- `~/.config/flowsync/config.toml`

环境变量覆盖：
- `FLOWSYNC_CONFIG=/path/to/config.toml`
- 兼容旧变量：`RUST_S3_SYNC_CONFIG`

兼容行为：
- 若 `~/.config/flowsync/config.toml` 不存在，但 `~/.config/rust-s3-sync/config.toml` 存在，会自动读取旧路径

示例：

```toml
[remotes.local]
type = "fs"
root = "/"

[remotes.s3prod]
type = "s3"
bucket = "my-bucket"
endpoint = "https://s3.us-east-1.amazonaws.com"
region = "us-east-1"
url_style = "path" # path | virtual_hosted
root = ""

[remotes.s3dyn]
type = "s3"
bucket = "" # 或省略；使用时写成 s3dyn:bucket/prefix
endpoint = "https://s3.us-east-1.amazonaws.com"
region = "us-east-1"
url_style = "path"

[remotes.sftp1]
type = "sftp"
endpoint = "ssh://user@example.com:22"
user = "user"
key = "/home/user/.ssh/id_rsa"
root = "/data"
```

说明：
- 当前构建只启用 `fs/s3/sftp`（未启用 `ftp` 运行时）
- S3 认证可通过配置字段，或环境变量 `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`

## Web 管理端

启动服务：

```bash
cargo run -- server --host 127.0.0.1 --port 3030 --db flowsync.db
```

浏览器访问：
- `http://127.0.0.1:3030`

### 管理员安全密码门禁（Web）

当前 Web 后台支持“匿名只读 + 高危操作口令验证”：
- 普通用户无需登录即可查看页面
- 新建/修改/暂停/删除/立即运行 等写操作，需先输入管理员安全密码
- 验证通过后，默认 `15` 分钟内可继续执行高危操作（前端会话内）

推荐：在启动服务时直接配置管理员密码：

```bash
cargo run -- server \
  --host 127.0.0.1 \
  --port 3030 \
  --db flowsync.db \
  --admin-password 'replace-with-a-strong-password'
```

也可用环境变量（避免密码出现在命令历史）：

```bash
FLOWSYNC_ADMIN_PASSWORD='replace-with-a-strong-password' \
cargo run -- server --host 127.0.0.1 --port 3030 --db flowsync.db
```

密码配置方式（优先级从高到低）：
1. 启动参数 `--admin-password`（或环境变量 `FLOWSYNC_ADMIN_PASSWORD`）
2. `window.__FLOWSYNC_UI_CONFIG__.adminPassword`
3. `localStorage['flowsync_admin_password']`（仅建议本地调试）

示例（方式 1，推荐）：

```html
<script>
  window.__FLOWSYNC_UI_CONFIG__ = {
    adminPassword: "replace-with-a-strong-password"
  };
</script>
```

示例（方式 2，本地调试）：

```js
localStorage.setItem("flowsync_admin_password", "replace-with-a-strong-password");
```

注意：
- 若未配置密码，Web 页面会阻止所有高危写操作。
- 该门禁目前是前端校验，只能防止页面误操作，不能阻止绕过页面直接调用 `/api/*` 写接口。

事件清理参数（可选）：

```bash
cargo run -- server \
  --host 127.0.0.1 \
  --port 3030 \
  --db flowsync.db \
  --event-retention-days 7 \
  --event-max-rows 200000 \
  --event-cleanup-interval-secs 300
```

- `--event-retention-days`：按天清理历史事件（`0` 表示关闭）
- `--event-max-rows`：最大事件行数（`0` 表示关闭）
- `--event-cleanup-interval-secs`：清理周期秒数（建议 `>=30`）

### 主要 API

- `GET /api/remotes`
- `POST /api/remotes`
- `PUT /api/remotes/:name`
- `DELETE /api/remotes/:name`
- `GET /api/tasks`
- `POST /api/tasks`
- `PUT /api/tasks/:id`
- `DELETE /api/tasks/:id`
- `POST /api/tasks/:id/run`
- `POST /api/tasks/:id/pause`
- `POST /api/tasks/:id/resume`
- `GET /api/runs?task_id=<id>&limit=50`
- `GET /api/events?task_id=<id>&limit=300`
- `GET /api/dashboard`
- `GET /ws`

### Cron 定时任务说明

FlowSync 的任务调度基于 `tokio-cron-scheduler`，任务中的 `cron_expr` 使用 Cron 表达式。

常用 6 段格式：
- `秒 分 时 日 月 周`

示例：
- `0 */10 * * * *`：每 10 分钟执行一次
- `0 0 * * * *`：每小时整点执行
- `0 30 2 * * *`：每天 `02:30` 执行
- `0 0 3 * * 1`：每周一 `03:00` 执行

使用建议：
- 先手动运行一次任务确认 `source/destination` 正确，再开启定时调度
- 对 `sync` / `move` 建议先用 `dry_run` 验证，确认无误后再关闭
- 高并发任务避免同一时间密集触发，建议错峰设置 `cron_expr`

## 测试

单元/集成（Rust）：

```bash
cargo test
```

格式与静态检查：

```bash
cargo fmt --all
cargo clippy --all-targets -- -D warnings
```

E2E 脚本（S3 端到端验证 `copy/sync/move/ls`）：

```bash
bash scripts/e2e_minio.sh
```

该脚本不会自动启动 Docker MinIO，默认连接以下端点（可用环境变量覆盖）：
- `S3_ENDPOINT=http://rustfs1.pops.metax-tech.com`
- `S3_BUCKET=test1`
- `S3_ACCESS_KEY_ID=rustfsadmin`
- `S3_SECRET_ACCESS_KEY=rustfsadmin`

常用变量：
- `KEEP_E2E_ARTIFACTS=1`：保留 `.tmp/e2e` 中间产物

## 安全提示

- `sync`、`move` 都可能删除数据，建议先执行 `--dry-run`
- 不要提交真实密钥到 `config.toml` 或数据库文件
- 生产任务建议先用小目录压测，确认并发与 `chunk-size` 设置
