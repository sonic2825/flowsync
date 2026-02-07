# rust-s3-sync

基于 Rust + OpenDAL 的多后端文件同步 CLI 工具。

## 已实现能力

- 后端：`fs`、`s3`、`sftp`（`ftp` 配置保留，但当前构建未启用）
- 命令：`copy`、`sync`、`move`、`ls`、`config init`
- 差异检测：
  - 默认 `size + mtime`
  - `--checksum` 启用 SHA256
  - `--ignore-existing` 跳过目标已存在文件
- 并发：
  - `--transfers` 控制并发传输
  - `--checkers` 控制差异检查并发（尤其在 `--checksum` 时）
- 可观测性：
  - `indicatif` 总进度条
  - `--log-level` 日志级别
  - 失败写入 `transfer_failures.log`
- 可靠性：
  - 读写/删除操作指数退避重试
- 安全执行：
  - `--dry-run` 仅输出计划操作

## 构建

```bash
cargo build
```

## 快速开始

1) 初始化配置：

```bash
cargo run -- config init
```

2) 复制：

```bash
cargo run -- copy local:/data/src s3prod:bucket/prefix
```

3) 同步（目标镜像源，包含删除）：

```bash
cargo run -- sync local:/data/src s3prod:bucket/prefix --dry-run
```

4) 移动（复制后删源）：

```bash
cargo run -- move local:/data/src s3prod:bucket/prefix
```

5) 列表：

```bash
cargo run -- ls s3prod:bucket/prefix
```

6) 启动 Web 管理后台与任务调度：

```bash
cargo run -- server --host 127.0.0.1 --port 3030 --db rust_s3_sync.db
```

打开浏览器访问：`http://127.0.0.1:3030`

## 全局参数说明

以下参数适用于 `copy` / `sync` / `move`（`--log-level` 为全局参数）：

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `--log-level <LEVEL>` | `info` | 日志级别（如 `trace`/`debug`/`info`/`warn`/`error`） |
| `--transfers <N>` | `4` | 传输并发数（实际复制/写入并发） |
| `--checkers <N>` | `8` | 差异检查并发数（决定是否需要复制的检查阶段） |
| `--dry-run` | `false` | 仅打印计划动作，不执行写入/删除 |
| `--include <GLOB>` | 空 | 仅包含匹配的相对路径，可重复传入 |
| `--exclude <GLOB>` | 空 | 排除匹配的相对路径，可重复传入 |
| `--bandwidth-limit <RATE>` | 不限速 | 限速，支持 `KB/MB/GB`（如 `50MB`） |
| `--checksum` | `false` | 使用 SHA256 比较源/目标内容（更准确，通常更慢） |
| `--ignore-existing` | `false` | 目标存在即跳过，不再比较内容 |

### 对比规则（是否复制）

- 默认：按 `size + mtime` 判断。
- 开启 `--checksum`：按 SHA256 判断（会额外读取源与目标文件）。
- 开启 `--ignore-existing`：目标存在即跳过，优先于内容比较。

## 参数使用示例

1) 高并发复制（网络和目标端可承受时）：

```bash
cargo run -- --transfers 32 --checkers 32 copy local:/data/src s3prod:bucket/prefix
```

2) 只看计划，不真正执行：

```bash
cargo run -- --dry-run sync local:/data/src s3prod:bucket/prefix
```

3) 用 checksum 做严格增量同步：

```bash
cargo run -- --checksum --checkers 16 sync local:/data/src s3prod:bucket/prefix
```

4) 限速同步（避免占满链路）：

```bash
cargo run -- --transfers 16 --bandwidth-limit 100MB sync local:/data/src s3prod:bucket/prefix
```

5) 只同步指定目录并排除临时文件：

```bash
cargo run -- --include "images/**" --include "docs/**" --exclude "**/*.tmp" sync local:/data/src s3prod:bucket/prefix
```

## Web 管理后台与任务调度

- 技术栈：`axum` + `SQLite(rusqlite)` + `tokio-cron-scheduler` + `WebSocket`
- 前端：单页管理界面（内嵌到二进制，启动 `server` 子命令即提供）
- 数据持久化：远端配置（remotes）、任务配置、执行历史、错误记录存储在 `--db` 指定的 SQLite 文件

### Remotes（后端存储配置）管理

- 支持类型：`fs`、`s3`、`sftp`、`ftp`（注意：当前构建下 `ftp` 后端未启用，配置可保存但执行会报错）
- 存储位置：Web 配置保存到 SQLite 的 `remotes` 表
- 生效优先级：任务执行时优先使用 SQLite 中的 remotes；若与 `config.toml` 同名，SQLite 配置会覆盖本地配置
- 页面入口：
  - 在「后端存储配置（Remotes）」卡片点击「新建 / 编辑 Remote」打开弹窗
  - 在列表中点击「编辑」会将该 Remote 回填到弹窗
  - 点击「删除」会删除该名称 Remote

#### 弹窗字段说明

- `remote 名称`：任务中引用的别名（例如 `local`、`s3prod`），后续在 `source/destination` 中以 `remote:path` 使用
- `类型(type)`：`fs` / `s3` / `sftp` / `ftp`
- `config_json`：JSON 对象，不要包含 `type` 字段（类型由下拉框决定）

#### config_json 示例

1) `fs`（本地文件系统）

```json
{
  "root": "/data/sync"
}
```

2) `s3`

```json
{
  "bucket": "my-bucket",
  "endpoint": "https://s3.us-east-1.amazonaws.com",
  "region": "us-east-1",
  "url_style": "path",
  "root": ""
}
```

3) `sftp`

```json
{
  "endpoint": "ssh://user@example.com:22",
  "user": "user",
  "key": "/home/user/.ssh/id_rsa",
  "root": "/data"
}
```

4) `ftp`（仅配置示例）

```json
{
  "endpoint": "ftp://example.com:21",
  "user": "user",
  "password": "pass",
  "root": "/"
}
```

#### `fs` 类型在 Windows / Linux 的区别

- Linux/macOS：使用 Unix 路径

```json
{
  "root": "/Users/td/script/rust-s3-sync/test"
}
```

- Windows：推荐正斜杠写法（避免转义）

```json
{
  "root": "D:/sync-data"
}
```

- Windows 也可用反斜杠，但 JSON 需转义

```json
{
  "root": "D:\\sync-data"
}
```

说明：
- `root` 是运行 `rust-s3-sync` 进程所在机器的本地路径，不是浏览器客户端机器路径
- 你给出的配置是有效的：

```json
{
  "root": "/Users/td/script/rust-s3-sync/test/"
}
```

#### 任务里如何引用 Remote

- `source` / `destination` 必须使用 `remote:path` 格式（例如 `local:docs/a.txt`、`s3prod:bucket/prefix`）
- `path` 部分会去掉开头的 `/` 再拼接到对应后端 `root` 下
- 建议把 `path` 写成相对路径（如 `local:subdir/file.txt`），避免路径歧义

#### 常见错误

- `invalid location, expected remote:path`：`source/destination` 没有 `:` 分隔
- `remote '<name>' not found in config`：任务引用了不存在的 remote 名称
- `config_json must be object`：`config_json` 不是 JSON 对象（例如传了字符串或数组）

#### 从零到可用（最短实操示例）

目标：把本机目录 `/Users/td/script/rust-s3-sync/test/src` 同步到 `/Users/td/script/rust-s3-sync/test/dst`。

1) 启动服务

```bash
cargo run -- server --host 127.0.0.1 --port 3030 --db rust_s3_sync.db
```

浏览器打开：`http://127.0.0.1:3030`

2) 在 Web 配置两个 `fs` Remote

- Remote A（源）：
  - 名称：`local_src`
  - 类型：`fs`
  - `config_json`：

```json
{
  "root": "/Users/td/script/rust-s3-sync/test/src"
}
```

- Remote B（目标）：
  - 名称：`local_dst`
  - 类型：`fs`
  - `config_json`：

```json
{
  "root": "/Users/td/script/rust-s3-sync/test/dst"
}
```

3) 创建任务并运行

- 点击「任务列表」右上角「创建任务」
- 填写：
  - 任务名：`sync-local-demo`
  - 模式：`sync`
  - source：`local_src:`
  - destination：`local_dst:`
  - 启用调度：先不勾（或 cron 留空）
- 提交后在任务列表点击「立即运行」

4) 验证结果

- 观察「实时事件」与「审计日志」是否成功
- 检查目标目录是否已有同步文件：

```bash
ls -la /Users/td/script/rust-s3-sync/test/dst
```

可选：
- 首次建议先用 `copy` 或勾选 `dry-run` 做验证，再切换到 `sync`
- Windows 用户将 `root` 换成类似 `D:/sync-demo/src`、`D:/sync-demo/dst`

### 后端 API（核心）

- `GET /api/remotes`：远端配置列表
- `POST /api/remotes`：创建远端配置
- `PUT /api/remotes/:name`：更新远端配置
- `DELETE /api/remotes/:name`：删除远端配置
- `GET /api/tasks`：任务列表
- `POST /api/tasks`：创建任务
- `PUT /api/tasks/:id`：编辑任务
- `DELETE /api/tasks/:id`：删除任务
- `POST /api/tasks/:id/run`：立即运行
- `POST /api/tasks/:id/pause`：暂停调度
- `POST /api/tasks/:id/resume`：恢复调度
- `GET /api/runs?task_id=<id>&limit=50`：执行历史
- `GET /api/dashboard`：仪表盘数据（CPU/内存、今日流量、运行任务数）
- `GET /ws`：实时任务进度与错误事件

### Cron 规则说明

- 使用 `tokio-cron-scheduler` 的 cron 表达式，常见 6 段格式示例：
  - `0 */10 * * * *`：每 10 分钟
  - `0 0 * * * *`：每小时整点
  - `0 30 2 * * *`：每天 02:30

## MinIO 端到端测试

项目内置了基于 Docker + MinIO 的 E2E 脚本，会自动验证 `copy/sync/move/ls`：

```bash
./scripts/e2e_minio.sh
```

脚本会自动：
- 启动 MinIO（`127.0.0.1:9000`，控制台 `127.0.0.1:9001`）
- 创建测试桶
- 生成测试数据与临时配置
- 执行并校验 `copy`、`sync --dry-run`、`sync`、`move`

依赖：`docker`、`cargo`、`curl`

说明：
- 默认 `KEEP_MINIO_UP=1`，测试通过后 MinIO 不会自动关闭，方便登录控制台验证
- 如需测试结束自动清理容器：

```bash
KEEP_MINIO_UP=0 ./scripts/e2e_minio.sh
```

## 配置文件

默认路径：`~/.config/rust-s3-sync/config.toml`  
可通过环境变量覆盖：`RUST_S3_SYNC_CONFIG=/path/to/config.toml`

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
# access_key_id / secret_access_key 可不写，走环境变量 AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY

[remotes.sftp1]
type = "sftp"
endpoint = "ssh://user@example.com:22"
user = "user"
key = "/Users/td/.ssh/id_rsa"
root = "/data"
```
