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
