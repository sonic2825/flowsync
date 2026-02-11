#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK_DIR="$ROOT_DIR/.tmp/e2e"
CONFIG_FILE="$WORK_DIR/config.toml"

S3_BUCKET="${S3_BUCKET:-test1}"
S3_ENDPOINT="${S3_ENDPOINT:-http://rustfs1.pops.metax-tech.com}"
S3_REGION="${S3_REGION:-us-east-1}"
S3_ACCESS_KEY_ID="${S3_ACCESS_KEY_ID:-rustfsadmin}"
S3_SECRET_ACCESS_KEY="${S3_SECRET_ACCESS_KEY:-rustfsadmin}"
S3_URL_STYLE="${S3_URL_STYLE:-path}"
S3_ROOT="${S3_ROOT:-}"

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

need_cmd cargo

cleanup() {
  if [[ "${KEEP_E2E_ARTIFACTS:-0}" != "1" ]]; then
    rm -rf "$WORK_DIR"
  fi
}

trap cleanup EXIT

echo "[1/4] 准备本地测试数据与配置..."
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR/src/dir1" "$WORK_DIR/move_src" "$WORK_DIR/expected"
printf 'alpha\n' > "$WORK_DIR/src/a.txt"
printf 'beta\n' > "$WORK_DIR/src/dir1/b.txt"
printf 'move-me\n' > "$WORK_DIR/move_src/m1.txt"

cat > "$CONFIG_FILE" <<CFG
[remotes.local]
type = "fs"
root = "$ROOT_DIR"

[remotes.s3test]
type = "s3"
bucket = "$S3_BUCKET"
endpoint = "$S3_ENDPOINT"
region = "$S3_REGION"
access_key_id = "$S3_ACCESS_KEY_ID"
secret_access_key = "$S3_SECRET_ACCESS_KEY"
url_style = "$S3_URL_STYLE"
root = "$S3_ROOT"
CFG

export FLOWSYNC_CONFIG="$CONFIG_FILE"

echo "[2/4] 编译并执行 copy + ls 验证..."
cargo run -- copy local:.tmp/e2e/src s3test:copy_case --transfers 4 --log-level info >/dev/null
LS_OUTPUT="$(cargo run -- ls s3test:copy_case)"
echo "$LS_OUTPUT" | grep -q "copy_case/a.txt"
echo "$LS_OUTPUT" | grep -q "copy_case/dir1/b.txt"

echo "[3/4] 执行 sync（含删除）验证..."
rm -f "$WORK_DIR/src/dir1/b.txt"
printf 'gamma\n' > "$WORK_DIR/src/c.txt"

DRY_RUN_OUTPUT="$(cargo run -- sync local:.tmp/e2e/src s3test:copy_case --dry-run)"
echo "$DRY_RUN_OUTPUT" | grep -q "\[DRY-RUN\] delete dir1/b.txt"

cargo run -- sync local:.tmp/e2e/src s3test:copy_case --transfers 4 >/dev/null
POST_SYNC="$(cargo run -- ls s3test:copy_case)"
echo "$POST_SYNC" | grep -q "copy_case/a.txt"
echo "$POST_SYNC" | grep -q "copy_case/c.txt"
if echo "$POST_SYNC" | grep -q "copy_case/dir1/b.txt"; then
  echo "sync 删除校验失败: b.txt 仍存在" >&2
  exit 1
fi

echo "[4/4] 执行 move 验证..."
cargo run -- move local:.tmp/e2e/move_src s3test:move_case >/dev/null
if [[ -f "$WORK_DIR/move_src/m1.txt" ]]; then
  echo "move 校验失败: 源文件未删除" >&2
  exit 1
fi
MOVE_LIST="$(cargo run -- ls s3test:move_case)"
echo "$MOVE_LIST" | grep -q "move_case/m1.txt"

echo
echo "E2E 测试通过"
echo "S3 endpoint: $S3_ENDPOINT"
echo "S3 bucket: $S3_BUCKET"
