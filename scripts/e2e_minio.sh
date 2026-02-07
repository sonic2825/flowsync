#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT_DIR/tests/e2e/docker-compose.minio.yml"
WORK_DIR="$ROOT_DIR/.tmp/e2e"
CONFIG_FILE="$WORK_DIR/config.toml"
BUCKET="e2e-bucket"
KEEP_MINIO_UP="${KEEP_MINIO_UP:-1}"

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 1
  fi
}

need_cmd docker
need_cmd cargo
need_cmd curl

compose() {
  docker compose -f "$COMPOSE_FILE" "$@"
}

cleanup() {
  if [[ "$KEEP_MINIO_UP" != "1" ]]; then
    compose down -v >/dev/null 2>&1 || true
  fi
  if [[ "${KEEP_E2E_ARTIFACTS:-0}" != "1" ]]; then
    rm -rf "$WORK_DIR"
  fi
}

trap cleanup EXIT

echo "[1/7] 启动 MinIO..."
compose up -d minio mc

echo "[2/7] 等待 MinIO 就绪..."
for i in $(seq 1 30); do
  if curl -sf "http://127.0.0.1:9000/minio/health/ready" >/dev/null 2>&1; then
    break
  fi
  if [[ "$i" -eq 30 ]]; then
    echo "MinIO 未就绪" >&2
    exit 1
  fi
  sleep 1
done

echo "[3/7] 创建测试桶..."
compose exec -T mc sh -lc "mc alias set local http://minio:9000 minioadmin minioadmin >/dev/null && mc mb --ignore-existing local/$BUCKET >/dev/null"

echo "[4/7] 准备本地测试数据与配置..."
rm -rf "$WORK_DIR"
mkdir -p "$WORK_DIR/src/dir1" "$WORK_DIR/move_src" "$WORK_DIR/expected"
printf 'alpha\n' > "$WORK_DIR/src/a.txt"
printf 'beta\n' > "$WORK_DIR/src/dir1/b.txt"
printf 'move-me\n' > "$WORK_DIR/move_src/m1.txt"

cat > "$CONFIG_FILE" <<CFG
[remotes.local]
type = "fs"
root = "$ROOT_DIR"

[remotes.minio]
type = "s3"
bucket = "$BUCKET"
endpoint = "http://127.0.0.1:9000"
region = "us-east-1"
access_key_id = "minioadmin"
secret_access_key = "minioadmin"
url_style = "path"
root = ""
CFG

export RUST_S3_SYNC_CONFIG="$CONFIG_FILE"

echo "[5/7] 编译并执行 copy + ls 验证..."
cargo run -- copy local:.tmp/e2e/src minio:copy_case --transfers 4 --log-level info >/dev/null
LS_OUTPUT="$(cargo run -- ls minio:copy_case)"
echo "$LS_OUTPUT" | grep -q "copy_case/a.txt"
echo "$LS_OUTPUT" | grep -q "copy_case/dir1/b.txt"

echo "[6/7] 执行 sync（含删除）验证..."
rm -f "$WORK_DIR/src/dir1/b.txt"
printf 'gamma\n' > "$WORK_DIR/src/c.txt"

DRY_RUN_OUTPUT="$(cargo run -- sync local:.tmp/e2e/src minio:copy_case --dry-run)"
echo "$DRY_RUN_OUTPUT" | grep -q "\[DRY-RUN\] delete dir1/b.txt"

cargo run -- sync local:.tmp/e2e/src minio:copy_case --transfers 4 >/dev/null
POST_SYNC="$(cargo run -- ls minio:copy_case)"
echo "$POST_SYNC" | grep -q "copy_case/a.txt"
echo "$POST_SYNC" | grep -q "copy_case/c.txt"
if echo "$POST_SYNC" | grep -q "copy_case/dir1/b.txt"; then
  echo "sync 删除校验失败: b.txt 仍存在" >&2
  exit 1
fi

echo "[7/7] 执行 move 验证..."
cargo run -- move local:.tmp/e2e/move_src minio:move_case >/dev/null
if [[ -f "$WORK_DIR/move_src/m1.txt" ]]; then
  echo "move 校验失败: 源文件未删除" >&2
  exit 1
fi
MOVE_LIST="$(cargo run -- ls minio:move_case)"
echo "$MOVE_LIST" | grep -q "move_case/m1.txt"

echo

echo "E2E 测试通过"
echo "MinIO 控制台: http://127.0.0.1:9001 (minioadmin / minioadmin)"
if [[ "$KEEP_MINIO_UP" == "1" ]]; then
  echo "MinIO 已保持运行。"
  echo "手动关闭命令: docker compose -f \"$COMPOSE_FILE\" down -v"
fi
