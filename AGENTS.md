# Repository Guidelines

## Project Structure & Module Organization
- `src/` contains the Rust application code:
- `main.rs` wires CLI commands and runtime options.
- `cli.rs` defines clap arguments/subcommands (`copy`, `sync`, `move`, `ls`, `config`, `server`).
- `engine.rs` implements transfer/sync behavior; `remote.rs` builds OpenDAL operators; `config.rs` handles config loading; `server.rs` provides the web admin API/UI host.
- `tests/e2e/` holds integration test infra (`docker-compose.minio.yml`).
- `scripts/e2e_minio.sh` is the primary end-to-end test script.
- `web/dist/index.html` is the bundled frontend artifact served by the backend.

## Build, Test, and Development Commands
- `cargo build` builds the binary.
- `cargo run -- <subcommand> ...` runs locally, for example:
- `cargo run -- copy local:/data/src s3prod:bucket/prefix`
- `cargo run -- server --host 127.0.0.1 --port 3030 --db rust_s3_sync.db`
- `cargo test` runs Rust tests (currently lightweight; rely on e2e for behavior coverage).
- `bash scripts/e2e_minio.sh` runs MinIO-backed e2e validation for `copy/sync/move/ls`.
- `cargo fmt --all` formats code; `cargo clippy --all-targets -- -D warnings` catches lints before PR.

## Coding Style & Naming Conventions
- Use standard Rust formatting (`rustfmt`) with 4-space indentation.
- Keep modules focused by responsibility (CLI parsing, config, engine, server).
- Use `snake_case` for functions/variables/modules, `PascalCase` for structs/enums, and descriptive command/flag names.
- Prefer `anyhow::Result` + `Context` for user-facing error propagation.

## Testing Guidelines
- Put integration infrastructure under `tests/e2e/`; keep test scripts reproducible and non-interactive.
- For feature changes, add or extend assertions in `scripts/e2e_minio.sh`.
- Validate risky sync behavior with `--dry-run` paths first in tests and manual checks.

## Commit & Pull Request Guidelines
- Existing history mixes generic `update...` commits with conventional prefixes (`feat:`). Prefer Conventional Commits going forward (for example, `feat: add checksum toggle for sync`).
- Keep commits scoped to one change.
- PRs should include:
- what changed and why,
- affected commands/modules (for example `src/engine.rs`),
- test evidence (`cargo test`, e2e script output),
- UI screenshots when changing `server`/`web` behavior.

## Security & Configuration Tips
- Never commit real secrets in `config.toml` or database files.
- Use `RUST_S3_SYNC_CONFIG` for test/local overrides.
- Treat `sync` and `move` as destructive operations; verify with `--dry-run` first.
