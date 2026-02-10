mod cli;
mod config;
mod engine;
mod remote;
mod server;

use anyhow::{Context, Result};
use clap::Parser;
use tracing_subscriber::EnvFilter;

use crate::cli::{Cli, Commands};
use crate::engine::{list, parse_bandwidth_limit, run_transfer, SyncMode, SyncOptions};
use crate::remote::Location;
use crate::server::run_server;

#[derive(Clone)]
struct RuntimeOptions {
    dry_run: bool,
    transfers: usize,
    checkers: usize,
    checksum: bool,
    ignore_existing: bool,
    include: Vec<String>,
    exclude: Vec<String>,
    bandwidth_limit: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_new(&cli.log_level).unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(false)
        .init();

    let runtime = RuntimeOptions {
        dry_run: cli.dry_run,
        transfers: cli.transfers,
        checkers: cli.checkers,
        checksum: cli.checksum,
        ignore_existing: cli.ignore_existing,
        include: cli.include.clone(),
        exclude: cli.exclude.clone(),
        bandwidth_limit: cli.bandwidth_limit.clone(),
    };

    match cli.command {
        Commands::Config(cfg) => match cfg.action {
            cli::ConfigAction::Init => config::run_config_wizard(),
        },
        Commands::Ls(args) => {
            let cfg = config::AppConfig::load()?;
            let target = Location::parse(&args.target)?;
            let (target, op) = remote::resolve_location_and_operator(&cfg, target)?;
            list(&op, &target.path).await
        }
        Commands::Copy(args) => {
            execute_transfer(SyncMode::Copy, args.source, args.destination, &runtime).await
        }
        Commands::Sync(args) => {
            execute_transfer(SyncMode::Sync, args.source, args.destination, &runtime).await
        }
        Commands::Move(args) => {
            execute_transfer(SyncMode::Move, args.source, args.destination, &runtime).await
        }
        Commands::Server(args) => {
            let policy = server::EventCleanupPolicy {
                retention_days: args.event_retention_days,
                max_rows: args.event_max_rows,
                interval_secs: args.event_cleanup_interval_secs,
            };
            run_server(args.host, args.port, args.db, policy).await
        }
    }
}

async fn execute_transfer(
    mode: SyncMode,
    source: String,
    destination: String,
    runtime: &RuntimeOptions,
) -> Result<()> {
    let cfg = config::AppConfig::load()?;
    let src = Location::parse(&source).with_context(|| format!("invalid source: {source}"))?;
    let dst = Location::parse(&destination)
        .with_context(|| format!("invalid destination: {destination}"))?;

    let (src, source_op) = remote::resolve_location_and_operator(&cfg, src)?;
    let (dst, target_op) = remote::resolve_location_and_operator(&cfg, dst)?;

    let options = SyncOptions {
        dry_run: runtime.dry_run,
        transfers: runtime.transfers,
        checkers: runtime.checkers,
        checksum: runtime.checksum,
        ignore_existing: runtime.ignore_existing,
        include: runtime.include.clone(),
        exclude: runtime.exclude.clone(),
        bandwidth_limit: parse_bandwidth_limit(runtime.bandwidth_limit.as_deref())?,
    };

    run_transfer(mode, source_op, src, target_op, dst, options).await
}
