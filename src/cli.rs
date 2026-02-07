use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Debug, Parser)]
#[command(
    name = "rust-s3-sync",
    version,
    about = "Multi backend sync tool based on OpenDAL"
)]
pub struct Cli {
    #[arg(long, global = true, default_value = "info")]
    pub log_level: String,

    #[arg(long, global = true, default_value_t = 4)]
    pub transfers: usize,

    #[arg(long, global = true, default_value_t = 8)]
    pub checkers: usize,

    #[arg(long, global = true)]
    pub dry_run: bool,

    #[arg(long, global = true)]
    pub include: Vec<String>,

    #[arg(long, global = true)]
    pub exclude: Vec<String>,

    #[arg(long, global = true)]
    pub bandwidth_limit: Option<String>,

    #[arg(long, global = true)]
    pub checksum: bool,

    #[arg(long, global = true)]
    pub ignore_existing: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    Copy(TransferArgs),
    Sync(TransferArgs),
    Move(TransferArgs),
    Ls(ListArgs),
    Config(ConfigArgs),
    Server(ServerArgs),
}

#[derive(Debug, Args)]
pub struct TransferArgs {
    pub source: String,
    pub destination: String,
}

#[derive(Debug, Args)]
pub struct ListArgs {
    pub target: String,
}

#[derive(Debug, Args)]
pub struct ConfigArgs {
    #[command(subcommand)]
    pub action: ConfigAction,
}

#[derive(Debug, Args)]
pub struct ServerArgs {
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    #[arg(long, default_value_t = 3030)]
    pub port: u16,

    #[arg(long, default_value = "rust_s3_sync.db")]
    pub db: String,
}

#[derive(Debug, Subcommand)]
pub enum ConfigAction {
    Init,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum HashAlgo {
    Md5,
    Sha256,
}
