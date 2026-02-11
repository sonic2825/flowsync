use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use dialoguer::{Input, Password, Select};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    #[serde(default)]
    pub remotes: HashMap<String, RemoteConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RemoteConfig {
    #[serde(rename = "fs")]
    Fs { root: String },
    #[serde(rename = "s3")]
    S3 {
        bucket: Option<String>,
        endpoint: Option<String>,
        region: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        session_token: Option<String>,
        url_style: Option<String>,
        root: Option<String>,
    },
    #[serde(rename = "ftp")]
    Ftp {
        endpoint: String,
        user: Option<String>,
        password: Option<String>,
        root: Option<String>,
    },
    #[serde(rename = "sftp")]
    Sftp {
        endpoint: String,
        user: Option<String>,
        password: Option<String>,
        key: Option<String>,
        root: Option<String>,
    },
}

impl AppConfig {
    pub fn load() -> Result<Self> {
        let path = load_config_path()?;
        if !path.exists() {
            return Ok(Self {
                remotes: HashMap::new(),
            });
        }
        let content = fs::read_to_string(&path)
            .with_context(|| format!("failed to read config file: {}", path.display()))?;
        let cfg: Self = toml::from_str(&content).context("failed to parse toml config")?;
        Ok(cfg)
    }

    pub fn save(&self) -> Result<()> {
        let path = default_config_path()?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create config dir: {}", parent.display()))?;
        }
        let body = toml::to_string_pretty(self).context("failed to serialize config")?;
        fs::write(&path, body)
            .with_context(|| format!("failed to write config file: {}", path.display()))?;
        Ok(())
    }
}

pub fn default_config_path() -> Result<PathBuf> {
    if let Ok(explicit) = env::var("FLOWSYNC_CONFIG") {
        return Ok(PathBuf::from(explicit));
    }
    if let Ok(explicit) = env::var("RUST_S3_SYNC_CONFIG") {
        return Ok(PathBuf::from(explicit));
    }
    let home = env::var("HOME").context("HOME env is not set")?;
    Ok(PathBuf::from(home)
        .join(".config")
        .join("flowsync")
        .join("config.toml"))
}

fn load_config_path() -> Result<PathBuf> {
    if let Ok(explicit) = env::var("FLOWSYNC_CONFIG") {
        return Ok(PathBuf::from(explicit));
    }
    if let Ok(explicit) = env::var("RUST_S3_SYNC_CONFIG") {
        return Ok(PathBuf::from(explicit));
    }
    let home = env::var("HOME").context("HOME env is not set")?;
    let preferred = PathBuf::from(&home)
        .join(".config")
        .join("flowsync")
        .join("config.toml");
    if preferred.exists() {
        return Ok(preferred);
    }
    let legacy = PathBuf::from(home)
        .join(".config")
        .join("rust-s3-sync")
        .join("config.toml");
    if legacy.exists() {
        return Ok(legacy);
    }
    Ok(preferred)
}

pub fn run_config_wizard() -> Result<()> {
    let mut cfg = AppConfig::load()?;

    let remote_name: String = Input::new()
        .with_prompt("Remote name")
        .interact_text()
        .context("failed to read remote name")?;

    let backend_idx = Select::new()
        .with_prompt("Backend type")
        .items(&["fs", "s3", "sftp"])
        .default(0)
        .interact()
        .context("failed to read backend type")?;

    let remote = match backend_idx {
        0 => {
            let root: String = Input::new()
                .with_prompt("FS root path")
                .default("/".to_string())
                .interact_text()
                .context("failed to read fs root")?;
            RemoteConfig::Fs { root }
        }
        1 => {
            let bucket: String = Input::new()
                .allow_empty(true)
                .with_prompt("S3 bucket (optional)")
                .interact_text()
                .context("failed to read bucket")?;
            let endpoint: String = Input::new()
                .allow_empty(true)
                .with_prompt("S3 endpoint (optional)")
                .interact_text()
                .context("failed to read endpoint")?;
            let region: String = Input::new()
                .allow_empty(true)
                .with_prompt("S3 region (optional)")
                .interact_text()
                .context("failed to read region")?;
            let ak: String = Input::new()
                .allow_empty(true)
                .with_prompt("Access key (optional, env fallback)")
                .interact_text()
                .context("failed to read access key")?;
            let sk: String = Password::new()
                .allow_empty_password(true)
                .with_prompt("Secret key (optional, env fallback)")
                .interact()
                .context("failed to read secret key")?;
            let url_style: String = Input::new()
                .allow_empty(true)
                .with_prompt("URL style (path|virtual_hosted, optional)")
                .interact_text()
                .context("failed to read url style")?;
            let root: String = Input::new()
                .allow_empty(true)
                .with_prompt("S3 root prefix (optional)")
                .interact_text()
                .context("failed to read root prefix")?;

            RemoteConfig::S3 {
                bucket: optional(bucket),
                endpoint: optional(endpoint),
                region: optional(region),
                access_key_id: optional(ak),
                secret_access_key: optional(sk),
                session_token: None,
                url_style: optional(url_style),
                root: optional(root),
            }
        }
        _ => {
            let endpoint: String = Input::new()
                .with_prompt("SFTP endpoint (host:port)")
                .interact_text()
                .context("failed to read sftp endpoint")?;
            let user: String = Input::new()
                .allow_empty(true)
                .with_prompt("SFTP user (optional)")
                .interact_text()
                .context("failed to read sftp user")?;
            let password: String = Password::new()
                .allow_empty_password(true)
                .with_prompt("SFTP password (optional)")
                .interact()
                .context("failed to read sftp password")?;
            let key: String = Input::new()
                .allow_empty(true)
                .with_prompt("SFTP private key path (optional)")
                .interact_text()
                .context("failed to read sftp key")?;
            let root: String = Input::new()
                .allow_empty(true)
                .with_prompt("SFTP root (optional)")
                .interact_text()
                .context("failed to read sftp root")?;
            RemoteConfig::Sftp {
                endpoint,
                user: optional(user),
                password: optional(password),
                key: optional(key),
                root: optional(root),
            }
        }
    };

    cfg.remotes.insert(remote_name, remote);
    cfg.save()?;

    let path = default_config_path()?;
    println!("Config saved at {}", path.display());
    Ok(())
}

fn optional(value: String) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}
