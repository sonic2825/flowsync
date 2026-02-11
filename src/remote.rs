use std::env;

use anyhow::{anyhow, Result};
use opendal::services;
use opendal::Operator;

use crate::config::{AppConfig, RemoteConfig};

#[derive(Debug, Clone)]
pub struct Location {
    pub remote: String,
    pub path: String,
}

struct S3ResolvedConfig<'a> {
    bucket: &'a str,
    endpoint: Option<&'a str>,
    region: Option<&'a str>,
    access_key_id: Option<&'a str>,
    secret_access_key: Option<&'a str>,
    session_token: Option<&'a str>,
    url_style: Option<&'a str>,
    root: Option<&'a str>,
}

impl Location {
    pub fn parse(input: &str) -> Result<Self> {
        let mut parts = input.splitn(2, ':');
        let remote = parts
            .next()
            .ok_or_else(|| anyhow!("invalid location: {input}"))?
            .to_string();
        let path = parts
            .next()
            .ok_or_else(|| anyhow!("invalid location, expected remote:path"))?
            .trim_start_matches('/')
            .to_string();
        Ok(Self { remote, path })
    }
}

pub fn resolve_location_and_operator(
    cfg: &AppConfig,
    mut location: Location,
) -> Result<(Location, Operator)> {
    let remote = cfg
        .remotes
        .get(&location.remote)
        .ok_or_else(|| anyhow!("remote `{}` not found in config", location.remote))?;

    let op = match remote {
        RemoteConfig::S3 {
            bucket,
            endpoint,
            region,
            access_key_id,
            secret_access_key,
            session_token,
            url_style,
            root,
        } => {
            let configured_bucket = bucket.as_deref().map(str::trim).filter(|v| !v.is_empty());
            let effective_bucket = if let Some(bucket) = configured_bucket {
                bucket.to_string()
            } else {
                let (bucket, path) = split_bucket_and_prefix(&location.path)
                    .ok_or_else(|| anyhow!("s3 remote `{}` requires bucket in `remote:bucket/path` when remote bucket is empty", location.remote))?;
                location.path = path;
                bucket
            };

            build_s3_operator(S3ResolvedConfig {
                bucket: &effective_bucket,
                endpoint: endpoint.as_deref(),
                region: region.as_deref(),
                access_key_id: access_key_id.as_deref(),
                secret_access_key: secret_access_key.as_deref(),
                session_token: session_token.as_deref(),
                url_style: url_style.as_deref(),
                root: root.as_deref(),
            })?
        }
        _ => build_operator(cfg, &location.remote)?,
    };

    Ok((location, op))
}

pub fn build_operator(cfg: &AppConfig, remote_name: &str) -> Result<Operator> {
    let remote = cfg
        .remotes
        .get(remote_name)
        .ok_or_else(|| anyhow!("remote `{}` not found in config", remote_name))?;

    let op = match remote {
        RemoteConfig::Fs { root } => {
            let builder = services::Fs::default().root(root);
            Operator::new(builder)?.finish()
        }
        RemoteConfig::S3 {
            bucket,
            endpoint,
            region,
            access_key_id,
            secret_access_key,
            session_token,
            url_style,
            root,
        } => {
            let bucket = bucket
                .as_deref()
                .map(str::trim)
                .filter(|v| !v.is_empty())
                .ok_or_else(|| {
                    anyhow!(
                        "s3 remote `{}` missing bucket; set remote bucket or use `remote:bucket/path`",
                        remote_name
                    )
                })?;
            build_s3_operator(S3ResolvedConfig {
                bucket,
                endpoint: endpoint.as_deref(),
                region: region.as_deref(),
                access_key_id: access_key_id.as_deref(),
                secret_access_key: secret_access_key.as_deref(),
                session_token: session_token.as_deref(),
                url_style: url_style.as_deref(),
                root: root.as_deref(),
            })?
        }
        RemoteConfig::Ftp { .. } => {
            return Err(anyhow!(
                "ftp backend is configured but this build doesn't enable ftp service"
            ))
        }
        RemoteConfig::Sftp {
            endpoint,
            user,
            password: _,
            key,
            root,
        } => {
            let mut builder = services::Sftp::default().endpoint(endpoint);
            if let Some(user) = user {
                builder = builder.user(user);
            }
            if let Some(key) = key {
                builder = builder.key(key);
            }
            if let Some(root) = root {
                builder = builder.root(root);
            }
            Operator::new(builder)?.finish()
        }
    };
    Ok(op)
}

fn build_s3_operator(cfg: S3ResolvedConfig<'_>) -> Result<Operator> {
    let mut builder = services::S3::default().bucket(cfg.bucket);
    if let Some(ep) = cfg.endpoint {
        builder = builder.endpoint(ep);
    }
    if let Some(region) = cfg.region {
        builder = builder.region(region);
    }
    if let Some(root) = cfg.root {
        builder = builder.root(root);
    }

    let ak = cfg
        .access_key_id
        .map(str::to_string)
        .or_else(|| env::var("AWS_ACCESS_KEY_ID").ok());
    let sk = cfg
        .secret_access_key
        .map(str::to_string)
        .or_else(|| env::var("AWS_SECRET_ACCESS_KEY").ok());
    let token = cfg
        .session_token
        .map(str::to_string)
        .or_else(|| env::var("AWS_SESSION_TOKEN").ok());

    if let Some(ak) = ak {
        builder = builder.access_key_id(&ak);
    }
    if let Some(sk) = sk {
        builder = builder.secret_access_key(&sk);
    }
    if let Some(token) = token {
        builder = builder.session_token(&token);
    }
    if let Some(style) = cfg.url_style {
        if style.eq_ignore_ascii_case("virtual_hosted") {
            builder = builder.enable_virtual_host_style();
        }
    }

    Ok(Operator::new(builder)?.finish())
}

fn split_bucket_and_prefix(path: &str) -> Option<(String, String)> {
    let trimmed = path.trim_start_matches('/');
    let mut parts = trimmed.splitn(2, '/');
    let bucket = parts.next()?.trim();
    if bucket.is_empty() {
        return None;
    }
    let prefix = parts
        .next()
        .unwrap_or("")
        .trim_start_matches('/')
        .to_string();
    Some((bucket.to_string(), prefix))
}
