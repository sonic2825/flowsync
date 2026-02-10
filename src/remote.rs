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

pub fn resolve_location_and_operator(cfg: &AppConfig, mut location: Location) -> Result<(Location, Operator)> {
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

            build_s3_operator(
                &effective_bucket,
                endpoint,
                region,
                access_key_id,
                secret_access_key,
                session_token,
                url_style,
                root,
            )?
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
            build_s3_operator(
                bucket,
                endpoint,
                region,
                access_key_id,
                secret_access_key,
                session_token,
                url_style,
                root,
            )?
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

fn build_s3_operator(
    bucket: &str,
    endpoint: &Option<String>,
    region: &Option<String>,
    access_key_id: &Option<String>,
    secret_access_key: &Option<String>,
    session_token: &Option<String>,
    url_style: &Option<String>,
    root: &Option<String>,
) -> Result<Operator> {
    let mut builder = services::S3::default().bucket(bucket);
    if let Some(ep) = endpoint {
        builder = builder.endpoint(ep);
    }
    if let Some(region) = region {
        builder = builder.region(region);
    }
    if let Some(root) = root {
        builder = builder.root(root);
    }

    let ak = access_key_id
        .clone()
        .or_else(|| env::var("AWS_ACCESS_KEY_ID").ok());
    let sk = secret_access_key
        .clone()
        .or_else(|| env::var("AWS_SECRET_ACCESS_KEY").ok());
    let token = session_token
        .clone()
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
    if let Some(style) = url_style {
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
    let prefix = parts.next().unwrap_or("").trim_start_matches('/').to_string();
    Some((bucket.to_string(), prefix))
}
