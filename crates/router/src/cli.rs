use std::str::FromStr;

use clap::Parser;
use ipnet::IpNet;
use url::Url;

use crate::dataset::Dataset;

/// Comma-separated list of CIDRs (e.g. "10.0.0.0/8,35.1.2.0/29").
/// Bare IPs are accepted as /32 (IPv4) or /128 (IPv6). Empty string -> empty list.
/// The wildcard token `*` expands to both `0.0.0.0/0` and `::/0` so it matches
/// any source — used as the "enforce for everyone" shorthand.
#[derive(Clone, Debug, Default)]
pub struct CidrList(pub Vec<IpNet>);

impl FromStr for CidrList {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut nets = Vec::new();
        for item in s.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            if item == "*" {
                nets.push("0.0.0.0/0".parse().expect("static CIDR"));
                nets.push("::/0".parse().expect("static CIDR"));
                continue;
            }
            let parsed = item
                .parse::<IpNet>()
                .or_else(|_| item.parse::<std::net::IpAddr>().map(IpNet::from))
                .map_err(|e| format!("invalid CIDR `{}`: {}", item, e))?;
            nets.push(parsed);
        }
        Ok(CidrList(nets))
    }
}

fn parse_dataset(s: &str) -> Result<Dataset, String> {
    let pos = s
        .find('=')
        .ok_or_else(|| format!("invalid KEY=value: no `=` found in `{}`", s))?;

    let name = s[..pos].to_string();
    if name.is_empty() {
        return Err(format!("invalid KEY=value: the KEY is empty in `{}`", s));
    }

    let value = &s[pos + 1..];
    if !value.starts_with("s3://") {
        return Err(format!("invalid S3 URL: `{}`", value));
    }

    let mut split = s[pos + 6..].split(':');
    let url = format!("s3://{}", split.next().unwrap());
    let start_block = match split.next() {
        Some(value) => {
            match value.parse::<u32>() {
                Ok(value) => Some(value),
                Err(_) => return Err(format!("invalid START_BLOCK: `{}`", value))
            }
        },
        None => None,
    };

    Ok(Dataset::new(name, url, start_block))
}

#[derive(Parser)]
pub struct Cli {
    /// Add dataset `NAME` pointing to S3 `URL`
    #[clap(short, long, value_parser = parse_dataset, value_name = "NAME=URL[:START_BLOCK]")]
    pub dataset: Vec<Dataset>,

    /// Add managed worker
    #[clap(short, long, value_name = "ID")]
    pub worker: Vec<String>,

    /// Data replication factor
    #[clap(short, long, value_name = "N")]
    pub replication: usize,

    /// Size of a data scheduling unit (in chunks)
    #[clap(short = 'u', long, value_name = "N")]
    pub scheduling_unit: usize,

    /// Scheduling interval (in seconds)
    #[clap(short = 'i', long, default_value = "300", value_name = "N")]
    pub scheduling_interval: u64,

    /// Comma-separated CIDRs that scope enforcement. The middleware returns
    /// 403 on missing/invalid keys ONLY when the resolved real-client IP
    /// matches one of these CIDRs; any other source falls through to the
    /// handler (parsing, cache, and Network API calls run either way — the
    /// list changes only the deny action).
    ///
    /// - empty (default) -> never enforce (observe-only canary mode);
    /// - `*` -> enforce for everyone (expands to `0.0.0.0/0,::/0` internally);
    /// - specific CIDRs -> canary scope (enforce just for these clients).
    ///
    /// Replaces the older `ENFORCE_V2_AUTH` boolean — one knob, no ambiguity.
    #[clap(long, env = "ENFORCE_V2_AUTH_FOR_IPS",
           value_name = "CIDR,CIDR,...", default_value = "")]
    pub enforce_v2_auth_for_ips: CidrList,

    /// Global kill switch. When true, the auth middleware short-circuits at
    /// the very top of `decide`: every request is allowed without parsing
    /// headers, touching the cache, or calling the Network API. Counted as
    /// `sqd_v2_auth_total{result="disabled"}` so the dashboard shows the
    /// switch is active. Use only as an emergency disable when something is
    /// broken and we need to drop the auth path entirely until it's fixed.
    #[clap(long, env = "DISABLE_V2_AUTH", default_value = "false")]
    pub disable_v2_auth: bool,

    /// Base URL of the Network API exposing POST /internal/validate.
    /// When unset, the auth middleware fails open on every cache miss.
    #[clap(long, env = "NETWORK_API_URL", value_name = "URL")]
    pub network_api_url: Option<Url>,

    /// Comma-separated CIDRs of trusted upstream proxies (e.g. the public LB
    /// in front of nginx-ingress). Used to walk `X-Original-Forwarded-For`
    /// rightmost-first and discard hops we put there ourselves; the first
    /// non-trusted IP from the right is treated as the real client.
    /// Without this list, the rightmost element of XOFF is taken verbatim,
    /// which may be the LB itself rather than the real client.
    #[clap(long, env = "TRUSTED_IPS", value_name = "CIDR,CIDR,...", default_value = "")]
    pub trusted_ips: CidrList,

    /// Comma-separated CIDRs allowed to bypass Bearer auth based on source IP
    /// (after `TRUSTED_IPS` stripping). Empty disables the IP-based bypass —
    /// every request goes through the standard Bearer path.
    #[clap(long, env = "INTERNAL_ALLOWLIST", value_name = "CIDR,CIDR,...", default_value = "")]
    pub internal_allowlist: CidrList,

    /// RSA private key PEM used to sign short-lived worker JWTs. Required
    /// when V2 auth enforcement is enabled so clients can authenticate to
    /// workers after receiving a worker URL from the router.
    #[clap(long, env = "WORKER_JWT_PRIVATE_KEY_PEM", value_name = "PEM")]
    pub worker_jwt_private_key_pem: Option<String>,

    /// Path to the RSA private key PEM used to sign short-lived worker JWTs.
    /// Ignored when WORKER_JWT_PRIVATE_KEY_PEM is set.
    #[clap(long, env = "WORKER_JWT_PRIVATE_KEY_FILE", value_name = "PATH")]
    pub worker_jwt_private_key_file: Option<std::path::PathBuf>,

    /// Optional key id included in the JWT header for worker key rotation.
    #[clap(long, env = "WORKER_JWT_KID", value_name = "KID")]
    pub worker_jwt_kid: Option<String>,

    /// Worker JWT lifetime in seconds.
    #[clap(long, env = "WORKER_JWT_TTL_SECS", value_name = "SECONDS", default_value = "3600")]
    pub worker_jwt_ttl_secs: u64,
}
