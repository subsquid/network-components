pub mod cache;
pub mod client;
pub mod clock;
pub mod middleware;
pub mod singleflight;
pub mod topkeys;

use std::sync::Arc;

use ipnet::IpNet;

pub use cache::KeyCache;
pub use client::NetworkApiClient;
pub use singleflight::Singleflight;
pub use topkeys::TopKeys;

pub struct AuthState {
    pub cache: KeyCache,
    pub client: NetworkApiClient,
    pub top_keys: TopKeys,
    pub inflight: Singleflight,
    /// Global kill switch. When true, the middleware bails out before any
    /// header parsing or cache work and counts the request as `disabled`.
    /// Overrides every other knob.
    pub disabled: bool,
    /// CIDRs whose source IPs trigger enforcement (deny on missing/invalid).
    /// One knob — empty means "never enforce", `*` (expanded to
    /// `0.0.0.0/0,::/0` at parse time) means "enforce for everyone",
    /// specific CIDRs mean "canary scope".
    pub enforce_for_ips: Vec<IpNet>,
    /// Trusted upstream proxies. Used by the middleware to walk
    /// `X-Original-Forwarded-For` rightmost-first and discard hops we put
    /// there ourselves; the first non-trusted IP from the right is treated
    /// as the real client. Empty -> XOFF is taken at face value.
    pub trusted_ips: Vec<IpNet>,
    /// Source IPs allowed to bypass Bearer auth. Matched against the
    /// resolved real-client IP (see `trusted_ips`). Empty -> bypass disabled.
    pub internal_allowlist: Vec<IpNet>,
}

impl AuthState {
    pub fn new(
        client: NetworkApiClient,
        disabled: bool,
        enforce_for_ips: Vec<IpNet>,
        trusted_ips: Vec<IpNet>,
        internal_allowlist: Vec<IpNet>,
    ) -> Arc<Self> {
        Arc::new(Self {
            cache: KeyCache::new(10_000),
            client,
            top_keys: TopKeys::new(100),
            inflight: Singleflight::new(),
            disabled,
            enforce_for_ips,
            trusted_ips,
            internal_allowlist,
        })
    }

    /// Test helper. `enforce` mirrors the old boolean: `true` -> enforce for
    /// every source (expand to `0.0.0.0/0` + `::/0`), `false` -> never enforce.
    /// Existing tests keep their boolean call shape; richer scope tests use
    /// [`AuthState::for_test_full`].
    #[cfg(test)]
    pub fn for_test(
        base_url: Option<url::Url>,
        enforce: bool,
        clock: Arc<dyn clock::Clock>,
    ) -> Arc<Self> {
        Self::for_test_full(
            base_url,
            clock,
            false,
            if enforce { all_ips() } else { Vec::new() },
            Vec::new(),
            Vec::new(),
        )
    }

    #[cfg(test)]
    pub fn for_test_with_bypass(
        base_url: Option<url::Url>,
        enforce: bool,
        clock: Arc<dyn clock::Clock>,
        trusted_ips: Vec<IpNet>,
        internal_allowlist: Vec<IpNet>,
    ) -> Arc<Self> {
        Self::for_test_full(
            base_url,
            clock,
            false,
            if enforce { all_ips() } else { Vec::new() },
            trusted_ips,
            internal_allowlist,
        )
    }

    /// Test helper exposing every knob directly. Use when verifying the
    /// kill switch or narrow-scope canary; otherwise prefer the simpler
    /// `for_test` / `for_test_with_bypass`.
    #[cfg(test)]
    pub fn for_test_full(
        base_url: Option<url::Url>,
        clock: Arc<dyn clock::Clock>,
        disabled: bool,
        enforce_for_ips: Vec<IpNet>,
        trusted_ips: Vec<IpNet>,
        internal_allowlist: Vec<IpNet>,
    ) -> Arc<Self> {
        Arc::new(Self {
            cache: KeyCache::with_clock(10_000, clock.clone()),
            client: NetworkApiClient::with_clock(base_url, clock),
            top_keys: TopKeys::new(100),
            inflight: Singleflight::new(),
            disabled,
            enforce_for_ips,
            trusted_ips,
            internal_allowlist,
        })
    }
}

/// `0.0.0.0/0` + `::/0` — the "enforce for every source" set used both by
/// the CLI parser (when the user writes `*`) and by the test boolean shim.
#[cfg(test)]
fn all_ips() -> Vec<IpNet> {
    vec![
        "0.0.0.0/0".parse().unwrap(),
        "::/0".parse().unwrap(),
    ]
}
