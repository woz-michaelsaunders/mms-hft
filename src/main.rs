mod fix_protocol;
mod order_book;
mod order_book_buffer;
mod sequencer;
mod alerts;
mod order_events;
#[cfg(test)]
mod mock_gateway;
#[cfg(test)]
mod tls_test_listener;
mod algo_runtime;

use fix_server::{algo, kdb_integration, kdb_query};
use anyhow::{anyhow, bail, Result};
use fix_protocol::{FIXMessage, tags};
use log::{info, error, warn};
use serde::{Deserialize, Deserializer};
use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::collections::{HashMap, HashSet, VecDeque};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc, broadcast};
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration, interval, Instant};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
#[cfg(unix)]
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use zmq;
use order_book::{OrderBookSnapshot, Side, UpdateAction};
use order_book_buffer::DoubleBufferedOrderBook;
use arc_swap::ArcSwap;
use kdb_integration::KdbWriter;
use axum::{
    extract::Path as AxumPath,
    http::{header::{CACHE_CONTROL, HeaderValue}, StatusCode},
    response::{Html, Json},
    routing::{get, post},
    Router,
};
use tower_http::cors::CorsLayer;

const MAX_TICK_BUFFER: usize = 10_000;
const STALE_THRESHOLD_MS: u64 = 30_000;
const QUIET_FEED_LOG_MS: u64 = 60_000;
const COMMAND_LOG_MD: &str = "fix_commands.log";
const COMMAND_LOG_OM: &str = "OM_fix_command.log";
const COMMAND_LOG_MAX_BYTES: u64 = 100 * 1024 * 1024; // 100 MiB; rotate iff len > this


// Configuration structure
#[derive(Clone, Debug, serde::Deserialize)]
struct Config {
    #[serde(default)]
    subscriptions: SubscriptionsConfig,
    sessions: Vec<SessionConfig>,
    #[serde(default)]
    credentials: HashMap<String, CredentialConfig>, // [credentials.NAME]
    connection: ConnectionConfig,
    /// OMS hard per-symbol caps. Empty ⇒ exposure gate off (EL-018).
    #[serde(default)]
    symbol_limits: Vec<SymbolLimitConfig>,
    /// Session-level risk knobs. Missing `[risk]` ⇒ drawdown guard off.
    #[serde(default)]
    risk: RiskConfig,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
enum TlsSetting {
    #[default]
    Off,
    On,
    Invalid,
}

impl TlsSetting {
    fn is_enabled(&self) -> bool {
        matches!(self, Self::On)
    }
    fn is_invalid(&self) -> bool {
        matches!(self, Self::Invalid)
    }
}

impl<'de> Deserialize<'de> for TlsSetting {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> serde::de::Visitor<'de> for V {
            type Value = TlsSetting;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "a boolean")
            }
            fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<TlsSetting, E> {
                Ok(if v { TlsSetting::On } else { TlsSetting::Off })
            }
            fn visit_str<E: serde::de::Error>(self, _v: &str) -> Result<TlsSetting, E> {
                Ok(TlsSetting::Invalid)
            }
            fn visit_i64<E: serde::de::Error>(self, _v: i64) -> Result<TlsSetting, E> {
                Ok(TlsSetting::Invalid)
            }
            fn visit_u64<E: serde::de::Error>(self, _v: u64) -> Result<TlsSetting, E> {
                Ok(TlsSetting::Invalid)
            }
            fn visit_f64<E: serde::de::Error>(self, _v: f64) -> Result<TlsSetting, E> {
                Ok(TlsSetting::Invalid)
            }
            fn visit_seq<A: serde::de::SeqAccess<'de>>(self, _a: A) -> Result<TlsSetting, A::Error> {
                Ok(TlsSetting::Invalid)
            }
            fn visit_map<M: serde::de::MapAccess<'de>>(self, _m: M) -> Result<TlsSetting, M::Error> {
                Ok(TlsSetting::Invalid)
            }
        }
        deserializer.deserialize_any(V)
    }
}

#[derive(Clone, Debug, serde::Deserialize)]
struct SessionConfig {
    name: String,
    role: SessionRole,
    host: String,
    port: u16,
    sender_comp_id: String,
    target_comp_id: String,
    credentials_ref: String,
    #[serde(default)]
    symbol_routing: Option<SymbolRouting>,
    /// ADR-0008 overrides. Missing TOML keys deserialize as `None`.
    /// Serde does NOT treat Option<T> as implicit default.
    #[serde(default)]
    heartbeat_interval: Option<u64>,
    #[serde(default)]
    max_reconnect_attempts: Option<u32>,
    #[serde(default)]
    reconnect_backoff_ms: Option<u64>,
    #[serde(default)]
    tls: TlsSetting,
    #[serde(default)]
    tls_ca_path: Option<String>,
    #[serde(default)]
    tls_server_name: Option<String>,
    /// Filled by `validate_config` for `role = market_data` only. Always `None` on OM.
    #[serde(skip)]
    compiled_routing: Option<CompiledSymbolRouting>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum SessionRole {
    OrderManagement,
    MarketData,
}

impl fmt::Display for SessionRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SessionRole::OrderManagement => write!(f, "order_management"),
            SessionRole::MarketData => write!(f, "market_data"),
        }
    }
}

/// Deserialized form. `FirstCharRanges` holds **raw TOML strings**.
#[derive(Clone, Debug, PartialEq, Eq)]
enum SymbolRouting {
    All,
    FirstCharRanges(Vec<String>),
}

impl<'de> Deserialize<'de> for SymbolRouting {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum SymbolRoutingRaw {
            All(String),
            Ranges { first_char_ranges: Vec<String> },
        }
        match SymbolRoutingRaw::deserialize(deserializer)? {
            SymbolRoutingRaw::All(s) if s == "all" => Ok(SymbolRouting::All),
            SymbolRoutingRaw::All(s) => Err(serde::de::Error::custom(format!(
                "symbol_routing string must be exactly \"all\", got {s:?}"
            ))),
            SymbolRoutingRaw::Ranges { first_char_ranges } => {
                Ok(SymbolRouting::FirstCharRanges(first_char_ranges))
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CompiledSymbolRouting {
    All,
    FirstCharRanges(Vec<CharRange>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CharRange {
    start: char, // inclusive, ASCII-uppercased by validate
    end: char,
}

#[derive(Clone, Debug, serde::Deserialize)]
struct CredentialConfig {
    api_key_env: String,
    api_secret_env: String,
}

/// In-memory secrets. NO `#[derive(Debug)]`.
struct ResolvedCredentials {
    api_key: String,
    api_secret: String,
}

impl fmt::Debug for ResolvedCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedCredentials")
            .field("api_key", &"<redacted>")
            .field("api_secret", &"<redacted>")
            .finish()
    }
}

struct SpawnedSession {
    handle: Arc<FIXSession>,
    config: SessionConfig,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SessionLoopKind {
    Connect,
    Heartbeat,
}

impl fmt::Display for SessionLoopKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SessionLoopKind::Connect => write!(f, "connect"),
            SessionLoopKind::Heartbeat => write!(f, "heartbeat"),
        }
    }
}

struct SessionTaskIdentity {
    name: String,
    role: SessionRole,
    host: String,
    port: u16,
    kind: SessionLoopKind,
}

/// Which instruments to subscribe to for market data.
///
/// Subscribing to everything the venue lists was self-defeating: each port
/// lists ~1,100 instruments and every reconnect re-sent an order-book AND a
/// trade request for each, ~24,600 requests in 36 minutes on 2026-09-20. That
/// flood broke the socket mid-send, which forced another reconnect, which sent
/// the flood again. Subscribe to what is actually traded.
#[derive(Debug, Clone, Default, serde::Deserialize)]
struct SubscriptionsConfig {
    /// Symbols to subscribe to. Empty means subscribe to nothing, loudly:
    /// silently defaulting to "everything" is what caused the flood.
    #[serde(default)]
    symbols: Vec<String>,
}

/// Intersect the venue's list for this port with the configured allowlist.
/// Kept separate from the loop so it can be tested.
fn subscriptions_for_port(venue_symbols: &[String], allowlist: &[String]) -> Vec<String> {
    venue_symbols
        .iter()
        .filter(|s| allowlist.iter().any(|a| a == *s))
        .cloned()
        .collect()
}

/// What the stale-feed watchdog should do about a session.
#[derive(Debug, PartialEq, Eq)]
enum FeedHealth {
    /// Traffic is flowing.
    Ok,
    /// The connection is alive (heartbeats arriving) but no market data for a
    /// while. On UAT most instruments are genuinely quiet -- 1,114 of 1,118
    /// snapshots were empty on 2026-09-19 -- so this is worth saying, never
    /// worth tearing a healthy session down for.
    QuietFeed { md_silent_ms: u64 },
    /// Nothing inbound at all, not even a heartbeat: the stream really is stalled.
    Stalled { silent_ms: u64 },
}

fn feed_health(now_ms: u64, last_inbound_ms: u64, last_md_ms: u64, stall_ms: u64, quiet_ms: u64) -> FeedHealth {
    if last_inbound_ms > 0 && now_ms.saturating_sub(last_inbound_ms) > stall_ms {
        return FeedHealth::Stalled { silent_ms: now_ms.saturating_sub(last_inbound_ms) };
    }
    if last_md_ms > 0 && now_ms.saturating_sub(last_md_ms) > quiet_ms {
        return FeedHealth::QuietFeed { md_silent_ms: now_ms.saturating_sub(last_md_ms) };
    }
    FeedHealth::Ok
}

static SUBSCRIPTION_ALLOWLIST: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();

#[derive(Debug, Clone, serde::Deserialize)]
struct ConnectionConfig {
    heartbeat_interval: u64,
    max_reconnect_attempts: u32,
    reconnect_backoff_ms: u64,
}

/// One `[[symbol_limits]]` row. Caps are OMS HARD limits (not the algo REST soft cap).
#[derive(Clone, Debug, serde::Deserialize)]
struct SymbolLimitConfig {
    symbol: String,
    max_position: f64,
    max_notional: f64,
}

/// `[risk]` table. `None` / finite `≤ 0` ⇒ drawdown guard off (EL-018).
#[derive(Clone, Debug, Default, serde::Deserialize)]
struct RiskConfig {
    #[serde(default)]
    session_max_drawdown: Option<f64>,
}

/// Runtime per-symbol caps. `symbol` is the HashMap key, not stored here.
#[derive(Clone, Copy, Debug)]
struct SymbolLimit {
    max_position: f64,
    max_notional: f64,
}

#[derive(Debug)]
struct Cli {
    preflight: bool,
    config_path: String,
    logon_check: bool,
    timeout_secs: u64,
}

#[derive(Debug)]
enum CliParse {
    Help,
    Run(Cli),
}

enum CredStatus {
    Ok(ResolvedCredentials),
    Fail,
}

enum TcpCheckErr {
    Timeout { secs: u64 },
    Io(std::io::Error),
}

impl fmt::Display for TcpCheckErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TcpCheckErr::Timeout { secs } => write!(f, "timed out after {secs}s"),
            TcpCheckErr::Io(e) => write!(f, "{e}"),
        }
    }
}

enum LogonAck {
    Ok,
    Reject,
    Logout,
    Timeout { secs: u64 },
    Io(String),
}

enum LogonCell {
    Skipped,
    Ok,
    Fail,
}

struct LogonCheckOutcome {
    reachable: bool,
    logon: LogonCell,
}

struct PreflightRow {
    session: String,
    role: SessionRole,
    addr: String,
    creds_ok: bool,
    reachable: bool,
    logon: LogonCell,
}

const PREFLIGHT_USAGE: &str = "USAGE: fix_server [--config PATH] | --preflight [--config PATH] [--logon-check] [--timeout-secs N] | --help";

fn usage_error(first: &str) -> String {
    format!("{first}\n{PREFLIGHT_USAGE}")
}

/// Append one line to `startup.log`, independent of the logging framework and
/// of whatever pm2 is doing with our stdout/stderr.
///
/// On 2026-09-11 the pm2 daemon was restarted; its log pipes were torn down
/// mid-cycle (`ERR_STREAM_DESTROYED`) and two consecutive `fix_server` starts
/// failed with *zero* diagnostic output in either log. This file is opened,
/// written and flushed per call, so it survives that and any future variant of
/// it. It only records startup milestones, so it stays small — the main logs
/// are already ~80 MB/day and must not be doubled.
fn startup_note(msg: &str) {
    use std::io::Write;
    let line = format!(
        "{} pid={} {}\n",
        chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
        std::process::id(),
        msg
    );
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true).append(true).open("startup.log")
    {
        let _ = f.write_all(line.as_bytes());
        let _ = f.flush();
    }
    let _ = std::io::stderr().write_all(line.as_bytes());
    let _ = std::io::stderr().flush();
}

fn print_preflight_help() {
    println!(
        "\
USAGE:
  fix_server [--config PATH]
      LIVE server. Starts kdb, FIX sessions, loops, and (if OM is present) order entry.
      Default PATH is config.toml.

  fix_server --preflight [--config PATH] [--timeout-secs N]
      Deploy GATE. Parses/validates config, checks credentials and TCP.
      Does not start sessions, kdb, dashboard, algo, or ZMQ. Exit 0/1.

  fix_server --preflight --logon-check [--config PATH] [--timeout-secs N]
      Same gate, plus opt-in FIX Logon/Logout. Requires --preflight.
      Venue-visible: Logon always sends 141=Y (sequence reset). Not the default gate.
      Do not run against production as part of initial rollout.

  fix_server --help

--config PATH without --preflight is HFT-042 cutover (live process on that file),
not a dry run. Unknown flags: exit 1."
    );
}

/// Parse `std::env::args().skip(1)`. No clap. Fail closed.
fn parse_cli<I, S>(args: I) -> Result<CliParse, String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let args: Vec<String> = args.into_iter().map(|s| s.as_ref().to_string()).collect();
    if args.iter().any(|a| a == "-h" || a == "--help") {
        return Ok(CliParse::Help);
    }

    let mut preflight = false;
    let mut config_path: Option<String> = None;
    let mut logon_check = false;
    let mut timeout_secs: Option<u64> = None;
    let mut timeout_present = false;

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--preflight" => preflight = true,
            "--logon-check" => logon_check = true,
            "--config" => {
                i += 1;
                let raw = args.get(i).map(|s| s.as_str()).unwrap_or("");
                if raw.trim().is_empty() {
                    return Err(usage_error("fix_server: --config requires a path"));
                }
                config_path = Some(raw.to_string());
            }
            "--timeout-secs" => {
                timeout_present = true;
                i += 1;
                let raw = args.get(i).map(|s| s.as_str());
                match raw.and_then(|s| s.parse::<u64>().ok()).filter(|&n| n >= 1) {
                    Some(n) => timeout_secs = Some(n),
                    None => {
                        return Err(usage_error(
                            "fix_server: --timeout-secs must be a positive integer",
                        ));
                    }
                }
            }
            other => {
                return Err(usage_error(&format!(
                    "fix_server: unknown argument: {other}"
                )));
            }
        }
        i += 1;
    }

    if logon_check && !preflight {
        return Err(usage_error(
            "fix_server: --logon-check requires --preflight",
        ));
    }
    if timeout_present && !preflight {
        return Err(usage_error(
            "fix_server: --timeout-secs requires --preflight",
        ));
    }

    Ok(CliParse::Run(Cli {
        preflight,
        config_path: config_path.unwrap_or_else(|| "config.toml".to_string()),
        logon_check,
        timeout_secs: timeout_secs.unwrap_or(5),
    }))
}

fn load_config(path: &str) -> Result<Config> {
    let config_content = std::fs::read_to_string(path)
        .map_err(|e| anyhow!("Failed to read config file {}: {}", path, e))?;
    parse_config(&config_content)
}

fn parse_config(toml_str: &str) -> Result<Config> {
    toml::from_str(toml_str)
        .map_err(|e| anyhow!("Failed to parse config file: {}", e))
}

/// Tags whose values are credentials or authentication material and must never
/// reach a log file. 553 is the API key itself, 554 the HMAC signature, 554's
/// input includes the key, and 96/95 are the nonce the signature is computed
/// over. Logs are 0644 and rotate rather than shred, so anything printed here
/// outlives the process on disk — the whole point of moving secrets out of
/// config.toml into a 0600 env file was to keep them off disk.
fn is_secret_tag(tag: usize) -> bool {
    matches!(tag, 553 | 554)
}

/// Render a field value for logging, redacting credential tags.
fn redact_tag(tag: usize, value: &str) -> String {
    if is_secret_tag(tag) {
        format!("<redacted {} chars>", value.len())
    } else {
        value.to_string()
    }
}

/// Redact credential fields out of a rendered FIX message before logging it.
/// Operates on the pipe- or SOH-delimited form.
fn redact_fix_text(text: &str) -> String {
    text.split('|')
        .map(|field| match field.split_once('=') {
            Some((t, v)) => match t.trim().parse::<usize>() {
                Ok(tag) if is_secret_tag(tag) => format!("{t}=<redacted {} chars>", v.len()),
                _ => field.to_string(),
            },
            None => field.to_string(),
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn parse_char_range(session_name: &str, raw: &str) -> Result<CharRange> {
    let chars: Vec<char> = raw.chars().collect();
    if chars.len() != 3 || chars[1] != '-' {
        bail!(
            "session '{session_name}' first_char_ranges entry '{raw}' is not X-Y"
        );
    }
    if !chars[0].is_ascii() || !chars[2].is_ascii() {
        bail!(
            "session '{session_name}' first_char_ranges entry '{raw}' must be ASCII"
        );
    }
    let start = chars[0].to_ascii_uppercase();
    let end = chars[2].to_ascii_uppercase();
    if start > end {
        bail!(
            "session '{session_name}' first_char_ranges entry '{raw}' is inverted (start > end after uppercase)"
        );
    }
    Ok(CharRange { start, end })
}

fn compile_symbol_routing(session_name: &str, routing: &SymbolRouting) -> Result<CompiledSymbolRouting> {
    match routing {
        SymbolRouting::All => Ok(CompiledSymbolRouting::All),
        SymbolRouting::FirstCharRanges(raw) => {
            if raw.is_empty() {
                bail!("session '{session_name}' first_char_ranges must not be empty");
            }
            let mut ranges = Vec::with_capacity(raw.len());
            for entry in raw {
                ranges.push(parse_char_range(session_name, entry)?);
            }
            Ok(CompiledSymbolRouting::FirstCharRanges(ranges))
        }
    }
}

fn session_admits_symbol(routing: &CompiledSymbolRouting, symbol: &str) -> bool {
    match routing {
        CompiledSymbolRouting::All => true, // including empty string
        CompiledSymbolRouting::FirstCharRanges(ranges) => {
            let Some(first) = symbol.chars().next() else {
                return false; // empty symbol
            };
            let ch = first.to_ascii_uppercase();
            ranges.iter().any(|r| r.start <= ch && ch <= r.end)
        }
    }
}

fn validate_config(config: &mut Config) -> Result<()> {
    if config.sessions.is_empty() {
        bail!("sessions must not be empty");
    }

    for session in &config.sessions {
        if session.name.trim().is_empty() {
            bail!("session name must not be empty");
        }
    }

    let mut seen_names: HashSet<String> = HashSet::new();
    for session in &config.sessions {
        let trimmed = session.name.trim().to_string();
        if !seen_names.insert(trimmed.clone()) {
            bail!("duplicate session name: {trimmed}");
        }
    }

    let n = config
        .sessions
        .iter()
        .filter(|s| s.role == SessionRole::OrderManagement)
        .count();
    if n > 1 {
        bail!("at most one session with role = order_management is allowed (found {n})");
    }

    for session in &config.sessions {
        if session.credentials_ref.trim().is_empty() {
            bail!(
                "session '{}' credentials_ref must not be empty",
                session.name
            );
        }
    }

    for session in &config.sessions {
        if !config.credentials.contains_key(&session.credentials_ref) {
            bail!(
                "session '{}' credentials_ref '{}' does not match any [credentials.*] block",
                session.name,
                session.credentials_ref
            );
        }
    }

    let mut keys: Vec<&String> = config.credentials.keys().collect();
    keys.sort();
    for block in keys {
        let cred = &config.credentials[block];
        if cred.api_key_env.trim().is_empty() {
            bail!("credentials.{block} api_key_env must not be empty");
        }
        if cred.api_secret_env.trim().is_empty() {
            bail!("credentials.{block} api_secret_env must not be empty");
        }
    }

    for session in &config.sessions {
        if session.role == SessionRole::MarketData && session.symbol_routing.is_none() {
            bail!(
                "session '{}' role = market_data requires symbol_routing",
                session.name
            );
        }
    }

    for session in &mut config.sessions {
        if session.role == SessionRole::MarketData {
            let routing = session
                .symbol_routing
                .as_ref()
                .expect("check 8 required symbol_routing for market_data");
            session.compiled_routing = Some(compile_symbol_routing(&session.name, routing)?);
        }
        // OM rows: if symbol_routing is Some, ignore it. compiled_routing stays None.
    }

    for session in &config.sessions {
        if session.host.contains("://") {
            bail!(
                "session '{}' host must not contain '://' (use host + port, not a URL)",
                session.name
            );
        }
    }

    for session in &config.sessions {
        if session.host.split_whitespace().nth(1).is_some()
            || session.host != session.host.trim()
            || session.host.chars().any(char::is_whitespace)
        {
            bail!("session '{}' host must not contain whitespace", session.name);
        }
    }

    let mut seen_limit_symbols: HashSet<String> = HashSet::new();
    for row in &config.symbol_limits {
        let trimmed = row.symbol.trim().to_string();
        if trimmed.is_empty() {
            bail!("symbol_limits symbol must not be empty");
        }
        if !seen_limit_symbols.insert(trimmed.clone()) {
            bail!("duplicate symbol_limits symbol: {trimmed}");
        }
        if !row.max_position.is_finite() || row.max_position <= 0.0 {
            bail!("symbol_limits '{trimmed}' max_position must be finite and > 0");
        }
        if !row.max_notional.is_finite() || row.max_notional <= 0.0 {
            bail!("symbol_limits '{trimmed}' max_notional must be finite and > 0");
        }
    }

    if let Some(dd) = config.risk.session_max_drawdown {
        if !dd.is_finite() {
            bail!("risk.session_max_drawdown must be finite");
        }
    }

    for session in &config.sessions {
        if session.tls.is_invalid() {
            bail!("session '{}' tls must be true or false", session.name);
        }
        if !session.tls.is_enabled() && session.tls_ca_path.is_some() {
            bail!(
                "session '{}' tls_ca_path is set but tls = false",
                session.name
            );
        }
        if !session.tls.is_enabled() && session.tls_server_name.is_some() {
            bail!(
                "session '{}' tls_server_name is set but tls = false",
                session.name
            );
        }
        if session.tls.is_enabled()
            && session
                .tls_ca_path
                .as_deref()
                .is_some_and(|s| s.trim().is_empty())
        {
            bail!("session '{}' tls_ca_path must not be empty", session.name);
        }
        if session.tls.is_enabled() {
            if let Some(s) = session.tls_server_name.as_deref() {
                if s.trim().is_empty() || s.chars().any(char::is_whitespace) || s.contains("://") {
                    bail!(
                        "session '{}' tls_server_name must be a hostname (no whitespace, no scheme)",
                        session.name
                    );
                }
            }
        }
        if session.tls.is_enabled() {
            if let Some(path) = session.tls_ca_path.as_deref() {
                if !path.trim().is_empty() {
                    if std::fs::read(path).is_err() {
                        bail!(
                            "session '{}' tls_ca_path '{}' is not a readable file",
                            session.name,
                            path
                        );
                    }
                    match load_trust_anchors(path) {
                        Ok(certs) if !certs.is_empty() => {}
                        _ => bail!(
                            "session '{}' tls_ca_path '{}' is not a valid PEM or DER certificate",
                            session.name,
                            path
                        ),
                    }
                }
            }
        }
    }

    Ok(())
}

fn used_credential_refs(sessions: &[SessionConfig]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for s in sessions {
        if seen.insert(s.credentials_ref.clone()) {
            out.push(s.credentials_ref.clone());
        }
    }
    out
}

fn read_env_nonempty(var_name: &str, cred_ref: &str, field: &str) -> Result<String> {
    match std::env::var(var_name) {
        Err(_) => bail!(
            "environment variable {var_name} (credentials.{cred_ref} {field}) is not set"
        ),
        Ok(v) if v.trim().is_empty() => bail!(
            "environment variable {var_name} (credentials.{cred_ref} {field}) is empty"
        ),
        Ok(v) => Ok(v), // keep raw value; do not trim secrets
    }
}

fn resolve_credentials(config: &Config) -> Result<HashMap<String, ResolvedCredentials>> {
    let mut out = HashMap::new();
    for ref_name in used_credential_refs(&config.sessions) {
        let cred = config
            .credentials
            .get(&ref_name)
            .expect("validate_config already required this credentials_ref");
        let api_key = read_env_nonempty(&cred.api_key_env, &ref_name, "api_key_env")?;
        let api_secret = read_env_nonempty(&cred.api_secret_env, &ref_name, "api_secret_env")?;
        out.insert(ref_name, ResolvedCredentials { api_key, api_secret });
    }
    Ok(out)
}

/// First-seen order of used refs (same as `used_credential_refs`).
/// On fail: eprintln the raw `read_env_nonempty` error once per ref, then Fail.
fn collect_credentials(config: &Config) -> HashMap<String, CredStatus> {
    let mut out = HashMap::new();
    for ref_name in used_credential_refs(&config.sessions) {
        let cred = config
            .credentials
            .get(&ref_name)
            .expect("validate_config already required this credentials_ref");
        match read_env_nonempty(&cred.api_key_env, &ref_name, "api_key_env") {
            Err(e) => {
                eprintln!("{e:#}");
                out.insert(ref_name, CredStatus::Fail);
            }
            Ok(api_key) => match read_env_nonempty(&cred.api_secret_env, &ref_name, "api_secret_env")
            {
                Err(e) => {
                    eprintln!("{e:#}");
                    out.insert(ref_name, CredStatus::Fail);
                }
                Ok(api_secret) => {
                    out.insert(
                        ref_name,
                        CredStatus::Ok(ResolvedCredentials { api_key, api_secret }),
                    );
                }
            },
        }
    }
    out
}

fn session_heartbeat_interval(session: &SessionConfig, conn: &ConnectionConfig) -> u64 {
    session.heartbeat_interval.unwrap_or(conn.heartbeat_interval)
}
fn session_max_reconnect_attempts(session: &SessionConfig, conn: &ConnectionConfig) -> u32 {
    session.max_reconnect_attempts.unwrap_or(conn.max_reconnect_attempts)
}
fn session_reconnect_backoff_ms(session: &SessionConfig, conn: &ConnectionConfig) -> u64 {
    session.reconnect_backoff_ms.unwrap_or(conn.reconnect_backoff_ms)
}

fn connect_addr(session: &SessionConfig) -> String {
    format!("{}:{}", session.host, session.port)
}

/// One attempt. No retry. No FIX bytes. Drop the stream immediately on success.
/// Bounds DNS + connect together.
async fn check_tcp_reachable(host: &str, port: u16, timeout: Duration) -> Result<(), TcpCheckErr> {
    match tokio::time::timeout(timeout, tokio::net::TcpStream::connect((host, port))).await {
        Ok(Ok(stream)) => {
            drop(stream);
            Ok(())
        }
        Ok(Err(e)) => Err(TcpCheckErr::Io(e)),
        Err(_elapsed) => Err(TcpCheckErr::Timeout {
            secs: timeout.as_secs(),
        }),
    }
}

fn load_trust_anchors(path: &str) -> Result<Vec<native_tls::Certificate>> {
    let bytes = std::fs::read(path).map_err(|e| anyhow!("failed to read tls_ca_path '{path}': {e}"))?;
    const BEGIN: &str = "-----BEGIN CERTIFICATE-----";
    const END: &str = "-----END CERTIFICATE-----";

    if let Ok(text) = std::str::from_utf8(&bytes) {
        if text.contains(BEGIN) {
            let mut certs = Vec::new();
            let mut search_from = 0usize;
            while let Some(brel) = text[search_from..].find(BEGIN) {
                let begin_at = search_from + brel;
                let after_begin = begin_at + BEGIN.len();
                let Some(erel) = text[after_begin..].find(END) else {
                    bail!("truncated PEM certificate in '{path}'");
                };
                let end_at = after_begin + erel + END.len();
                let block = &text[begin_at..end_at];
                let cert = native_tls::Certificate::from_pem(block.as_bytes())
                    .map_err(|e| anyhow!("invalid PEM certificate in '{path}': {e}"))?;
                certs.push(cert);
                search_from = end_at;
            }
            if certs.is_empty() {
                bail!("no CERTIFICATE blocks in '{path}'");
            }
            return Ok(certs);
        }
    }

    let cert = native_tls::Certificate::from_der(&bytes)
        .map_err(|e| anyhow!("invalid DER certificate in '{path}': {e}"))?;
    Ok(vec![cert])
}

fn build_tls_connector(ca_path: Option<&str>) -> Result<tokio_native_tls::TlsConnector> {
    let mut builder = native_tls::TlsConnector::builder();
    builder.min_protocol_version(Some(native_tls::Protocol::Tlsv12));
    if let Some(path) = ca_path {
        builder.disable_built_in_roots(true);
        let certs = load_trust_anchors(path)?;
        let n = certs.len();
        for cert in certs {
            builder.add_root_certificate(cert);
        }
        info!("🔒 TLS trust anchor loaded from {path} ({n} certs)");
    }
    let native = builder
        .build()
        .map_err(|e| anyhow!("TLS connector build failed: {e}"))?;
    Ok(tokio_native_tls::TlsConnector::from(native))
}

fn lookup_om(spawned: &[SpawnedSession]) -> Option<Arc<FIXSession>> {
    spawned
        .iter()
        .find(|s| s.config.role == SessionRole::OrderManagement)
        .map(|s| Arc::clone(&s.handle))
}

fn lookup_primary_md(spawned: &[SpawnedSession]) -> Option<Arc<FIXSession>> {
    spawned
        .iter()
        .find(|s| s.config.role == SessionRole::MarketData)
        .map(|s| Arc::clone(&s.handle))
}

fn lookup_all_md(spawned: &[SpawnedSession]) -> Vec<(Arc<FIXSession>, String, u16)> {
    spawned
        .iter()
        .filter(|s| s.config.role == SessionRole::MarketData)
        .map(|s| (Arc::clone(&s.handle), s.config.name.clone(), s.config.port))
        .collect()
}


#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct OrderRequest {
    // Required fields
    pub cl_ord_id: String,      // Tag 11: ClOrdID
    pub symbol: String,          // Tag 55: Symbol
    pub side: char,              // Tag 54: Side (1=BUY, 2=SELL)
    pub qty: f64,                // Tag 38: OrderQty
    pub price: Option<f64>,     // Tag 44: Price (required for LIMIT orders)
    
    // Optional fields
    pub ord_type: Option<String>, // Tag 40: OrdType (1=MARKET, 2=LIMIT, D=PREVIOUSLY_QUOTED)
    pub time_in_force: Option<String>, // Tag 59: TimeInForce (1=GTC, 3=IOC, 4=FOK, 0=Day)
    pub currency: Option<String>, // Tag 15: Currency (for OTC orders)
    pub exec_inst: Option<String>, // Tag 18: ExecInst (6=POST_ONLY, 8=SMART_POST_ONLY)
    pub quote_id: Option<String>, // Tag 117: QuoteID (for OTC orders, must be "RFS" for OTC RFS)
    pub cash_order_qty: Option<f64>, // Tag 152: CashOrderQty (for MARKET BUY orders)
    pub cash_margin: Option<String>, // Tag 544: CashMargin (1=SPOT no margin, 2=SPOT with margin)
    pub trigger_price_type: Option<String>, // Tag 1107: TriggerPriceType (2=LAST_PRICE, I=INDEX_PRICE, M=MARK_PRICE)
    pub self_match_prevention_scope: Option<String>, // Tag 12362: SelfMatchPreventionScope (M, S)
    pub self_match_prevention_instruction: Option<String>, // Tag 2964: SelfMatchPreventionInstruction (1, 2, 3)
    pub self_match_prevention_id: Option<String>, // Tag 2362: SelfMatchPreventionID (0-32767)
    pub broker_id: Option<String>, // Tag 7933: BrokerId
    pub receive_window: Option<u32>, // Tag 20100: ReceiveWindow (100-5000 milliseconds)
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct CancelOrderRequest {
    // Required fields
    pub cl_ord_id: String,          // Tag 11: ClOrdID - Unique ID for this cancel request
    pub orig_cl_ord_id: String,      // Tag 41: OrigClOrdID - ClOrdID of the original order to cancel
    pub symbol: String,              // Tag 55: Symbol
    pub side: String,                // Tag 54: Side ("1"=BUY, "2"=SELL) - stored as String for JSON compatibility
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct OperatorHaltTriggerResult {
    pub halted: bool,            // always true after trigger_operator_halt
    pub cancel_attempted: u32,   // snapshot.len() as u32
    pub cancel_ok: u32,          // cancel_order Ok count
    pub cancel_failed: u32,      // cancel_order Err count
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ReplaceOrderRequest {
    // Required fields
    pub cl_ord_id: String,           // Tag 11: ClOrdID - new ID for the replacement order
    pub orig_cl_ord_id: String,      // Tag 41: OrigClOrdID - ClOrdID of the working order being replaced
    pub symbol: String,              // Tag 55: Symbol
    pub side: char,                  // Tag 54: Side ('1'=BUY, '2'=SELL)
    // At least one of qty/price should be set; both are optional so a
    // caller can amend just the price or just the quantity.
    pub qty: Option<f64>,            // Tag 38: OrderQty (new)
    pub price: Option<f64>,          // Tag 44: Price (new)
}

/// Open order information extracted from execution reports
#[derive(Debug, Clone, serde::Serialize)]
pub struct OpenOrder {
    pub cl_ord_id: String,
    pub order_id: String,
    pub exec_id: String,
    pub symbol: String,
    pub side: String,
    pub order_qty: String,
    pub price: String,
    pub ord_status: String,
    pub exec_type: String,
    pub cum_qty: String,
    pub leaves_qty: String,
    pub last_px: String,
    pub last_qty: String,
    pub avg_px: String,
    pub transact_time: String,
    pub order_create_time: String,
    pub rejection_reason: String,
}

/// Thread-safe, lock-free store for open orders using ArcSwap for zero-lock snapshots
pub struct OpenOrdersStore {
    // ArcSwap allows lock-free atomic swaps of the entire HashMap
    // Readers can snapshot without blocking writers
    store: ArcSwap<HashMap<String, OpenOrder>>,
}

impl OpenOrdersStore {
    pub fn new() -> Self {
        Self {
            store: ArcSwap::from_pointee(HashMap::new()),
        }
    }

    /// Update or insert an order (thread-safe, uses ArcSwap for lock-free updates)
    pub fn update_order(&self, cl_ord_id: String, order: OpenOrder) {
        // Load current snapshot
        let current = self.store.load();
        
        // Create new HashMap with updated order
        let mut new_map = (**current).clone();
        new_map.insert(cl_ord_id, order);
        
        // Atomically swap the entire HashMap (lock-free)
        self.store.store(Arc::new(new_map));
    }

    /// Remove an order (thread-safe)
    pub fn remove_order(&self, cl_ord_id: &str) {
        let current = self.store.load();
        let mut new_map = (**current).clone();
        new_map.remove(cl_ord_id);
        self.store.store(Arc::new(new_map));
    }

    /// Get a lock-free snapshot of all open orders
    /// This is zero-copy and doesn't block writers
    pub fn snapshot(&self) -> Arc<HashMap<String, OpenOrder>> {
        self.store.load_full()
    }

    /// Get open orders only (filters out closed/rejected/cancelled orders)
    pub fn get_open_orders(&self) -> Vec<OpenOrder> {
        let snapshot = self.snapshot();
        snapshot
            .values()
            .filter(|order| {
                let leaves_qty: f64 = order.leaves_qty.parse().unwrap_or(0.0);
                let ord_status = &order.ord_status;
                let exec_type = &order.exec_type;
                
                // Order is open if:
                // - Has remaining quantity
                // - Not rejected (ord_status != "8")
                // - Not cancelled (ord_status != "4")
                // - Not fully filled (ord_status != "2")
                // - Not rejected (exec_type != "8")
                leaves_qty > 0.0
                    && ord_status != "8"
                    && ord_status != "4"
                    && ord_status != "2"
                    && exec_type != "8"
            })
            .cloned()
            .collect()
    }
}

/// Aggregated position for a single symbol, updated on every execution fill.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Position {
    pub symbol: String,
    /// Net quantity: positive = long, negative = short
    pub net_qty: f64,
    /// Volume-weighted average entry price of the current open position
    pub avg_entry_px: f64,
    /// Cumulative realized P&L (closed legs only)
    pub realized_pnl: f64,
    /// Unrealized P&L vs latest mid price (updated on read via order book)
    pub unrealized_pnl: f64,
    /// Most recent mid price used for unrealized P&L calculation
    pub last_mid_px: f64,
    /// Total number of fill events counted
    pub trade_count: u32,
}

impl Position {
    fn new(symbol: String) -> Self {
        Position {
            symbol,
            net_qty: 0.0,
            avg_entry_px: 0.0,
            realized_pnl: 0.0,
            unrealized_pnl: 0.0,
            last_mid_px: 0.0,
            trade_count: 0,
        }
    }

    /// Apply a fill: side "1"=buy, "2"=sell.  Returns updated self.
    fn apply_fill(&mut self, side: &str, last_qty: f64, last_px: f64) {
        if last_qty <= 0.0 || last_px <= 0.0 {
            return;
        }
        let signed_qty = if side == "1" { last_qty } else { -last_qty };
        let prev_net = self.net_qty;
        let new_net = prev_net + signed_qty;

        if prev_net == 0.0 {
            // Opening a new position
            self.avg_entry_px = last_px;
        } else if prev_net > 0.0 && signed_qty > 0.0 {
            // Adding to long
            let total_cost = self.avg_entry_px * prev_net + last_px * last_qty;
            self.avg_entry_px = total_cost / new_net;
        } else if prev_net < 0.0 && signed_qty < 0.0 {
            // Adding to short
            let total_cost = self.avg_entry_px * prev_net.abs() + last_px * last_qty;
            self.avg_entry_px = total_cost / new_net.abs();
        } else {
            // Closing / reversing
            let closing_qty = last_qty.min(prev_net.abs());
            if prev_net > 0.0 {
                // Long being reduced by a sell
                self.realized_pnl += closing_qty * (last_px - self.avg_entry_px);
            } else {
                // Short being reduced by a buy
                self.realized_pnl += closing_qty * (self.avg_entry_px - last_px);
            }
            if new_net.abs() < 1e-10 {
                self.avg_entry_px = 0.0;
            } else if new_net.signum() != prev_net.signum() {
                // Position reversed
                self.avg_entry_px = last_px;
            }
        }

        self.net_qty = new_net;
        self.trade_count += 1;
    }

    /// Refresh unrealized P&L using a live mid price.
    pub fn refresh_unrealized(&mut self, mid_px: f64) {
        if mid_px > 0.0 && self.net_qty.abs() > 1e-10 && self.avg_entry_px > 0.0 {
            self.unrealized_pnl = self.net_qty * (mid_px - self.avg_entry_px);
            self.last_mid_px = mid_px;
        } else {
            self.unrealized_pnl = 0.0;
        }
    }
}

/// Thread-safe, lock-free store for live positions using ArcSwap.
pub struct PositionsStore {
    store: ArcSwap<HashMap<String, Position>>,
}

impl PositionsStore {
    pub fn new() -> Self {
        Self {
            store: ArcSwap::from_pointee(HashMap::new()),
        }
    }

    /// Apply a fill event for a symbol (side "1"=buy / "2"=sell).
    pub fn apply_fill(&self, symbol: &str, side: &str, last_qty: f64, last_px: f64) {
        let current = self.store.load();
        let mut new_map = (**current).clone();
        let pos = new_map
            .entry(symbol.to_string())
            .or_insert_with(|| Position::new(symbol.to_string()));
        pos.apply_fill(side, last_qty, last_px);
        self.store.store(Arc::new(new_map));
    }

    /// Return a snapshot of all positions as a Vec.
    pub fn snapshot(&self) -> Vec<Position> {
        self.store.load().values().cloned().collect()
    }

    /// Net quantity for `symbol`, or `0.0` if the symbol has no position yet.
    pub fn net_qty_of(&self, symbol: &str) -> f64 {
        self.store
            .load()
            .get(symbol)
            .map(|p| p.net_qty)
            .unwrap_or(0.0)
    }

    /// Sum of realized P&L across all symbols. Unrealized is excluded (EL-004).
    pub fn sum_realized_pnl(&self) -> f64 {
        self.snapshot().iter().map(|p| p.realized_pnl).sum()
    }

    /// Replay fills from data/trades.csv to rebuild positions after a restart.
    /// CSV columns: timestamp_ns,cl_ord_id,order_id,symbol,side,exec_type,last_qty,last_px,avg_px
    pub fn load_from_trades_csv(&self) {
        use std::io::BufRead;
        let file = match std::fs::File::open("data/trades.csv") {
            Ok(f) => f,
            Err(_) => return, // no file yet — first run
        };
        let mut replayed = 0u32;
        let mut first = true;
        for line in std::io::BufReader::new(file).lines().flatten() {
            if first { first = false; continue; } // skip header
            let cols: Vec<&str> = line.splitn(9, ',').collect();
            if cols.len() < 9 { continue; }
            // cols: ts, cl_ord_id, order_id, symbol, side, exec_type, last_qty, last_px, avg_px
            let symbol   = cols[3];
            let side     = cols[4]; // "1" or "2"
            let last_qty: f64 = cols[6].parse().unwrap_or(0.0);
            let last_px:  f64 = cols[7].parse().unwrap_or(0.0);
            if last_qty > 0.0 && last_px > 0.0 {
                self.apply_fill(symbol, side, last_qty, last_px);
                replayed += 1;
            }
        }
        if replayed > 0 {
            info!("📂 Restored positions from data/trades.csv ({} fills replayed)", replayed);
        }
    }
}

/// Order processing timing metrics
#[derive(Debug, Clone, serde::Serialize)]
pub struct OrderTiming {
    pub file_read_us: Option<u64>,
    pub json_parse_us: Option<u64>,
    pub channel_send_us: Option<u64>,
    pub fix_encode_us: Option<u64>,
    pub fix_send_us: Option<u64>,
    pub execution_report_us: Option<u64>,
    pub total_us: Option<u64>,
    pub file_read_timestamp: Option<i64>,
    pub execution_report_timestamp: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct SecurityInfo {
    pub symbol: String,
    pub security_id: Option<String>,
    pub exchange: Option<String>,
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct TickData {
    pub timestamp: i64,  // nanoseconds since epoch
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    pub bid_size: f64,
    pub ask_size: f64,
    pub last_price: f64,
    pub last_size: f64,
}

/// kdb+ side-effects deferred until `order_books` is released.
/// Converted to kdb `char`s at push time so replay does not re-derive.
enum PendingMdIo {
    Level {
        side: char,   // 'B' | 'A'
        price: f64,
        size: f64,
        action: char, // 'N' | 'C' | 'D'
        seq_num: Option<u32>,
    },
    Trade {
        price: f64,
        size: f64,
        timestamp: i64,
        seq_num: Option<u32>,
    },
    Snapshot {
        best_bid: f64,
        best_ask: f64,
        best_bid_size: f64,
        best_ask_size: f64,
        spread: f64,
        seq_num: Option<u32>,
    },
}

enum SessionStream {
    Plain(tokio::net::TcpStream),
    Tls(tokio_native_tls::TlsStream<tokio::net::TcpStream>),
}

impl AsyncRead for SessionStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            SessionStream::Plain(s) => Pin::new(s).poll_read(cx, buf),
            SessionStream::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for SessionStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match self.get_mut() {
            SessionStream::Plain(s) => Pin::new(s).poll_write(cx, buf),
            SessionStream::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            SessionStream::Plain(s) => Pin::new(s).poll_flush(cx),
            SessionStream::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.get_mut() {
            SessionStream::Plain(s) => Pin::new(s).poll_shutdown(cx),
            SessionStream::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

pub struct FIXSession {
    stream: Arc<Mutex<Option<SessionStream>>>,
    tls_enabled: bool,
    tls_ca_path: Option<String>,
    tls_domain: String,
    sender_comp_id: String,
    target_comp_id: String,
    seq_num: Arc<Mutex<u32>>,
    persist_seq_to_disk: bool,
    is_connected: Arc<Mutex<bool>>,
    connection_state: Arc<Mutex<ConnectionState>>,
    api_key: String,
    api_secret: String,
    // Track order submission times for latency measurement (keyed by ClOrdID)
    order_times: Arc<Mutex<HashMap<String, Instant>>>,
    // Track order processing timings (keyed by ClOrdID)
    order_timings: Arc<Mutex<HashMap<String, OrderTiming>>>,
    // Track original order data for writing processed JSON (keyed by ClOrdID)
    order_data: Arc<Mutex<HashMap<String, OrderRequest>>>,
    // Track securities received from market data server
    securities: Arc<Mutex<Vec<SecurityInfo>>>,
    // Track expected security count for auto-save trigger
    expected_security_count: Arc<Mutex<Option<usize>>>,
    // Track when we started receiving securities
    securities_last_save_time: Arc<Mutex<Instant>>,
    // Track if we've already saved to file
    securities_file_saved: Arc<Mutex<bool>>,
    // Buffer for partial FIX messages (TCP fragmentation)
    message_buffer: Arc<Mutex<Vec<u8>>>,
    // Store tick data for parquet export
    tick_data: Arc<Mutex<Vec<TickData>>>,
    // High-performance in-memory order books with double buffering (keyed by symbol)
    order_books: Arc<Mutex<HashMap<String, DoubleBufferedOrderBook>>>,
    // Lock-free open orders store using ArcSwap for zero-lock snapshots
    open_orders: Arc<OpenOrdersStore>,
    /// Manual operator brake (console/API label: Kill Switch).
    /// Independent of algo enabled / paper_mode / max_orders_session.
    /// Runtime-only; default false. Not `kill_switch` (kdb capture-symbols clash).
    operator_halt: Arc<AtomicBool>,
    // Lock-free positions store — aggregated fills per symbol
    positions: Arc<PositionsStore>,
    /// OMS hard per-symbol caps. Empty map ⇒ exposure gate off (EL-018).
    symbol_limits: Arc<ArcSwap<HashMap<String, SymbolLimit>>>,
    /// Session drawdown threshold as f64 bits. `0.0` ⇒ guard off.
    session_max_drawdown_bits: AtomicU64,
    /// Peak Σ realized_pnl as f64 bits. Rebased on `reset_operator_halt`.
    drawdown_peak_bits: AtomicU64,
    // Event-driven update channels (optional, set after creation)
    order_update_tx: Arc<Mutex<Option<broadcast::Sender<()>>>>,
    order_book_update_tx: Arc<Mutex<Option<broadcast::Sender<String>>>>,
    // kdb+ writer for order book data (shared across sessions)
    kdb_writer: Arc<KdbWriter>,
    // Inbound MsgSeqNum gap detection / recovery state (session-level; see
    // `sequencer` module doc). Keyed internally, one FIXSession = one FIX session.
    sequencer: Arc<Mutex<sequencer::Sequencer>>,
    // Last market-data message timestamp (Unix ms). Zero = never received.
    // Updated atomically on every W/X message; read by the stale-feed watchdog.
    last_md_recv_ms: Arc<AtomicU64>,
    /// Timestamp (Unix ms) of the LAST INBOUND MESSAGE OF ANY TYPE, heartbeats
    /// included. Liveness of the TCP stream, as distinct from last_md_recv_ms
    /// which only tracks market data.
    last_inbound_ms: Arc<AtomicU64>,
    /// Per-session in-process alert buffer (cap 200, FIFO). Tests poll this;
    /// production operators grep `ALERT[` on the error log.
    alerts: Arc<Mutex<VecDeque<alerts::Alert>>>,
    /// Per-session reconnect-storm rolling window. First TCP connect is not a
    /// reconnect; `record` returns Some only on the 3-in-window edge.
    reconnect_storm: Arc<Mutex<alerts::ReconnectStormTracker>>,
    // Persistence allow-list for kdb capture (CAPTURE_SYMBOLS). Does not
    // affect FIX subscription or the in-memory Live book.
    capture_symbols: HashSet<String>,
}

#[derive(Debug, Clone, Copy)]
enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Logging,
    Ready,
    Disconnecting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Direction {
    Inbound,
    Outbound,
}

impl Direction {
    fn as_tag(self) -> &'static str {
        match self {
            Direction::Inbound => "IN",
            Direction::Outbound => "OUT",
        }
    }
}

fn command_log_msg_type_name(msg_type: char) -> &'static str {
    match msg_type {
        'A' => "Logon",
        '5' => "Logout",
        '0' => "Heartbeat",
        '1' => "TestRequest",
        '2' => "ResendRequest",
        '3' => "Reject",
        '4' => "SequenceReset",
        '8' => "ExecutionReport",
        'D' => "NewOrderSingle",
        'F' => "OrderCancelRequest",
        'G' => "OrderCancelReplaceRequest",
        'H' => "OrderStatusRequest",
        'V' => "MarketDataRequest",
        'W' => "MarketDataSnapshotFullRefresh",
        'X' => "MarketDataIncrementalRefresh",
        'Y' => "OrderCancelReject",
        'x' => "SecurityListRequest",
        'j' => "BusinessMessageReject",
        _ => "Unknown",
    }
}

fn is_om_command_log(sender_id: &str, target_id: &str) -> bool {
    sender_id.contains(".UD") || target_id.contains(".UD")
}

fn command_log_filename(is_om: bool) -> &'static str {
    if is_om {
        COMMAND_LOG_OM
    } else {
        COMMAND_LOG_MD
    }
}

fn rotate_log_if_needed(path: &Path, max_bytes: u64) {
    let Ok(metadata) = std::fs::metadata(path) else {
        return;
    };
    if metadata.len() <= max_bytes {
        return;
    }
    // Backup is `<file>.1` (e.g. OM_fix_command.log.1), not Path::with_extension("1")
    // which would strip `.log` and yield OM_fix_command.1.
    let backup = format!("{}.1", path.as_os_str().to_string_lossy());
    let _ = std::fs::rename(path, backup);
}

fn open_command_log(path: &Path) -> std::io::Result<std::fs::File> {
    let mut opts = OpenOptions::new();
    opts.create(true).append(true);
    #[cfg(unix)]
    {
        opts.mode(0o600);
    }
    let file = opts.open(path)?;
    #[cfg(unix)]
    {
        if let Ok(meta) = file.metadata() {
            let mut perms = meta.permissions();
            perms.set_mode(0o600);
            let _ = file.set_permissions(perms);
        }
    }
    Ok(file)
}

fn append_command_log_entry(path: &Path, entry: String, max_bytes: u64) {
    let path = path.to_path_buf();
    let write = move || {
        rotate_log_if_needed(&path, max_bytes);
        if let Ok(mut file) = open_command_log(&path) {
            let _ = writeln!(file, "{}", entry);
            let _ = file.flush();
        }
    };
    // Tests assert on log contents immediately after send/receive. The
    // fire-and-forget thread is a release-mode race: inbound ExecutionReport
    // can flush before the earlier outbound NewOrderSingle, which made
    // `inbound_fix_commands_logged_in_full` fail under `cargo test --release`.
    #[cfg(test)]
    {
        write();
    }
    #[cfg(not(test))]
    {
        std::thread::spawn(write);
    }
}

/// Parse CAPTURE_SYMBOLS. Unset → {BTC_USDT}. Empty/whitespace → kill switch (no persist).
/// Invalid tokens (non ASCII alnum/underscore) are dropped with a warning.
fn parse_capture_symbols(raw: Option<&str>) -> HashSet<String> {
    match raw {
        None => {
            let mut s = HashSet::new();
            s.insert("BTC_USDT".to_string());
            s
        }
        Some(v) if v.trim().is_empty() => {
            HashSet::new()
        }
        Some(v) => {
            let mut set = HashSet::new();
            for tok in v.split(',') {
                let t = tok.trim();
                if t.is_empty() {
                    continue;
                }
                if t.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                    set.insert(t.to_string());
                } else {
                    warn!("CAPTURE_SYMBOLS: dropping invalid token {:?}", t);
                }
            }
            set
        }
    }
}

const SMP_FIELD_MAX_LEN: usize = 32;

/// True iff `s` is a single FIX field value.
/// Empty string is OK. Length is UTF-8 byte length (`s.len()`).
fn smp_field_wire_ok(s: &str) -> bool {
    s.len() <= SMP_FIELD_MAX_LEN
        && !s.as_bytes().iter().any(|&b| matches!(b, 0x00 | 0x01 | b'=' | b'\n' | b'\r'))
}

fn smp_arg_or_reject(field: &'static str, v: &Option<String>) -> Result<()> {
    match v {
        None => Ok(()),
        Some(s) if smp_field_wire_ok(s) => Ok(()),
        Some(s) => {
            let reason = if s.len() > SMP_FIELD_MAX_LEN {
                "exceeds max length"
            } else {
                "contains forbidden delimiter"
            };
            warn!("SMP field {} rejected: {} (len={})", field, reason, s.len());
            Err(anyhow!(
                "SMP field {} rejected: {} (len={})",
                field, reason, s.len()
            ))
        }
    }
}

impl FIXSession {
    pub fn new(sender: &str, target: &str) -> Self {
        Self::new_with_credentials(sender, target, "", "")
    }
    
    pub fn new_with_credentials(sender: &str, target: &str, api_key: &str, api_secret: &str) -> Self {
        let kdb_host = std::env::var("KDB_HOST").unwrap_or_else(|_| "localhost".to_string());
        let kdb_port: u16 = std::env::var("KDB_PORT")
            .unwrap_or_else(|_| "5001".to_string())
            .parse()
            .unwrap_or(5001);
        Self::new_with_credentials_and_kdb(sender, target, api_key, api_secret, Arc::new(KdbWriter::new(kdb_host, kdb_port)))
    }

    fn build_session(
        sender: &str,
        target: &str,
        api_key: &str,
        api_secret: &str,
        kdb_writer: Arc<KdbWriter>,
        persist_seq_to_disk: bool,
        load_positions_from_csv: bool,
    ) -> Self {
        // Restore sequence number from disk so restarts don't cause MsgSeqNum gaps.
        let safe_sender = sender.replace('.', "_").replace(' ', "_");
        let seq_path = format!("data/seqnum_{}.txt", safe_sender);
        let initial_seq: u32 = std::fs::read_to_string(&seq_path)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0);
        if initial_seq > 0 {
            info!("🔄 Restored seq_num={} for {} from {}", initial_seq, sender, seq_path);
        }

        let positions = {
            let ps = Arc::new(PositionsStore::new());
            if load_positions_from_csv {
                ps.load_from_trades_csv();
            }
            ps
        };
        // EL-009: peak starts at CSV-replayed Σ realized even if the guard is off.
        // Do not trip halt in the ctor.
        let peak_sum = positions.sum_realized_pnl();
        let peak_bits = if peak_sum.is_finite() {
            peak_sum.to_bits()
        } else {
            0.0f64.to_bits()
        };

        FIXSession {
            stream: Arc::new(Mutex::new(None)),
            tls_enabled: false,
            tls_ca_path: None,
            tls_domain: String::new(),
            sender_comp_id: sender.to_string(),
            target_comp_id: target.to_string(),
            seq_num: Arc::new(Mutex::new(initial_seq)),
            persist_seq_to_disk,
            is_connected: Arc::new(Mutex::new(false)),
            connection_state: Arc::new(Mutex::new(ConnectionState::Disconnected)),
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            order_times: Arc::new(Mutex::new(HashMap::new())),
            order_timings: Arc::new(Mutex::new(HashMap::new())),
            order_data: Arc::new(Mutex::new(HashMap::new())),
            securities: Arc::new(Mutex::new(Vec::new())),
            expected_security_count: Arc::new(Mutex::new(None)),
            securities_last_save_time: Arc::new(Mutex::new(Instant::now())),
            securities_file_saved: Arc::new(Mutex::new(false)),
            message_buffer: Arc::new(Mutex::new(Vec::new())),
            tick_data: Arc::new(Mutex::new(Vec::new())),
            order_books: Arc::new(Mutex::new(HashMap::new())),
            open_orders: Arc::new(OpenOrdersStore::new()),
            operator_halt: Arc::new(AtomicBool::new(false)),
            positions,
            symbol_limits: Arc::new(ArcSwap::from_pointee(HashMap::new())),
            session_max_drawdown_bits: AtomicU64::new(0.0f64.to_bits()),
            drawdown_peak_bits: AtomicU64::new(peak_bits),
            order_update_tx: Arc::new(Mutex::new(None)),
            order_book_update_tx: Arc::new(Mutex::new(None)),
            kdb_writer,
            sequencer: Arc::new(Mutex::new(sequencer::Sequencer::new())),
            last_md_recv_ms: Arc::new(AtomicU64::new(0)),
            last_inbound_ms: Arc::new(AtomicU64::new(0)),
            alerts: Arc::new(Mutex::new(VecDeque::new())),
            reconnect_storm: Arc::new(Mutex::new(alerts::ReconnectStormTracker::new())),
            capture_symbols: parse_capture_symbols(std::env::var("CAPTURE_SYMBOLS").ok().as_deref()),
        }
    }
    
    pub fn new_with_credentials_and_kdb(sender: &str, target: &str, api_key: &str, api_secret: &str, kdb_writer: Arc<KdbWriter>) -> Self {
        Self::build_session(sender, target, api_key, api_secret, kdb_writer, true, true)
    }

    fn new_for_preflight(sender: &str, target: &str, api_key: &str, api_secret: &str) -> Self {
        let kdb_host = std::env::var("KDB_HOST").unwrap_or_else(|_| "localhost".to_string());
        let kdb_port: u16 = std::env::var("KDB_PORT")
            .unwrap_or_else(|_| "5001".to_string())
            .parse()
            .unwrap_or(5001);
        Self::build_session(
            sender,
            target,
            api_key,
            api_secret,
            Arc::new(KdbWriter::new(kdb_host, kdb_port)),
            false,
            false,
        )
    }

    fn with_transport(
        mut self,
        tls_enabled: bool,
        tls_ca_path: Option<String>,
        tls_domain: String,
    ) -> Self {
        self.tls_enabled = tls_enabled;
        self.tls_ca_path = tls_ca_path;
        self.tls_domain = tls_domain;
        self
    }

    #[cfg(test)]
    pub fn with_capture_symbols(mut self, symbols: HashSet<String>) -> Self {
        self.capture_symbols = symbols;
        self
    }

    #[cfg(test)]
    pub fn drain_capture_probe(&self) -> Vec<kdb_integration::ProbeEvent> {
        self.kdb_writer.drain_probe()
    }
    
    /// Set event-driven update channels
    pub async fn set_update_channels(
        &self,
        order_tx: broadcast::Sender<()>,
        order_book_tx: broadcast::Sender<String>,
    ) {
        *self.order_update_tx.lock().await = Some(order_tx);
        *self.order_book_update_tx.lock().await = Some(order_book_tx);
        log::info!("✅ Update channels set for session");
    }

    /// Only writer of the per-session alert buffer and the greppable `ALERT[`
    /// log line. Infallible: never returns Result, never uses `?`.
    async fn emit_alert(&self, kind: alerts::AlertKind, detail: String) {
        let session = alerts::sanitize_alert_text(&self.sender_comp_id);
        let detail = alerts::sanitize_alert_text(&detail);
        let at_ms = chrono::Utc::now().timestamp_millis() as u64;
        let alert = alerts::Alert {
            kind,
            session: session.clone(),
            detail: detail.clone(),
            at_ms,
        };
        {
            let mut buf = self.alerts.lock().await;
            alerts::push_bounded(&mut buf, alert);
        }
        error!("ALERT[{kind}] session={session} {detail}");
    }

    /// Stall/quiet evaluation extracted from the MD watchdog loop so tests can
    /// poke `last_inbound_ms` and tick without a 30s sleep. QuietFeed logs
    /// only; Stalled emits StaleBook then tears the stream down.
    async fn apply_feed_watchdog_tick(&self, now_ms: u64, name: &str) -> FeedHealth {
        if !self.is_ready().await {
            return FeedHealth::Ok;
        }
        let health = feed_health(
            now_ms,
            self.last_inbound_ms.load(Ordering::Relaxed),
            self.last_md_recv_ms.load(Ordering::Relaxed),
            STALE_THRESHOLD_MS,
            QUIET_FEED_LOG_MS,
        );
        match health {
            FeedHealth::QuietFeed { md_silent_ms } => {
                info!(
                    "📭 No market data on {} ({}) for {}ms — connection alive (heartbeats arriving), not reconnecting",
                    name, self.sender_comp_id, md_silent_ms
                );
                health
            }
            FeedHealth::Stalled { silent_ms } => {
                self.emit_alert(
                    alerts::AlertKind::StaleBook,
                    alerts::stale_book_detail(silent_ms),
                )
                .await;
                warn!(
                    "⚠️  Stalled stream on {} ({}) — nothing inbound for {}ms, forcing reconnect",
                    name, self.sender_comp_id, silent_ms
                );
                *self.stream.lock().await = None;
                *self.connection_state.lock().await = ConnectionState::Disconnected;
                *self.is_connected.lock().await = false;
                self.last_md_recv_ms.store(0, Ordering::Relaxed);
                self.last_inbound_ms.store(0, Ordering::Relaxed);
                health
            }
            FeedHealth::Ok => FeedHealth::Ok,
        }
    }

    async fn dial_session_stream(&self, gateway_url: &str) -> Result<SessionStream> {
        let tcp = TcpStream::connect(gateway_url).await?;
        let _ = tcp.set_nodelay(true);
        if !self.tls_enabled {
            return Ok(SessionStream::Plain(tcp));
        }
        if self.tls_domain.trim().is_empty() {
            bail!("🔒 TLS enabled but SNI/verify domain is empty");
        }
        let connector = build_tls_connector(self.tls_ca_path.as_deref())?;
        info!("🔒 TLS handshake starting (sni={})", self.tls_domain);
        match connector.connect(&self.tls_domain, tcp).await {
            Ok(tls) => {
                info!("🔒 TLS handshake complete (sni={})", self.tls_domain);
                Ok(SessionStream::Tls(tls))
            }
            Err(e) => {
                error!("🔒 TLS handshake failed (sni={}): {e}", self.tls_domain);
                Err(anyhow!("🔒 TLS handshake failed: {e}"))
            }
        }
    }

    /// Single attempt. Stores stream + ConnectionState::Connected on success.
    /// Does NOT set Ready. Does NOT record ReconnectStorm. Does NOT retry.
    /// Calls `dial_session_stream` (plain TCP or TLS handshake as live).
    async fn connect_once(&self, gateway_url: &str, timeout: Duration) -> Result<(), TcpCheckErr> {
        match tokio::time::timeout(timeout, self.dial_session_stream(gateway_url)).await {
            Ok(Ok(stream)) => {
                *self.stream.lock().await = Some(stream);
                *self.connection_state.lock().await = ConnectionState::Connected;
                Ok(())
            }
            Ok(Err(e)) => Err(TcpCheckErr::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                e.to_string(),
            ))),
            Err(_elapsed) => Err(TcpCheckErr::Timeout {
                secs: timeout.as_secs(),
            }),
        }
    }

    /// Connect with automatic reconnection
    pub async fn connect_with_retry(&self, gateway_url: &str, max_attempts: u32, initial_backoff_ms: u64) -> Result<()> {
        let mut attempt = 0;
        let mut backoff = initial_backoff_ms;

        loop {
            attempt += 1;
            info!("Connection attempt {} to {}", attempt, gateway_url);

            // TCP + optional TLS handshake share the existing 30s timeout.
            // Store the stream and mark Connected only after the timeout returns.
            match tokio::time::timeout(Duration::from_secs(30), self.dial_session_stream(gateway_url)).await {
                Ok(Ok(stream)) => {
                    *self.stream.lock().await = Some(stream);
                    *self.connection_state.lock().await = ConnectionState::Connected;
                    let storm_count: Option<usize> = {
                        let mut tracker = self.reconnect_storm.lock().await;
                        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
                        tracker.record(now_ms)
                    };
                    if let Some(count) = storm_count {
                        self.emit_alert(
                            alerts::AlertKind::ReconnectStorm,
                            alerts::reconnect_storm_detail(count),
                        )
                        .await;
                    }
                    info!("✅ Connected successfully to FIX gateway at attempt {}", attempt);
                    return Ok(());
                }
                Ok(Err(e)) => {
                    error!("Connection attempt {} failed: {}", attempt, e);

                    if attempt >= max_attempts {
                        return Err(anyhow!(
                            "Max reconnection attempts ({}) exceeded",
                            max_attempts
                        ));
                    }

                    warn!("Retrying in {}ms...", backoff);
                    sleep(Duration::from_millis(backoff)).await;

                    // Exponential backoff with max 30 seconds
                    backoff = std::cmp::min(backoff * 2, 30_000);
                }
                Err(_) => {
                    error!("Connection attempt {} timeout (30s)", attempt);

                    if attempt >= max_attempts {
                        return Err(anyhow!(
                            "Max reconnection attempts ({}) exceeded",
                            max_attempts
                        ));
                    }

                    warn!("Retrying in {}ms...", backoff);
                    sleep(Duration::from_millis(backoff)).await;

                    // Exponential backoff with max 30 seconds
                    backoff = std::cmp::min(backoff * 2, 30_000);
                }
            }
        }
    }

    pub async fn logon(&self, heartbeat_interval: u64) -> Result<()> {
        self.logon_with_auth(heartbeat_interval, false).await
    }

    pub async fn logon_market_data(&self, heartbeat_interval: u64) -> Result<()> {
        self.logon_with_auth(heartbeat_interval, true).await
    }

    async fn logon_with_auth(&self, heartbeat_interval: u64, is_market_data: bool) -> Result<()> {
        *self.connection_state.lock().await = ConnectionState::Logging;

        // FIX 4.4 §2.4: when ResetSeqNumFlag (141=Y) is sent in the Logon, the MsgSeqNum
        // in that same message MUST be 1 — we are telling the other side that both counters
        // reset.  Reset our own counter to 0 so next_seq_num() returns 1.
        {
            let mut seq = self.seq_num.lock().await;
            *seq = 0;
        }

        let seq_num = self.next_seq_num().await; // returns 1
        let mut msg = FIXMessage::new('A', &self.sender_comp_id, &self.target_comp_id, seq_num);

        // Generate RawData - must be nonce in milliseconds per documentation
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let raw_data = now_ms.to_string();
        let raw_data_len = raw_data.len().to_string();

        // Different signature patterns based on flow:
        // Order Management: "public/auth" + MsgSeqNum (34) + api_key + "system_labelONEEX" + RawData (96)
        // Market Data: "public/auth" + MsgSeqNum (34) + api_key + RawData (96)
        let signature_input = if is_market_data {
            format!(
                "public/auth{}{}{}",
                seq_num,         // MsgSeqNum (field 34)
                self.api_key,         // api_key
                raw_data         // RawData (field 96)
            )
        } else {
            format!(
                "public/auth{}{}system_labelONEEX{}",
                seq_num,         // MsgSeqNum (field 34)
                self.api_key,         // api_key
                raw_data         // RawData (field 96)
            )
        };

        let mut mac = Hmac::<Sha256>::new_from_slice(self.api_secret.as_bytes())?;
        mac.update(signature_input.as_bytes());
        let signature_bytes = mac.finalize();
        let signature_hex = format!("{:x}", signature_bytes.into_bytes());

        // Set authentication fields per documentation
        msg.set_field(95, &raw_data_len);                    // RawDataLength (95) - REQUIRED
        msg.set_field(96, &raw_data);                        // RawData (96) - REQUIRED
        msg.set_field(98, "0");                              // EncryptMethod (98) - REQUIRED, must be 0
        msg.set_field(108, &heartbeat_interval.to_string()); // HeartBtInt (108) - REQUIRED
        msg.set_field(553, &self.api_key);                         // Username (553) - REQUIRED
        msg.set_field(554, &signature_hex);                  // Digital Signature (554) - REQUIRED
        msg.set_field(141, "Y");                             // ResetSeqNumFlag (141) - Reset sequence numbers

        let flow_type = if is_market_data { "Market Data" } else { "Order Management" };
        info!("Sending Logon (35=A) message ({}) with HMAC-SHA256 Digital Signature...", flow_type);
        info!("  Sender CompID (49): {}", self.sender_comp_id);
        info!("  Target CompID (56): {}", self.target_comp_id);
        info!("  MsgSeqNum (34): {}", seq_num);
        info!("  RawData (96): {}", raw_data);
        info!("  RawDataLength (95): {}", raw_data_len);
        // Never log signature_input: it is "public/auth" + seq + API KEY + nonce.
        info!("  Signature input: <redacted {} chars>", signature_input.len());
        info!("  Digital Signature (554): <redacted {} chars>", signature_hex.len());
        self.send_message(&msg).await?;

        // Don't set Ready yet - wait for ACK first
        // Connection state will be set to Ready when ACK is received
        *self.is_connected.lock().await = true;
        info!("Logon message transmitted. Waiting for ACK...");

        Ok(())
    }

    pub async fn logout(&self) -> Result<()> {
        *self.connection_state.lock().await = ConnectionState::Disconnecting;

        let seq_num = self.next_seq_num().await;
        let msg = FIXMessage::new('5', &self.sender_comp_id, &self.target_comp_id, seq_num);

        self.send_message(&msg).await?;

        *self.is_connected.lock().await = false;
        *self.connection_state.lock().await = ConnectionState::Disconnected;

        info!("Logout sent");

        Ok(())
    }

    pub async fn heartbeat(&self) -> Result<()> {
        self.heartbeat_with_test_req_id(None).await
    }

    /// Send a Heartbeat (35=0). When answering a TestRequest (35=1) the
    /// TestReqID (112) from that request MUST be echoed back, otherwise the
    /// counterparty treats the session as unresponsive and disconnects it.
    pub async fn heartbeat_with_test_req_id(&self, test_req_id: Option<&str>) -> Result<()> {
        let seq_num = self.next_seq_num().await;
        let mut msg = FIXMessage::new('0', &self.sender_comp_id, &self.target_comp_id, seq_num);
        if let Some(id) = test_req_id {
            msg.set_field(tags::TEST_REQ_ID, id);
        }
        self.send_message(&msg).await?;
        Ok(())
    }

    pub async fn order_status_request(
        &self,
        cl_ord_id: &str,
        symbol: &str,
    ) -> Result<()> {
        if !self.is_ready().await {
            return Err(anyhow!("Session not ready"));
        }

        let seq_num = self.next_seq_num().await;
        let mut msg = FIXMessage::new('H', &self.sender_comp_id, &self.target_comp_id, seq_num);

        // Required fields for Order Status Request
        msg.set_field(tags::CL_ORD_ID, cl_ord_id);
        msg.set_field(tags::SYMBOL, symbol);
        msg.set_field(tags::TRANSACT_TIME, &FIXMessage::timestamp());

        info!("Sending Order Status Request:");
        info!("  ClOrdID (11): {}", cl_ord_id);
        info!("  Symbol (55): {}", symbol);
        info!("  TransactTime (60): {}", FIXMessage::timestamp());

        self.send_message(&msg).await?;
        info!("Order Status Request sent successfully");

        Ok(())
    }

    /// Snapshot the API working set and send OrderStatusRequest (35=H)
    /// for each locally-open ClOrdID. Per-row send errors are logged and
    /// skipped; the pass always returns Ok(()) after attempting every row.
    /// Does not wait for replies — inbound 35=8 is applied by the existing
    /// handle_message '8' arm once the receive loop (or test pump) runs.
    pub async fn reconcile_open_orders(&self) -> Result<()> {
        let open = self.open_orders.get_open_orders();
        for order in open {
            if let Err(e) = self
                .order_status_request(&order.cl_ord_id, &order.symbol)
                .await
            {
                warn!(
                    "Failed to send OrderStatusRequest (35=H) for ClOrdID {}: {}",
                    order.cl_ord_id, e
                );
            }
        }
        Ok(())
    }

    /// Shared gate. No log here — callers log 35=D vs 35=G.
    fn reject_if_operator_halt(&self) -> Result<()> {
        if self.operator_halt.load(Ordering::SeqCst) {
            Err(anyhow!(OPERATOR_HALT_REJECT))
        } else {
            Ok(())
        }
    }

    pub fn operator_halt_is_set(&self) -> bool {
        self.operator_halt.load(Ordering::SeqCst)
    }

    pub fn reset_operator_halt(&self) {
        self.operator_halt.store(false, Ordering::SeqCst);
        let peak = self.session_realized_pnl();
        let peak = if peak.is_finite() { peak } else { 0.0 };
        store_f64(&self.drawdown_peak_bits, peak);
        info!("operator_halt=false drawdown_peak_rebased_to={}", peak);
    }

    /// Production: once, on the OM handle after spawn. Tests: arm fixtures.
    /// Replaces the whole map. Does NOT rebase peak. Does NOT touch operator_halt.
    fn apply_risk_config(
        &self,
        limits: HashMap<String, SymbolLimit>,
        session_max_drawdown: Option<f64>,
    ) {
        let keys: Vec<String> = limits.keys().cloned().collect();
        self.symbol_limits.store(Arc::new(limits));
        let threshold = match session_max_drawdown {
            Some(v) if v.is_finite() && v > 0.0 => v,
            Some(v) if !v.is_finite() => {
                warn!("session_max_drawdown is non-finite; treating as off");
                0.0
            }
            _ => 0.0,
        };
        store_f64(&self.session_max_drawdown_bits, threshold);
        info!(
            "risk_config symbols={:?} session_max_drawdown={}",
            keys, threshold
        );
    }

    fn reject_if_exposure_breach(&self, order: &OrderRequest) -> Result<()> {
        let limits = self.symbol_limits.load();
        let net = self.positions.net_qty_of(&order.symbol);
        match check_exposure(order, net, limits.as_ref()) {
            Ok(()) => Ok(()),
            Err(r) => {
                let signed = if order.side == '1' {
                    order.qty
                } else {
                    -order.qty
                };
                let projected = net + signed;
                let lim = limits.get(&order.symbol);
                let max_pos = lim
                    .map(|l| l.max_position.to_string())
                    .unwrap_or_else(|| "n/a".into());
                let max_not = lim
                    .map(|l| l.max_notional.to_string())
                    .unwrap_or_else(|| "n/a".into());
                let notional = if let Some(p) = order.price {
                    (order.qty * p).to_string()
                } else if let Some(c) = order.cash_order_qty {
                    c.to_string()
                } else {
                    "n/a".into()
                };
                warn!(
                    "exposure_gate=reject reason={} cl_ord_id={} symbol={} side={} qty={} net_qty={} projected={} max_position={} notional={} max_notional={}",
                    exposure_reject_reason_tag(r),
                    order.cl_ord_id,
                    order.symbol,
                    order.side,
                    order.qty,
                    net,
                    projected,
                    max_pos,
                    notional,
                    max_not,
                );
                Err(anyhow!(exposure_reject_str(r)))
            }
        }
    }

    fn session_realized_pnl(&self) -> f64 {
        self.positions.sum_realized_pnl()
    }

    /// Realized-only drawdown (EL-004 residual: no mark-to-market / MD books).
    async fn evaluate_session_drawdown(&self) {
        let threshold = load_f64(&self.session_max_drawdown_bits);
        if !threshold.is_finite() || threshold <= 0.0 {
            return;
        }
        let current = self.session_realized_pnl();
        if !current.is_finite() {
            warn!("session_drawdown non-finite realized_pnl; fail-closed halt");
            if !self.operator_halt_is_set() {
                self.trigger_operator_halt().await;
            }
            return;
        }
        let mut peak = load_f64(&self.drawdown_peak_bits);
        if !peak.is_finite() || current > peak {
            store_f64(&self.drawdown_peak_bits, current);
            peak = current;
        }
        if peak - current >= threshold {
            info!(
                "operator_halt=true reason=drawdown peak={} current={} threshold={}",
                peak, current, threshold
            );
            if !self.operator_halt_is_set() {
                self.trigger_operator_halt().await;
            }
        }
    }

    pub async fn trigger_operator_halt(&self) -> OperatorHaltTriggerResult {
        self.operator_halt.store(true, Ordering::SeqCst);

        let snapshot: Vec<OpenOrder> = self.open_orders.get_open_orders();
        let mut cancel_ok: u32 = 0;
        let mut cancel_failed: u32 = 0;

        for order in &snapshot {
            let req = CancelOrderRequest {
                cl_ord_id: format!("HALT_{}", uuid::Uuid::new_v4().simple()),
                orig_cl_ord_id: order.cl_ord_id.clone(),
                symbol: order.symbol.clone(),
                side: order.side.clone(),
            };
            match self.cancel_order(&req).await {
                Ok(()) => cancel_ok += 1,
                Err(e) => {
                    cancel_failed += 1;
                    warn!(
                        "operator_halt=true cancel_failed orig_cl_ord_id={} symbol={} err={}",
                        order.cl_ord_id, order.symbol, e
                    );
                }
            }
        }

        let result = OperatorHaltTriggerResult {
            halted: true,
            cancel_attempted: snapshot.len() as u32,
            cancel_ok,
            cancel_failed,
        };
        info!(
            "operator_halt=true cancel_attempted={} cancel_ok={} cancel_failed={}",
            result.cancel_attempted, result.cancel_ok, result.cancel_failed
        );
        result
    }

    /// Cancel an order using OrderCancelRequest (35=F)
    pub async fn cancel_order(&self, cancel_request: &CancelOrderRequest) -> Result<()> {
        if !self.is_ready().await {
            return Err(anyhow!("Session not ready"));
        }

        let seq_num = self.next_seq_num().await;
        let mut msg = FIXMessage::new('F', &self.sender_comp_id, &self.target_comp_id, seq_num);

        // Required fields for Order Cancel Request
        msg.set_field(tags::CL_ORD_ID, &cancel_request.cl_ord_id);
        msg.set_field(tags::ORIG_CL_ORD_ID, &cancel_request.orig_cl_ord_id);
        msg.set_field(tags::SYMBOL, &cancel_request.symbol);
        msg.set_field(tags::SIDE, &cancel_request.side);
        msg.set_field(tags::TRANSACT_TIME, &FIXMessage::timestamp());

        info!("Sending Order Cancel Request:");
        info!("  ClOrdID (11): {} (Cancel Request ID)", cancel_request.cl_ord_id);
        info!("  OrigClOrdID (41): {} (Original Order ID)", cancel_request.orig_cl_ord_id);
        info!("  Symbol (55): {}", cancel_request.symbol);
        info!("  Side (54): {}", cancel_request.side);
        info!("  TransactTime (60): {}", FIXMessage::timestamp());

        // Log FIX command
        let encoded = msg.encode()?;
        Self::log_fix_command(&msg, &encoded, Direction::Outbound);

        // Send message
        let mut stream_guard = self.stream.lock().await;
        let stream = stream_guard.as_mut().ok_or_else(|| anyhow!("Not connected"))?;
        stream.write_all(&encoded).await?;
        stream.flush().await?;
        drop(stream_guard);

        info!("Order Cancel Request sent successfully");

        Ok(())
    }

    /// Order Cancel/Replace Request (35=G): amend price and/or quantity of a
    /// working order. `new_cl_ord_id` must be a fresh ClOrdID; `orig_cl_ord_id`
    /// identifies the order being replaced.
    pub async fn order_cancel_replace_request(&self, request: &ReplaceOrderRequest) -> Result<()> {
        if let Err(e) = self.reject_if_operator_halt() {
            warn!(
                "operator_halt=true rejected 35=G cl_ord_id={} orig_cl_ord_id={} symbol={}",
                request.cl_ord_id, request.orig_cl_ord_id, request.symbol
            );
            return Err(e);
        }

        if !self.is_ready().await {
            return Err(anyhow!("Session not ready"));
        }

        let already_known = self.open_orders.snapshot().contains_key(&request.cl_ord_id)
            || self.order_times.lock().await.contains_key(&request.cl_ord_id);
        if already_known {
            return Err(anyhow!("Duplicate ClOrdID rejected: {}", request.cl_ord_id));
        }

        let seq_num = self.next_seq_num().await;
        let mut msg = FIXMessage::new('G', &self.sender_comp_id, &self.target_comp_id, seq_num);

        msg.set_field(tags::CL_ORD_ID, &request.cl_ord_id);
        msg.set_field(tags::ORIG_CL_ORD_ID, &request.orig_cl_ord_id);
        msg.set_field(tags::SYMBOL, &request.symbol);
        msg.set_field(tags::SIDE, &request.side.to_string());
        if let Some(qty) = request.qty {
            msg.set_field(tags::ORDER_QTY, &qty.to_string());
        }
        if let Some(price) = request.price {
            msg.set_field(tags::PRICE, &price.to_string());
        }
        msg.set_field(tags::ORD_TYPE, "2"); // LIMIT
        msg.set_field(tags::TRANSACT_TIME, &FIXMessage::timestamp());

        info!("Sending Order Cancel/Replace Request (35=G):");
        info!("  ClOrdID (11): {} (new)", request.cl_ord_id);
        info!("  OrigClOrdID (41): {}", request.orig_cl_ord_id);
        info!("  Symbol (55): {}", request.symbol);

        self.order_times.lock().await.insert(request.cl_ord_id.clone(), Instant::now());
        self.send_message(&msg).await?;
        info!("Order Cancel/Replace Request sent successfully");

        Ok(())
    }

    /// Create order from OrderRequest struct (supports all FIX order fields)
    pub async fn new_order_single_from_request(&self, order: &OrderRequest) -> Result<()> {
        if let Err(e) = self.reject_if_operator_halt() {
            warn!(
                "operator_halt=true rejected 35=D cl_ord_id={} symbol={}",
                order.cl_ord_id, order.symbol
            );
            return Err(e);
        }

        if let Err(e) = self.reject_if_exposure_breach(order) {
            return Err(e);
        }

        if !self.is_ready().await {
            return Err(anyhow!("Session not ready"));
        }

        // ClOrdId idempotency: reject before submission rather than let the
        // venue silently treat a resent/duplicated ClOrdID as a new order.
        // Checks both confirmed orders (open_orders, populated from
        // ExecutionReports) and in-flight ones (order_times, populated at
        // submission time) so a duplicate can't slip through the window
        // before the first ExecutionReport comes back.
        let already_known = self.open_orders.snapshot().contains_key(&order.cl_ord_id)
            || self.order_times.lock().await.contains_key(&order.cl_ord_id);
        if already_known {
            return Err(anyhow!("Duplicate ClOrdID rejected: {}", order.cl_ord_id));
        }

        let seq_num = self.next_seq_num().await;
        let mut msg = FIXMessage::new('D', &self.sender_comp_id, &self.target_comp_id, seq_num);

        // Required fields
        msg.set_field(tags::CL_ORD_ID, &order.cl_ord_id);
        msg.set_field(tags::SYMBOL, &order.symbol);
        msg.set_field(tags::SIDE, &order.side.to_string());
        msg.set_field(tags::ORDER_QTY, &order.qty.to_string());
        msg.set_field(tags::TRANSACT_TIME, &FIXMessage::timestamp());

        // OrdType (40): Default to LIMIT (2) if not specified, but support MARKET (1) and PREVIOUSLY_QUOTED (D)
        let ord_type = order.ord_type.as_deref().unwrap_or("2");
        msg.set_field(tags::ORD_TYPE, ord_type);

        // Price (44): Required for LIMIT orders, optional for MARKET
        if let Some(price) = order.price {
            msg.set_field(tags::PRICE, &price.to_string());
        } else if ord_type == "2" {
            return Err(anyhow!("Price is required for LIMIT orders (OrdType=2)"));
        }

        // TimeInForce (59): Default to GTC (1) if not specified
        let time_in_force = order.time_in_force.as_deref().unwrap_or("1");
        msg.set_field(tags::TIME_IN_FORCE, time_in_force);

        // Optional fields
        if let Some(ref currency) = order.currency {
            msg.set_field(15, currency); // Tag 15: Currency
        }
        if let Some(ref exec_inst) = order.exec_inst {
            msg.set_field(18, exec_inst); // Tag 18: ExecInst
        }
        if let Some(ref quote_id) = order.quote_id {
            msg.set_field(117, quote_id); // Tag 117: QuoteID
        }
        if let Some(cash_order_qty) = order.cash_order_qty {
            msg.set_field(152, &cash_order_qty.to_string()); // Tag 152: CashOrderQty
        }
        if let Some(ref cash_margin) = order.cash_margin {
            msg.set_field(544, cash_margin); // Tag 544: CashMargin
        }
        if let Some(ref trigger_price_type) = order.trigger_price_type {
            msg.set_field(1107, trigger_price_type); // Tag 1107: TriggerPriceType
        }
        if let Some(ref smp_scope) = order.self_match_prevention_scope {
            msg.set_field(12362, smp_scope); // Tag 12362: SelfMatchPreventionScope
        }
        if let Some(ref smp_instruction) = order.self_match_prevention_instruction {
            msg.set_field(2964, smp_instruction); // Tag 2964: SelfMatchPreventionInstruction
        }
        if let Some(ref smp_id) = order.self_match_prevention_id {
            msg.set_field(2362, smp_id); // Tag 2362: SelfMatchPreventionID
        }
        if let Some(ref broker_id) = order.broker_id {
            msg.set_field(7933, broker_id); // Tag 7933: BrokerId
        }
        if let Some(receive_window) = order.receive_window {
            // Validate range: 100-5000 milliseconds
            let valid_window = receive_window.max(100).min(5000);
            msg.set_field(20100, &valid_window.to_string()); // Tag 20100: ReceiveWindow
        }

        // Log the order for debugging
        let transact_time = FIXMessage::timestamp();
        info!("Order message being sent:");
        info!("  ClOrdID (11): {}", order.cl_ord_id);
        info!("  Symbol (55): {}", order.symbol);
        info!("  Side (54): {}", order.side);
        info!("  OrderQty (38): {}", order.qty);
        if let Some(price) = order.price {
            info!("  Price (44): {}", price);
        }
        info!("  OrdType (40): {}", ord_type);
        info!("  TimeInForce (59): {}", time_in_force);
        info!("  TransactTime (60): {}", transact_time);

        // Measure FIX encoding time
        let fix_encode_start = Instant::now();
        let encoded = msg.encode()?;
        let fix_encode_us = fix_encode_start.elapsed().as_micros() as u64;
        
        // Log FIX command to separate file
        Self::log_fix_command(&msg, &encoded, Direction::Outbound);
        
        // Measure FIX send time
        let fix_send_start = Instant::now();
        let mut stream_guard = self.stream.lock().await;
        let stream = stream_guard.as_mut().ok_or_else(|| anyhow!("Not connected"))?;
        stream.write_all(&encoded).await?;
        stream.flush().await?;
        let fix_send_us = fix_send_start.elapsed().as_micros() as u64;
        drop(stream_guard);
        
        // Record submission time for latency measurement
        let submission_time = Instant::now();
        self.order_times.lock().await.insert(order.cl_ord_id.clone(), submission_time);
        
        // Update timing metrics
        {
            let mut timings = self.order_timings.lock().await;
            if let Some(timing) = timings.get_mut(&order.cl_ord_id) {
                timing.fix_encode_us = Some(fix_encode_us);
                timing.fix_send_us = Some(fix_send_us);
            }
        }
        
        info!("Order {} sent (Encode: {}µs, Send: {}µs)", order.cl_ord_id, fix_encode_us, fix_send_us);

        Ok(())
    }

    /// Legacy builder the algo loop actually calls (`algo_runtime::run`).
    /// SMP args are copied onto OrderRequest as-is after SEC-001.
    /// Other optional OrderRequest fields stay None; ord_type stays "2"; TIF stays "1".
    pub async fn new_order_single(
        &self,
        cl_ord_id: &str,
        symbol: &str,
        side: char,
        qty: f64,
        price: f64,
        self_match_prevention_scope: Option<String>,
        self_match_prevention_instruction: Option<String>,
        self_match_prevention_id: Option<String>,
    ) -> Result<()> {
        smp_arg_or_reject("self_match_prevention_scope", &self_match_prevention_scope)?;
        smp_arg_or_reject("self_match_prevention_instruction", &self_match_prevention_instruction)?;
        smp_arg_or_reject("self_match_prevention_id", &self_match_prevention_id)?;
        let order = OrderRequest {
            cl_ord_id: cl_ord_id.to_string(),
            symbol: symbol.to_string(),
            side,
            qty,
            price: Some(price),
            ord_type: Some("2".to_string()), // LIMIT
            time_in_force: Some("1".to_string()), // GTC
            currency: None,
            exec_inst: None,
            quote_id: None,
            cash_order_qty: None,
            cash_margin: None,
            trigger_price_type: None,
            self_match_prevention_scope,
            self_match_prevention_instruction,
            self_match_prevention_id,
            broker_id: None,
            receive_window: None,
        };
        self.new_order_single_from_request(&order).await
    }

    pub async fn security_list_request(&self, req_id: &str) -> Result<()> {
        if !self.is_ready().await {
            return Err(anyhow!("Session not ready"));
        }

        let seq_num = self.next_seq_num().await;
        let mut msg = FIXMessage::new('x', &self.sender_comp_id, &self.target_comp_id, seq_num);

        // Required fields for SecurityListRequest per Crypto.com documentation
        // NOTE: Field order matters! Gateway expects: 320, 559, 263, 60 (NOT sorted)
        msg.set_field(tags::SECURITY_REQ_ID, req_id);                    // Tag 320 - REQUIRED
        msg.set_field(tags::SECURITY_LIST_REQUEST_TYPE, "0");            // Tag 559 - REQUIRED (0 = all securities per Crypto.com)
        msg.set_field(tags::SUBSCRIPTION_REQUEST_TYPE, "0");             // Tag 263 - REQUIRED (0=snapshot only, no streaming subscription)
        msg.set_field(tags::TRANSACT_TIME, &FIXMessage::timestamp());    // Tag 60

        info!("📋 Requesting all available securities...");
        info!("Sending SecurityListRequest (35=x):");
        info!("  SecurityReqID (320): {}", req_id);
        info!("  SecurityListRequestType (559): 0 (All Securities)");
        info!("  SubscriptionRequestType (263): 0 (Snapshot Only - No Streaming)");

        self.send_message(&msg).await?;
        info!("SecurityListRequest sent successfully");

        Ok(())
    }

    pub async fn market_data_request(&self, symbol: &str, market_depth: u32) -> Result<()> {
        if !self.is_ready().await {
            return Err(anyhow!("Session not ready"));
        }

        let seq_num = self.next_seq_num().await;
        let mut msg = FIXMessage::new('V', &self.sender_comp_id, &self.target_comp_id, seq_num);

        // MarketDataRequest fields - Crypto.com format
        // Per Crypto.com docs: MDReqID (262) must be in format: book.{Symbol}.{MarketDepth}
        // IMPORTANT: Field order and structure must match Crypto.com expectations exactly
        let md_req_id = format!("book.{}.{}", symbol, market_depth);
        
        // Set fields in the exact order Crypto.com expects (based on FIX 4.4 standard order)
        msg.set_field(262, &md_req_id);                       // Tag 262 - MarketDataReqID (format: book.SYMBOL.DEPTH)
        msg.set_field(263, "1");                              // Tag 263 - SubscriptionRequestType (1 = snapshot + subscribe)
        msg.set_field(264, &market_depth.to_string());        // Tag 264 - MarketDepth
        msg.set_field(265, "1");                              // Tag 265 - MDUpdateType (1 = Incremental Refresh - REQUIRED)
        msg.set_field(266, "Y");                              // Tag 266 - AggregatedBook (Y when 262 = book.{Symbol}.{MarketDepth} - REQUIRED)
        msg.set_field(547, "N");                              // Tag 547 - MDImplicitDelete (Must be N - REQUIRED)
        msg.set_field(267, "2");                              // Tag 267 - NoMDEntryTypes (2 entries: Bid and Ask)
        // Repeating group: MDEntryType
        // Group 1: Bid
        msg.set_field(269, "0");                              // Tag 269 - MDEntryType (0 = Bid)
        // Group 2: Ask  
        msg.set_field(269, "1");                              // Tag 269 - MDEntryType (1 = Ask)
        // Related symbols group
        msg.set_field(146, "1");                              // Tag 146 - NoRelatedSym (1 symbol)
        msg.set_field(55, symbol);                            // Tag 55 - Symbol
        msg.set_field(60, &FIXMessage::timestamp());          // Tag 60 - TransactTime

        info!("📊 Sending MarketDataRequest for {} (Real-time subscription)", symbol);
        info!("    MarketDataReqID (262): {} (format: book.SYMBOL.DEPTH)", md_req_id);
        info!("    MarketDepth (264): {}", market_depth);
        info!("    SubscriptionType (263): 1 (Subscribe)");
        info!("    MDUpdateType (265): 1 (Incremental Refresh - REQUIRED)");
        info!("    AggregatedBook (266): Y (REQUIRED for book.* format)");
        info!("    MDImplicitDelete (547): N (REQUIRED)");

        // Debug: Print the message before sending
        if let Ok(encoded) = msg.encode() {
            let msg_readable = String::from_utf8_lossy(&encoded);
            info!("DEBUG: MarketDataRequest message format: {}", msg_readable.replace("\x01", "|"));
        }

        self.send_message(&msg).await?;
        info!("✅ MarketDataRequest for {} sent successfully", symbol);

        Ok(())
    }

    pub async fn trade_data_request(&self, symbol: &str) -> Result<()> {
        if !self.is_ready().await {
            return Err(anyhow!("Session not ready"));
        }

        let seq_num = self.next_seq_num().await;
        let mut msg = FIXMessage::new('V', &self.sender_comp_id, &self.target_comp_id, seq_num);

        // MarketDataRequest fields for trade data - Crypto.com format
        // Per Crypto.com docs: MDReqID (262) must be in format: trade.{Symbol}
        let md_req_id = format!("trade.{}", symbol);
        
        // Set fields in the exact order Crypto.com expects
        msg.set_field(262, &md_req_id);                       // Tag 262 - MarketDataReqID (format: trade.SYMBOL)
        msg.set_field(263, "1");                              // Tag 263 - SubscriptionRequestType (1 = snapshot + subscribe)
        msg.set_field(264, "0");                              // Tag 264 - MarketDepth (Must be 0 when 262 = trade.{Symbol})
        msg.set_field(265, "1");                              // Tag 265 - MDUpdateType (1 = Incremental Refresh - REQUIRED)
        msg.set_field(266, "N");                              // Tag 266 - AggregatedBook (N when 262 = trade.{Symbol} - REQUIRED)
        msg.set_field(547, "N");                              // Tag 547 - MDImplicitDelete (Must be N - REQUIRED)
        msg.set_field(267, "1");                              // Tag 267 - NoMDEntryTypes (1 entry: Trade)
        // Repeating group: MDEntryType
        msg.set_field(269, "2");                              // Tag 269 - MDEntryType (2 = Trade)
        // Related symbols group
        msg.set_field(146, "1");                              // Tag 146 - NoRelatedSym (1 symbol)
        msg.set_field(55, symbol);                            // Tag 55 - Symbol
        msg.set_field(60, &FIXMessage::timestamp());          // Tag 60 - TransactTime

        info!("📊 Sending TradeDataRequest for {} (Real-time subscription)", symbol);
        info!("    MarketDataReqID (262): {} (format: trade.SYMBOL)", md_req_id);
        info!("    MarketDepth (264): 0 (REQUIRED for trade.* format)");
        info!("    SubscriptionType (263): 1 (Subscribe)");
        info!("    MDUpdateType (265): 1 (Incremental Refresh - REQUIRED)");
        info!("    AggregatedBook (266): N (REQUIRED for trade.* format)");
        info!("    MDImplicitDelete (547): N (REQUIRED)");
        info!("    MDEntryType (269): 2 (Trade)");

        // Debug: Print the message before sending
        if let Ok(encoded) = msg.encode() {
            let msg_readable = String::from_utf8_lossy(&encoded);
            info!("DEBUG: TradeDataRequest message format: {}", msg_readable.replace("\x01", "|"));
        }

        self.send_message(&msg).await?;
        info!("✅ TradeDataRequest for {} sent successfully", symbol);

        Ok(())
    }

    /// Get snapshot of order book for a symbol (lock-free read)
    pub async fn get_order_book_snapshot(&self, symbol: &str) -> Option<Arc<OrderBookSnapshot>> {
        let books = self.order_books.lock().await;
        books.get(symbol).map(|book| book.get_snapshot())
    }

    /// Get best bid/ask for a symbol (from snapshot, lock-free)
    pub async fn get_best_quote(&self, symbol: &str) -> Option<(f64, f64, f64)> {
        let books = self.order_books.lock().await;
        if let Some(book) = books.get(symbol) {
            let snap = book.get_snapshot();
            let bid = snap.best_bid;
            let ask = snap.best_ask;
            if bid > 0.0 && ask > 0.0 {
                Some((bid, ask, snap.spread()))
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Get order book depth (top N levels) from snapshot (lock-free)
    pub async fn get_order_book_depth(&self, symbol: &str, side: Side, depth: usize) -> Vec<(f64, f64)> {
        let books = self.order_books.lock().await;
        if let Some(book) = books.get(symbol) {
            book.get_snapshot().get_depth(side, depth)
        } else {
            Vec::new()
        }
    }

    /// Place small marketable orders to generate test trades
    /// Places buy orders at ask price and sell orders at bid price to ensure execution
    pub async fn place_test_orders_for_trades(&self, order_session: Arc<FIXSession>, symbol: &str, count: usize) -> Result<()> {
        info!("");
        info!("💹 Placing {} small test orders to generate trades...", count);
        
        // Get current market prices from order book
        let books = self.order_books.lock().await;
        let best_bid = books.get(symbol)
            .map(|b| b.get_active().best_bid())
            .unwrap_or(0.0);
        let best_ask = books.get(symbol)
            .map(|b| b.get_active().best_ask())
            .unwrap_or(0.0);
        drop(books);

        if best_bid <= 0.0 || best_ask <= 0.0 {
            warn!("⚠️  Cannot place test orders - order book not ready (Bid={}, Ask={})", best_bid, best_ask);
            return Err(anyhow!("Order book not ready"));
        }

        info!("  Current market: Bid={:.2}, Ask={:.2}, Spread={:.2}", best_bid, best_ask, best_ask - best_bid);
        
        // Place alternating buy/sell orders at market prices
        for i in 0..count {
            let order_num = i + 1;
            let side = if i % 2 == 0 { '1' } else { '2' }; // Alternate: Buy, Sell, Buy, Sell...
            let (price, side_name) = if side == '1' {
                // Buy order - place at ask price (will execute immediately)
                (best_ask, "Buy")
            } else {
                // Sell order - place at bid price (will execute immediately)
                (best_bid, "Sell")
            };
            
            // Very small quantity to minimize impact
            let qty = 0.0001; // 0.0001 BTC
            
            let cl_ord_id = format!("TEST_TRADE_{:03}", order_num);
            
            info!("  [{}/{}] Placing {} order: {} @ {:.2} (qty: {})", 
                  order_num, count, side_name, cl_ord_id, price, qty);
            
            if let Err(e) = order_session.new_order_single(&cl_ord_id, symbol, side, qty, price, None, None, None).await {
                warn!("    Failed to place order {}: {}", cl_ord_id, e);
            } else {
                info!("    ✅ Order {} placed successfully", cl_ord_id);
            }
            
            // Small delay between orders
            sleep(Duration::from_millis(500)).await;
        }
        
        info!("✅ Finished placing {} test orders", count);
        info!("   Trades should appear in order book if orders execute");
        
        Ok(())
    }

    /// Dump order book to file (creates snapshot, writes to file)

    pub async fn receive_message(&self) -> Result<FIXMessage> {
        // First, try to extract from existing buffer if it has enough data
        let buffer_len = {
            let msg_buffer = self.message_buffer.lock().await;
            msg_buffer.len()
        };

        if buffer_len >= 15 {
            // Buffer has potential message data, try to extract
            match self.extract_complete_message().await {
                Ok(msg) => return Ok(msg),
                Err(e) => {
                    // If buffer is incomplete, we need to read more data
                    let err_str = e.to_string();
                    if !err_str.contains("waiting for more data") && !err_str.contains("waiting for message start") {
                        // Real error, not just incomplete message
                        return Err(e);
                    }
                }
            }
        }

        // Read more data from network
        let mut stream_guard = self.stream.lock().await;
        let stream = stream_guard.as_mut().ok_or_else(|| anyhow!("Not connected"))?;

        let mut read_buffer = vec![0; 4096];
        let n = stream.read(&mut read_buffer).await?;

        if n == 0 {
            return Err(anyhow!("Connection closed by gateway"));
        }

        read_buffer.truncate(n);

        // Debug: log raw received data
        let hex_str = read_buffer.iter()
            .take(100)
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<_>>()
            .join("");
        info!("DEBUG RX: Received {} bytes (hex first 100 chars): {}", n, &hex_str[..std::cmp::min(100, hex_str.len())]);

        // Add to message buffer
        let mut msg_buffer = self.message_buffer.lock().await;
        msg_buffer.extend_from_slice(&read_buffer);
        drop(msg_buffer);

        // Try to parse a complete message from the buffer
        self.extract_complete_message().await
    }

    /// Extract a complete FIX message from the buffer
    /// Handles fragmented messages by waiting for the full message to arrive
    async fn extract_complete_message(&self) -> Result<FIXMessage> {
        // Frame exactly one FIX message from the front of the buffer, using
        // BodyLength (9), on raw bytes.
        //
        //   8=FIX.4.4<SOH>9=<len><SOH><body: len bytes>10=NNN<SOH>
        //
        // The message ends at body_start + len + 7. This used to locate the
        // checksum with rfind("10=") over a lossy UTF-8 copy -- the LAST
        // "10=" in the whole buffer, which also matches inside tags such as
        // 110= -- and drain everything up to it as one message. When a TCP read
        // carried several messages, all but one were silently lost; their
        // MsgSeqNums then read as a gap, recovery triggered bursts that
        // coalesced again, and the client looped in recovery dropping inbound
        // messages (ExecutionReports included). Only the first message is
        // consumed here; the rest stay buffered for the next call.
        const SOH: u8 = 0x01;
        const TRAILER_LEN: usize = 7; // "10=" + 3 digits + SOH
        let mut buf = self.message_buffer.lock().await;

        loop {
            // Resync to the start of a message if there is leading junk, so a
            // single bad frame can never wedge the stream.
            if !buf.starts_with(b"8=") {
                match buf.windows(5).position(|w| w == b"8=FIX") {
                    Some(0) => {}
                    Some(i) => {
                        warn!("FIX framing: discarding {} bytes of leading junk", i);
                        buf.drain(..i);
                    }
                    None => {
                        // Keep a tail that could be the start of "8=FIX".
                        let keep = buf.len().min(4);
                        let cut = buf.len() - keep;
                        if cut > 0 {
                            warn!("FIX framing: discarding {} bytes with no message start", cut);
                            buf.drain(..cut);
                        }
                        return Err(anyhow!("Incomplete FIX message - waiting for message start"));
                    }
                }
            }

            // Header: BeginString<SOH> 9=<len><SOH>
            let Some(p1) = buf.iter().position(|&b| b == SOH) else {
                return Err(anyhow!("Incomplete FIX message - waiting for more data"));
            };
            if buf.len() < p1 + 3 {
                return Err(anyhow!("Incomplete FIX message - waiting for more data"));
            }
            if &buf[p1 + 1..p1 + 3] != b"9=" {
                warn!("FIX framing: BodyLength(9) does not follow BeginString; resyncing");
                buf.drain(..1);
                continue;
            }
            let Some(rel) = buf[p1 + 3..].iter().position(|&b| b == SOH) else {
                return Err(anyhow!("Incomplete FIX message - waiting for more data"));
            };
            let p2 = p1 + 3 + rel;
            let body_len = match std::str::from_utf8(&buf[p1 + 3..p2]).ok().and_then(|v| v.parse::<usize>().ok()) {
                Some(n) => n,
                None => {
                    warn!("FIX framing: unparseable BodyLength; resyncing");
                    buf.drain(..1);
                    continue;
                }
            };

            let trailer_start = p2 + 1 + body_len;
            let message_end = trailer_start + TRAILER_LEN;
            if buf.len() < message_end {
                return Err(anyhow!("Incomplete FIX message - waiting for more data"));
            }
            if &buf[trailer_start..trailer_start + 3] != b"10=" || buf[message_end - 1] != SOH {
                warn!("FIX framing: CheckSum(10) not where BodyLength says it is; resyncing");
                buf.drain(..1);
                continue;
            }

            let complete_msg: Vec<u8> = buf.drain(..message_end).collect();
            drop(buf);
            info!("✅ Extracted complete FIX message ({} bytes) from buffer", complete_msg.len());
            let msg = FIXMessage::decode(&complete_msg)?;
            Self::log_fix_command(&msg, &complete_msg, Direction::Inbound);
            return Ok(msg);
        }
    }

    pub async fn handle_message(&self, msg: &FIXMessage) -> Result<()> {
        // Liveness: any inbound message proves the stream is alive.
        self.last_inbound_ms.store(chrono::Utc::now().timestamp_millis() as u64, Ordering::Relaxed);
        // Mandatory sequence-integrity choke point: no message's body is
        // processed below without first passing through here. This is what
        // makes AC2/AC4 (gap detection -> recovery, no silent corruption)
        // hold for every message type, not just market data.
        match msg.msg_type {
            'A' => {
                // ResetSeqNumFlag=Y (see logon_with_auth) means both sides
                // restart at MsgSeqNum=1 on every session — (re)initialize here
                // rather than gate the Logon ACK itself on a stale counter.
                // The Logon ACK's own MsgSeqNum becomes the new baseline.
                let mut seqr = self.sequencer.lock().await;
                seqr.reset("inbound");
                seqr.resolve_recovery("inbound", msg.seq_num);
            }
            '4' => {
                // SequenceReset (plain or GapFill): NewSeqNo (36) tells us what
                // to expect next, which resolves any in-progress recovery.
                if let Some(new_seq_no) = msg.get_field(tags::NEW_SEQ_NO).and_then(|s| s.parse::<u32>().ok()) {
                    self.sequencer.lock().await.resolve_recovery("inbound", new_seq_no.saturating_sub(1));
                    info!("🔄 SequenceReset (35=4) processed: inbound sequence resynced, next expected {}", new_seq_no);
                }
            }
            'W' if self.sequencer.lock().await.is_recovering("inbound") => {
                // This venue's recovery path is a fresh full snapshot rather
                // than historical delta replay: receiving one while recovering
                // IS the resolution. Falls through below to actually apply it.
                self.sequencer.lock().await.resolve_recovery("inbound", msg.seq_num);
                info!("🔄 Sequence gap recovered via fresh MarketDataSnapshotFullRefresh (seq {})", msg.seq_num);
            }
            _ => {
                let result = self.sequencer.lock().await.check("inbound", msg.seq_num);
                match result {
                    sequencer::SequenceResult::Accepted => {}
                    sequencer::SequenceResult::DuplicateIgnored => {
                        log::debug!("Duplicate inbound message (type {}, MsgSeqNum {}) ignored", msg.msg_type, msg.seq_num);
                        return Ok(());
                    }
                    sequencer::SequenceResult::GapDetected { expected, received } => {
                        self.emit_alert(
                            alerts::AlertKind::SequenceGap,
                            alerts::sequence_gap_detail(expected, received, msg.msg_type),
                        )
                        .await;
                        warn!(
                            "⚠️  Inbound sequence gap detected (type {}): expected {}, got {} — requesting recovery",
                            msg.msg_type, expected, received
                        );
                        let symbol: &str = msg
                            .get_field(tags::SYMBOL)
                            .unwrap_or(self.sender_comp_id.as_str());
                        let gap_size = received.saturating_sub(expected);
                        if let Err(e) = self.kdb_writer
                            .write_capture_gap(symbol, expected, received)
                            .await
                        {
                            warn!("kdb write failed (capture_gap {symbol}): {e}");
                        }
                        #[cfg(test)]
                        self.kdb_writer.record_probe(kdb_integration::ProbeEvent::CaptureGap {
                            symbol: symbol.to_string(),
                            expected,
                            received,
                            gap_size,
                        });
                        return self.request_recovery(expected).await;
                    }
                    sequencer::SequenceResult::StaleDuringRecovery => {
                        log::debug!(
                            "Dropping message (type {}, MsgSeqNum {}) received while recovering from a sequence gap",
                            msg.msg_type, msg.seq_num
                        );
                        return Ok(());
                    }
                }
            }
        }

        match msg.msg_type {
            'A' => info!("Logon response received"),
            '0' => log::debug!("Heartbeat received"),
            // A TestRequest is the venue asking whether we are still alive.
            // FIX 4.4 requires a Heartbeat echoing TestReqID (112); without it
            // the venue concludes the session is dead and logs us out. This
            // arm was missing, which disconnected every session every 15-25s.
            '1' => {
                let test_req_id = msg.get_field(tags::TEST_REQ_ID).unwrap_or_default().to_string();
                info!("TestRequest received (112={}) - answering with Heartbeat", test_req_id);
                if let Err(e) = self.heartbeat_with_test_req_id(Some(&test_req_id)).await {
                    warn!("Failed to answer TestRequest {}: {}", test_req_id, e);
                }
            }
            '8' => {
                // Execution Report - measure latency from order submission
                if let Some(cl_ord_id_str) = msg.get_field(tags::CL_ORD_ID) {
                    let execution_report_time = Instant::now();
                    let mut order_times = self.order_times.lock().await;
                    let cl_ord_id_owned = cl_ord_id_str.to_string();

                    let execution_report_us = if let Some(submission_time) = order_times.remove(&cl_ord_id_owned) {
                        let latency_us = execution_report_time.duration_since(submission_time).as_micros() as u64;
                        let latency_ms = latency_us as f64 / 1000.0;
                        info!("📊 Order-to-ACK Latency for {}: {} µs ({:.2} ms)",
                              cl_ord_id_owned, latency_us, latency_ms);
                        Some(latency_us)
                    } else {
                        None
                    };
                    drop(order_times);
                    
                    // Complete timing and write processed JSON
                    let mut timings = self.order_timings.lock().await;
                    let mut order_data_map = self.order_data.lock().await;
                    
                    if let (Some(mut timing), Some(order)) = (
                        timings.remove(&cl_ord_id_owned),
                        order_data_map.remove(&cl_ord_id_owned)
                    ) {
                        timing.execution_report_us = execution_report_us;
                        timing.execution_report_timestamp = Some(chrono::Utc::now().timestamp_millis());
                        
                        // Calculate total time (from file read to execution report)
                        if let Some(file_read_ts) = timing.file_read_timestamp {
                            if let Some(exec_ts) = timing.execution_report_timestamp {
                                timing.total_us = Some((exec_ts - file_read_ts) as u64 * 1000); // Convert ms to µs
                            }
                        }
                        
                        // Create processed JSON with execution report at the top
                        let processed_data = serde_json::json!({
                            "execution_report": {
                                "order_id": msg.get_field(tags::ORDER_ID),
                                "exec_id": msg.get_field(tags::EXEC_ID),
                                "cl_ord_id": msg.get_field(tags::CL_ORD_ID),
                                "orig_cl_ord_id": msg.get_field(41),
                                "symbol": msg.get_field(tags::SYMBOL),
                                "side": msg.get_field(tags::SIDE),
                                "order_qty": msg.get_field(tags::ORDER_QTY),
                                "price": msg.get_field(tags::PRICE),
                                "exec_type": msg.get_field(150), // ExecType
                                "ord_status": msg.get_field(tags::EXEC_STATUS), // OrdStatus (39)
                                "cum_qty": msg.get_field(14),
                                "leaves_qty": msg.get_field(tags::LEAVES_QTY),
                                "last_px": msg.get_field(tags::LAST_PX),
                                "last_qty": msg.get_field(tags::LAST_QTY),
                                "avg_px": msg.get_field(6),
                                "commission": msg.get_field(12),
                                "commission_currency": msg.get_field(2643),
                                "comm_type": msg.get_field(13),
                                "exec_inst": msg.get_field(18),
                                "exec_ref_id": msg.get_field(19),
                                "working_indicator": msg.get_field(636),
                                "transact_time": msg.get_field(tags::TRANSACT_TIME),
                                "order_create_time": msg.get_field(6616),
                                "last_liquidity_ind": msg.get_field(851),
                                "cash_margin": msg.get_field(544),
                                "trd_match_id": msg.get_field(880),
                                "trade_transact_time": msg.get_field(10060),
                                "match_count": msg.get_field(20101),
                                "match_index": msg.get_field(20102),
                                "rejection_reason_code": msg.get_field(103), // OrdRejReason
                                "rejection_reason_text": msg.get_field(tags::TEXT)
                            },
                            "order": {
                                "cl_ord_id": order.cl_ord_id,
                                "symbol": order.symbol,
                                "side": order.side,
                                "qty": order.qty,
                                "price": order.price
                            },
                            "timing": {
                                "file_read_us": timing.file_read_us,
                                "json_parse_us": timing.json_parse_us,
                                "channel_send_us": timing.channel_send_us,
                                "fix_encode_us": timing.fix_encode_us,
                                "fix_send_us": timing.fix_send_us,
                                "execution_report_us": timing.execution_report_us,
                                "total_us": timing.total_us,
                                "file_read_timestamp": timing.file_read_timestamp,
                                "execution_report_timestamp": timing.execution_report_timestamp
                            }
                        });
                        
                        // Write processed JSON file
                        let processed_file = format!("orders/processed/{}.json", cl_ord_id_owned);
                        if let Ok(json_str) = serde_json::to_string_pretty(&processed_data) {
                            if let Err(e) = tokio::fs::write(&processed_file, json_str).await {
                                warn!("Failed to write processed order file {}: {}", processed_file, e);
                            } else {
                                info!("💾 Processed order saved: {} (Total: {}µs)", processed_file, 
                                      timing.total_us.unwrap_or(0));
                            }
                        }
                    }
                    drop(timings);
                    drop(order_data_map);

                    // Update open orders store with execution report data
                    let open_order = OpenOrder {
                        cl_ord_id: msg.get_field(tags::CL_ORD_ID).unwrap_or_default().to_string(),
                        order_id: msg.get_field(tags::ORDER_ID).unwrap_or_default().to_string(),
                        exec_id: msg.get_field(tags::EXEC_ID).unwrap_or_default().to_string(),
                        symbol: msg.get_field(tags::SYMBOL).unwrap_or_default().to_string(),
                        side: msg.get_field(tags::SIDE).unwrap_or_default().to_string(),
                        order_qty: msg.get_field(tags::ORDER_QTY).unwrap_or_default().to_string(),
                        price: msg.get_field(tags::PRICE).unwrap_or_default().to_string(),
                        ord_status: msg.get_field(tags::EXEC_STATUS).unwrap_or_default().to_string(),
                        exec_type: msg.get_field(150).unwrap_or_default().to_string(), // ExecType
                        cum_qty: msg.get_field(14).unwrap_or_default().to_string(),
                        leaves_qty: msg.get_field(tags::LEAVES_QTY).unwrap_or_default().to_string(),
                        last_px: msg.get_field(tags::LAST_PX).unwrap_or_default().to_string(),
                        last_qty: msg.get_field(tags::LAST_QTY).unwrap_or_default().to_string(),
                        avg_px: msg.get_field(6).unwrap_or_default().to_string(),
                        transact_time: msg.get_field(tags::TRANSACT_TIME).unwrap_or_default().to_string(),
                        order_create_time: msg.get_field(6616).unwrap_or_default().to_string(),
                        rejection_reason: msg.get_field(tags::TEXT)
                            .or_else(|| msg.get_field(103)) // OrdRejReason
                            .unwrap_or_default()
                            .to_string(),
                    };
                    
                    // Update the store (lock-free, uses ArcSwap)
                    self.open_orders.update_order(cl_ord_id_owned.clone(), open_order);
                    
                    // If order is closed (rejected, cancelled, or fully filled), remove it
                    let leaves_qty: f64 = msg.get_field(tags::LEAVES_QTY)
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0);
                    let ord_status = msg.get_field(tags::EXEC_STATUS).unwrap_or_default();
                    let exec_type = msg.get_field(150).unwrap_or_default();

                    if leaves_qty == 0.0 || ord_status == "8" || ord_status == "4" || ord_status == "2" || exec_type == "8" {
                        self.open_orders.remove_order(&cl_ord_id_owned);
                    }

                    // Typed event mapping (AC3): ExecType (150) is authoritative, with
                    // OrdStatus (39) — distinct from the EXEC_STATUS(150) alias used
                    // above for legacy display — as a fallback for venues that only
                    // populate one of the two consistently.
                    let ord_status_39 = msg.get_field(tags::ORD_STATUS).unwrap_or_default();
                    let typed_event = order_events::map_exec_type(exec_type, ord_status_39);
                    info!("📬 Typed order event for {}: {:?}", cl_ord_id_owned, typed_event);
                    // On every fill (partial or full), persist to kdb+ own_trades and update positions.
                    // ExecType "F" = partial fill, "2" = full fill, "1" = partial fill (some venues).
                    if exec_type == "F" || exec_type == "2" || exec_type == "1" {
                        let fill_symbol = msg.get_field(tags::SYMBOL).unwrap_or_default();
                        let fill_side   = msg.get_field(tags::SIDE).unwrap_or_default();
                        let last_px: f64 = msg.get_field(tags::LAST_PX)
                            .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        let last_qty: f64 = msg.get_field(tags::LAST_QTY)
                            .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        let avg_px: f64 = msg.get_field(6)
                            .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        let fill_order_id = msg.get_field(tags::ORDER_ID).unwrap_or_default();
                        let exec_type_char = exec_type.chars().next().unwrap_or('?');
                        let side_char = fill_side.chars().next().unwrap_or('?');

                        // Write fill to kdb+ own_trades table
                        if let Err(e) = self.kdb_writer.write_own_trade(
                            fill_symbol,
                            side_char,
                            last_px,
                            last_qty,
                            avg_px,
                            &cl_ord_id_owned,
                            fill_order_id,
                            exec_type_char,
                        ).await
                        {
                            warn!("kdb write failed (own_trade {fill_symbol}): {e}");
                        }

                        // Durably persist this fill to data/trades.csv immediately.
                        // This file survives KDB+ and server restarts.
                        append_trade_to_csv(
                            &cl_ord_id_owned,
                            fill_order_id,
                            fill_symbol,
                            side_char,
                            exec_type_char,
                            last_qty,
                            last_px,
                            avg_px,
                        );

                        // Update in-memory positions store
                        if last_qty > 0.0 && last_px > 0.0 {
                            self.positions.apply_fill(fill_symbol, fill_side, last_qty, last_px);
                            info!("📈 Position updated: symbol={} side={} qty={} px={}", fill_symbol, fill_side, last_qty, last_px);
                            self.evaluate_session_drawdown().await;
                        }
                    }

                    // Signal CSV update (event-driven, immediate)
                    if let Some(tx) = self.order_update_tx.lock().await.as_ref() {
                        let _ = tx.send(());
                    }
                    
                    info!("");
                    info!("═══════════════════════════════════════════════════════════");
                    info!("✅ EXECUTION REPORT RECEIVED");
                    info!("═══════════════════════════════════════════════════════════");
                    info!("Client Order ID: {}", cl_ord_id_owned);

                    if let Some(order_id) = msg.get_field(tags::ORDER_ID) {
                        info!("Order ID: {}", order_id);
                    }
                    if let Some(symbol) = msg.get_field(tags::SYMBOL) {
                        info!("Symbol: {}", symbol);
                    }
                    if let Some(side) = msg.get_field(tags::SIDE) {
                        let side_name = match side {
                            "1" => "BUY",
                            "2" => "SELL",
                            _ => side,
                        };
                        info!("Side: {}", side_name);
                    }
                    if let Some(qty) = msg.get_field(tags::ORDER_QTY) {
                        info!("Quantity: {}", qty);
                    }
                    if let Some(price) = msg.get_field(tags::PRICE) {
                        info!("Price: {}", price);
                    }
                    if let Some(exec_status) = msg.get_field(tags::EXEC_STATUS) {
                        let status_name = match exec_status {
                            "0" => "New",
                            "1" => "Partially Filled",
                            "2" => "Filled",
                            "4" => "Canceled",
                            "8" => "Rejected",
                            _ => exec_status,
                        };
                        info!("Status: {}", status_name);
                        
                        // If rejected, show rejection reason
                        if exec_status == "8" {
                            if let Some(reject_text) = msg.get_field(tags::TEXT) {
                                warn!("❌ Rejection Reason (58/TEXT): {}", reject_text);
                            }
                            if let Some(reject_code) = msg.get_field(103) {
                                let code_name = match reject_code {
                                    "0" => "Broker/Exchange Option",
                                    "1" => "Unknown Symbol",
                                    "2" => "Exchange Closed",
                                    "3" => "Order Exceeds Limit",
                                    "4" => "Too Late To Enter",
                                    "5" => "Unknown Order",
                                    "6" => "Duplicate Order",
                                    "7" => "Duplicate Of A Verbally Communicated Order",
                                    "8" => "Stale Order",
                                    "9" => "Trade Along Required",
                                    "10" => "Invalid Investor ID",
                                    "11" => "Unsupported Order Characteristic",
                                    "12" => "Surveillance Option",
                                    "13" => "Incorrect Quantity",
                                    "14" => "Incorrect Allocated Quantity",
                                    "15" => "Unknown Account(s)",
                                    "99" => "Other",
                                    _ => "Unknown Code",
                                };
                                warn!("❌ Rejection Code (103/OrdRejReason): {} ({})", reject_code, code_name);
                            }
                        }
                    }
                    if let Some(last_qty) = msg.get_field(tags::LAST_QTY) {
                        info!("Executed Quantity: {}", last_qty);
                    }
                    if let Some(leaves_qty) = msg.get_field(tags::LEAVES_QTY) {
                        info!("Remaining Quantity: {}", leaves_qty);
                    }
                    info!("═══════════════════════════════════════════════════════════");
                    info!("");
                }
            }
            'j' => {
                // Business Message Reject (35=j)
                info!("");
                info!("═══════════════════════════════════════════════════════════");
                warn!("❌ BUSINESS MESSAGE REJECT (35=j)");
                info!("═══════════════════════════════════════════════════════════");

                // Log key rejection details for MarketDataRequest debugging
                if let Some(ref_msg_type) = msg.get_field(372) {
                    info!("Rejected Message Type (372): {} ({})", ref_msg_type, 
                          if ref_msg_type == "V" { "MarketDataRequest" } else { "Other" });
                }
                if let Some(ref_seq_num) = msg.get_field(45) {
                    info!("Rejected Message SeqNum (45): {}", ref_seq_num);
                }
                if let Some(business_ref_id) = msg.get_field(379) {
                    info!("BusinessRejectRefID (379): {} (may indicate problematic field/tag)", business_ref_id);
                }
                if let Some(reject_reason) = msg.get_field(380) {
                    let reason_text = match reject_reason {
                        "0" => "Other",
                        "1" => "Unknown ID",
                        "2" => "Unknown Security",
                        "3" => "Unsupported Message Type",
                        "4" => "Application not available",
                        "5" => "Conditionally Required Field Missing",
                        "6" => "Not authorized",
                        "7" => "DeliverTo firm not available",
                        _ => "Unknown",
                    };
                    info!("BusinessRejectReason (380): {} ({})", reject_reason, reason_text);
                }

                if let Some(cl_ord_id_str) = msg.get_field(tags::CL_ORD_ID) {
                    warn!("Client Order ID: {}", cl_ord_id_str);
                    let mut order_times = self.order_times.lock().await;
                    if let Some(submission_time) = order_times.remove(cl_ord_id_str) {
                        let ack_time = Instant::now();
                        let latency_us = ack_time.duration_since(submission_time).as_micros();
                        let latency_ms = latency_us as f64 / 1000.0;
                        info!("Time to Rejection: {} µs ({:.2} ms)", latency_us, latency_ms);
                    }
                }

                // Tag 58 is TEXT field for rejection reason
                if let Some(text) = msg.get_field(tags::TEXT) {
                    warn!("Rejection Reason (58): {}", text);
                }

                // Print all fields for debugging
                info!("");
                info!("Full Message Fields:");
                for (tag, value) in &msg.fields {
                    if value.len() > 100 {
                        info!("  Tag {}: {}...", tag, &value[..100]);
                    } else {
                        info!("  Tag {}: {}", tag, value);
                    }
                }

                info!("═══════════════════════════════════════════════════════════");
                info!("");
            }
            'D' => info!("Logout received"),
            '3' => {
                if let Some(text) = msg.get_field(tags::TEXT) {
                    warn!("Message rejected: {}", text);
                }
            }
            'H' => {
                info!("Order Status Request Response (35=H) received");
            }
            'y' => {
                // Security List Response (35=y)
                info!("");
                info!("═══════════════════════════════════════════════════════════");
                info!("📋 SECURITY LIST RESPONSE RECEIVED");
                info!("═══════════════════════════════════════════════════════════");

                let mut last_frag = false;
                let mut expected_count: Option<usize> = None;

                if let Some(req_id) = msg.get_field(320) {
                    info!("SecurityReqID (320): {}", req_id);
                }
                if let Some(resp_id) = msg.get_field(322) {
                    info!("SecurityResponseID (322): {}", resp_id);
                }
                if let Some(result) = msg.get_field(560) {
                    let result_text = match result {
                        "0" => "Valid",
                        "2" => "Not found",
                        "3" => "Not authorised",
                        "4" => "Unavailable",
                        _ => result,
                    };
                    info!("SecurityRequestResult (560): {} ({})", result, result_text);
                }
                if let Some(frag) = msg.get_field(893) {
                    last_frag = frag == "Y";
                    info!("LastFragment (893): {} ({})", frag, if last_frag { "Last message" } else { "More messages coming" });
                }
                if let Some(num_symbols) = msg.get_field(146) {
                    if let Ok(count) = num_symbols.parse::<usize>() {
                        expected_count = Some(count);
                        info!("NoRelatedSym (146): {} securities total in list", count);
                        *self.expected_security_count.lock().await = Some(count);
                    }
                }

                // Extract repeating group instances (tag 146 = NoRelatedSym, tag 55 = Symbol)
                let repeating_groups = msg.get_repeating_group(146, 55);
                let has_repeating_groups = !repeating_groups.is_empty();
                let repeating_groups_count = repeating_groups.len();

                if has_repeating_groups {
                    info!("📊 Extracting {} securities from repeating groups", repeating_groups_count);

                    let mut securities = self.securities.lock().await;
                    for group in repeating_groups {
                        if let Some(symbol_val) = group.get(&55).map(|s| s.as_str()) {
                            let mut security = SecurityInfo {
                                symbol: symbol_val.to_string(),
                                security_id: group.get(&48).map(|s| s.as_str()).map(|s| s.to_string()),
                                exchange: group.get(&207).map(|s| s.as_str()).map(|s| s.to_string()),
                                attributes: HashMap::new(),
                            };

                            // Collect instrument attributes
                            for (tag, value) in &group {
                                if *tag >= 871 && *tag <= 872 {
                                    security.attributes.insert(tag.to_string(), value.clone());
                                }
                            }

                            if !securities.iter().any(|s| s.symbol == symbol_val) {
                                securities.push(security.clone());
                                info!("  ✅ {}", symbol_val);
                            }
                        }
                    }
                    info!("📊 Total securities loaded: {}", securities.len());
                } else {
                    // Fallback to old logic for non-repeating group format
                    if let Some(symbol) = msg.get_field(55) {
                        let mut security = SecurityInfo {
                            symbol: symbol.to_string(),
                            security_id: msg.get_field(48).map(|s| s.to_string()),
                            exchange: msg.get_field(207).map(|s| s.to_string()),
                            attributes: HashMap::new(),
                        };

                        // Collect instrument attributes
                        for (tag, value) in &msg.fields {
                            if *tag >= 871 && *tag <= 872 {
                                security.attributes.insert(tag.to_string(), value.clone());
                            }
                        }

                        let mut securities = self.securities.lock().await;
                        if !securities.iter().any(|s| s.symbol == symbol) {
                            securities.push(security.clone());
                            info!("✅ Saved security: {} (Total: {})", symbol, securities.len());
                        }
                    } else {
                        info!("ℹ️ No securities found in SecurityListResponse");
                    }
                }

                // Always save on each SecurityListResponse with repeating groups (to accumulate all securities)
                if has_repeating_groups {
                    let securities = self.securities.lock().await;
                    let current_count = securities.len();
                    drop(securities);

                    info!("💾 Saving {} securities to database (background thread)...", current_count);
                    if let Err(e) = self.save_securities_to_db().await {
                        error!("Failed to save securities to database: {}", e);
                    }

                    // Also save file on first response only
                    let should_save_file = {
                        let saved = self.securities_file_saved.lock().await;
                        !*saved
                    };

                    if should_save_file {
                        *self.securities_file_saved.lock().await = true;
                        if let Err(e) = self.save_securities_to_file().await {
                            error!("Failed to save securities to file: {}", e);
                        }
                    }
                }

                info!("");
                info!("Sample fields from response:");
                for (tag, value) in &msg.fields {
                    if *tag >= 55 && *tag <= 900 && value.len() <= 50 {
                        info!("  Tag {}: {}", tag, value);
                    }
                }
                info!("═══════════════════════════════════════════════════════════");
                info!("");
            }
            'd' => {
                // Security Definition (35=d)
                info!("");
                info!("📄 SECURITY DEFINITION RECEIVED");

                if let Some(symbol) = msg.get_field(55) {
                    info!("Symbol (55): {}", symbol);
                }
                if let Some(security_id) = msg.get_field(48) {
                    info!("SecurityID (48): {}", security_id);
                }
                if let Some(exchange) = msg.get_field(207) {
                    info!("SecurityExchange (207): {}", exchange);
                }

                // Log key attribute tags if present
                if let Some(num_attribs) = msg.get_field(870) {
                    info!("NoInstrAttrib (870): {} attributes", num_attribs);
                }

                info!("");
            }
            'W' => {
                // Market Data Snapshot Full Refresh (35=W)
                self.last_md_recv_ms.store(
                    chrono::Utc::now().timestamp_millis() as u64,
                    Ordering::Relaxed,
                );
                info!("");
                info!("📈 MARKET DATA SNAPSHOT RECEIVED (35=W)");

                if let Some(md_req_id) = msg.get_field(tags::MARKET_DATA_REQ_ID) {
                    info!("MarketDataReqID (262): {}", md_req_id);
                }
                if let Some(symbol) = msg.get_field(tags::SYMBOL) {
                    info!("Symbol (55): {}", symbol);
                }

                // Extract repeating group for market data entries
                // For Market Data messages, the group can start with either 279 (MDUpdateAction) or 269 (MDEntryType)
                // Try 279 first (for incremental refresh), then fall back to 269 (for snapshot)
                let expected_count = msg.get_field(268).and_then(|s| s.parse::<usize>().ok()).unwrap_or(0);
                let repeating_groups = if msg.fields_ordered.iter().any(|(tag, _)| *tag == 279 && *tag > 268) {
                    // Use 279 as group start tag (MDUpdateAction comes first in incremental refresh)
                    msg.get_repeating_group(268, 279)
                } else {
                    // Use 269 as group start tag (standard for snapshot)
                    msg.get_repeating_group(268, 269)
                };
                info!("NoMDEntries (268): {} entries (extracted: {})", expected_count, repeating_groups.len());
                
                // Debug: Show all fields after 268 to understand structure
                info!("Debug: Fields after 268 in message (first 20):");
                let mut found_268 = false;
                let mut count_after_268 = 0;
                for (tag, value) in &msg.fields_ordered {
                    if *tag == 268 {
                        found_268 = true;
                        continue;
                    }
                    if found_268 {
                        count_after_268 += 1;
                        if count_after_268 <= 20 { // Show first 20 fields
                            info!("  Tag {}: {}", tag, value);
                        } else {
                            break;
                        }
                    }
                }

                if !repeating_groups.is_empty() {
                    // Get symbol for consistency
                    let symbol = msg.get_field(tags::SYMBOL).map(|s| s.to_string()).unwrap_or_else(|| "UNKNOWN".to_string());
                    let timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

                    // Mutate the book under `order_books`, then drop the guard
                    // before any kdb / tick / CSV I/O (those `.await`s used to
                    // pin this mutex for the entire snapshot, hanging /api/market-data-status).
                    let (pending, combined_tick, best_bid, best_ask, best_bid_size, best_ask_size) = {
                        let mut pending: Vec<PendingMdIo> =
                            Vec::with_capacity(repeating_groups.len() + 1);
                        let mut combined_tick = TickData {
                            timestamp,
                            symbol: symbol.clone(),
                            bid: 0.0,
                            ask: 0.0,
                            bid_size: 0.0,
                            ask_size: 0.0,
                            last_price: 0.0,
                            last_size: 0.0,
                        };

                        let mut books = self.order_books.lock().await;
                        let book = books.entry(symbol.clone())
                            .or_insert_with(|| DoubleBufferedOrderBook::new(symbol.clone(), 100_000)); // $1k dense range (reduced for memory)

                        // For snapshot, clear the active book first (full refresh)
                        book.get_active_mut().clear();

                        {
                            let active_book = book.get_active_mut();
                            for (idx, entry) in repeating_groups.iter().enumerate() {
                                if let Some(entry_type) = entry.get(&269) {
                                    // 0 = Bid, 1 = Ask, 2 = Trade, 4 = Opening Price, etc.
                                    let entry_type_name = match entry_type.as_str() {
                                        "0" => "Bid",
                                        "1" => "Ask",
                                        "2" => "Trade",
                                        "4" => "Opening",
                                        _ => "Other",
                                    };
                                    info!("  [{}] {} (type: {})", idx, entry_type_name, entry_type);

                                    // Extract price (tag 270) and size (tag 271)
                                    if let Some(price_str) = entry.get(&270) {
                                        if let Ok(price) = price_str.parse::<f64>() {
                                            let size = entry.get(&271).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                                            info!("      Price: {}, Size: {}", price, size);

                                            // Update active order book (snapshot = New action)
                                            match entry_type.as_str() {
                                                "0" => {
                                                    // Bid update
                                                    active_book.update_level(Side::Bid, price, size, UpdateAction::New);
                                                    combined_tick.bid = price;
                                                    combined_tick.bid_size = size;
                                                    pending.push(PendingMdIo::Level { side: 'B', price, size, action: 'N', seq_num: Some(msg.seq_num) });
                                                }
                                                "1" => {
                                                    // Ask update
                                                    active_book.update_level(Side::Ask, price, size, UpdateAction::New);
                                                    combined_tick.ask = price;
                                                    combined_tick.ask_size = size;
                                                    pending.push(PendingMdIo::Level { side: 'A', price, size, action: 'N', seq_num: Some(msg.seq_num) });
                                                }
                                                "2" => {
                                                    // Trade/Last price - add to order book trade history
                                                    active_book.add_trade(price, size, timestamp);
                                                    combined_tick.last_price = price;
                                                    combined_tick.last_size = size;
                                                    pending.push(PendingMdIo::Trade { price, size, timestamp, seq_num: Some(msg.seq_num) });
                                                }
                                                _ => {
                                                    // Unknown entry type - skip
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        let (best_bid, best_ask, spread, best_bid_size, best_ask_size) = {
                            let a = book.get_active();
                            (a.best_bid(), a.best_ask(), a.spread(), a.best_bid_size(), a.best_ask_size())
                        };
                        info!("  📊 Order Book: Bid={:.2}, Ask={:.2}, Spread={:.2}", best_bid, best_ask, spread);

                        // CRITICAL: Swap snapshot after snapshot processing to make updates visible
                        book.swap_snapshot();
                        pending.push(PendingMdIo::Snapshot {
                            best_bid, best_ask, best_bid_size, best_ask_size, spread,
                            seq_num: Some(msg.seq_num),
                        });
                        (pending, combined_tick, best_bid, best_ask, best_bid_size, best_ask_size)
                    }; // order_books MutexGuard dropped here — no kdb/tick await above

                    self.replay_pending_md_io(&symbol, pending).await;

                    // Only add tick if we have at least bid or ask data
                    if combined_tick.bid > 0.0 || combined_tick.ask > 0.0 || combined_tick.last_price > 0.0 {
                        info!("  💾 Saving tick: Bid={}, Ask={}, BidSize={}, AskSize={}", 
                              combined_tick.bid, combined_tick.ask, combined_tick.bid_size, combined_tick.ask_size);
                        self.add_tick(combined_tick).await;
                    }
                    append_snapshot_to_csv_with_sizes(&symbol, best_bid, best_ask, best_bid_size, best_ask_size);
                }

                info!("═══════════════════════════════════════════════════════════");
                info!("");
            }
            'X' => {
                // Market Data Incremental Refresh (35=X) - Real-time updates
                self.last_md_recv_ms.store(
                    chrono::Utc::now().timestamp_millis() as u64,
                    Ordering::Relaxed,
                );
                info!("");
                info!("📈 MARKET DATA INCREMENTAL REFRESH RECEIVED (35=X)");

                let md_req_id = msg.get_field(tags::MARKET_DATA_REQ_ID);
                if let Some(ref md_req_id) = md_req_id {
                    info!("MarketDataReqID (262): {}", md_req_id);
                    if md_req_id.starts_with("trade.") {
                        info!("💹 TRADE DATA MESSAGE RECEIVED for {}", md_req_id);
                    }
                }
                if let Some(symbol) = msg.get_field(tags::SYMBOL) {
                    info!("Symbol (55): {}", symbol);
                }

                // Extract repeating group for market data entries
                // For incremental refresh, group starts with 279 (MDUpdateAction)
                let expected_count = msg.get_field(268).and_then(|s| s.parse::<usize>().ok()).unwrap_or(0);
                let repeating_groups = msg.get_repeating_group(268, 279);
                info!("NoMDEntries (268): {} entries (extracted: {})", expected_count, repeating_groups.len());
                if !repeating_groups.is_empty() {
                    // Get symbol - try from message, or extract from MDReqID if it's trade.BTC_USDT format
                    let symbol = if let Some(s) = msg.get_field(tags::SYMBOL) {
                        s.to_string()
                    } else if let Some(ref req_id) = md_req_id {
                        // Extract symbol from trade.BTC_USDT or book.BTC_USDT.50 format
                        if req_id.starts_with("trade.") {
                            req_id.strip_prefix("trade.").unwrap_or("UNKNOWN").to_string()
                        } else if req_id.starts_with("book.") {
                            req_id.strip_prefix("book.")
                                .and_then(|s| s.split('.').next())
                                .unwrap_or("UNKNOWN")
                                .to_string()
                        } else {
                            "UNKNOWN".to_string()
                        }
                    } else {
                        "UNKNOWN".to_string()
                    };
                    let timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

                    // Mutate the book under `order_books`, then drop the guard
                    // before kdb / ticks / CSV / ZMQ. Inner `continue` cannot skip
                    // the drop because the guard's lifetime is this block.
                    let (pending, ticks, best_bid, best_ask, best_bid_size, best_ask_size, zmq_symbol) = {
                        let mut pending: Vec<PendingMdIo> =
                            Vec::with_capacity(repeating_groups.len() + 1);
                        let mut ticks: Vec<TickData> = Vec::with_capacity(repeating_groups.len());

                        let mut books = self.order_books.lock().await;
                        let book = books.entry(symbol.clone())
                            .or_insert_with(|| DoubleBufferedOrderBook::new(symbol.clone(), 100_000)); // $1k dense range (reduced for memory)

                        {
                            let active_book = book.get_active_mut();
                            for (idx, entry) in repeating_groups.iter().enumerate() {
                                if let Some(entry_type) = entry.get(&269) {
                                    // Check for special case: 'J' = Empty Book (clear entire order book)
                                    if entry_type == "J" {
                                        warn!("⚠️  EMPTY BOOK (269=J) - Clearing entire order book for {}", symbol);
                                        active_book.clear();
                                        continue;
                                    }

                                    // 0 = Bid, 1 = Ask, 2 = Trade, 4 = Opening Price, etc.
                                    let entry_type_name = match entry_type.as_str() {
                                        "0" => "Bid",
                                        "1" => "Ask",
                                        "2" => "Trade",
                                        "4" => "Opening",
                                        _ => "Other",
                                    };

                                    // Check MDUpdateAction (tag 279) - 0=New, 1=Change, 2=Delete
                                    let update_action_str = entry.get(&279).map(|s| s.as_str()).unwrap_or("0");
                                    let update_action = UpdateAction::from(update_action_str);
                                    let action_name = match update_action {
                                        UpdateAction::New => "New",
                                        UpdateAction::Change => "Change",
                                        UpdateAction::Delete => "Delete",
                                    };

                                    info!("  [{}] {} {} (type: {})", idx, action_name, entry_type_name, entry_type);

                                    // Extract price (tag 270) and size (tag 271)
                                    if let Some(price_str) = entry.get(&270) {
                                        if let Ok(price) = price_str.parse::<f64>() {
                                            let size = entry.get(&271).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                                            info!("      Price: {}, Size: {}", price, size);

                                            let action_char = match update_action {
                                                UpdateAction::New => 'N',
                                                UpdateAction::Change => 'C',
                                                UpdateAction::Delete => 'D',
                                            };
                                            match entry_type.as_str() {
                                                "0" => {
                                                    // Bid update
                                                    info!("📝 Processing Bid update: {} {} {} {}", symbol, price, size, action_char);
                                                    active_book.update_level(Side::Bid, price, size, update_action);
                                                    info!("📝 Calling write_order_book_level for Bid");
                                                    pending.push(PendingMdIo::Level { side: 'B', price, size, action: action_char, seq_num: Some(msg.seq_num) });
                                                }
                                                "1" => {
                                                    // Ask update
                                                    info!("📝 Processing Ask update: {} {} {} {}", symbol, price, size, action_char);
                                                    active_book.update_level(Side::Ask, price, size, update_action);
                                                    info!("📝 Calling write_order_book_level for Ask");
                                                    pending.push(PendingMdIo::Level { side: 'A', price, size, action: action_char, seq_num: Some(msg.seq_num) });
                                                }
                                                "2" => {
                                                    // Trade/Last price - add to order book trade history
                                                    info!("💹 Adding trade to order book: Price={}, Size={}, Symbol={}", price, size, symbol);
                                                    active_book.add_trade(price, size, timestamp);
                                                    info!("💹 Trade added! Total trades in book: {}", active_book.trade_count());
                                                    info!("📝 Calling write_trade");
                                                    pending.push(PendingMdIo::Trade { price, size, timestamp, seq_num: Some(msg.seq_num) });
                                                }
                                                _ => {
                                                    // Unknown entry type - skip
                                                    continue;
                                                }
                                            }

                                            // Create tick data for CSV export
                                            let tick = match entry_type.as_str() {
                                                "0" => {
                                                    // Bid update
                                                    TickData {
                                                        timestamp,
                                                        symbol: symbol.clone(),
                                                        bid: price,
                                                        ask: 0.0,
                                                        bid_size: size,
                                                        ask_size: 0.0,
                                                        last_price: 0.0,
                                                        last_size: 0.0,
                                                    }
                                                }
                                                "1" => {
                                                    // Ask update
                                                    TickData {
                                                        timestamp,
                                                        symbol: symbol.clone(),
                                                        bid: 0.0,
                                                        ask: price,
                                                        bid_size: 0.0,
                                                        ask_size: size,
                                                        last_price: 0.0,
                                                        last_size: 0.0,
                                                    }
                                                }
                                                "2" => {
                                                    // Trade/Last price
                                                    TickData {
                                                        timestamp,
                                                        symbol: symbol.clone(),
                                                        bid: 0.0,
                                                        ask: 0.0,
                                                        bid_size: 0.0,
                                                        ask_size: 0.0,
                                                        last_price: price,
                                                        last_size: size,
                                                    }
                                                }
                                                _ => {
                                                    // Unknown entry type - skip
                                                    continue;
                                                }
                                            };
                                            ticks.push(tick);
                                        }
                                    }
                                }
                            }
                        }

                        let (best_bid, best_ask, spread, best_bid_size, best_ask_size) = {
                            let a = book.get_active();
                            (a.best_bid(), a.best_ask(), a.spread(), a.best_bid_size(), a.best_ask_size())
                        };

                        // CRITICAL: Swap snapshot after incremental update to make updates visible
                        book.swap_snapshot();

                        info!("  📊 Order Book: Bid={:.2}, Ask={:.2}, Spread={:.2}", best_bid, best_ask, spread);

                        pending.push(PendingMdIo::Snapshot {
                            best_bid, best_ask, best_bid_size, best_ask_size, spread,
                            seq_num: Some(msg.seq_num),
                        });
                        (pending, ticks, best_bid, best_ask, best_bid_size, best_ask_size, symbol.clone())
                    }; // order_books MutexGuard dropped here — no kdb/tick/ZMQ await above

                    self.replay_pending_md_io(&symbol, pending).await;
                    for t in ticks {
                        self.add_tick(t).await;
                    }
                    append_snapshot_to_csv_with_sizes(&symbol, best_bid, best_ask, best_bid_size, best_ask_size);

                    // Signal ZeroMQ update ONCE per incremental refresh (after all entries processed)
                    if let Some(tx) = self.order_book_update_tx.lock().await.as_ref() {
                        let _ = tx.send(zmq_symbol);
                    }
                } else {
                    info!("⚠️  No market data entries found in message");
                }

                info!("═══════════════════════════════════════════════════════════");
                info!("");
            }
            'j' | 'J' => {
                // Business message or other response
                info!("Message type {} received - likely order status response", msg.msg_type);
                // Print all fields for debugging
                info!("Message Fields:");
                for (tag, value) in &msg.fields {
                    if value.len() > 100 {
                        info!("  Tag {}: {}...", tag, &value[..100]);
                    } else {
                        info!("  Tag {}: {}", tag, value);
                    }
                }
            }
            _ => log::debug!("Unknown message type: {} - fields: {:?}", msg.msg_type, msg.fields),
        }

        Ok(())
    }

    /// Recover from a detected inbound sequence gap (AC2/AC4): ask the venue
    /// to fill the session-level gap, and independently re-subscribe every
    /// symbol we currently track so market data recovers via a fresh
    /// snapshot even if the venue's ResendRequest handling only covers
    /// session-admin messages. Both requests are safe to send unconditionally;
    /// on an order-management session `order_books` is simply empty.
    async fn request_recovery(&self, begin_seq: u32) -> Result<()> {
        let seq_num = self.next_seq_num().await;
        let mut resend = FIXMessage::new('2', &self.sender_comp_id, &self.target_comp_id, seq_num);
        resend.set_field(tags::BEGIN_SEQ_NO, &begin_seq.to_string());
        resend.set_field(tags::END_SEQ_NO, "0"); // 0 = through the most recent message
        self.send_message(&resend).await?;
        info!("📨 ResendRequest (35=2) sent: BeginSeqNo={}", begin_seq);

        let symbols: Vec<String> = self.order_books.lock().await.keys().cloned().collect();
        for symbol in symbols {
            if let Err(e) = self.market_data_request(&symbol, 50).await {
                warn!("Failed to re-subscribe {} during gap recovery: {}", symbol, e);
            }
        }

        Ok(())
    }

    // Save securities to file in a background thread (non-blocking)
    async fn save_securities_to_file(&self) -> Result<()> {
        let securities = self.securities.lock().await;

        // Clone the securities data to move into the background thread
        let securities_data = securities.clone();
        let security_count = securities_data.len();

        // Spawn a standard thread to write to file without blocking the async runtime
        std::thread::spawn(move || {
            match (|| -> Result<()> {
                let filename = "securities.txt";
                let mut file = File::create(filename)?;

                writeln!(file, "╔════════════════════════════════════════════════════════════════════════════════╗")?;
                writeln!(file, "║                    CRYPTO.COM MARKET DATA SECURITIES LIST                     ║")?;
                writeln!(file, "║                           Generated by FIX Server                             ║")?;
                writeln!(file, "╚════════════════════════════════════════════════════════════════════════════════╝")?;
                writeln!(file)?;
                writeln!(file, "Total Securities: {}\n", security_count)?;
                writeln!(file, "─────────────────────────────────────────────────────────────────────────────────\n")?;

                for (idx, security) in securities_data.iter().enumerate() {
                    writeln!(file, "{}. Symbol: {}", idx + 1, security.symbol)?;

                    if let Some(id) = &security.security_id {
                        writeln!(file, "   SecurityID: {}", id)?;
                    }

                    if let Some(exch) = &security.exchange {
                        writeln!(file, "   Exchange: {}", exch)?;
                    }

                    if !security.attributes.is_empty() {
                        writeln!(file, "   Attributes:")?;
                        for (tag, value) in &security.attributes {
                            writeln!(file, "     Tag {}: {}", tag, value)?;
                        }
                    }

                    writeln!(file)?;
                }

                writeln!(file, "─────────────────────────────────────────────────────────────────────────────────")?;
                writeln!(file, "End of Security List")?;

                Ok(())
            })() {
                Ok(_) => {
                    info!("✅ Saved {} securities to securities.txt", security_count);
                }
                Err(e) => {
                    error!("❌ Failed to write securities file: {}", e);
                }
            }
        });

        Ok(())
    }

    // Save securities to kdb+ database (replaced SQLite)
    async fn save_securities_to_db(&self) -> Result<()> {
        let securities = self.securities.lock().await;
        let securities_data = securities.clone();
        let security_count = securities_data.len();

        // Prepare data for kdb+ insert
        let securities_for_kdb: Vec<(String, Option<String>, Option<String>, String)> = securities_data
            .iter()
            .map(|s| {
                let attributes_json = serde_json::to_string(&s.attributes)
                    .unwrap_or_else(|_| "{}".to_string());
                (s.symbol.clone(), s.security_id.clone(), s.exchange.clone(), attributes_json)
            })
            .collect();

        // Write to kdb+
        if let Err(e) = self.kdb_writer.write_securities(&securities_for_kdb).await {
            error!("❌ Failed to write securities to kdb+: {}", e);
            return Err(e);
        }

        info!("✅ Saved {} securities to kdb+", security_count);
        Ok(())
    }

    /// Save tick data to CSV file (compatible with Parquet tools).
    ///
    /// Uses append mode so multiple calls accumulate data across sessions.
    /// The CSV header is only written when the file is first created.
    pub async fn save_ticks_to_parquet(&self, filename: &str) -> Result<()> {
        let mut ticks = self.tick_data.lock().await;
        let tick_count = ticks.len();

        if tick_count == 0 {
            return Ok(());
        }

        // Drain the buffer so next call only writes new ticks.
        let ticks_clone: Vec<TickData> = ticks.drain(..).collect();
        drop(ticks);

        let filename_owned = filename.to_string();

        std::thread::spawn(move || {
            match (|| -> Result<()> {
                // Ensure parent directory exists.
                if let Some(parent) = Path::new(&filename_owned).parent() {
                    std::fs::create_dir_all(parent)?;
                }

                let file_is_new = !Path::new(&filename_owned).exists();
                let mut file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&filename_owned)?;

                if file_is_new {
                    writeln!(file, "timestamp,symbol,bid,ask,bid_size,ask_size,last_price,last_size")?;
                }

                for tick in &ticks_clone {
                    writeln!(file, "{},{},{},{},{},{},{},{}",
                        tick.timestamp,
                        tick.symbol,
                        tick.bid,
                        tick.ask,
                        tick.bid_size,
                        tick.ask_size,
                        tick.last_price,
                        tick.last_size
                    )?;
                }

                Ok(())
            })() {
                Ok(_) => {
                    info!("✅ Appended {} ticks to {}", tick_count, filename_owned);
                }
                Err(e) => {
                    error!("❌ Failed to write tick data file: {}", e);
                }
            }
        });

        Ok(())
    }

    /// Add a tick to the tick buffer
    pub async fn add_tick(&self, tick: TickData) {
        let mut ticks = self.tick_data.lock().await;
        ticks.push(tick);
        let len = ticks.len();
        if len > MAX_TICK_BUFFER {
            ticks.drain(..len - MAX_TICK_BUFFER);
        }
    }

    /// Replay deferred kdb writes after `order_books` has been dropped.
    /// Error policy matches the previous in-lock call sites: kdb failure is ignored.
    /// Persistence is gated by CAPTURE_SYMBOLS; the in-memory book is already applied.
    async fn replay_pending_md_io(&self, symbol: &str, pending: Vec<PendingMdIo>) {
        if !self.capture_symbols.contains(symbol) {
            return;
        }
        for io in pending {
            match io {
                PendingMdIo::Level { side, price, size, action, seq_num } => {
                    #[cfg(test)]
                    self.kdb_writer.record_probe(kdb_integration::ProbeEvent::PendingMdIo {
                        kind: "level", symbol: symbol.to_string(), seq_num,
                    });
                    if let Err(e) = self.kdb_writer
                        .write_order_book_level(symbol, side, price, size, action, seq_num)
                        .await
                    {
                        warn!("kdb write failed (level {symbol}): {e}");
                    }
                }
                PendingMdIo::Trade { price, size, timestamp, seq_num } => {
                    #[cfg(test)]
                    self.kdb_writer.record_probe(kdb_integration::ProbeEvent::PendingMdIo {
                        kind: "trade", symbol: symbol.to_string(), seq_num,
                    });
                    if let Err(e) = self.kdb_writer
                        .write_trade(symbol, price, size, timestamp, seq_num)
                        .await
                    {
                        warn!("kdb write failed (trade {symbol}): {e}");
                    }
                }
                PendingMdIo::Snapshot { best_bid, best_ask, best_bid_size, best_ask_size, spread, seq_num } => {
                    #[cfg(test)]
                    self.kdb_writer.record_probe(kdb_integration::ProbeEvent::PendingMdIo {
                        kind: "snapshot", symbol: symbol.to_string(), seq_num,
                    });
                    if let Err(e) = self.kdb_writer
                        .write_snapshot(symbol, best_bid, best_ask, best_bid_size, best_ask_size, spread, seq_num)
                        .await
                    {
                        warn!("kdb write failed (snapshot {symbol}): {e}");
                    }
                }
            }
        }
    }

    // Private helper methods
    async fn send_message(&self, msg: &FIXMessage) -> Result<()> {
        // CRITICAL: Only allow logon (A) and logout (5) messages before session is ready
        // All other messages (including heartbeat, SecurityListRequest, MarketDataRequest, orders, etc.) 
        // must wait until logon ACK is received
        if msg.msg_type != 'A' && msg.msg_type != '5' {
            if !self.is_ready().await {
                return Err(anyhow!("Cannot send message type {} - session not ready (logon ACK not received)", msg.msg_type));
            }
        }
        
        let encoded = msg.encode()?;
        info!("Sending FIX message (type: {}, size: {} bytes)", msg.msg_type, encoded.len());
        
        // Log FIX command to separate file
        Self::log_fix_command(msg, &encoded, Direction::Outbound);

        // Debug: Log the message content in readable format
        if msg.msg_type == 'A' {
            // Logon message - log all fields for debugging
            info!("DEBUG: Logon message fields:");
            for (&tag, value) in &msg.fields {
                if tag == 96 {
                    info!("  Tag {}: {} (RawData - length: {})", tag, value, value.len());
                } else if tag != 8 && tag != 9 && tag != 93 { // Skip checksum fields for brevity
                    info!("  Tag {}: {}", tag, redact_tag(tag, value));
                }
            }

            // The raw hex dump is the entire Logon, API key and signature
            // included, so it cannot be logged at all for this message type.
            // The readable form is logged with credential tags redacted.
            let readable = redact_fix_text(
                &String::from_utf8_lossy(&encoded).replace('\x01', "|"),
            );
            info!("DEBUG: Readable format (redacted, first 300 chars): {}", &readable[..std::cmp::min(300, readable.len())]);
        } else if msg.msg_type == 'D' {
            // NewOrderSingle message - log all fields for debugging
            info!("DEBUG: NewOrderSingle message fields:");
            for (&tag, value) in &msg.fields {
                if tag != 8 && tag != 9 && tag != 10 { // Skip header and checksum
                    info!("  Tag {}: {}", tag, value);
                }
            }

            // Log readable format with SOH markers
            let readable = String::from_utf8_lossy(&encoded)
                .replace('\x01', "|");
            info!("DEBUG: Message readable format: {}", readable);
        }

        let mut stream_guard = self.stream.lock().await;
        let stream = stream_guard.as_mut().ok_or_else(|| anyhow!("Not connected"))?;
        stream.write_all(&encoded).await?;
        stream.flush().await?;
        info!("Message sent successfully");
        Ok(())
    }
    
    /// Log FIX command to separate log file
    /// Routes Order Management messages to OM_fix_command.log
    /// Routes Market Data messages to fix_commands.log
    fn log_fix_command(msg: &FIXMessage, encoded: &[u8], direction: Direction) {
        // Convert FIX message to readable format (replace SOH with |)
        let readable = String::from_utf8_lossy(encoded)
            .replace('\x01', "|");

        let sender_id = msg.get_field(tags::SENDER_COMP_ID).unwrap_or("");
        let target_id = msg.get_field(tags::TARGET_COMP_ID).unwrap_or("");
        let is_om = is_om_command_log(sender_id, target_id);

        let timestamp = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3f UTC");
        let log_entry = format!(
            "[{}] FIX {} {} (35={}) Sender={} Target={} - {} bytes\n{}\n\n",
            timestamp,
            direction.as_tag(),
            command_log_msg_type_name(msg.msg_type),
            msg.msg_type,
            sender_id,
            target_id,
            encoded.len(),
            readable
        );

        append_command_log_entry(
            Path::new(command_log_filename(is_om)),
            log_entry,
            COMMAND_LOG_MAX_BYTES,
        );
    }

    async fn next_seq_num(&self) -> u32 {
        let mut seq = self.seq_num.lock().await;
        *seq += 1;
        let n = *seq;
        // Persist so restarts don't reset to 0 and cause MsgSeqNum-too-low rejections.
        let safe_sender = self.sender_comp_id.replace('.', "_").replace(' ', "_");
        let seq_path = format!("data/seqnum_{}.txt", safe_sender);
        if self.persist_seq_to_disk {
            let _ = std::fs::create_dir_all("data");
            let _ = std::fs::write(&seq_path, n.to_string());
        }
        n
    }

    async fn is_ready(&self) -> bool {
        *self.is_connected.lock().await
            && matches!(
                *self.connection_state.lock().await,
                ConnectionState::Ready
            )
    }
}

/// Distinguishes halt rejects from `Session not ready` and `Duplicate ClOrdID rejected`.
const OPERATOR_HALT_REJECT: &str = "Operator halt is active; new orders are blocked";

const EXPOSURE_UNKNOWN_SYMBOL_REJECT: &str =
    "Symbol is not in the configured symbol_limits table; new orders are blocked";
const EXPOSURE_POSITION_REJECT: &str =
    "Projected position exceeds the configured per-symbol max_position; new orders are blocked";
const EXPOSURE_NOTIONAL_REJECT: &str =
    "Order notional exceeds the configured per-symbol max_notional; new orders are blocked";
const EXPOSURE_INVALID_QTY_REJECT: &str =
    "Order quantity must be a finite value greater than zero";
const EXPOSURE_INVALID_SIDE_REJECT: &str =
    "Order side must be 1 (buy) or 2 (sell)";
const EXPOSURE_INVALID_PRICE_REJECT: &str =
    "Order price must be a finite value greater than zero when present";
const EXPOSURE_INVALID_CASH_QTY_REJECT: &str =
    "CashOrderQty must be a finite value greater than zero when present";

/// Same epsilon already used by `Position::apply_fill`. Do not invent a second epsilon.
const POSITION_FLAT_EPS: f64 = 1e-10;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ExposureReject {
    UnknownSymbol,
    Position,
    Notional,
    InvalidQty,
    InvalidSide,
    InvalidPrice,
    InvalidCashQty,
}

fn exposure_reject_str(r: ExposureReject) -> &'static str {
    match r {
        ExposureReject::UnknownSymbol => EXPOSURE_UNKNOWN_SYMBOL_REJECT,
        ExposureReject::Position => EXPOSURE_POSITION_REJECT,
        ExposureReject::Notional => EXPOSURE_NOTIONAL_REJECT,
        ExposureReject::InvalidQty => EXPOSURE_INVALID_QTY_REJECT,
        ExposureReject::InvalidSide => EXPOSURE_INVALID_SIDE_REJECT,
        ExposureReject::InvalidPrice => EXPOSURE_INVALID_PRICE_REJECT,
        ExposureReject::InvalidCashQty => EXPOSURE_INVALID_CASH_QTY_REJECT,
    }
}

fn exposure_reject_reason_tag(r: ExposureReject) -> &'static str {
    match r {
        ExposureReject::UnknownSymbol => "unknown_symbol",
        ExposureReject::Position => "position",
        ExposureReject::Notional => "notional",
        ExposureReject::InvalidQty => "invalid_qty",
        ExposureReject::InvalidSide => "invalid_side",
        ExposureReject::InvalidPrice => "invalid_price",
        ExposureReject::InvalidCashQty => "invalid_cash_qty",
    }
}

fn load_f64(a: &AtomicU64) -> f64 {
    f64::from_bits(a.load(Ordering::SeqCst))
}

fn store_f64(a: &AtomicU64, v: f64) {
    a.store(v.to_bits(), Ordering::SeqCst);
}

/// Pure pre-trade 35=D exposure predicate. Empty `limits` ⇒ Ok (gate off, EL-018).
/// Residual (EL-003): both `price` and `cash_order_qty` None ⇒ skip notional, position-only.
fn check_exposure(
    order: &OrderRequest,
    net_qty: f64,
    limits: &HashMap<String, SymbolLimit>,
) -> std::result::Result<(), ExposureReject> {
    if limits.is_empty() {
        return Ok(());
    }
    if !order.qty.is_finite() || order.qty <= 0.0 {
        return Err(ExposureReject::InvalidQty);
    }
    if order.side != '1' && order.side != '2' {
        return Err(ExposureReject::InvalidSide);
    }
    if let Some(p) = order.price {
        if !p.is_finite() || p <= 0.0 {
            return Err(ExposureReject::InvalidPrice);
        }
    }
    if let Some(c) = order.cash_order_qty {
        if !c.is_finite() || c <= 0.0 {
            return Err(ExposureReject::InvalidCashQty);
        }
    }
    let Some(limit) = limits.get(&order.symbol) else {
        return Err(ExposureReject::UnknownSymbol);
    };
    let signed = if order.side == '1' {
        order.qty
    } else {
        -order.qty
    };
    let projected = net_qty + signed;
    // Reduce/flatten (`|projected| ≤ |net| + EPS`) skips the position cap (AC3 / EL-006).
    if projected.abs() > net_qty.abs() + POSITION_FLAT_EPS && projected.abs() > limit.max_position {
        return Err(ExposureReject::Position);
    }
    let notional = if let Some(p) = order.price {
        Some(order.qty * p)
    } else if let Some(c) = order.cash_order_qty {
        Some(c)
    } else {
        None
    };
    if let Some(n) = notional {
        if n > limit.max_notional {
            return Err(ExposureReject::Notional);
        }
    }
    Ok(())
}

const STATUS_READ_SKIPPED_LOCK_CONTENDED: &str = "skipped_lock_contended";

fn connection_state_status_str(state: ConnectionState) -> &'static str {
    match state {
        ConnectionState::Disconnected => "DISCONNECTED",
        ConnectionState::Connecting => "CONNECTING",
        ConnectionState::Connected => "CONNECTED",
        ConnectionState::Logging => "LOGGING_IN",
        ConnectionState::Ready => "READY",
        ConnectionState::Disconnecting => "DISCONNECTING",
    }
}

/// Non-blocking per-session status sample for `GET /api/market-data-status`.
/// Plain `fn` so it cannot `.await` a session mutex. Contended fields are JSON
/// `null` (keys always present); `status_read` is emitted only on contention.
fn market_data_session_status(
    session: &FIXSession,
    description: &str,
    port: u16,
) -> serde_json::Value {
    let mut contended = false;

    let is_connected: Option<bool> = match session.is_connected.try_lock() {
        Ok(g) => Some(*g),
        Err(_) => {
            contended = true;
            None
        }
    };

    let connection_state: Option<ConnectionState> = match session.connection_state.try_lock() {
        Ok(g) => Some(*g),
        Err(_) => {
            contended = true;
            None
        }
    };

    let is_ready: Option<bool> = match (is_connected, connection_state) {
        (Some(c), Some(s)) => Some(c && matches!(s, ConnectionState::Ready)),
        _ => None,
    };

    let seq_num: Option<u32> = match session.seq_num.try_lock() {
        Ok(g) => Some(*g),
        Err(_) => {
            contended = true;
            None
        }
    };

    let (book_count, total_trades, symbols): (Option<usize>, Option<usize>, Option<Vec<String>>) =
        match session.order_books.try_lock() {
            Ok(books) => {
                let count = books.len();
                let trades = books.values().map(|b| b.get_active().trade_count()).sum();
                let syms = books.keys().cloned().collect();
                (Some(count), Some(trades), Some(syms))
            }
            Err(_) => {
                contended = true;
                (None, None, None)
            }
        };

    let securities_count: Option<usize> = match session.securities.try_lock() {
        Ok(sec) => Some(sec.len()),
        Err(_) => {
            contended = true;
            None
        }
    };

    let connection_state_str: Option<&'static str> =
        connection_state.map(connection_state_status_str);

    let mut body = serde_json::json!({
        "description": description,
        "port": port,
        "sender_comp_id": session.sender_comp_id,
        "target_comp_id": session.target_comp_id,
        "is_connected": is_connected,
        "is_ready": is_ready,
        "connection_state": connection_state_str,
        "sequence_number": seq_num,
        "order_books": book_count,
        "total_trades": total_trades,
        "symbols": symbols,
        "securities_count": securities_count,
    });
    if contended {
        body["status_read"] = serde_json::Value::String(
            STATUS_READ_SKIPPED_LOCK_CONTENDED.to_string(),
        );
    }
    body
}

/// Message processing loop with persistent connection support
async fn message_loop(
    session: Arc<FIXSession>,
    gateway_url: String,
    mut order_rx: mpsc::Receiver<OrderRequest>,
    heartbeat_interval: u64,
    max_reconnect_attempts: u32,
    reconnect_backoff_ms: u64,
) -> Result<()> {
    loop {
        // Ensure connection
        if let Err(e) = session.connect_with_retry(&gateway_url, max_reconnect_attempts, reconnect_backoff_ms).await {
            error!("Failed to establish connection: {}", e);
            sleep(Duration::from_secs(10)).await;
            continue;
        }

        // Logon
        if let Err(e) = session.logon(heartbeat_interval).await {
            error!("Logon failed: {}", e);
            let _ = session.logout().await;
            sleep(Duration::from_secs(5)).await;
            continue;
        }

        // CRITICAL: Wait for Logon ACK (35=A) from gateway
        // Per documentation: "We recommend adding 50 ms wait interval after receiving 35=A on both side"
        info!("Logon sent. Waiting for Logon ACK (35=A) from gateway...");

        let mut logon_acked = false;
        let logon_timeout = Instant::now();

        while logon_timeout.elapsed() < Duration::from_secs(5) {
            match tokio::time::timeout(Duration::from_millis(500), session.receive_message()).await {
                Ok(Ok(msg)) => {
                    info!("📨 Received message type: {}", msg.msg_type);

                    if msg.msg_type == 'A' {
                        // This is the Logon ACK response
                        logon_acked = true;
                        info!("✅ Logon ACK (35=A) received from gateway!");
                        
                        // Set connection state to Ready only after receiving ACK
                        *session.connection_state.lock().await = ConnectionState::Ready;
                        session.handle_message(&msg).await?;

                        // Per documentation: wait 50ms after receiving Logon ACK before sending first request
                        info!("Waiting 50ms as per documentation before proceeding...");
                        sleep(Duration::from_millis(50)).await;
                        break;
                    } else {
                        // Other message type - process and keep waiting for logon ACK
                        session.handle_message(&msg).await?;
                    }
                }
                Ok(Err(e)) => {
                    warn!("⚠️  Connection error while waiting for Logon ACK: {}", e);
                    break;
                }
                Err(_) => {
                    // Timeout, continue waiting
                    continue;
                }
            }
        }

        if !logon_acked {
            warn!("❌ Logon ACK not received from gateway - connection rejected");
            sleep(Duration::from_secs(5)).await;
            continue;
        }

        // After Ready + 50ms, before select!: re-ask the venue about every
        // locally-open ClOrdID. First connect is a no-op (empty store).
        // Fail-open: a reconcile Err must not tear down this session iteration.
        if let Err(e) = session.reconcile_open_orders().await {
            warn!("Open-order reconciliation after logon failed: {}", e);
        }

        info!("🔗 Connection established - waiting for messages");
        info!("Waiting for gateway responses or connection close...");

        // Message processing - keep connection alive and monitor for messages
        // Run continuously - no timeout limit
        let mut connection_active = true;
        while connection_active {
            tokio::select! {
                // Handle incoming orders from the request channel (unused in current test)
                Some(order) = order_rx.recv() => {
                    if let Err(e) = session.new_order_single_from_request(&order).await {
                        warn!("Failed to send order {}: {}", order.cl_ord_id, e);
                    }
                }
                // Handle incoming messages from gateway
                result = session.receive_message() => {
                    match result {
                        Ok(msg) => {
                            info!("📨 Received message type: {} - processing", msg.msg_type);
                            if let Err(e) = session.handle_message(&msg).await {
                                error!("Message handling error: {}", e);
                            }
                        }
                        Err(e) => {
                            warn!("⚠️  Connection closed: {}", e);
                            connection_active = false;
                        }
                    }
                }
            }
        }

        // Wait before reconnecting
        info!("Reconnecting in 5 seconds...");
        sleep(Duration::from_secs(5)).await;
    }
}

/// Market Data connection loop for securities requests
/// symbol_filter: Optional function to filter which symbols this server should subscribe to
async fn market_data_loop(
    session: Arc<FIXSession>,
    gateway_url: String,
    heartbeat_interval: u64,
    max_reconnect_attempts: u32,
    reconnect_backoff_ms: u64,
    symbol_filter: Option<Box<dyn Fn(&str) -> bool + Send + Sync>>,
) -> Result<()> {
    loop {
        // Ensure connection to market data server
        if let Err(e) = session.connect_with_retry(&gateway_url, max_reconnect_attempts, reconnect_backoff_ms).await {
            error!("Failed to establish market data connection to {}: {}", gateway_url, e);
            sleep(Duration::from_secs(10)).await;
            continue;
        }

        info!("📊 Connected to Market Data Server: {}", gateway_url);
        info!("  Using Sender CompID: {} -> Target CompID: {}", session.sender_comp_id, session.target_comp_id);

        // Logon to market data server with Market Data auth pattern
        if let Err(e) = session.logon_market_data(heartbeat_interval).await {
            error!("Market Data Logon failed for {}: {}", gateway_url, e);
            let _ = session.logout().await;
            sleep(Duration::from_secs(5)).await;
            continue;
        }

        info!("Logon sent to Market Data Server ({}). Waiting for Logon ACK...", gateway_url);

        let mut logon_acked = false;
        let logon_timeout = Instant::now();

        while logon_timeout.elapsed() < Duration::from_secs(5) {
            match tokio::time::timeout(Duration::from_millis(500), session.receive_message()).await {
                Ok(Ok(msg)) => {
                    info!("📨 Market Data received message type: {}", msg.msg_type);

                    if msg.msg_type == 'A' {
                        logon_acked = true;
                        info!("✅ Market Data Logon ACK (35=A) received!");
                        
                        // Set connection state to Ready only after receiving ACK
                        *session.connection_state.lock().await = ConnectionState::Ready;
                        session.handle_message(&msg).await?;
                        info!("Waiting 50ms as per documentation...");
                        sleep(Duration::from_millis(50)).await;
                        break;
                    } else {
                        session.handle_message(&msg).await?;
                    }
                }
                Ok(Err(e)) => {
                    warn!("Connection error while waiting for Market Data Logon ACK ({}): {}", gateway_url, e);
                    break;
                }
                Err(_) => {
                    continue;
                }
            }
        }

        if !logon_acked {
            warn!("❌ Market Data Logon ACK not received from {}", gateway_url);
            sleep(Duration::from_secs(5)).await;
            continue;
        }

        info!("🔗 Market Data connection established ({}): ready for requests", gateway_url);

        // Send SecurityListRequest immediately after logon succeeds
        info!("📋 Sending SecurityListRequest to Market Data Server...");
        if let Err(e) = session.security_list_request("SEC_LIST_MD_001").await {
            error!("Failed to send SecurityListRequest: {}", e);
        } else {
            info!("✅ SecurityListRequest sent successfully!");
        }

        // Wait for the SecurityList while CONTINUING TO READ the socket.
        //
        // This task is the only reader for this session, and it used to wait
        // passively (first a fixed 3s sleep, then a 30s poll) before entering
        // its read loop. Nothing read the socket in the meantime, so the
        // SecurityList sat unread in the kernel buffer and was only processed
        // ~2s AFTER each wait ended, however long the wait was -- the list
        // could never arrive in time, by construction. (PR #9 misread this as
        // the venue being slow and lengthened the wait, which only made the
        // same deadlock take 30s.) Now the wait pumps messages exactly as the
        // main loop below does, so 35=y fragments are handled as they arrive.
        let wait_started = Instant::now();
        let deadline = wait_started + Duration::from_secs(30);
        let mut last_len = 0usize;
        let mut stable_since = Instant::now();
        let mut lost_connection = false;
        loop {
            match tokio::time::timeout(Duration::from_millis(250), session.receive_message()).await {
                Ok(Ok(msg)) => {
                    if let Err(e) = session.handle_message(&msg).await {
                        error!("Market Data message handling error: {}", e);
                    }
                }
                Ok(Err(e)) => {
                    let err_str = e.to_string();
                    if !(err_str.contains("Incomplete FIX message") || err_str.contains("waiting for more data")) {
                        warn!("⚠️  Market Data connection closed while awaiting SecurityList: {}", e);
                        lost_connection = true;
                        break;
                    }
                }
                Err(_) => {} // nothing to read this tick
            }
            let len = session.securities.lock().await.len();
            if len != last_len {
                last_len = len;
                stable_since = Instant::now();
            }
            if len > 0 && stable_since.elapsed() >= Duration::from_secs(2) {
                info!("📜 SecurityList settled after {:.1}s ({} entries)", wait_started.elapsed().as_secs_f64(), len);
                break;
            }
            if Instant::now() >= deadline {
                warn!("⚠️  SecurityList not settled after 30s ({} entries so far)", len);
                break;
            }
        }
        if lost_connection {
            info!("Market Data session ended. Reconnecting in 5 seconds...");
            sleep(Duration::from_secs(5)).await;
            continue;
        }

        // Verify session is still ready before sending MarketDataRequest
        if !session.is_ready().await {
            warn!("⚠️  Session not ready, skipping MarketDataRequest");
            continue;
        }

        // Subscribe to what the venue actually lists, not to a fixed list.
        //
        // This previously held 12 hardcoded symbols, every one of them a BTC
        // pair. The UAT venue lists none of them — it answers each request with
        // "35=j ... 379=55 ... Symbol: BTC_USDT is not supported", 379 being
        // RefTagID for Symbol(55). So every subscription was rejected and no
        // order book ever populated. The SecurityList the venue sends on logon
        // was already parsed into `securities` and written to securities.txt,
        // then ignored.
        //
        // Per Crypto.com docs the MDReqID format is book.{Symbol}.{MarketDepth}
        // or trade.{Symbol}.
        // Deduplicated: the list accumulates across repeated SecurityList
        // responses (450 entries for a venue listing 50 on 2026-09-19), and
        // subscribing to a symbol more than once is at best noise.
        let venue_symbols: Vec<String> = {
            let securities = session.securities.lock().await;
            let unique: std::collections::BTreeSet<String> =
                securities.iter().map(|s| s.symbol.clone()).collect();
            unique.into_iter().collect()
        };

        if venue_symbols.is_empty() {
            warn!("⚠️  No SecurityList received — subscribing to nothing rather than \
                   guessing at symbols the venue may not list");
        } else {
            info!("📜 Venue published {} securities", venue_symbols.len());
        }

        // Only what is configured: see SubscriptionsConfig.
        let allowlist = SUBSCRIPTION_ALLOWLIST.get().cloned().unwrap_or_default();
        let venue_symbols: Vec<String> = if allowlist.is_empty() {
            warn!("⚠️  [subscriptions].symbols is empty in config.toml — subscribing to nothing. \
                   Set the instruments to trade; subscribing to every listed symbol floods the venue.");
            Vec::new()
        } else {
            let picked = subscriptions_for_port(&venue_symbols, &allowlist);
            info!("📜 Subscribing to {} of {} configured symbols listed on this port", picked.len(), allowlist.len());
            picked
        };

        // Filter symbols based on the symbol_filter function
        let symbols_to_subscribe: Vec<&str> = if let Some(filter) = &symbol_filter {
            venue_symbols.iter().map(|s| s.as_str()).filter(|s| filter(s)).collect()
        } else {
            venue_symbols.iter().map(|s| s.as_str()).collect()
        };
        
        info!("");
        info!("📊 Subscribing to {} markets for order book and trade data...", symbols_to_subscribe.len());
        if symbols_to_subscribe.is_empty() {
            info!("   (No symbols match this server's routing criteria)");
        }
        
        // Subscribe to each symbol
        for symbol in &symbols_to_subscribe {
            // Send MarketDataRequest (Order Book)
            info!("");
            info!("📈 Sending MarketDataRequest for {} (Order Book)...", symbol);
            if !session.is_ready().await {
                warn!("⚠️  Session not ready, skipping {}", symbol);
                continue;
            }
            
            if let Err(e) = session.market_data_request(symbol, 50).await {
                error!("Failed to send MarketDataRequest for {}: {}", symbol, e);
            } else {
                info!("✅ MarketDataRequest for {} sent successfully!", symbol);
            }
            
            // Small delay between requests to avoid overwhelming the server
            sleep(Duration::from_millis(200)).await;
            
            // Send TradeDataRequest (Trades)
            info!("💹 Sending TradeDataRequest for {} (Trades)...", symbol);
            if let Err(e) = session.trade_data_request(symbol).await {
                error!("Failed to send TradeDataRequest for {}: {}", symbol, e);
            } else {
                info!("✅ TradeDataRequest for {} sent successfully!", symbol);
            }
            
            // Small delay between symbols
            sleep(Duration::from_millis(200)).await;
        }
        
        info!("");
        info!("✅ Subscribed to {} markets successfully!", symbols_to_subscribe.len());
        info!("   Waiting for market data updates...");
        info!("   Note: Trades may come in incremental refresh messages (35=X)");
        
        // Wait a moment to see if we get any rejections
        sleep(Duration::from_secs(2)).await;

        // Message processing loop for market data
        // Run continuously - no timeout limit
        let mut connection_active = true;
        let mut last_order_book_dump = Instant::now();
        
        while connection_active {
            match tokio::time::timeout(Duration::from_secs(5), session.receive_message()).await {
                Ok(Ok(msg)) => {
                    info!("📨 Market Data message type: {} - processing", msg.msg_type);
                    if let Err(e) = session.handle_message(&msg).await {
                        error!("Market Data message handling error: {}", e);
                    }
                }
                Ok(Err(e)) => {
                    let err_str = e.to_string();
                    // Check if it's just an incomplete message or a real error
                    if err_str.contains("Incomplete FIX message") || err_str.contains("waiting for more data") {
                        // This is expected - fragment received, continue reading
                        info!("📦 Waiting for complete FIX message: {}", err_str);
                        continue;
                    } else {
                        warn!("⚠️  Market Data connection closed: {}", e);
                        connection_active = false;
                    }
                }
                Err(_) => {
                    // Timeout - continue waiting
                    continue;
                }
            }
        }

        // Connection closed - will reconnect
        info!("Market Data session ended. Reconnecting in 5 seconds...");
        sleep(Duration::from_secs(5)).await;
    }
}

/// Heartbeat sender with connection check
async fn heartbeat_loop(session: Arc<FIXSession>, heartbeat_interval: u64) -> Result<()> {
    let mut interval = interval(Duration::from_secs(heartbeat_interval));

    loop {
        interval.tick().await;

        // Only send heartbeat if session is ready (logon ACK received)
        if session.is_ready().await {
            match session.heartbeat().await {
                Ok(()) => {}
                Err(e) => {
                    warn!("Heartbeat error: {}", e);
                }
            }
        } else {
            // Session not ready yet - skip this heartbeat
            // This prevents sending heartbeats before logon ACK is received
        }
    }
}

/// Generate CSV summary from OpenOrdersStore snapshot (lock-free)
/// Append a single fill to data/trades.csv.
/// Called on every execution report with ExecType F/2/1 (partial or full fill).
/// Uses append mode so the file accumulates fills across server restarts.
fn append_trade_to_csv(
    cl_ord_id: &str,
    order_id: &str,
    symbol: &str,
    side: char,
    exec_type: char,
    last_qty: f64,
    last_px: f64,
    avg_px: f64,
) {
    let _ = (|| -> std::io::Result<()> {
        std::fs::create_dir_all("data")?;
        let path = Path::new("data/trades.csv");
        let file_is_new = !path.exists();
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        if file_is_new {
            writeln!(file, "timestamp_ns,cl_ord_id,order_id,symbol,side,exec_type,last_qty,last_px,avg_px")?;
        }
        let ts = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        writeln!(file, "{},{},{},{},{},{},{},{},{}",
            ts, cl_ord_id, order_id, symbol, side, exec_type,
            last_qty, last_px, avg_px
        )
    })();
}

/// Append a best-bid/ask snapshot to data/snapshots.csv for backtest persistence.
/// Called on every 35=W and 35=X message where both best_bid and best_ask are available.
fn append_snapshot_to_csv(symbol: &str, best_bid: f64, best_ask: f64) {
    append_snapshot_to_csv_with_sizes(symbol, best_bid, best_ask, 0.0, 0.0);
}

fn append_snapshot_to_csv_with_sizes(symbol: &str, best_bid: f64, best_ask: f64, best_bid_size: f64, best_ask_size: f64) {
    if best_bid <= 0.0 || best_ask <= 0.0 || best_ask <= best_bid {
        return;
    }
    let _ = (|| -> std::io::Result<()> {
        std::fs::create_dir_all("data")?;
        let path = Path::new("data/snapshots.csv");
        let file_is_new = !path.exists();
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        if file_is_new {
            writeln!(file, "timestamp_ms,symbol,best_bid,best_ask,best_bid_size,best_ask_size")?;
        }
        let ts_ms = chrono::Utc::now().timestamp_millis();
        writeln!(file, "{},{},{},{},{},{}", ts_ms, symbol, best_bid, best_ask, best_bid_size, best_ask_size)
    })();
}

fn generate_open_orders_csv(open_orders: &[OpenOrder]) -> Result<String> {
    let mut csv_content = String::from("ClOrdID,OrderID,ExecID,Symbol,Side,OrderQty,Price,OrdStatus,ExecType,CumQty,LeavesQty,LastPx,LastQty,AvgPx,TransactTime,OrderCreateTime,RejectionReason\n");
    
    // Helper to escape CSV fields
    let escape_csv = |s: &str| {
        if s.contains(',') || s.contains('"') || s.contains('\n') {
            format!("\"{}\"", s.replace("\"", "\"\""))
        } else {
            s.to_string()
        }
    };
    
    for order in open_orders {
        csv_content.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
            escape_csv(&order.cl_ord_id),
            escape_csv(&order.order_id),
            escape_csv(&order.exec_id),
            escape_csv(&order.symbol),
            escape_csv(&order.side),
            escape_csv(&order.order_qty),
            escape_csv(&order.price),
            escape_csv(&order.ord_status),
            escape_csv(&order.exec_type),
            escape_csv(&order.cum_qty),
            escape_csv(&order.leaves_qty),
            escape_csv(&order.last_px),
            escape_csv(&order.last_qty),
            escape_csv(&order.avg_px),
            escape_csv(&order.transact_time),
            escape_csv(&order.order_create_time),
            escape_csv(&order.rejection_reason)
        ));
    }
    
    Ok(csv_content)
}

/// Event-driven CSV generator - updates immediately when execution reports arrive
/// Uses ArcSwap for zero-lock snapshots - doesn't block order updates
async fn open_orders_csv_generator_event_driven(
    open_orders_store: Arc<OpenOrdersStore>,
    mut update_rx: broadcast::Receiver<()>,
) -> Result<()> {
    let output_file = "orders/processed/open_orders_summary.csv";
    
    // Ensure processed directory exists
    std::fs::create_dir_all("orders/processed")?;
    
    info!("📊 Starting event-driven open orders CSV generator (updates immediately on execution reports)");
    
    // Initial write
    let open_orders = open_orders_store.get_open_orders();
    if let Ok(csv_content) = generate_open_orders_csv(&open_orders) {
        let file_path = output_file.to_string();
        if let Err(e) = tokio::task::spawn_blocking(move || {
            std::fs::write(&file_path, csv_content)
        }).await {
            warn!("Failed to write initial open orders summary: {}", e);
        }
    }
    
    loop {
        // Wait for update signal (event-driven)
        if update_rx.recv().await.is_ok() {
            // Get lock-free snapshot (zero-copy, doesn't block writers)
            let open_orders = open_orders_store.get_open_orders();
            
            // Generate CSV from snapshot
            match generate_open_orders_csv(&open_orders) {
                Ok(csv_content) => {
                    // Write to file (blocking I/O in spawn_blocking to avoid blocking async runtime)
                    let file_path = output_file.to_string();
                    if let Err(e) = tokio::task::spawn_blocking(move || {
                        std::fs::write(&file_path, csv_content)
                    }).await {
                        warn!("Failed to write open orders summary: {}", e);
                    } else {
                        log::debug!("✅ Updated open orders summary CSV ({} open orders)", open_orders.len());
                    }
                }
                Err(e) => {
                    warn!("Failed to generate open orders CSV: {}", e);
                }
            }
        }
    }
}


/// Watch orders directory and process order files
/// Supports both JSON and binary (MessagePack) formats for microsecond-level performance
/// Binary format is ~2-3x faster to parse than JSON
/// Order file format (JSON):
/// {
///   "cl_ord_id": "ORDER_001",
///   "symbol": "BTC_USDT",
///   "side": "1",  // "1" for Buy, "2" for Sell
///   "qty": 0.001,
///   "price": 45000.0
/// }
async fn watch_orders_directory(
    order_tx: mpsc::Sender<OrderRequest>,
    orders_dir: &str,
    session: Arc<FIXSession>,
) -> Result<()> {
    // Create orders directory if it doesn't exist
    std::fs::create_dir_all(orders_dir)?;
    
    // Create processed subdirectory
    let processed_dir = format!("{}/processed", orders_dir);
    std::fs::create_dir_all(&processed_dir)?;
    
    info!("📁 Watching orders directory: {}", orders_dir);
    info!("   Supported formats: JSON (.json) - recommended for human readability");
    info!("   Processed orders will be moved to: {}", processed_dir);
    
    let mut check_interval = interval(Duration::from_millis(100)); // Check every 100ms for low latency
    
    loop {
        check_interval.tick().await;
        
        // Read directory entries (async)
        match tokio::fs::read_dir(orders_dir).await {
            Ok(mut entries) => {
                loop {
                    match entries.next_entry().await {
                        Ok(Some(entry)) => {
                            let path = entry.path();
                            
                            // Process JSON files
                            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                                let file_name = path.file_name().unwrap().to_string_lossy().to_string();
                                let file_read_start = Instant::now();
                                
                                // Read and parse JSON file (async I/O for better performance)
                                match tokio::fs::read_to_string(&path).await {
                                    Ok(content) => {
                                        let file_read_us = file_read_start.elapsed().as_micros() as u64;
                                        let json_parse_start = Instant::now();
                                        
                                        // Try to parse as CancelOrderRequest first (check for orig_cl_ord_id field)
                                        let json_value: serde_json::Value = match serde_json::from_str(&content) {
                                            Ok(v) => v,
                                            Err(e) => {
                                                error!("Failed to parse JSON in {}: {}", file_name, e);
                                                let error_name = format!("{}.error", file_name);
                                                let error_path = Path::new(&processed_dir).join(&error_name);
                                                if let Err(e) = tokio::fs::rename(&path, &error_path).await {
                                                    warn!("Failed to move error file: {}", e);
                                                }
                                                continue;
                                            }
                                        };
                                        
                                        // Check if this is a cancel order (has orig_cl_ord_id field)
                                        if json_value.get("orig_cl_ord_id").is_some() {
                                            // This is a cancel order
                                            match serde_json::from_value::<CancelOrderRequest>(json_value) {
                                                Ok(cancel_request) => {
                                                    let json_parse_us = json_parse_start.elapsed().as_micros() as u64;
                                                    
                                                    info!("🚫 Processing cancel order: {} (Original Order: {}, Symbol: {}, Side: {})", 
                                                          cancel_request.cl_ord_id, cancel_request.orig_cl_ord_id, 
                                                          cancel_request.symbol, cancel_request.side);
                                                    
                                                    // Send cancel order directly
                                                    match session.cancel_order(&cancel_request).await {
                                                        Ok(_) => {
                                                            info!("✅ Cancel order {} sent successfully", cancel_request.cl_ord_id);
                                                            
                                                            // Move to processed directory
                                                            let processed_path = Path::new(&processed_dir).join(&file_name);
                                                            if let Err(e) = tokio::fs::rename(&path, &processed_path).await {
                                                                warn!("Failed to move processed cancel order file {}: {}", file_name, e);
                                                            }
                                                        }
                                                        Err(e) => {
                                                            error!("Failed to send cancel order {}: {}", cancel_request.cl_ord_id, e);
                                                            // Move to processed with error suffix
                                                            let error_name = format!("{}.error", file_name);
                                                            let error_path = Path::new(&processed_dir).join(&error_name);
                                                            if let Err(e) = tokio::fs::rename(&path, &error_path).await {
                                                                warn!("Failed to move error file: {}", e);
                                                            }
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    error!("Failed to parse cancel order JSON in {}: {}", file_name, e);
                                                    // Move to processed with error suffix
                                                    let error_name = format!("{}.error", file_name);
                                                    let error_path = Path::new(&processed_dir).join(&error_name);
                                                    if let Err(e) = tokio::fs::rename(&path, &error_path).await {
                                                        warn!("Failed to move error file: {}", e);
                                                    }
                                                }
                                            }
                                        } else {
                                            // This is a regular order
                                            match serde_json::from_value::<OrderRequest>(json_value) {
                                                Ok(order) => {
                                                    let json_parse_us = json_parse_start.elapsed().as_micros() as u64;
                                                    let channel_send_start = Instant::now();
                                                    
                                                    let price_str = order.price.map(|p| p.to_string()).unwrap_or_else(|| "N/A (MARKET)".to_string());
                                                    info!("📄 Processing order: {} (Symbol: {}, Side: {}, Qty: {}, Price: {})", 
                                                          order.cl_ord_id, order.symbol, order.side, order.qty, price_str);
                                                    
                                                    // Initialize timing structure
                                                    let mut timing = OrderTiming {
                                                        file_read_us: Some(file_read_us),
                                                        json_parse_us: Some(json_parse_us),
                                                        channel_send_us: None,
                                                        fix_encode_us: None,
                                                        fix_send_us: None,
                                                        execution_report_us: None,
                                                        total_us: None,
                                                        file_read_timestamp: Some(chrono::Utc::now().timestamp_millis()),
                                                        execution_report_timestamp: None,
                                                    };
                                                    
                                                    // Send order to channel
                                                    match order_tx.send(order.clone()).await {
                                                        Ok(_) => {
                                                            let channel_send_us = channel_send_start.elapsed().as_micros() as u64;
                                                            timing.channel_send_us = Some(channel_send_us);
                                                            
                                                            // Store timing and order data in session (will be updated when execution report arrives)
                                                            {
                                                                let mut timings = session.order_timings.lock().await;
                                                                let mut order_data = session.order_data.lock().await;
                                                                timings.insert(order.cl_ord_id.clone(), timing);
                                                                order_data.insert(order.cl_ord_id.clone(), order);
                                                            }
                                                            
                                                            // Move to processed directory (async for better performance)
                                                            let processed_path = Path::new(&processed_dir).join(&file_name);
                                                            if let Err(e) = tokio::fs::rename(&path, &processed_path).await {
                                                                warn!("Failed to move processed file {}: {}", file_name, e);
                                                            } else {
                                                                info!("✅ Order {} queued (File read: {}µs, Parse: {}µs, Channel: {}µs)", 
                                                                      file_name, file_read_us, json_parse_us, channel_send_us);
                                                            }
                                                        }
                                                        Err(e) => {
                                                            error!("Failed to send order to queue: {}", e);
                                                            // Move to processed with error suffix
                                                            let error_name = format!("{}.error", file_name);
                                                            let error_path = Path::new(&processed_dir).join(&error_name);
                                                            if let Err(e) = tokio::fs::rename(&path, &error_path).await {
                                                                warn!("Failed to move error file: {}", e);
                                                            }
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    error!("Failed to parse order JSON in {}: {}", file_name, e);
                                                    // Move to processed with error suffix
                                                    let error_name = format!("{}.error", file_name);
                                                    let error_path = Path::new(&processed_dir).join(&error_name);
                                                    if let Err(e) = tokio::fs::rename(&path, &error_path).await {
                                                        warn!("Failed to move error file: {}", e);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!("Failed to read order file {}: {}", file_name, e);
                                    }
                                }
                            }
                        }
                        Ok(None) => break, // No more entries
                        Err(e) => {
                            warn!("Error reading directory entry: {}", e);
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Failed to read orders directory: {}", e);
            }
        }
    }
}

/// Event-driven ZeroMQ Publisher - publishes immediately on order book updates
/// Uses the read copy (snapshot) of the order book for lock-free reads
/// Topics: symbol (e.g., "BTC_USDT")
/// Message format: [topic, message_type, json_data]
async fn zmq_order_book_publisher_event_driven(
    session: Arc<FIXSession>,
    bind_addr: &str,
    mut update_rx: broadcast::Receiver<String>,
) -> Result<()> {
    // Create ZeroMQ context and publisher socket in a blocking thread
    let bind_addr_for_log = bind_addr.to_string();
    let bind_addr_clone = bind_addr.to_string();
    let (socket, ctx) = tokio::task::spawn_blocking(move || -> Result<(zmq::Socket, zmq::Context)> {
        let ctx = zmq::Context::new();
        let socket = ctx.socket(zmq::PUB)?;
        socket.bind(&bind_addr_clone)?;
        Ok((socket, ctx))
    }).await??;
    
    // Wrap socket in Arc<Mutex<>> to share across async tasks
    let socket = Arc::new(Mutex::new(socket));
    let _ctx = Arc::new(ctx); // Keep context alive
    
    info!("📡 ZeroMQ Publisher started (event-driven, publishes immediately on order book updates)");
    info!("   Bind address: {}", bind_addr_for_log);
    info!("   Topics: <symbol> (e.g., BTC_USDT)");
    info!("   Message types: bids, asks, trades");
    info!("   Example: Subscribe to 'BTC_USDT' to receive all BTC_USDT updates");
    
    loop {
        // Wait for update signal (event-driven)
        if let Ok(symbol) = update_rx.recv().await {
            log::info!("📡 ZeroMQ received update signal for: {}", symbol);
            // Get order book snapshot (lock-free read)
            let snapshot = {
                let books = session.order_books.lock().await;
                log::info!("📚 Checking order books, total symbols: {}", books.len());
                if let Some(book) = books.get(&symbol) {
                    log::info!("✅ Found order book for {}", symbol);
                    Some(book.get_snapshot())
                } else {
                    log::warn!("⚠️  No order book found for symbol: {}", symbol);
                    None
                }
            };
            
            if let Some(order_book) = snapshot {
                let best_bid = order_book.best_bid;
                let best_ask = order_book.best_ask;
                
                log::info!("📊 Order book snapshot: best_bid={}, best_ask={}", best_bid, best_ask);
                
                // Only publish if order book has valid data
                if best_bid > 0.0 && best_ask > 0.0 {
                    log::info!("✅ Publishing ZeroMQ messages for {}", symbol);
                    // Get bid and ask levels (top 50)
                    let bid_levels = order_book.get_depth(Side::Bid, 50);
                    let ask_levels = order_book.get_depth(Side::Ask, 50);
                    
                    // Get recent trades
                    let recent_trades = order_book.get_recent_trades(100);
                    
                    // Generate a single timestamp for all messages to ensure consistency
                    let snapshot_timestamp = chrono::Utc::now().timestamp_millis();
                    
                    // Prepare JSON data (all with the same timestamp)
                    let bids_data = serde_json::json!({
                        "type": "bids",
                        "symbol": symbol,
                        "timestamp": snapshot_timestamp,
                        "best_bid": best_bid,
                        "best_ask": best_ask,
                        "spread": best_ask - best_bid,
                        "levels": bid_levels.iter().map(|(p, s)| {
                            serde_json::json!({"price": p, "size": s})
                        }).collect::<Vec<_>>()
                    });
                    
                    let asks_data = serde_json::json!({
                        "type": "asks",
                        "symbol": symbol,
                        "timestamp": snapshot_timestamp,
                        "best_bid": best_bid,
                        "best_ask": best_ask,
                        "spread": best_ask - best_bid,
                        "levels": ask_levels.iter().map(|(p, s)| {
                            serde_json::json!({"price": p, "size": s})
                        }).collect::<Vec<_>>()
                    });
                    
                    let trades_data = serde_json::json!({
                        "type": "trades",
                        "symbol": symbol,
                        "timestamp": snapshot_timestamp,
                        "trades": recent_trades.iter().map(|(p, s, ts)| {
                            serde_json::json!({"price": p, "size": s, "timestamp": ts})
                        }).collect::<Vec<_>>(),
                        "trade_count": order_book.trade_count()
                    });
                    
                    // Serialize JSON
                    let bids_json = match serde_json::to_string(&bids_data) {
                        Ok(s) => s,
                        Err(e) => {
                            warn!("Failed to serialize bids JSON: {}", e);
                            continue;
                        }
                    };
                    
                    let asks_json = match serde_json::to_string(&asks_data) {
                        Ok(s) => s,
                        Err(e) => {
                            warn!("Failed to serialize asks JSON: {}", e);
                            continue;
                        }
                    };
                    
                    let trades_json = match serde_json::to_string(&trades_data) {
                        Ok(s) => s,
                        Err(e) => {
                            warn!("Failed to serialize trades JSON: {}", e);
                            continue;
                        }
                    };
                    
                    // Publish messages with symbol as topic (in blocking thread)
                    let socket_clone = Arc::clone(&socket);
                    let symbol_bytes = symbol.as_bytes().to_vec();
                    let bids_type = b"bids".to_vec();
                    let asks_type = b"asks".to_vec();
                    let trades_type = b"trades".to_vec();
                    let bids_bytes = bids_json.into_bytes();
                    let asks_bytes = asks_json.into_bytes();
                    let trades_bytes = trades_json.into_bytes();
                    
                    let _ = tokio::task::spawn_blocking(move || {
                        let socket_guard = socket_clone.blocking_lock();
                        // Send with topic prefix: [symbol, message_type, data]
                        if let Err(e) = socket_guard.send_multipart(&[&symbol_bytes, &bids_type, &bids_bytes], 0) {
                            warn!("ZeroMQ publish bids error: {}", e);
                        } else {
                            log::info!("✅ Sent bids message");
                        }
                        if let Err(e) = socket_guard.send_multipart(&[&symbol_bytes, &asks_type, &asks_bytes], 0) {
                            warn!("ZeroMQ publish asks error: {}", e);
                        } else {
                            log::info!("✅ Sent asks message");
                        }
                        if let Err(e) = socket_guard.send_multipart(&[&symbol_bytes, &trades_type, &trades_bytes], 0) {
                            warn!("ZeroMQ publish trades error: {}", e);
                        } else {
                            log::info!("✅ Sent trades message");
                        }
                    }).await;
                } else {
                    log::warn!("⚠️  Skipping publish - invalid order book data: best_bid={}, best_ask={}", best_bid, best_ask);
                }
            }
        }
    }
}

/// ZeroMQ Publisher for order book data (bids, asks, trades) - OLD VERSION (kept for reference)
/// Uses the read copy (snapshot) of the order book for lock-free reads
/// Topics: symbol (e.g., "BTC_USDT")
/// Message format: [topic, message_type, json_data]
#[allow(dead_code)]
async fn zmq_order_book_publisher(session: Arc<FIXSession>, bind_addr: &str) -> Result<()> {
    // Create ZeroMQ context and publisher socket in a blocking thread
    let bind_addr_for_log = bind_addr.to_string();
    let bind_addr_clone = bind_addr.to_string();
    let (socket, ctx) = tokio::task::spawn_blocking(move || -> Result<(zmq::Socket, zmq::Context)> {
        let ctx = zmq::Context::new();
        let socket = ctx.socket(zmq::PUB)?;
        socket.bind(&bind_addr_clone)?;
        Ok((socket, ctx))
    }).await??;
    
    // Wrap socket in Arc<Mutex<>> to share across async tasks
    let socket = Arc::new(Mutex::new(socket));
    let _ctx = Arc::new(ctx); // Keep context alive
    
    info!("📡 ZeroMQ Publisher bound to: {}", bind_addr_for_log);
    info!("   Topics: <symbol> (e.g., BTC_USDT)");
    info!("   Message types: bids, asks, trades");
    info!("   Example: Subscribe to 'BTC_USDT' to receive all BTC_USDT updates");
    
    let mut update_interval = interval(Duration::from_millis(500)); // Publish every 0.5 seconds
    
    loop {
        update_interval.tick().await;
        
        // Get order book snapshot (lock-free read)
        let snapshot = {
            let books = session.order_books.lock().await;
            if let Some(book) = books.get("BTC_USDT") {
                Some(book.get_snapshot())
            } else {
                None
            }
        };
        
        if let Some(order_book) = snapshot {
            let symbol = "BTC_USDT";
            let best_bid = order_book.best_bid;
            let best_ask = order_book.best_ask;
            
            // Only publish if order book has valid data
            if best_bid > 0.0 && best_ask > 0.0 {
                // Get bid and ask levels (top 50)
                let bid_levels = order_book.get_depth(Side::Bid, 50);
                let ask_levels = order_book.get_depth(Side::Ask, 50);
                
                // Get recent trades
                let recent_trades = order_book.get_recent_trades(100);
                
                // Prepare JSON data
                let bids_data = serde_json::json!({
                    "type": "bids",
                    "symbol": symbol,
                    "timestamp": chrono::Utc::now().timestamp_millis(),
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "spread": best_ask - best_bid,
                    "levels": bid_levels.iter().map(|(p, s)| {
                        serde_json::json!({"price": p, "size": s})
                    }).collect::<Vec<_>>()
                });
                
                let asks_data = serde_json::json!({
                    "type": "asks",
                    "symbol": symbol,
                    "timestamp": chrono::Utc::now().timestamp_millis(),
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "spread": best_ask - best_bid,
                    "levels": ask_levels.iter().map(|(p, s)| {
                        serde_json::json!({"price": p, "size": s})
                    }).collect::<Vec<_>>()
                });
                
                let trades_data = serde_json::json!({
                    "type": "trades",
                    "symbol": symbol,
                    "timestamp": chrono::Utc::now().timestamp_millis(),
                    "trades": recent_trades.iter().map(|(p, s, ts)| {
                        serde_json::json!({"price": p, "size": s, "timestamp": ts})
                    }).collect::<Vec<_>>(),
                    "trade_count": order_book.trade_count()
                });
                
                // Serialize JSON
                let bids_json = match serde_json::to_string(&bids_data) {
                    Ok(s) => s,
                    Err(e) => {
                        warn!("Failed to serialize bids JSON: {}", e);
                        continue;
                    }
                };
                
                let asks_json = match serde_json::to_string(&asks_data) {
                    Ok(s) => s,
                    Err(e) => {
                        warn!("Failed to serialize asks JSON: {}", e);
                        continue;
                    }
                };
                
                let trades_json = match serde_json::to_string(&trades_data) {
                    Ok(s) => s,
                    Err(e) => {
                        warn!("Failed to serialize trades JSON: {}", e);
                        continue;
                    }
                };
                
                // Publish messages with symbol as topic (in blocking thread)
                let socket_clone = Arc::clone(&socket);
                let symbol_bytes = symbol.as_bytes().to_vec();
                let bids_type = b"bids".to_vec();
                let asks_type = b"asks".to_vec();
                let trades_type = b"trades".to_vec();
                let bids_bytes = bids_json.into_bytes();
                let asks_bytes = asks_json.into_bytes();
                let trades_bytes = trades_json.into_bytes();
                
                tokio::task::spawn_blocking(move || {
                    let socket_guard = socket_clone.blocking_lock();
                    // Send with topic prefix: [symbol, message_type, data]
                    if let Err(e) = socket_guard.send_multipart(&[&symbol_bytes, &bids_type, &bids_bytes], 0) {
                        warn!("ZeroMQ publish bids error: {}", e);
                    }
                    if let Err(e) = socket_guard.send_multipart(&[&symbol_bytes, &asks_type, &asks_bytes], 0) {
                        warn!("ZeroMQ publish asks error: {}", e);
                    }
                    if let Err(e) = socket_guard.send_multipart(&[&symbol_bytes, &trades_type, &trades_bytes], 0) {
                        warn!("ZeroMQ publish trades error: {}", e);
                    }
                }).await;
            }
        }
    }
}

const WINDOW_NAME_MAX: usize = 64;
const GRID_CELL_CAP: usize = 200;
const SNAPSHOT_ROW_CAP: i64 = 500_000;
const CALIBRATION_RESULTS_DIR: &str = "calibration/results";

#[derive(Debug)]
enum CalibrateHttpError {
    WindowNameRequired,            // E1
    WindowNameNotFileSafe,         // E2
    BoundsRequired,                // E3
    BoundsNotIntegers,             // E4
    StartAfterEnd,                 // E5
    GridTooLarge { n: usize },     // E6
    NoSnapshots,                   // E7
    WindowTooLarge { n: i64 },     // E8
    KdbUnavailable,                // E9
    RecordFailed,                  // E10
    AlreadyRunning,                // E11
    GridNotFinite,                 // E12
}

impl CalibrateHttpError {
    fn status_code(&self) -> StatusCode {
        match self {
            CalibrateHttpError::KdbUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            CalibrateHttpError::RecordFailed => StatusCode::INTERNAL_SERVER_ERROR,
            _ => StatusCode::BAD_REQUEST,
        }
    }

    fn json_body(&self) -> String {
        let (error, hint) = match self {
            CalibrateHttpError::WindowNameRequired => (
                "window_name is required".to_string(),
                "Pass window_name as a file-safe label (letters, digits, . _ -).".to_string(),
            ),
            CalibrateHttpError::WindowNameNotFileSafe => (
                "window_name is not file-safe".to_string(),
                "Use only [A-Za-z0-9._-], length 1–64, not \".\" or \"..\".".to_string(),
            ),
            CalibrateHttpError::BoundsRequired => (
                "start_ts and end_ts are required".to_string(),
                "Pass both as integer Unix milliseconds.".to_string(),
            ),
            CalibrateHttpError::BoundsNotIntegers => (
                "start_ts and end_ts must be integers".to_string(),
                "Use millisecond epoch values that fit kdb timestamp conversion.".to_string(),
            ),
            CalibrateHttpError::StartAfterEnd => (
                "start_ts must be <= end_ts".to_string(),
                "The window is a closed [start_ts, end_ts] interval.".to_string(),
            ),
            CalibrateHttpError::GridTooLarge { n } => (
                format!("Grid is too large ({} combinations)", n),
                "Each axis is at most 200 values; the product of the four axes must be <= 200.".to_string(),
            ),
            CalibrateHttpError::NoSnapshots => (
                "No snapshots in this window".to_string(),
                "Check symbol and bounds against kdb orderbook_snapshots. The early-September 2026 ticks.csv hole is a source gap.".to_string(),
            ),
            CalibrateHttpError::WindowTooLarge { n } => (
                format!("Window is too large ({} rows)", n),
                "Narrow start_ts/end_ts so the snapshot count is <= 500000.".to_string(),
            ),
            CalibrateHttpError::KdbUnavailable => (
                "kdb+ is unavailable".to_string(),
                "Start kdb (q kdb/start_kdb_server.q) and confirm KDB_HOST/KDB_PORT. Calibration does not fall back to CSV.".to_string(),
            ),
            CalibrateHttpError::RecordFailed => (
                "Failed to record calibration results".to_string(),
                "Check that the process can create calibration/results in its working directory.".to_string(),
            ),
            CalibrateHttpError::AlreadyRunning => (
                "Calibration already running".to_string(),
                "Wait for the in-flight GET to finish. Only one calibration runs at a time.".to_string(),
            ),
            CalibrateHttpError::GridNotFinite => (
                "Grid values must be finite numbers".to_string(),
                "Each CSV token must parse as a finite f64. Bad tokens are not skipped.".to_string(),
            ),
        };
        match self {
            CalibrateHttpError::GridTooLarge { n } => serde_json::json!({
                "error": error,
                "hint": hint,
                "grid_product": n,
            }),
            CalibrateHttpError::WindowTooLarge { n } => serde_json::json!({
                "error": error,
                "hint": hint,
                "n": n,
            }),
            _ => serde_json::json!({
                "error": error,
                "hint": hint,
            }),
        }
        .to_string()
    }
}

fn calibrate_http_reply(
    err: CalibrateHttpError,
) -> (StatusCode, [(axum::http::header::HeaderName, &'static str); 1], String) {
    (
        err.status_code(),
        [(axum::http::header::CONTENT_TYPE, "application/json")],
        err.json_body(),
    )
}

/// Ok(name) if non-empty, not "."/"..", len<=64, chars in [A-Za-z0-9._-].
fn sanitize_window_name(raw: &str) -> Result<&str, CalibrateHttpError> {
    if raw.is_empty() {
        return Err(CalibrateHttpError::WindowNameRequired);
    }
    if raw == "." || raw == ".." || raw.len() > WINDOW_NAME_MAX {
        return Err(CalibrateHttpError::WindowNameNotFileSafe);
    }
    if !raw
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
    {
        return Err(CalibrateHttpError::WindowNameNotFileSafe);
    }
    Ok(raw)
}

/// Returns ("calibration/results/{name}.json", "calibration/results/{name}.md").
/// FORBIDDEN: Path::join / PathBuf::push of operator input.
fn calibration_result_paths(name: &str) -> (String, String) {
    (
        format!("{}/{}.json", CALIBRATION_RESULTS_DIR, name),
        format!("{}/{}.md", CALIBRATION_RESULTS_DIR, name),
    )
}

/// kdb_ns = unix_ms * 1_000_000 - KDB_TIMESTAMP_OFFSET_NS, checked.
fn unix_ms_to_kdb_ns(ms: i64) -> Result<i64, CalibrateHttpError> {
    kdb_query::unix_ms_to_kdb_ns(ms).ok_or(CalibrateHttpError::BoundsNotIntegers)
}

/// The ONLY q timestamp fragment. `n` is the checked i64, never the query string.
fn kdb_ts_atom(n: i64) -> String {
    kdb_query::kdb_ts_atom(n)
}

fn parse_csv_f64_bounded(
    raw: Option<&str>,
    fallback: f64,
) -> Result<Vec<f64>, CalibrateHttpError> {
    let Some(raw) = raw.filter(|s| !s.trim().is_empty()) else {
        return Ok(vec![fallback]);
    };
    let mut out = Vec::new();
    for token in raw.split(',') {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        let value: f64 = token
            .parse()
            .map_err(|_| CalibrateHttpError::GridNotFinite)?;
        if !value.is_finite() {
            return Err(CalibrateHttpError::GridNotFinite);
        }
        out.push(value);
        if out.len() > GRID_CELL_CAP {
            return Err(CalibrateHttpError::GridTooLarge { n: out.len() });
        }
    }
    if out.is_empty() {
        Ok(vec![fallback])
    } else {
        Ok(out)
    }
}

fn grid_product(
    maker: &[f64],
    taker: &[f64],
    spread: &[f64],
    jump: &[f64],
) -> Result<usize, CalibrateHttpError> {
    let product = maker
        .len()
        .checked_mul(taker.len())
        .and_then(|x| x.checked_mul(spread.len()))
        .and_then(|x| x.checked_mul(jump.len()));
    match product {
        None => Err(CalibrateHttpError::GridTooLarge { n: usize::MAX }),
        Some(p) if p > GRID_CELL_CAP => Err(CalibrateHttpError::GridTooLarge { n: p }),
        Some(p) => Ok(p),
    }
}

fn snapshots_from_kdb_json(raw_json: &str) -> Vec<algo::Snapshot> {
    kdb_query::snapshots_from_kdb_json(raw_json)
}

fn parse_kdb_count_c(raw_json: &str) -> Option<i64> {
    let v: serde_json::Value = serde_json::from_str(raw_json).ok()?;
    let row = v.as_array()?.first()?;
    row.get("c").and_then(|c| {
        c.as_i64()
            .or_else(|| c.as_u64().and_then(|u| i64::try_from(u).ok()))
            .or_else(|| c.as_f64().and_then(|f| if f.is_finite() { Some(f as i64) } else { None }))
    })
}

#[derive(Debug, Clone, serde::Serialize)]
struct CalibrationReport {
    window_name: String,
    symbol: String,
    start_ts: i64,
    end_ts: i64,
    snapshot_count: usize,
    trades_coverage_count: u64,
    observed_min_time_ms: i64,
    observed_max_time_ms: i64,
    old_params: algo::CalibratedParams,
    new_params: algo::CalibratedParams,
    old_spread_capture: algo::BacktestSummary,
    old_market_imbalance: algo::BacktestSummary,
    new_spread_capture: algo::BacktestSummary,
    new_market_imbalance: algo::BacktestSummary,
    grid_product: usize,
    held_constant: algo::HeldConstant,
}

fn md_escape(s: &str) -> String {
    s.replace('|', "\\|")
        .replace('`', "\\`")
        .replace('\n', " ")
        .replace('\r', " ")
}

fn render_summary_bullets(s: &algo::BacktestSummary) -> String {
    format!(
        "- total_trades: {}\n\
         - winning_trades: {}\n\
         - losing_trades: {}\n\
         - win_rate_pct: {}\n\
         - total_pnl: {}\n\
         - max_drawdown: {}\n\
         - sharpe_ratio: {}\n\
         - sortino_ratio: {}\n\
         - profit_factor: {}\n\
         - avg_trade_duration_ms: {}\n\
         - max_consecutive_losses: {}\n\
         - final_position: {}\n\
         - snapshots_processed: {}",
        s.total_trades,
        s.winning_trades,
        s.losing_trades,
        s.win_rate_pct,
        s.total_pnl,
        s.max_drawdown,
        s.sharpe_ratio,
        s.sortino_ratio,
        s.profit_factor,
        s.avg_trade_duration_ms,
        s.max_consecutive_losses,
        s.final_position,
        s.snapshots_processed
    )
}

fn render_calibration_markdown(report: &CalibrationReport) -> String {
    let name = md_escape(&report.window_name);
    let symbol = md_escape(&report.symbol);
    format!(
        "# Calibration report: {name}\n\
         \n\
         ## Window\n\
         - symbol: `{symbol}`\n\
         - start_ts (ms): {start_ts}\n\
         - end_ts (ms): {end_ts}\n\
         - snapshot_count: {snapshot_count}\n\
         - trades_coverage_count: {trades_coverage_count}\n\
         - observed_min_time_ms: {observed_min_time_ms}\n\
         - observed_max_time_ms: {observed_max_time_ms}\n\
         - grid_product: {grid_product}\n\
         \n\
         ## Old parameters (in-code defaults)\n\
         | Field | Value |\n\
         |---|---|\n\
         | maker_fee_bps | {old_maker} |\n\
         | taker_fee_bps | {old_taker} |\n\
         | max_spread_frac | {old_spread} |\n\
         | max_mid_jump_frac | {old_jump} |\n\
         \n\
         ## New parameters (calibrated)\n\
         | Field | Value |\n\
         |---|---|\n\
         | maker_fee_bps | {new_maker} |\n\
         | taker_fee_bps | {new_taker} |\n\
         | max_spread_frac | {new_spread} |\n\
         | max_mid_jump_frac | {new_jump} |\n\
         \n\
         ## Held constant\n\
         | Field | Value |\n\
         |---|---|\n\
         | order_qty | {order_qty} |\n\
         | max_position | {max_position} |\n\
         | tick_size | {tick_size} |\n\
         | min_spread_bps | {min_spread_bps} |\n\
         | imbalance_threshold | {imbalance_threshold} |\n\
         | imbalance_depth_levels | {imbalance_depth_levels} |\n\
         | slippage_bps | {slippage_bps} |\n\
         \n\
         ## Old backtest summaries\n\
         ### SpreadCapture\n\
         {old_sc}\n\
         \n\
         ### MarketImbalance\n\
         {old_mi}\n\
         \n\
         ## New backtest summaries\n\
         ### SpreadCapture\n\
         {new_sc}\n\
         \n\
         ### MarketImbalance\n\
         {new_mi}\n",
        start_ts = report.start_ts,
        end_ts = report.end_ts,
        snapshot_count = report.snapshot_count,
        trades_coverage_count = report.trades_coverage_count,
        observed_min_time_ms = report.observed_min_time_ms,
        observed_max_time_ms = report.observed_max_time_ms,
        grid_product = report.grid_product,
        old_maker = report.old_params.maker_fee_bps,
        old_taker = report.old_params.taker_fee_bps,
        old_spread = report.old_params.max_spread_frac,
        old_jump = report.old_params.max_mid_jump_frac,
        new_maker = report.new_params.maker_fee_bps,
        new_taker = report.new_params.taker_fee_bps,
        new_spread = report.new_params.max_spread_frac,
        new_jump = report.new_params.max_mid_jump_frac,
        order_qty = report.held_constant.order_qty,
        max_position = report.held_constant.max_position,
        tick_size = report.held_constant.tick_size,
        min_spread_bps = report.held_constant.min_spread_bps,
        imbalance_threshold = report.held_constant.imbalance_threshold,
        imbalance_depth_levels = report.held_constant.imbalance_depth_levels,
        slippage_bps = report.held_constant.slippage_bps,
        old_sc = render_summary_bullets(&report.old_spread_capture),
        old_mi = render_summary_bullets(&report.old_market_imbalance),
        new_sc = render_summary_bullets(&report.new_spread_capture),
        new_mi = render_summary_bullets(&report.new_market_imbalance),
    )
}

fn write_calibration_artifacts(report: &CalibrationReport) -> Result<(), CalibrateHttpError> {
    std::fs::create_dir_all(CALIBRATION_RESULTS_DIR)
        .map_err(|_| CalibrateHttpError::RecordFailed)?;
    let json_bytes =
        serde_json::to_vec(report).map_err(|_| CalibrateHttpError::RecordFailed)?;
    let md = render_calibration_markdown(report);
    let (json_path, md_path) = calibration_result_paths(&report.window_name);
    let json_tmp = format!("{}.tmp", json_path);
    let md_tmp = format!("{}.tmp", md_path);
    std::fs::write(&json_tmp, &json_bytes).map_err(|_| CalibrateHttpError::RecordFailed)?;
    std::fs::rename(&json_tmp, &json_path).map_err(|_| CalibrateHttpError::RecordFailed)?;
    std::fs::write(&md_tmp, md.as_bytes()).map_err(|_| CalibrateHttpError::RecordFailed)?;
    std::fs::rename(&md_tmp, &md_path).map_err(|_| CalibrateHttpError::RecordFailed)?;
    Ok(())
}

/// Web server for displaying order book on a webpage and API endpoints for console
/// Serves an HTML page with real-time order book updates
async fn order_book_web_server(
    order_session: Arc<FIXSession>,  // Order management session (has open_orders)
    market_data_session: Arc<FIXSession>,  // Market data session (has order books for web display)
    market_data_sessions: Vec<(Arc<FIXSession>, String, u16)>,  // All market data sessions for status API
    kdb_writer: Arc<KdbWriter>,  // kdb+ connection for historical queries
    algo: Arc<algo::TradingAlgo>,  // Trading algorithm (SpreadCapture — bound to /api/algo/*)
    algo_imbalance: Arc<algo::TradingAlgo>,  // MarketImbalance shadow — status only
    bind_addr: &str,
) -> Result<()> {
    use serde::Deserialize;
    
    #[derive(Deserialize)]
    struct SymbolQuery {
        symbol: Option<String>,
    }
    
    // HTML page with embedded JavaScript for real-time updates
    let html_page = include_str!("dashboard.html");

    // Shared state for API endpoints
    #[derive(Clone)]
    struct AppState {
        order_session: Arc<FIXSession>,  // Order management session (has open_orders)
        market_data_session: Arc<FIXSession>,  // Market data session (has order books)
        market_data_sessions: Vec<(Arc<FIXSession>, String, u16)>,
        all_symbols: Vec<String>,  // All symbols to subscribe to
        kdb: Arc<KdbWriter>,  // kdb+ connection for historical queries
        algo: Arc<algo::TradingAlgo>,  // Trading algorithm
        algo_imbalance: Arc<algo::TradingAlgo>,
        calibrate_lock: Arc<tokio::sync::Mutex<()>>,
    }
    
    let all_symbols_list = vec![
        "BTC_USDT".to_string(),
        "BTC_USD".to_string(),
        "BTC_EUR".to_string(),
        "SOL_BTC".to_string(),
        "XRP_BTC".to_string(),
        "DOGE_BTC".to_string(),
        "ADA_BTC".to_string(),
        "LINK_BTC".to_string(),
        "AVAX_BTC".to_string(),
        "LTC_BTC".to_string(),
        "ATOM_BTC".to_string(),
        "ALGO_BTC".to_string(),
    ];
    
    let app_state = AppState {
        order_session: Arc::clone(&order_session),
        market_data_session: Arc::clone(&market_data_session),
        market_data_sessions: market_data_sessions.clone(),
        all_symbols: all_symbols_list.clone(),
        kdb: Arc::clone(&kdb_writer),
        algo: Arc::clone(&algo),
        algo_imbalance: Arc::clone(&algo_imbalance),
        calibrate_lock: Arc::new(tokio::sync::Mutex::new(())),
    };
    
    // API endpoint to get order book data
    async fn get_order_book_api(
        AxumPath(symbol): AxumPath<String>,
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> Result<Json<serde_json::Value>, StatusCode> {
        // Get order book snapshot from market data session (order books are stored there)
        let snapshot = state.market_data_session.get_order_book_snapshot(&symbol).await;
        
        if let Some(order_book) = snapshot {
            let best_bid = order_book.best_bid;
            let best_ask = order_book.best_ask;
            let spread = order_book.spread();
            
            // Get depth (top 20 levels)
            let bid_levels = order_book.get_depth(Side::Bid, 20);
            let ask_levels = order_book.get_depth(Side::Ask, 20);
            
            // Get recent trades
            let recent_trades = order_book.get_recent_trades(50);
            
            let response = serde_json::json!({
                "symbol": symbol,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread": spread,
                "bids": bid_levels.iter().map(|(p, s)| {
                    serde_json::json!({"price": p, "size": s})
                }).collect::<Vec<_>>(),
                "asks": ask_levels.iter().map(|(p, s)| {
                    serde_json::json!({"price": p, "size": s})
                }).collect::<Vec<_>>(),
                "trades": recent_trades.iter().map(|(p, s, ts)| {
                    serde_json::json!({"price": p, "size": s, "timestamp": ts})
                }).collect::<Vec<_>>(),
                "trade_count": order_book.trade_count(),
                "timestamp": chrono::Utc::now().timestamp_millis(),
            });
            
            Ok(Json(response))
        } else {
            // Return empty order book
            let response = serde_json::json!({
                "symbol": symbol,
                "best_bid": 0.0,
                "best_ask": 0.0,
                "spread": 0.0,
                "bids": [],
                "asks": [],
                "trades": [],
                "trade_count": 0,
                "timestamp": chrono::Utc::now().timestamp_millis(),
            });
            Ok(Json(response))
        }
    }
    
    // API endpoint to list available symbols
    async fn list_symbols_api(
        axum::extract::State(_state): axum::extract::State<AppState>,
    ) -> Result<Json<serde_json::Value>, StatusCode> {
        // Return all subscribed symbols (even if they haven't received data yet)
        // This ensures the web interface shows all available markets
        let subscribed_symbols = vec![
            "BTC_USDT".to_string(),
            "BTC_USD".to_string(),
            "BTC_EUR".to_string(),
            "SOL_BTC".to_string(),
            "XRP_BTC".to_string(),
            "DOGE_BTC".to_string(),
            "ADA_BTC".to_string(),
            "LINK_BTC".to_string(),
            "AVAX_BTC".to_string(),
            "LTC_BTC".to_string(),
            "ATOM_BTC".to_string(),
            "ALGO_BTC".to_string(),
        ];
        
        Ok(Json(serde_json::json!({
            "symbols": subscribed_symbols
        })))
    }
    
    // API endpoint to get open orders (for console)
    async fn get_open_orders_api(
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> Result<Json<serde_json::Value>, StatusCode> {
        let open_orders = state.order_session.open_orders.get_open_orders();
        let orders_json: Vec<serde_json::Value> = open_orders.iter().map(|order| {
            serde_json::json!({
                "cl_ord_id": order.cl_ord_id,
                "order_id": order.order_id,
                "exec_id": order.exec_id,
                "symbol": order.symbol,
                "side": order.side,
                "order_qty": order.order_qty,
                "price": order.price,
                "ord_status": order.ord_status,
                "exec_type": order.exec_type,
                "cum_qty": order.cum_qty,
                "leaves_qty": order.leaves_qty,
                "last_px": order.last_px,
                "last_qty": order.last_qty,
                "avg_px": order.avg_px,
                "transact_time": order.transact_time,
                "order_create_time": order.order_create_time,
                "rejection_reason": order.rejection_reason,
            })
        }).collect();
        
        Ok(Json(serde_json::json!({
            "orders": orders_json,
            "count": open_orders.len()
        })))
    }
    
    // API endpoint to get all market data server statuses (for console)
    async fn get_all_market_data_status_api(
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> Result<Json<serde_json::Value>, StatusCode> {
        let mut statuses = Vec::with_capacity(state.market_data_sessions.len());
        for (session, description, port) in &state.market_data_sessions {
            statuses.push(market_data_session_status(session, description, *port));
        }
        Ok(Json(serde_json::json!({ "servers": statuses })))
    }
    
    // API endpoint to subscribe to all symbols on a specific server
    async fn subscribe_all_symbols_api(
        AxumPath(port_str): AxumPath<String>,
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> Result<Json<serde_json::Value>, StatusCode> {
        let port: u16 = match port_str.parse() {
            Ok(p) => p,
            Err(_) => {
                return Ok(Json(serde_json::json!({
                    "error": format!("Invalid port: {}", port_str)
                })));
            }
        };
        
        // Find the session for this port
        let session_opt = state.market_data_sessions.iter()
            .find(|(_, _, p)| *p == port)
            .map(|(s, _, _)| Arc::clone(s));
        
        if let Some(session) = session_opt {
            if !session.is_ready().await {
                return Ok(Json(serde_json::json!({
                    "error": format!("Server on port {} is not ready", port)
                })));
            }
            
            let mut subscribed = Vec::new();
            let mut failed = Vec::new();
            
            // Subscribe to all symbols
            for symbol in &state.all_symbols {
                match session.market_data_request(symbol, 50).await {
                    Ok(_) => {
                        subscribed.push(symbol.clone());
                        // Small delay between requests
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                    Err(e) => {
                        failed.push(serde_json::json!([symbol.clone(), e.to_string()]));
                    }
                }
            }
            
            Ok(Json(serde_json::json!({
                "port": port,
                "subscribed": subscribed.len(),
                "failed": failed.len(),
                "subscribed_symbols": subscribed,
                "failed_symbols": failed
            })))
        } else {
            Ok(Json(serde_json::json!({
                "error": format!("No server found on port {}", port)
            })))
        }
    }
    
    // Build a WHERE predicate that safely handles symbols with punctuation by
    // constructing a symbol atom via `$"..."` instead of raw backtick literals.
    fn build_kdb_symbol_predicate(symbol: &str) -> String {
        kdb_query::kdb_symbol_predicate(symbol)
    }

    // GET /api/history/:symbol — last 500 best-bid/ask snapshots for a symbol (from kdb+)
    async fn get_history_api(
        AxumPath(symbol): AxumPath<String>,
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> impl axum::response::IntoResponse {
        use axum::http::header;
        // Use binary IPC to avoid .j.j 'type errors with timestamp$ columns.
        // The K table is converted to JSON on the Rust side; "time" comes out as Unix ms integer.
        let symbol_predicate = build_kdb_symbol_predicate(&symbol);
        let q = format!(
            "neg[500] sublist select from orderbook_snapshots where {}",
            symbol_predicate
        );
        match state.kdb.query_table_to_json(&q).await {
            Ok(json) => (StatusCode::OK, [(header::CONTENT_TYPE, "application/json")], json),
            Err(e) => {
                let payload = serde_json::json!({
                    "error": format!("kdb+ query failed: {}", e),
                    "context": {
                        "symbol": symbol,
                        "where": symbol_predicate
                    }
                });
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    [(header::CONTENT_TYPE, "application/json")],
                    payload.to_string(),
                )
            }
        }
    }

    // GET /api/securities — all securities from kdb+
    async fn get_securities_api(
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> impl axum::response::IntoResponse {
        use axum::http::header;
        // Deduplicate: keep only the last record per symbol
        let q = "select last time, last security_id, last exchange, last attributes_json by symbol from securities";
        match state.kdb.query_json(q).await {
            Ok(raw_json) => {
                // .j.j on a keyed table returns an object; unwrap into array of records
                let securities_json = match serde_json::from_str::<serde_json::Value>(&raw_json) {
                    Ok(v) => {
                        // Keyed table result: object whose keys are symbols and values are row objects
                        let arr: Vec<serde_json::Value> = if let Some(obj) = v.as_object() {
                            obj.iter().map(|(sym, row)| {
                                let mut rec = row.clone();
                                if let Some(m) = rec.as_object_mut() {
                                    m.insert("symbol".to_string(), serde_json::Value::String(sym.clone()));
                                }
                                rec
                            }).collect()
                        } else if let Some(arr) = v.as_array() {
                            arr.clone()
                        } else {
                            vec![v]
                        };
                        serde_json::json!({ "securities": arr, "count": arr.len() })
                    }
                    Err(_) => serde_json::json!({ "securities": [], "error": raw_json }),
                };
                (StatusCode::OK, [(header::CONTENT_TYPE, "application/json")],
                 securities_json.to_string())
            }
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, [(header::CONTENT_TYPE, "application/json")],
                       format!("{{\"error\":\"{}\"}}", e)),
        }
    }

    // GET /api/orders/history — execution reports from orders/processed/*.json
    async fn get_orders_history_api(
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> Result<Json<serde_json::Value>, StatusCode> {
        // Fall back to open orders store for current orders
        let open_orders = state.order_session.open_orders.get_open_orders();
        let mut orders: Vec<serde_json::Value> = open_orders.iter().map(|o| serde_json::json!({
            "cl_ord_id": o.cl_ord_id, "order_id": o.order_id, "symbol": o.symbol,
            "side": o.side, "order_qty": o.order_qty, "price": o.price,
            "ord_status": o.ord_status, "exec_type": o.exec_type,
            "cum_qty": o.cum_qty, "leaves_qty": o.leaves_qty,
            "avg_px": o.avg_px, "transact_time": o.transact_time,
            "order_create_time": o.order_create_time, "rejection_reason": o.rejection_reason,
        })).collect();

        // Also load saved JSON files from orders/processed/
        if let Ok(mut dir) = tokio::fs::read_dir("orders/processed").await {
            while let Ok(Some(entry)) = dir.next_entry().await {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) == Some("json") {
                    if let Ok(contents) = tokio::fs::read_to_string(&path).await {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&contents) {
                            // Extract the execution_report section if present
                            let record = v.get("execution_report").cloned().unwrap_or(v);
                            orders.push(record);
                        }
                    }
                }
            }
        }

        Ok(Json(serde_json::json!({ "orders": orders, "count": orders.len() })))
    }

    // GET /api/positions — live position snapshot with unrealized P&L from order book
    async fn get_positions_api(
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> Result<Json<serde_json::Value>, StatusCode> {
        let mut positions = state.order_session.positions.snapshot();

        // Refresh unrealized P&L from live order book mid prices
        {
            let books = state.market_data_session.order_books.lock().await;
            for pos in positions.iter_mut() {
                if let Some(db) = books.get(&pos.symbol) {
                    let snap = db.get_snapshot();
                    let mid = snap.mid_price();
                    pos.refresh_unrealized(mid);
                }
            }
        }

        // Sort by absolute net_qty descending
        positions.sort_by(|a, b| b.net_qty.abs().partial_cmp(&a.net_qty.abs()).unwrap_or(std::cmp::Ordering::Equal));

        let positions_json: Vec<serde_json::Value> = positions.iter().map(|p| {
            serde_json::json!({
                "symbol": p.symbol,
                "net_qty": p.net_qty,
                "avg_entry_px": p.avg_entry_px,
                "realized_pnl": p.realized_pnl,
                "unrealized_pnl": p.unrealized_pnl,
                "last_mid_px": p.last_mid_px,
                "trade_count": p.trade_count
            })
        }).collect();

        Ok(Json(serde_json::json!({
            "positions": positions_json,
            "count": positions_json.len()
        })))
    }

    // GET /api/algo/status — current algo configuration and stats
    async fn get_algo_status_api(
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> Json<serde_json::Value> {
        let status = state.algo.status().await;
        Json(serde_json::to_value(&status).unwrap_or_default())
    }

    // POST /api/algo/enable — enable the trading algo
    async fn algo_enable_api(
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> Json<serde_json::Value> {
        let mut cfg = state.algo.config.lock().await.clone();
        cfg.enabled = true;
        state.algo.update_config(cfg).await;
        Json(serde_json::json!({"ok": true, "enabled": true}))
    }

    // POST /api/algo/disable — disable the trading algo
    async fn algo_disable_api(
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> Json<serde_json::Value> {
        let mut cfg = state.algo.config.lock().await.clone();
        cfg.enabled = false;
        state.algo.update_config(cfg).await;
        Json(serde_json::json!({"ok": true, "enabled": false}))
    }

    async fn operator_halt_trigger_api(
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> Json<serde_json::Value> {
        let r = state.order_session.trigger_operator_halt().await;
        Json(serde_json::json!({
            "ok": true,
            "halted": r.halted,
            "cancel_attempted": r.cancel_attempted,
            "cancel_ok": r.cancel_ok,
            "cancel_failed": r.cancel_failed,
        }))
    }

    async fn operator_halt_reset_api(
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> Json<serde_json::Value> {
        state.order_session.reset_operator_halt();
        Json(serde_json::json!({ "ok": true, "halted": false }))
    }

    async fn operator_halt_status_api(
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> Json<serde_json::Value> {
        Json(serde_json::json!({
            "halted": state.order_session.operator_halt_is_set()
        }))
    }

    // POST /api/algo/config — update algo config fields
    async fn algo_config_api(
        axum::extract::State(state): axum::extract::State<AppState>,
        Json(new_cfg): Json<algo::AlgoConfig>,
    ) -> Json<serde_json::Value> {
        state.algo.update_config(new_cfg).await;
        Json(serde_json::json!({"ok": true}))
    }

    // GET /api/shadow/status — paper P&L for both shadow instances. No POST.
    async fn get_shadow_status_api(
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> axum::Json<algo::ShadowStatus> {
        axum::Json(algo::ShadowStatus {
            spread_capture: state.algo.status().await,
            market_imbalance: state.algo_imbalance.status().await,
        })
    }

    // Load historical snapshots from data/snapshots.csv.
    // Used as fallback when KDB+ has no in-memory snapshot data.
    // CSV columns: timestamp_ms,symbol,best_bid,best_ask
    fn load_snapshots_from_csv(symbol: &str, limit: usize) -> Vec<algo::Snapshot> {
        use std::io::BufRead;
        let file = match std::fs::File::open("data/snapshots.csv") {
            Ok(f) => f,
            Err(_) => return Vec::new(),
        };
        let reader = std::io::BufReader::new(file);
        let mut snapshots: Vec<algo::Snapshot> = Vec::new();
        let mut first_line = true;

        for line in reader.lines().flatten() {
            if first_line {
                first_line = false;
                continue; // skip header
            }
            // Support both old 4-column format and new 6-column format with sizes
            let cols: Vec<&str> = line.splitn(6, ',').collect();
            if cols.len() < 4 {
                continue;
            }
            if cols[1] != symbol {
                continue;
            }
            let timestamp_ms: i64 = cols[0].parse().unwrap_or(0);
            let best_bid: f64     = cols[2].parse().unwrap_or(0.0);
            let best_ask: f64     = cols[3].parse().unwrap_or(0.0);
            let best_bid_size: f64 = cols.get(4).and_then(|v| v.parse().ok()).unwrap_or(0.0);
            let best_ask_size: f64 = cols.get(5).and_then(|v| v.parse().ok()).unwrap_or(0.0);
            if best_bid > 0.0 && best_ask > 0.0 && best_ask > best_bid {
                snapshots.push(algo::Snapshot { timestamp_ms, best_bid, best_ask, best_bid_size, best_ask_size, ..Default::default() });
            }
        }

        // Return the most recent `limit` rows (CSV is chronological).
        if snapshots.len() > limit {
            snapshots.drain(..snapshots.len() - limit);
        }
        snapshots
    }

    // GET /api/backtest — run backtest simulation against historical kdb+ snapshots
    // Query params: symbol, limit, min_spread_bps, order_qty, max_position, tick_size
    async fn get_backtest_api(
        axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> impl axum::response::IntoResponse {
        use axum::http::header;

        let symbol = params.get("symbol").cloned().unwrap_or_else(|| "BTC_USDT".to_string());
        let limit: usize = params
            .get("limit")
            .and_then(|v| v.parse().ok())
            .unwrap_or(5000)
            .clamp(1, 100_000);
        let min_spread_bps: f64 = params.get("min_spread_bps").and_then(|v| v.parse().ok()).unwrap_or(5.0);
        let order_qty: f64 = params.get("order_qty").and_then(|v| v.parse().ok()).unwrap_or(0.001);
        let max_position: f64 = params.get("max_position").and_then(|v| v.parse().ok()).unwrap_or(0.01);
        let tick_size: f64 = params.get("tick_size").and_then(|v| v.parse().ok()).unwrap_or(0.01);
        let imbalance_threshold: f64 = params.get("imbalance_threshold").and_then(|v| v.parse().ok()).unwrap_or(algo::default_imbalance_threshold());
        let imbalance_depth_levels: usize = params.get("imbalance_depth_levels").and_then(|v| v.parse().ok()).unwrap_or(algo::default_imbalance_depth_levels());
        let slippage_bps: f64 = params.get("slippage_bps").and_then(|v| v.parse().ok()).unwrap_or(algo::default_slippage_bps());
        let fee_bps: f64 = params.get("fee_bps").and_then(|v| v.parse().ok()).unwrap_or(algo::default_fee_bps());
        let maker_fee_bps: f64 = params.get("maker_fee_bps").and_then(|v| v.parse().ok()).unwrap_or(algo::default_maker_fee_bps());
        let taker_fee_bps: f64 = params.get("taker_fee_bps").and_then(|v| v.parse().ok()).unwrap_or(fee_bps);
        let strategy = match params.get("strategy").map(|s| s.as_str()) {
            Some("market_imbalance") => algo::StrategyType::MarketImbalance,
            _ => algo::StrategyType::SpreadCapture,
        };

        // Build an AlgoConfig for this backtest run (enabled flag doesn't matter here).
        let cfg = algo::AlgoConfig {
            enabled: false,
            symbol: symbol.clone(),
            order_qty,
            min_spread_bps,
            max_position,
            poll_interval_ms: 1000,
            tick_size,
            time_in_force: "3".to_string(),
            strategy,
            imbalance_threshold,
            imbalance_depth_levels,
            slippage_bps,
            fee_bps,
            maker_fee_bps,
            taker_fee_bps,
            max_spread_frac: algo::default_max_spread_frac(),
            max_mid_jump_frac: algo::default_max_mid_jump_frac(),
            min_signal_interval_ms: 0, // not relevant in backtest
            max_book_staleness_ms: 0,
            max_orders_session: 0,
            self_match_prevention_scope: None,
            self_match_prevention_instruction: None,
            self_match_prevention_id: None,
        };

        // Query kdb+ for historical snapshots. On any error or empty result, fall back
        // to data/ticks.csv so backtests work even when kdb+ has no in-memory data.
        let symbol_predicate = build_kdb_symbol_predicate(&symbol);
        let q = format!(
            "neg[{}] sublist select time, best_bid, best_ask, best_bid_size, best_ask_size from orderbook_snapshots where {}",
            limit, symbol_predicate
        );
        let kdb_snapshots: Vec<algo::Snapshot> = match state.kdb.query_table_to_json(&q).await {
            Err(e) => {
                info!("📊 kdb+ query error ({}), using CSV fallback", e);
                Vec::new()
            }
            Ok(raw_json) => snapshots_from_kdb_json(&raw_json),
        };

        // If KDB+ returned nothing (empty table or error), fall back to data/ticks.csv.
        let (snapshots, data_source) = if kdb_snapshots.is_empty() {
            let csv_snaps = load_snapshots_from_csv(&symbol, limit);
            (csv_snaps, "csv")
        } else {
            (kdb_snapshots, "kdb")
        };

        if snapshots.is_empty() {
            return (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                serde_json::json!({
                    "error": "No snapshot data found for this symbol",
                    "hint": "Run the server connected to live market data to accumulate snapshots. Data is auto-saved to data/ticks.csv every 60 seconds."
                }).to_string(),
            );
        }

        // Run the simulation
        info!("📊 Backtest: {} snapshots from {} for {}", snapshots.len(), data_source, symbol);
        let result = algo::run_backtest(&cfg, &snapshots);

        match serde_json::to_string(&result) {
            Ok(json) => (StatusCode::OK, [(header::CONTENT_TYPE, "application/json")], json),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(header::CONTENT_TYPE, "application/json")],
                format!("{{\"error\":\"Serialization failed: {}\"}}", e),
            ),
        }
    }

    // GET /api/backtest/sweep — run grid search over multiple AlgoConfig combinations.
    // Accepts comma-separated or repeated query params: imbalance_threshold, fee_bps.
    // Returns an array of { config, summary } sorted by Sharpe ratio (desc).
    // Capped at 200 combinations to prevent runaway CPU usage.
    async fn get_backtest_sweep_api(
        axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> impl axum::response::IntoResponse {
        use axum::http::header;

        let symbol = params.get("symbol").cloned().unwrap_or_else(|| "BTC_USDT".to_string());
        let limit: usize = params.get("limit").and_then(|v| v.parse().ok()).unwrap_or(5000).clamp(1, 50_000);
        let order_qty: f64 = params.get("order_qty").and_then(|v| v.parse().ok()).unwrap_or(0.001);
        let max_position: f64 = params.get("max_position").and_then(|v| v.parse().ok()).unwrap_or(0.01);
        let tick_size: f64 = params.get("tick_size").and_then(|v| v.parse().ok()).unwrap_or(0.01);
        let min_spread_bps_base: f64 = params.get("min_spread_bps").and_then(|v| v.parse().ok()).unwrap_or(5.0);

        // Parse comma-separated sweep arrays (e.g. imbalance_threshold=0.55,0.60,0.65)
        let parse_csv_f64 = |key: &str, fallback: f64| -> Vec<f64> {
            params.get(key)
                .map(|s| s.split(',').filter_map(|v| v.trim().parse().ok()).collect())
                .filter(|v: &Vec<f64>| !v.is_empty())
                .unwrap_or_else(|| vec![fallback])
        };
        let parse_csv_usize = |key: &str, fallback: usize| -> Vec<usize> {
            params.get(key)
                .map(|s| s.split(',').filter_map(|v| v.trim().parse().ok()).collect())
                .filter(|v: &Vec<usize>| !v.is_empty())
                .unwrap_or_else(|| vec![fallback])
        };

        let thresholds = parse_csv_f64("imbalance_threshold", algo::default_imbalance_threshold());
        let depths = parse_csv_usize("imbalance_depth_levels", algo::default_imbalance_depth_levels());
        let fee_bps_list = parse_csv_f64("fee_bps", algo::default_fee_bps());
        let slippage_bps_list = parse_csv_f64("slippage_bps", algo::default_slippage_bps());

        // Load data once, reuse across all runs
        let symbol_predicate = build_kdb_symbol_predicate(&symbol);
        let q = format!(
            "neg[{}] sublist select time, best_bid, best_ask, best_bid_size, best_ask_size from orderbook_snapshots where {}",
            limit, symbol_predicate
        );
        let snapshots: Vec<algo::Snapshot> = match state.kdb.query_table_to_json(&q).await {
            Ok(raw_json) => snapshots_from_kdb_json(&raw_json),
            Err(_) => load_snapshots_from_csv(&symbol, limit),
        };

        if snapshots.is_empty() {
            return (StatusCode::OK, [(header::CONTENT_TYPE, "application/json")],
                serde_json::json!({"error": "No snapshot data found for symbol", "symbol": symbol}).to_string());
        }

        // Build all combinations and run — cap at 200
        let mut results: Vec<serde_json::Value> = Vec::new();
        'outer: for &threshold in &thresholds {
            for &depth in &depths {
                for &fee in &fee_bps_list {
                    for &slip in &slippage_bps_list {
                        if results.len() >= 200 { break 'outer; }
                        let cfg = algo::AlgoConfig {
                            enabled: false,
                            symbol: symbol.clone(),
                            order_qty,
                            min_spread_bps: min_spread_bps_base,
                            max_position,
                            poll_interval_ms: 1000,
                            tick_size,
                            time_in_force: "3".to_string(),
                            strategy: algo::StrategyType::MarketImbalance,
                            imbalance_threshold: threshold,
                            imbalance_depth_levels: depth,
                            slippage_bps: slip,
                            fee_bps: fee,
                            maker_fee_bps: algo::default_maker_fee_bps(),
                            taker_fee_bps: fee,
                            max_spread_frac: algo::default_max_spread_frac(),
                            max_mid_jump_frac: algo::default_max_mid_jump_frac(),
                            min_signal_interval_ms: 0,
                            max_book_staleness_ms: 0,
                            max_orders_session: 0,
                            self_match_prevention_scope: None,
                            self_match_prevention_instruction: None,
                            self_match_prevention_id: None,
                        };
                        let result = algo::run_backtest(&cfg, &snapshots);
                        results.push(serde_json::json!({
                            "config": {
                                "imbalance_threshold": threshold,
                                "imbalance_depth_levels": depth,
                                "fee_bps": fee,
                                "slippage_bps": slip,
                            },
                            "summary": result.summary,
                        }));
                    }
                }
            }
        }

        // Sort by Sharpe ratio descending
        results.sort_by(|a, b| {
            let sa = a["summary"]["sharpe_ratio"].as_f64().unwrap_or(f64::NEG_INFINITY);
            let sb = b["summary"]["sharpe_ratio"].as_f64().unwrap_or(f64::NEG_INFINITY);
            sb.partial_cmp(&sa).unwrap_or(std::cmp::Ordering::Equal)
        });

        info!("📊 Sweep: {} runs on {} snapshots for {}", results.len(), snapshots.len(), symbol);
        match serde_json::to_string(&results) {
            Ok(json) => (StatusCode::OK, [(header::CONTENT_TYPE, "application/json")], json),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, [(header::CONTENT_TYPE, "application/json")],
                format!("{{\"error\":\"Serialization failed: {}\"}}", e)),
        }
    }

    // GET /api/backtest/calibrate — grid-search fee/threshold axes over a named kdb window.
    async fn get_backtest_calibrate_api(
        axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> impl axum::response::IntoResponse {
        use axum::http::header;

        let window_name = match params.get("window_name") {
            None => return calibrate_http_reply(CalibrateHttpError::WindowNameRequired),
            Some(raw) => match sanitize_window_name(raw) {
                Ok(n) => n.to_string(),
                Err(e) => return calibrate_http_reply(e),
            },
        };

        let parse_required_i64 = |key: &str| -> Result<i64, CalibrateHttpError> {
            match params.get(key) {
                None => Err(CalibrateHttpError::BoundsRequired),
                Some(s) if s.trim().is_empty() => Err(CalibrateHttpError::BoundsRequired),
                Some(s) => s
                    .parse::<i64>()
                    .map_err(|_| CalibrateHttpError::BoundsNotIntegers),
            }
        };
        let start_ts = match parse_required_i64("start_ts") {
            Ok(v) => v,
            Err(e) => return calibrate_http_reply(e),
        };
        let end_ts = match parse_required_i64("end_ts") {
            Ok(v) => v,
            Err(e) => return calibrate_http_reply(e),
        };
        if start_ts > end_ts {
            return calibrate_http_reply(CalibrateHttpError::StartAfterEnd);
        }
        let ts0 = match unix_ms_to_kdb_ns(start_ts) {
            Ok(v) => kdb_ts_atom(v),
            Err(e) => return calibrate_http_reply(e),
        };
        let ts1 = match unix_ms_to_kdb_ns(end_ts) {
            Ok(v) => kdb_ts_atom(v),
            Err(e) => return calibrate_http_reply(e),
        };

        let symbol = params
            .get("symbol")
            .cloned()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| "BTC_USDT".to_string());

        let makers = match parse_csv_f64_bounded(
            params.get("maker_fee_bps").map(|s| s.as_str()),
            algo::default_maker_fee_bps(),
        ) {
            Ok(v) => v,
            Err(e) => return calibrate_http_reply(e),
        };
        let takers = match parse_csv_f64_bounded(
            params.get("taker_fee_bps").map(|s| s.as_str()),
            algo::default_taker_fee_bps(),
        ) {
            Ok(v) => v,
            Err(e) => return calibrate_http_reply(e),
        };
        let spreads = match parse_csv_f64_bounded(
            params.get("max_spread_frac").map(|s| s.as_str()),
            algo::default_max_spread_frac(),
        ) {
            Ok(v) => v,
            Err(e) => return calibrate_http_reply(e),
        };
        let jumps = match parse_csv_f64_bounded(
            params.get("max_mid_jump_frac").map(|s| s.as_str()),
            algo::default_max_mid_jump_frac(),
        ) {
            Ok(v) => v,
            Err(e) => return calibrate_http_reply(e),
        };
        if let Err(e) = grid_product(&makers, &takers, &spreads, &jumps) {
            return calibrate_http_reply(e);
        }

        let order_qty: f64 = params
            .get("order_qty")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.001);
        let max_position: f64 = params
            .get("max_position")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.01);
        let tick_size: f64 = params
            .get("tick_size")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.01);
        let min_spread_bps: f64 = params
            .get("min_spread_bps")
            .and_then(|v| v.parse().ok())
            .unwrap_or(5.0);
        let imbalance_threshold: f64 = params
            .get("imbalance_threshold")
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(algo::default_imbalance_threshold);
        let imbalance_depth_levels: usize = params
            .get("imbalance_depth_levels")
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(algo::default_imbalance_depth_levels);
        let slippage_bps: f64 = params
            .get("slippage_bps")
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(algo::default_slippage_bps);

        let baseline = algo::AlgoConfig {
            enabled: false,
            symbol: symbol.clone(),
            order_qty,
            min_spread_bps,
            max_position,
            poll_interval_ms: 1000,
            tick_size,
            time_in_force: "3".to_string(),
            strategy: algo::StrategyType::SpreadCapture,
            imbalance_threshold,
            imbalance_depth_levels,
            slippage_bps,
            fee_bps: algo::default_fee_bps(),
            maker_fee_bps: algo::default_maker_fee_bps(),
            taker_fee_bps: algo::default_taker_fee_bps(),
            max_spread_frac: algo::default_max_spread_frac(),
            max_mid_jump_frac: algo::default_max_mid_jump_frac(),
            min_signal_interval_ms: 0,
            max_book_staleness_ms: 0,
            max_orders_session: 0,
            self_match_prevention_scope: None,
            self_match_prevention_instruction: None,
            self_match_prevention_id: None,
        };

        let Ok(_calibrate_guard) = state.calibrate_lock.try_lock() else {
            return calibrate_http_reply(CalibrateHttpError::AlreadyRunning);
        };

        let pred = build_kdb_symbol_predicate(&symbol);
        let q1 = format!(
            "select c:count i from orderbook_snapshots where {}, time within ({};{})",
            pred, ts0, ts1
        );
        let count = match state.kdb.query_table_to_json(&q1).await {
            Ok(raw) => match parse_kdb_count_c(&raw) {
                Some(c) => c,
                None => return calibrate_http_reply(CalibrateHttpError::KdbUnavailable),
            },
            Err(_) => return calibrate_http_reply(CalibrateHttpError::KdbUnavailable),
        };
        if count == 0 {
            return calibrate_http_reply(CalibrateHttpError::NoSnapshots);
        }
        if count > SNAPSHOT_ROW_CAP {
            return calibrate_http_reply(CalibrateHttpError::WindowTooLarge { n: count });
        }

        let q2 = format!(
            "select c:count i from trades where {}, time within ({};{})",
            pred, ts0, ts1
        );
        let trades_coverage_count: u64 = match state.kdb.query_table_to_json(&q2).await {
            Ok(raw) => parse_kdb_count_c(&raw)
                .and_then(|c| u64::try_from(c).ok())
                .unwrap_or(0),
            Err(_) => 0,
        };

        let q3 = format!(
            "select time, best_bid, best_ask, best_bid_size, best_ask_size from orderbook_snapshots where {}, time within ({};{})",
            pred, ts0, ts1
        );
        let snapshots = match state.kdb.query_table_to_json(&q3).await {
            Ok(raw) => snapshots_from_kdb_json(&raw),
            Err(_) => return calibrate_http_reply(CalibrateHttpError::KdbUnavailable),
        };
        if snapshots.is_empty() {
            return calibrate_http_reply(CalibrateHttpError::NoSnapshots);
        }

        let snapshot_count = snapshots.len();
        let observed_min_time_ms = snapshots
            .iter()
            .map(|s| s.timestamp_ms)
            .min()
            .unwrap_or(start_ts);
        let observed_max_time_ms = snapshots
            .iter()
            .map(|s| s.timestamp_ms)
            .max()
            .unwrap_or(end_ts);

        let window_name_for_cal = window_name.clone();
        let cal_result = match tokio::task::spawn_blocking(move || {
            algo::calibrate(
                &snapshots,
                &window_name_for_cal,
                &makers,
                &takers,
                &spreads,
                &jumps,
                &baseline,
            )
        })
        .await
        {
            Ok(Ok(r)) => r,
            Ok(Err(algo::CalibrateError::GridTooLarge { product })) => {
                return calibrate_http_reply(CalibrateHttpError::GridTooLarge { n: product });
            }
            Ok(Err(algo::CalibrateError::EmptyAxis | algo::CalibrateError::NonFiniteAxis)) => {
                return calibrate_http_reply(CalibrateHttpError::GridNotFinite);
            }
            Err(_) => return calibrate_http_reply(CalibrateHttpError::RecordFailed),
        };

        let report = CalibrationReport {
            window_name: cal_result.window_name,
            symbol,
            start_ts,
            end_ts,
            snapshot_count,
            trades_coverage_count,
            observed_min_time_ms,
            observed_max_time_ms,
            old_params: cal_result.old_params,
            new_params: cal_result.new_params,
            old_spread_capture: cal_result.old_spread_capture,
            old_market_imbalance: cal_result.old_market_imbalance,
            new_spread_capture: cal_result.new_spread_capture,
            new_market_imbalance: cal_result.new_market_imbalance,
            grid_product: cal_result.grid_product,
            held_constant: cal_result.held_constant,
        };

        let report_for_write = report.clone();
        match tokio::task::spawn_blocking(move || write_calibration_artifacts(&report_for_write))
            .await
        {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return calibrate_http_reply(e),
            Err(_) => return calibrate_http_reply(CalibrateHttpError::RecordFailed),
        }

        match serde_json::to_string(&report) {
            Ok(json) => (StatusCode::OK, [(header::CONTENT_TYPE, "application/json")], json),
            Err(_) => calibrate_http_reply(CalibrateHttpError::RecordFailed),
        }
    }

    // Build the router
    let html_page_clone = html_page.to_string();
    let app = Router::new()
        .route("/", get(move || {
            let html = html_page_clone.clone();
            async move {
                let mut headers = axum::http::HeaderMap::new();
                headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store, no-cache, must-revalidate"));
                (headers, Html(html))
            }
        }))
        .route("/api/orderbook/:symbol", get(get_order_book_api))
        .route("/api/symbols", get(list_symbols_api))
        .route("/api/open-orders", get(get_open_orders_api))
        .route("/api/market-data-status", get(get_all_market_data_status_api))
        .route("/api/subscribe-all-symbols/:port", post(subscribe_all_symbols_api))
        .route("/api/history/:symbol", get(get_history_api))
        .route("/api/securities", get(get_securities_api))
        .route("/api/orders/history", get(get_orders_history_api))
        .route("/api/positions", get(get_positions_api))
        .route("/api/algo/status", get(get_algo_status_api))
        .route("/api/algo/enable", post(algo_enable_api))
        .route("/api/algo/disable", post(algo_disable_api))
        .route("/api/algo/config", post(algo_config_api))
        .route("/api/kill-switch", post(operator_halt_trigger_api))
        .route("/api/kill-switch/reset", post(operator_halt_reset_api))
        .route("/api/kill-switch/status", get(operator_halt_status_api))
        .route("/api/shadow/status", get(get_shadow_status_api))
        .route("/api/backtest", get(get_backtest_api))
        .route("/api/backtest/sweep", get(get_backtest_sweep_api))
        .route("/api/backtest/calibrate", get(get_backtest_calibrate_api))
        .layer(CorsLayer::permissive())
        .with_state(app_state);
    
    // Parse bind address
    use std::net::SocketAddr;
    let addr: SocketAddr = bind_addr.parse().map_err(|e| anyhow!("Invalid bind address {}: {}", bind_addr, e))?;
    
    info!("🌐 Order Book Web Server starting...");
    info!("   Bind address: http://{}", bind_addr);
    info!("   Open http://{} in your browser to view the order book", addr);
    
    let listener = tokio::net::TcpListener::bind(&addr).await
        .map_err(|e| anyhow!("Failed to bind to {}: {}", addr, e))?;
    
    axum::serve(listener, app).await
        .map_err(|e| anyhow!("Web server error: {}", e))?;
    
    Ok(())
}

/// Management Console - Interactive menu for server management
async fn management_console(
    session: Arc<FIXSession>,
    market_data_sessions: Vec<(Arc<FIXSession>, String, u16)>, // (session, description, port)
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
) -> Result<()> {
    use tokio::io::{AsyncBufReadExt, BufReader};
    
    let stdin = tokio::io::stdin();
    let reader = BufReader::new(stdin);
    let mut lines = reader.lines();
    
    loop {
        // Clear screen and display menu
        print!("\x1B[2J\x1B[1;1H"); // ANSI escape codes to clear screen and move cursor to top
        println!("╔════════════════════════════════════════════════════════════╗");
        println!("║           FIX Server Management Console                    ║");
        println!("╚════════════════════════════════════════════════════════════╝");
        println!();
        println!("Menu Options:");
        println!("  1 -> Trade Table");
        println!("  2 -> Debug Market Data Servers");
        println!("  99 -> Exit");
        println!();
        print!("Select option: ");
        std::io::stdout().flush().map_err(|e| anyhow!("Failed to flush stdout: {}", e))?;
        
        // Wait for either user input or shutdown signal
        tokio::select! {
            line_result = lines.next_line() => {
                match line_result {
                    Ok(Some(line)) => {
                        let input = line.trim();
                        match input {
                            "1" => {
                                // Display Trade Table
                                display_trade_table(&session).await;
                                println!();
                                println!("Press Enter to return to menu...");
                                let _ = lines.next_line().await;
                            }
                            "2" => {
                                // Debug Market Data Servers
                                display_market_data_debug(&market_data_sessions).await;
                                println!();
                                println!("Press Enter to return to menu...");
                                let _ = lines.next_line().await;
                            }
                            "99" => {
                                println!();
                                println!("Shutting down server...");
                                // Send shutdown signal to all tasks
                                let _ = shutdown_tx.send(());
                                return Ok(());
                            }
                            _ => {
                                println!();
                                println!("Invalid option. Please select 1, 2, or 99.");
                                println!("Press Enter to continue...");
                                let _ = lines.next_line().await;
                            }
                        }
                    }
                    Ok(None) => {
                        // EOF - exit gracefully
                        return Ok(());
                    }
                    Err(e) => {
                        error!("Error reading input: {}", e);
                        sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }
}

/// Display Trade Table (Open Orders)
async fn display_trade_table(session: &FIXSession) {
    let open_orders = session.open_orders.get_open_orders();
    
    println!();
    println!("╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                                                                  TRADE TABLE                                                                                    ║");
    println!("╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣");
    
    if open_orders.is_empty() {
        println!("║  No open orders                                                                                                                                    ║");
    } else {
        // Header
        println!("║ {:<15} {:<15} {:<12} {:<12} {:<6} {:<12} {:<12} {:<10} {:<12} {:<12} {:<12} {:<20} ║",
            "ClOrdID", "OrderID", "Symbol", "Side", "Status", "OrderQty", "Price", "CumQty", "LeavesQty", "LastPx", "AvgPx", "TransactTime");
        println!("╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣");
        
        // Order rows
        for order in &open_orders {
            let side_str = match order.side.as_str() {
                "1" => "BUY",
                "2" => "SELL",
                _ => &order.side,
            };
            
            let status_str = match order.ord_status.as_str() {
                "0" => "NEW",
                "1" => "PARTIAL",
                "2" => "FILLED",
                "4" => "CANCELED",
                "8" => "REJECTED",
                _ => &order.ord_status,
            };
            
            // Truncate long fields for display
            let cl_ord_id = if order.cl_ord_id.len() > 15 { 
                format!("{}...", &order.cl_ord_id[..12]) 
            } else { 
                order.cl_ord_id.clone() 
            };
            
            let order_id = if order.order_id.len() > 15 { 
                format!("{}...", &order.order_id[..12]) 
            } else { 
                order.order_id.clone() 
            };
            
            let transact_time = if order.transact_time.len() > 20 {
                format!("{}...", &order.transact_time[..17])
            } else {
                order.transact_time.clone()
            };
            
            println!("║ {:<15} {:<15} {:<12} {:<12} {:<6} {:<12} {:<12} {:<10} {:<12} {:<12} {:<12} {:<20} ║",
                cl_ord_id,
                order_id,
                order.symbol,
                side_str,
                status_str,
                order.order_qty,
                order.price,
                order.cum_qty,
                order.leaves_qty,
                order.last_px,
                order.avg_px,
                transact_time
            );
        }
    }
    
    println!("╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝");
    println!();
    println!("Total Open Orders: {}", open_orders.len());
}

/// Display Market Data Servers Debug Information
async fn display_market_data_debug(sessions: &[(Arc<FIXSession>, String, u16)]) {
    println!();
    println!("╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                                                    MARKET DATA SERVERS DEBUG                                                      ║");
    println!("╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣");
    
    for (session, description, port) in sessions {
        // Get connection status
        let is_connected = *session.is_connected.lock().await;
        let connection_state = *session.connection_state.lock().await;
        let is_ready = session.is_ready().await;
        let seq_num = *session.seq_num.lock().await;
        
        // Get order book statistics
        let order_books = session.order_books.lock().await;
        let symbol_count = order_books.len();
        let total_trades: usize = order_books.values()
            .map(|book| book.get_active().trade_count())
            .sum();
        let symbols: Vec<String> = order_books.keys().cloned().collect();
        drop(order_books);
        
        // Get securities count
        let securities = session.securities.lock().await;
        let securities_count = securities.len();
        drop(securities);
        
        // Format connection state
        let state_str = match connection_state {
            ConnectionState::Disconnected => "DISCONNECTED",
            ConnectionState::Connecting => "CONNECTING",
            ConnectionState::Connected => "CONNECTED",
            ConnectionState::Logging => "LOGGING IN",
            ConnectionState::Ready => "READY",
            ConnectionState::Disconnecting => "DISCONNECTING",
        };
        
        let status_icon = if is_ready { "✅" } else if is_connected { "⚠️ " } else { "❌" };
        
        println!("║");
        println!("║ {} {} (Port {})", status_icon, description, port);
        println!("║   ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────");
        println!("║   Connection Status: {}", if is_ready { "READY" } else if is_connected { "CONNECTED" } else { "DISCONNECTED" });
        println!("║   State: {}", state_str);
        println!("║   Sender CompID: {}", session.sender_comp_id);
        println!("║   Target CompID: {}", session.target_comp_id);
        println!("║   Sequence Number: {}", seq_num);
        println!("║   Order Books: {} symbols", symbol_count);
        println!("║   Total Trades: {}", total_trades);
        println!("║   Securities: {}", securities_count);
        
        if !symbols.is_empty() {
            println!("║   Symbols: {}", symbols.join(", "));
        } else {
            println!("║   Symbols: (none)");
        }
        
        // Show top 5 order books with best bid/ask
        if symbol_count > 0 {
            println!("║");
            println!("║   Top Order Books:");
            let order_books = session.order_books.lock().await;
            let mut symbol_list: Vec<_> = order_books.iter().collect();
            symbol_list.sort_by_key(|(symbol, _)| symbol.clone());
            
            for (symbol, book) in symbol_list.iter().take(5) {
                let snapshot = book.get_snapshot();
                let bid = snapshot.best_bid;
                let ask = snapshot.best_ask;
                let spread = snapshot.spread();
                
                if bid > 0.0 && ask > 0.0 {
                    println!("║     {}: Bid={:.8}, Ask={:.8}, Spread={:.8}", 
                        symbol, bid, ask, spread);
                } else {
                    println!("║     {}: (no data)", symbol);
                }
            }
            if symbol_count > 5 {
                println!("║     ... and {} more", symbol_count - 5);
            }
            drop(order_books);
        }
    }
    
    println!("╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝");
}

async fn wait_logon_ack(session: &FIXSession, budget: Duration) -> LogonAck {
    let start = Instant::now();
    loop {
        let remaining = budget.saturating_sub(start.elapsed());
        if remaining.is_zero() {
            return LogonAck::Timeout {
                secs: budget.as_secs(),
            };
        }
        match tokio::time::timeout(remaining, session.receive_message()).await {
            Err(_elapsed) => {
                return LogonAck::Timeout {
                    secs: budget.as_secs(),
                };
            }
            Ok(Ok(msg)) => match msg.msg_type {
                'A' => return LogonAck::Ok,
                '3' => return LogonAck::Reject,
                '5' => return LogonAck::Logout,
                _ => continue,
            },
            Ok(Err(e)) => {
                let s = e.to_string();
                if s.contains("waiting for more data") || s.contains("waiting for message start") {
                    continue;
                }
                return LogonAck::Io(s);
            }
        }
    }
}

async fn drop_preflight_session(session: &FIXSession) {
    *session.stream.lock().await = None;
    *session.connection_state.lock().await = ConnectionState::Disconnected;
    *session.is_connected.lock().await = false;
}

async fn run_logon_check_session(
    session_cfg: &SessionConfig,
    creds: &CredStatus,
    conn: &ConnectionConfig,
    timeout: Duration,
) -> LogonCheckOutcome {
    let addr = connect_addr(session_cfg);
    let name = session_cfg.name.as_str();
    match creds {
        CredStatus::Fail => {
            let tcp = check_tcp_reachable(&session_cfg.host, session_cfg.port, timeout).await;
            let reachable = tcp.is_ok();
            if let Err(e) = tcp {
                eprintln!("session '{name}' {addr}: {e}");
            }
            LogonCheckOutcome {
                reachable,
                logon: LogonCell::Fail,
            }
        }
        CredStatus::Ok(resolved) => {
            let session = FIXSession::new_for_preflight(
                &session_cfg.sender_comp_id,
                &session_cfg.target_comp_id,
                &resolved.api_key,
                &resolved.api_secret,
            )
            .with_transport(
                session_cfg.tls.is_enabled(),
                session_cfg.tls_ca_path.clone(),
                session_cfg
                    .tls_server_name
                    .clone()
                    .unwrap_or_else(|| session_cfg.host.clone()),
            );
            if let Err(e) = session.connect_once(&addr, timeout).await {
                eprintln!("session '{name}' {addr}: {e}");
                return LogonCheckOutcome {
                    reachable: false,
                    logon: LogonCell::Fail,
                };
            }

            let hb = session_heartbeat_interval(session_cfg, conn);
            let sent = match session_cfg.role {
                SessionRole::OrderManagement => {
                    match tokio::time::timeout(timeout, session.logon(hb)).await {
                        Ok(Ok(())) => true,
                        Ok(Err(e)) => {
                            eprintln!("session '{name}' logon: {e}");
                            false
                        }
                        Err(_) => {
                            eprintln!(
                                "session '{name}' logon: timed out after {}s",
                                timeout.as_secs()
                            );
                            false
                        }
                    }
                }
                SessionRole::MarketData => {
                    match tokio::time::timeout(timeout, session.logon_market_data(hb)).await {
                        Ok(Ok(())) => true,
                        Ok(Err(e)) => {
                            eprintln!("session '{name}' logon: {e}");
                            false
                        }
                        Err(_) => {
                            eprintln!(
                                "session '{name}' logon: timed out after {}s",
                                timeout.as_secs()
                            );
                            false
                        }
                    }
                }
            };

            let logon = if sent {
                match wait_logon_ack(&session, timeout).await {
                    LogonAck::Ok => LogonCell::Ok,
                    LogonAck::Reject => {
                        eprintln!("session '{name}' logon: reject");
                        LogonCell::Fail
                    }
                    LogonAck::Logout => {
                        eprintln!("session '{name}' logon: logout");
                        LogonCell::Fail
                    }
                    LogonAck::Timeout { secs } => {
                        eprintln!("session '{name}' logon: timed out after {secs}s");
                        LogonCell::Fail
                    }
                    LogonAck::Io(s) => {
                        eprintln!("session '{name}' logon: {s}");
                        LogonCell::Fail
                    }
                }
            } else {
                LogonCell::Fail
            };

            if sent {
                let _ = tokio::time::timeout(timeout, session.logout()).await;
            }
            drop_preflight_session(&session).await;
            LogonCheckOutcome {
                reachable: true,
                logon,
            }
        }
    }
}

fn format_table(rows: &[PreflightRow]) -> String {
    let headers = ["session", "role", "host:port", "creds", "reachable", "logon"];
    let mut cells: Vec<[String; 6]> = Vec::with_capacity(rows.len());
    for r in rows {
        let creds = if r.creds_ok { "ok" } else { "fail" };
        let reachable = if r.reachable { "true" } else { "false" };
        let logon = match r.logon {
            LogonCell::Skipped => "skipped",
            LogonCell::Ok => "ok",
            LogonCell::Fail => "fail",
        };
        cells.push([
            r.session.clone(),
            r.role.to_string(),
            r.addr.clone(),
            creds.to_string(),
            reachable.to_string(),
            logon.to_string(),
        ]);
    }
    let mut widths = [0usize; 6];
    for (i, h) in headers.iter().enumerate() {
        widths[i] = h.len();
    }
    for row in &cells {
        for (i, c) in row.iter().enumerate() {
            widths[i] = widths[i].max(c.len());
        }
    }
    let pad = |s: &str, w: usize| format!("{s:<w$}");
    let mut out = String::new();
    let header_line = headers
        .iter()
        .enumerate()
        .map(|(i, h)| pad(h, widths[i]))
        .collect::<Vec<_>>()
        .join("  ");
    out.push_str(header_line.trim_end());
    out.push('\n');
    for row in &cells {
        let line = row
            .iter()
            .enumerate()
            .map(|(i, c)| pad(c, widths[i]))
            .collect::<Vec<_>>()
            .join("  ");
        out.push_str(line.trim_end());
        out.push('\n');
    }
    out
}

fn print_table(rows: &[PreflightRow]) {
    print!("{}", format_table(rows));
}

fn preflight_exit_code(rows: &[PreflightRow], logon_check: bool) -> i32 {
    for row in rows {
        if !row.creds_ok || !row.reachable {
            return 1;
        }
        if logon_check && !matches!(row.logon, LogonCell::Ok) {
            return 1;
        }
    }
    0
}

async fn run_preflight(cli: &Cli) -> i32 {
    let timeout = Duration::from_secs(cli.timeout_secs);

    let mut config = match load_config(&cli.config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e:#}");
            return 1;
        }
    };
    if let Err(e) = validate_config(&mut config) {
        eprintln!("{e:#}");
        return 1;
    }

    let cred_map = collect_credentials(&config);
    let n = config.sessions.len();
    eprintln!(
        "Checking {n} session{}. Timeout {}s. Logon check is {}.",
        if n == 1 { "" } else { "s" },
        cli.timeout_secs,
        if cli.logon_check { "on" } else { "off" },
    );
    let mut rows: Vec<PreflightRow> = Vec::with_capacity(n);

    if !cli.logon_check {
        let mut tcp_results: Vec<Option<Result<(), TcpCheckErr>>> = (0..n).map(|_| None).collect();
        let mut set: JoinSet<(usize, Result<(), TcpCheckErr>)> = JoinSet::new();
        for (i, session) in config.sessions.iter().enumerate() {
            let host = session.host.clone();
            let port = session.port;
            set.spawn(async move { (i, check_tcp_reachable(&host, port, timeout).await) });
        }
        while let Some(joined) = set.join_next().await {
            if let Ok((i, result)) = joined {
                tcp_results[i] = Some(result);
            }
        }
        for (i, session) in config.sessions.iter().enumerate() {
            let creds_ok = matches!(
                cred_map.get(&session.credentials_ref),
                Some(CredStatus::Ok(_))
            );
            let tcp = tcp_results[i].take().unwrap_or_else(|| {
                Err(TcpCheckErr::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "tcp check did not complete",
                )))
            });
            let reachable = tcp.is_ok();
            if let Err(e) = &tcp {
                eprintln!("session '{}' {}: {}", session.name, connect_addr(session), e);
            }
            rows.push(PreflightRow {
                session: session.name.clone(),
                role: session.role,
                addr: connect_addr(session),
                creds_ok,
                reachable,
                logon: LogonCell::Skipped,
            });
        }
    } else {
        let fail = CredStatus::Fail;
        for session in &config.sessions {
            let creds = cred_map.get(&session.credentials_ref).unwrap_or(&fail);
            let creds_ok = matches!(creds, CredStatus::Ok(_));
            let out = run_logon_check_session(session, creds, &config.connection, timeout).await;
            rows.push(PreflightRow {
                session: session.name.clone(),
                role: session.role,
                addr: connect_addr(session),
                creds_ok,
                reachable: out.reachable,
                logon: out.logon,
            });
        }
    }

    print_table(&rows);
    preflight_exit_code(&rows, cli.logon_check)
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    let cli = match parse_cli(std::env::args().skip(1)) {
        Err(msg) => {
            eprintln!("{msg}");
            std::process::exit(1);
        }
        Ok(CliParse::Help) => {
            print_preflight_help();
            std::process::exit(0);
        }
        Ok(CliParse::Run(c)) => c,
    };

    if cli.preflight {
        let code = run_preflight(&cli).await;
        std::process::exit(code);
    }

    startup_note("starting");

    let mut config = match load_config(&cli.config_path) {
        Ok(c) => {
            startup_note(&format!("{} loaded", cli.config_path));
            c
        }
        Err(e) => {
            startup_note(&format!(
                "FATAL: could not load {}: {e:#}",
                cli.config_path
            ));
            return Err(e);
        }
    };

    if let Err(e) = validate_config(&mut config) {
        startup_note(&format!("FATAL: config validation failed: {e:#}"));
        return Err(e);
    }

    let resolved = match resolve_credentials(&config) {
        Ok(m) => m,
        Err(e) => {
            startup_note(&format!("FATAL: credential resolve failed: {e:#}"));
            return Err(e);
        }
    };

    // names as written, list order; refs unique first-seen
    let names: Vec<&str> = config.sessions.iter().map(|s| s.name.as_str()).collect();
    startup_note(&format!(
        "sessions: n={} names=[{}]",
        names.len(),
        names.join(", ")
    ));
    let refs = used_credential_refs(&config.sessions);
    startup_note(&format!(
        "credentials resolved: refs=[{}]",
        refs.join(", ")
    ));

    // Do NOT info!("{:?}", config);
    let _ = SUBSCRIPTION_ALLOWLIST.set(config.subscriptions.symbols.clone());
    info!("📜 Subscription allowlist: {:?}", config.subscriptions.symbols);

    // KDB_HOST / KDB_PORT defaults UNCHANGED
    let kdb_host = std::env::var("KDB_HOST").unwrap_or_else(|_| "localhost".to_string());
    let kdb_port: u16 = std::env::var("KDB_PORT")
        .unwrap_or_else(|_| "5001".to_string())
        .parse()
        .unwrap_or(5001);

    let kdb_writer = Arc::new(KdbWriter::new(kdb_host.clone(), kdb_port));
    kdb_writer.initialize().await.map_err(|e| {
        error!("❌ FATAL: Failed to initialize kdb+ writer ({}:{}): {}", kdb_host, kdb_port, e);
        error!("❌ Server cannot start without kdb+ connectivity.");
        error!("💡 To fix: Start kdb+ server with: q -p {}", kdb_port);
        error!("💡 Or set KDB_HOST and KDB_PORT environment variables if kdb+ is on a different host/port.");
        anyhow::anyhow!("kdb+ connection required but failed: {}", e)
    })?;
    info!("✅ kdb+ writer initialized. Connected to {}:{}", kdb_host, kdb_port);

    // ---- channels BEFORE the spawn loop (moved earlier than today) ----
    let (order_update_tx, _) = broadcast::channel::<()>(1000);
    let (order_book_update_tx, _) = broadcast::channel::<String>(1000);
    let (shutdown_tx, _shutdown_rx) = broadcast::channel::<()>(1);

    let mut session_tasks: JoinSet<(SessionTaskIdentity, Result<()>)> = JoinSet::new();
    let mut spawned: Vec<SpawnedSession> = Vec::new();
    let mut order_rx_holder: Option<mpsc::Receiver<OrderRequest>> = None;
    let mut order_tx_holder: Option<mpsc::Sender<OrderRequest>> = None;

    if config.sessions.iter().any(|s| s.role == SessionRole::OrderManagement) {
        let (tx, rx) = mpsc::channel::<OrderRequest>(100);
        order_tx_holder = Some(tx);
        order_rx_holder = Some(rx);
    }

    // ---- spawn loop, list order ----
    for session in config.sessions.iter() {
        let creds = resolved
            .get(&session.credentials_ref)
            .expect("validate_config + resolve_credentials");
        let handle = Arc::new(
            FIXSession::new_with_credentials_and_kdb(
                &session.sender_comp_id,
                &session.target_comp_id,
                &creds.api_key,
                &creds.api_secret,
                Arc::clone(&kdb_writer),
            )
            .with_transport(
                session.tls.is_enabled(),
                session.tls_ca_path.clone(),
                session
                    .tls_server_name
                    .clone()
                    .unwrap_or_else(|| session.host.clone()),
            ),
        );
        handle
            .set_update_channels(order_update_tx.clone(), order_book_update_tx.clone())
            .await;

        let addr = connect_addr(session);
        let hb = session_heartbeat_interval(session, &config.connection);
        let max = session_max_reconnect_attempts(session, &config.connection);
        let backoff = session_reconnect_backoff_ms(session, &config.connection);

        let mut spawn_msg = format!(
            "spawned session {} role={} {}:{} sender={} target={} credentials_ref={} tls={}",
            session.name,
            session.role,
            session.host,
            session.port,
            session.sender_comp_id,
            session.target_comp_id,
            session.credentials_ref,
            session.tls.is_enabled(),
        );
        if session.tls.is_enabled() {
            if let Some(sni) = &session.tls_server_name {
                spawn_msg.push_str(&format!(" sni={sni}"));
            }
        }
        info!("{spawn_msg}");

        match session.role {
            SessionRole::OrderManagement => {
                let order_rx = order_rx_holder
                    .take()
                    .expect("validate forbade 2+ OM; channel created before loop");
                let h1 = Arc::clone(&handle);
                let addr1 = addr.clone();
                let id_connect = SessionTaskIdentity {
                    name: session.name.clone(),
                    role: session.role,
                    host: session.host.clone(),
                    port: session.port,
                    kind: SessionLoopKind::Connect,
                };
                session_tasks.spawn(async move {
                    let result = message_loop(h1, addr1, order_rx, hb, max, backoff).await;
                    if let Err(ref e) = result {
                        error!("Fatal error in message loop: {e}");
                    }
                    (id_connect, result)
                });
                let h2 = Arc::clone(&handle);
                let id_hb = SessionTaskIdentity {
                    name: session.name.clone(),
                    role: session.role,
                    host: session.host.clone(),
                    port: session.port,
                    kind: SessionLoopKind::Heartbeat,
                };
                session_tasks.spawn(async move {
                    let result = heartbeat_loop(h2, hb).await;
                    if let Err(ref e) = result {
                        error!("Fatal error in heartbeat loop: {e}");
                    }
                    (id_hb, result)
                });
            }
            SessionRole::MarketData => {
                let routing = session
                    .compiled_routing
                    .clone()
                    .expect("validate_config fills compiled_routing for MD");
                let filter: Box<dyn Fn(&str) -> bool + Send + Sync> =
                    Box::new(move |sym: &str| session_admits_symbol(&routing, sym));
                let h1 = Arc::clone(&handle);
                let addr1 = addr.clone();
                let id_connect = SessionTaskIdentity {
                    name: session.name.clone(),
                    role: session.role,
                    host: session.host.clone(),
                    port: session.port,
                    kind: SessionLoopKind::Connect,
                };
                session_tasks.spawn(async move {
                    let result =
                        market_data_loop(h1, addr1, hb, max, backoff, Some(filter)).await;
                    if let Err(ref e) = result {
                        error!("Fatal error in market data loop: {e}");
                    }
                    (id_connect, result)
                });
                let h2 = Arc::clone(&handle);
                let id_hb = SessionTaskIdentity {
                    name: session.name.clone(),
                    role: session.role,
                    host: session.host.clone(),
                    port: session.port,
                    kind: SessionLoopKind::Heartbeat,
                };
                session_tasks.spawn(async move {
                    let result = heartbeat_loop(h2, hb).await;
                    if let Err(ref e) = result {
                        error!("Fatal error in heartbeat loop: {e}");
                    }
                    (id_hb, result)
                });
            }
        }

        startup_note(&format!(
            "session task spawned: {} ({} {}:{})",
            session.name, session.role, session.host, session.port
        ));
        spawned.push(SpawnedSession {
            handle,
            config: session.clone(),
        });
    }

    let om = lookup_om(&spawned);
    let primary_md = lookup_primary_md(&spawned);
    let all_md = lookup_all_md(&spawned);

    if let Some(ref om_h) = om {
        let mut map = HashMap::new();
        for row in &config.symbol_limits {
            map.insert(
                row.symbol.trim().to_string(),
                SymbolLimit {
                    max_position: row.max_position,
                    max_notional: row.max_notional,
                },
            );
        }
        om_h.apply_risk_config(map, config.risk.session_max_drawdown);
    }

    // ---- OM bundle ----
    let watcher_opt: Option<tokio::task::JoinHandle<()>> = if let Some(ref om_h) = om {
        let order_tx = order_tx_holder
            .take()
            .expect("OM present ⇒ channel created");
        let om_w = Arc::clone(om_h);
        Some(tokio::spawn(async move {
            if let Err(e) = watch_orders_directory(order_tx, "orders", om_w).await {
                error!("Fatal error in orders directory watcher: {}", e);
            }
        }))
    } else {
        None
    };

    if let Some(ref om_h) = om {
        // prune (fire-and-forget)
        let session_for_prune = Arc::clone(om_h);
        let _order_prune_handle = tokio::spawn(async move {
            const PRUNE_INTERVAL_SECS: u64 = 300;
            const MAX_AGE_SECS: i64 = 300;
            let mut prune_interval = interval(Duration::from_secs(PRUNE_INTERVAL_SECS));
            prune_interval.tick().await;
            loop {
                prune_interval.tick().await;
                let now_ms = chrono::Utc::now().timestamp_millis();
                let cutoff_ms = now_ms - MAX_AGE_SECS * 1000;
                {
                    let mut timings = session_for_prune.order_timings.lock().await;
                    let before = timings.len();
                    timings.retain(|_, t| {
                        t.file_read_timestamp.map(|ts| ts > cutoff_ms).unwrap_or(true)
                    });
                    let pruned = before - timings.len();
                    if pruned > 0 {
                        warn!("🧹 Pruned {} stale order_timings entries", pruned);
                    }
                }
                {
                    let timings = session_for_prune.order_timings.lock().await;
                    let mut data = session_for_prune.order_data.lock().await;
                    let before = data.len();
                    data.retain(|k, _| timings.contains_key(k));
                    let pruned = before - data.len();
                    if pruned > 0 {
                        warn!("🧹 Pruned {} stale order_data entries", pruned);
                    }
                }
                {
                    let mut times = session_for_prune.order_times.lock().await;
                    let before = times.len();
                    times.retain(|_, inst| inst.elapsed().as_secs() < MAX_AGE_SECS as u64);
                    let pruned = before - times.len();
                    if pruned > 0 {
                        warn!("🧹 Pruned {} stale order_times entries", pruned);
                    }
                }
            }
        });

        let open_orders_store = Arc::clone(&om_h.open_orders);
        let csv_update_rx = order_update_tx.subscribe();
        let _csv_generator_handle = tokio::spawn(async move {
            if let Err(e) =
                open_orders_csv_generator_event_driven(open_orders_store, csv_update_rx).await
            {
                error!("Fatal error in open orders CSV generator: {}", e);
            }
        });
    }

    // algo + web only if OM Some AND primary MD Some
    if let (Some(om_h), Some(md_h)) = (om.clone(), primary_md.clone()) {
        let mut sc_cfg = algo::AlgoConfig::default();
        sc_cfg.enabled = true; // spawn override only; Default.enabled stays false
        let algo = Arc::new(algo::TradingAlgo::new(sc_cfg));

        let mut mi_cfg = algo::AlgoConfig::default();
        mi_cfg.strategy = algo::StrategyType::MarketImbalance;
        mi_cfg.enabled = true;
        let algo_imbalance = Arc::new(algo::TradingAlgo::new(mi_cfg));
        info!("MarketImbalance shadow started (paper_mode=true); not bound to /api/algo/*");

        let algo_for_web = Arc::clone(&algo);
        let algo_imbalance_for_web = Arc::clone(&algo_imbalance);
        let algo_order_session = Arc::clone(&om_h);
        let algo_market_session = Arc::clone(&md_h);
        let algo_positions = Arc::clone(&om_h.positions);
        let _algo_handle = tokio::spawn({
            let algo = Arc::clone(&algo);
            let om = Arc::clone(&algo_order_session);
            let md = Arc::clone(&algo_market_session);
            let pos = Arc::clone(&algo_positions);
            async move {
                algo_runtime::run(algo, om, md, pos).await;
            }
        });
        let _algo_imbalance_handle = tokio::spawn({
            let algo = Arc::clone(&algo_imbalance);
            let om = Arc::clone(&algo_order_session);
            let md = Arc::clone(&algo_market_session);
            let pos = Arc::clone(&algo_positions);
            async move {
                algo_runtime::run(algo, om, md, pos).await;
            }
        });

        let order_session_for_web = Arc::clone(&om_h);
        let market_data_for_web = Arc::clone(&md_h);
        let market_data_sessions_for_web = all_md.clone(); // (handle, name, port) list order
        let kdb_for_web = Arc::clone(&kdb_writer);
        let _web_server_handle = tokio::spawn(async move {
            if let Err(e) = order_book_web_server(
                order_session_for_web,
                market_data_for_web,
                market_data_sessions_for_web,
                kdb_for_web,
                algo_for_web,
                algo_imbalance_for_web,
                "0.0.0.0:8080",
            )
            .await
            {
                error!("Fatal error in web server: {}", e);
            }
        });
    }

    // ---- MD bundle ----
    let zmq_opt: Option<tokio::task::JoinHandle<()>> = if let Some(ref md_h) = primary_md {
        let market_data_for_autosave = Arc::clone(md_h);
        let _tick_autosave_handle = tokio::spawn(async move {
            let mut save_interval = interval(Duration::from_secs(60));
            save_interval.tick().await;
            loop {
                save_interval.tick().await;
                let _ = market_data_for_autosave
                    .save_ticks_to_parquet("data/ticks.csv")
                    .await;
            }
        });

        let market_data_for_zmq = Arc::clone(md_h);
        let zmq_update_rx = order_book_update_tx.subscribe();
        Some(tokio::spawn(async move {
            // Bind address is configurable so a second, read-only instance
            // (e.g. production market data alongside UAT) can run on the same
            // box. Previously hardcoded: the second process failed to bind,
            // the publisher task exited, and that took the whole process down
            // in a restart loop. Default is unchanged.
            let zmq_bind = std::env::var("ZMQ_BIND")
                .unwrap_or_else(|_| "tcp://0.0.0.0:5555".to_string());
            if let Err(e) = zmq_order_book_publisher_event_driven(
                market_data_for_zmq,
                &zmq_bind,
                zmq_update_rx,
            )
            .await
            {
                error!("Fatal error in ZeroMQ publisher: {}", e);
            }
        }))
    } else {
        None
    };

    if !all_md.is_empty() {
        // watchdog over all MD; log name AND sender_comp_id
        let stale_sessions: Vec<(Arc<FIXSession>, String)> = spawned
            .iter()
            .filter(|s| s.config.role == SessionRole::MarketData)
            .map(|s| (Arc::clone(&s.handle), s.config.name.clone()))
            .collect();
        let _stale_feed_watchdog = tokio::spawn(async move {
            let mut check_interval = interval(Duration::from_secs(15));
            check_interval.tick().await;
            loop {
                check_interval.tick().await;
                let now_ms = chrono::Utc::now().timestamp_millis() as u64;
                for (sess, name) in &stale_sessions {
                    let _ = sess.apply_feed_watchdog_tick(now_ms, name).await;
                }
            }
        });
    }

    info!("Waiting for connections and logons to complete...");
    sleep(Duration::from_secs(3)).await;
    startup_note("startup complete — all services active");

    let mut shutdown_rx_main = shutdown_tx.subscribe();
    tokio::select! {
        Some(res) = session_tasks.join_next() => {
            match res {
                Ok((id, Ok(()))) => {
                    error!(
                        "session task exited: {} ({} {}:{} {})",
                        id.name, id.role, id.host, id.port, id.kind
                    );
                }
                Ok((id, Err(e))) => {
                    error!(
                        "session task exited: {} ({} {}:{} {}): {e}",
                        id.name, id.role, id.host, id.port, id.kind
                    );
                }
                Err(join_err) => {
                    error!("session task panicked: {join_err}");
                }
            }
        }
        _ = async {
            match zmq_opt {
                Some(h) => {
                    let _ = h.await;
                }
                None => std::future::pending::<()>().await,
            }
        } => {
            error!("ZeroMQ publisher exited");
        }
        _ = async {
            match watcher_opt {
                Some(h) => {
                    let _ = h.await;
                }
                None => std::future::pending::<()>().await,
            }
        } => {
            error!("Orders directory watcher exited");
        }
        _ = shutdown_rx_main.recv() => {
            info!("Shutdown signal received - shutting down server...");
        }
    }

    Ok(())
}

// ============================================================================
// Integration tests: FIXSession driven against the in-process mock FIX
// counterparty (src/mock_gateway.rs). These are the "verified against the
// mock FIX counterparty" tests called for by the market-data/exchange-
// connectivity acceptance criteria (AC2, AC3, AC4, AC6). Pure sequencing
// logic (gap/dup/recovery, no networking) is covered separately in
// src/sequencer.rs's own unit tests.
// ============================================================================
#[cfg(test)]
mod config_tests {
    use super::*;

    fn skeleton_md() -> String {
        r#"
[connection]
heartbeat_interval = 30
max_reconnect_attempts = 10
reconnect_backoff_ms = 1000

[credentials.UAT]
api_key_env = "CRYPTOCOM_API_KEY"
api_secret_env = "CRYPTOCOM_API_SECRET"

[[sessions]]
name = "s1"
role = "market_data"
host = "fix-uat.crypto.com"
port = 34402
sender_comp_id = "S"
target_comp_id = "T"
credentials_ref = "UAT"
symbol_routing = "all"
"#
        .to_string()
    }

    fn validate_err_contains(toml: &str, needle: &str) {
        let mut c = parse_config(toml).unwrap_or_else(|e| panic!("parse should succeed: {e:#}"));
        let err = validate_config(&mut c).expect_err("validate should fail");
        let msg = format!("{err:#}");
        assert!(msg.contains(needle), "expected {needle:?} in {msg:?}");
    }

    #[test]
    fn session_admits_symbol_uat_3_way_split() {
        let ad = compile_symbol_routing(
            "t",
            &SymbolRouting::FirstCharRanges(vec!["0-9".into(), "A-D".into()]),
        )
        .unwrap();
        let em = compile_symbol_routing(
            "t",
            &SymbolRouting::FirstCharRanges(vec!["E-M".into()]),
        )
        .unwrap();
        let nz = compile_symbol_routing(
            "t",
            &SymbolRouting::FirstCharRanges(vec!["N-Z".into()]),
        )
        .unwrap();
        assert!(session_admits_symbol(&ad, "BTC_USDT"));
        assert!(!session_admits_symbol(&em, "BTC_USDT"));
        assert!(!session_admits_symbol(&nz, "BTC_USDT"));
        assert!(session_admits_symbol(&em, "ETH_USDT"));
        assert!(!session_admits_symbol(&ad, "ETH_USDT"));
        assert!(!session_admits_symbol(&nz, "ETH_USDT"));
        assert!(session_admits_symbol(&nz, "SOL_USDT"));
        assert!(!session_admits_symbol(&ad, "SOL_USDT"));
        assert!(!session_admits_symbol(&em, "SOL_USDT"));
        assert!(!session_admits_symbol(&ad, ""));
        assert!(!session_admits_symbol(&em, ""));
        assert!(!session_admits_symbol(&nz, ""));
        assert!(session_admits_symbol(&ad, "btc_usdt"));
    }

    #[test]
    fn session_admits_symbol_all_admits_everything_including_empty() {
        let all = CompiledSymbolRouting::All;
        assert!(session_admits_symbol(&all, "BTC_USDT"));
        assert!(session_admits_symbol(&all, "ETH_USDT"));
        assert!(session_admits_symbol(&all, "SOL_USDT"));
        assert!(session_admits_symbol(&all, ""));
        assert!(session_admits_symbol(&all, "中文"));
    }

    #[test]
    fn session_admits_symbol_2_way_split() {
        let first = compile_symbol_routing(
            "t",
            &SymbolRouting::FirstCharRanges(vec!["0-9".into(), "A-M".into()]),
        )
        .unwrap();
        let second = compile_symbol_routing(
            "t",
            &SymbolRouting::FirstCharRanges(vec!["N-Z".into()]),
        )
        .unwrap();
        assert!(session_admits_symbol(&first, "BTC_USDT"));
        assert!(session_admits_symbol(&first, "ETH_USDT"));
        assert!(!session_admits_symbol(&second, "BTC_USDT"));
        assert!(!session_admits_symbol(&second, "ETH_USDT"));
        assert!(session_admits_symbol(&second, "SOL_USDT"));
        assert!(!session_admits_symbol(&first, "SOL_USDT"));
        assert!(!session_admits_symbol(&first, ""));
        assert!(!session_admits_symbol(&second, ""));
    }

    #[test]
    fn session_admits_symbol_1_way_split() {
        let one = compile_symbol_routing(
            "t",
            &SymbolRouting::FirstCharRanges(vec!["A-Z".into()]),
        )
        .unwrap();
        assert!(session_admits_symbol(&one, "BTC_USDT"));
        assert!(session_admits_symbol(&one, "ETH_USDT"));
        assert!(session_admits_symbol(&one, "SOL_USDT"));
        assert!(!session_admits_symbol(&one, "9_FOO"));
        assert!(!session_admits_symbol(&one, ""));
    }

    #[test]
    fn session_admits_symbol_non_ascii_first_char_false_for_ranges() {
        let ad = compile_symbol_routing(
            "t",
            &SymbolRouting::FirstCharRanges(vec!["A-D".into()]),
        )
        .unwrap();
        assert!(!session_admits_symbol(&ad, "Ω_USDT"));
        assert!(session_admits_symbol(&CompiledSymbolRouting::All, "Ω_USDT"));
    }

    #[test]
    fn parse_config_uat_example_file() {
        let src = include_str!("../config.example.toml");
        let c = parse_config(src).unwrap();
        assert_eq!(c.sessions.len(), 5);

        assert_eq!(c.sessions[0].name, "uat_om");
        assert_eq!(c.sessions[0].role, SessionRole::OrderManagement);
        assert_eq!(c.sessions[0].port, 34404);
        assert_eq!(c.sessions[0].host, "fix-uat.crypto.com");
        assert_eq!(c.sessions[0].sender_comp_id, "UAT1.WOZINGA.UD.00");
        assert_eq!(c.sessions[0].target_comp_id, "UAT1.CDC.UD");
        assert_eq!(c.sessions[0].credentials_ref, "UAT");
        assert!(c.sessions[0].heartbeat_interval.is_none());
        assert!(c.sessions[0].max_reconnect_attempts.is_none());
        assert!(c.sessions[0].reconnect_backoff_ms.is_none());

        assert_eq!(c.sessions[1].name, "uat_md_btc");
        assert_eq!(c.sessions[1].role, SessionRole::MarketData);
        assert_eq!(c.sessions[1].port, 34402);
        assert_eq!(c.sessions[1].symbol_routing, Some(SymbolRouting::All));
        assert_eq!(
            c.sessions
                .iter()
                .find(|s| s.role == SessionRole::MarketData)
                .unwrap()
                .name,
            "uat_md_btc"
        );

        assert_eq!(c.sessions[2].name, "uat_md_others_34405");
        assert_eq!(c.sessions[2].port, 34405);
        assert_eq!(
            c.sessions[2].symbol_routing,
            Some(SymbolRouting::FirstCharRanges(vec![
                "0-9".into(),
                "A-D".into()
            ]))
        );

        assert_eq!(c.sessions[3].name, "uat_md_others_34406");
        assert_eq!(c.sessions[3].port, 34406);
        assert_eq!(
            c.sessions[3].symbol_routing,
            Some(SymbolRouting::FirstCharRanges(vec!["E-M".into()]))
        );

        assert_eq!(c.sessions[4].name, "uat_md_others_34407");
        assert_eq!(c.sessions[4].port, 34407);
        assert_eq!(
            c.sessions[4].symbol_routing,
            Some(SymbolRouting::FirstCharRanges(vec!["N-Z".into()]))
        );

        assert_eq!(c.credentials["UAT"].api_key_env, "CRYPTOCOM_API_KEY");
        assert_eq!(c.credentials["UAT"].api_secret_env, "CRYPTOCOM_API_SECRET");
        assert_eq!(
            c.subscriptions.symbols,
            vec!["BTC_USDT", "BTC_USD", "BTCUSD-PERP"]
        );
        assert_eq!(c.connection.heartbeat_interval, 30);
        assert_eq!(c.connection.max_reconnect_attempts, 10);
        assert_eq!(c.connection.reconnect_backoff_ms, 1000);
        assert!(!src.contains("api_key ="));
        assert!(src.contains("api_key_env"));
        assert!(!src.contains("YOUR_API_KEY"));
        assert!(!src.contains("YOUR_API_SECRET"));
    }

    #[test]
    fn parse_config_prd3_example_file() {
        let src = include_str!("../config.prd3.example.toml");
        let c = parse_config(src).unwrap();
        assert_eq!(c.sessions.len(), 6);

        let om = c
            .sessions
            .iter()
            .find(|s| s.name == "prd3_om")
            .expect("prd3_om row");
        assert_eq!(om.role, SessionRole::OrderManagement);
        assert_eq!(om.port, 31302);
        assert_eq!(om.credentials_ref, "PRD3");
        assert!(src.contains("name             = \"prd3_om\""));
        assert!(!src.contains("# NOT ENABLED — order entry pending separate approval"));
        assert!(src.contains("config.prd3.toml"));
        assert!(!src.contains("Copy to config.toml"));

        let first_md = c
            .sessions
            .iter()
            .find(|s| s.role == SessionRole::MarketData)
            .unwrap();
        assert_eq!(first_md.port, 34402);
        assert_eq!(first_md.symbol_routing, Some(SymbolRouting::All));
        assert_eq!(first_md.name, "prd3_md_btc");

        let eth = c
            .sessions
            .iter()
            .find(|s| s.name == "prd3_md_eth")
            .unwrap();
        assert_eq!(eth.port, 34403);
        assert_eq!(eth.symbol_routing, Some(SymbolRouting::All));
        assert_ne!(first_md.name, eth.name);

        let p34405 = c
            .sessions
            .iter()
            .find(|s| s.port == 34405)
            .unwrap();
        assert_eq!(
            p34405.symbol_routing,
            Some(SymbolRouting::FirstCharRanges(vec![
                "0-9".into(),
                "A-D".into()
            ]))
        );
        let p34406 = c
            .sessions
            .iter()
            .find(|s| s.port == 34406)
            .unwrap();
        assert_eq!(
            p34406.symbol_routing,
            Some(SymbolRouting::FirstCharRanges(vec!["E-M".into()]))
        );
        let p34407 = c
            .sessions
            .iter()
            .find(|s| s.port == 34407)
            .unwrap();
        assert_eq!(
            p34407.symbol_routing,
            Some(SymbolRouting::FirstCharRanges(vec!["N-Z".into()]))
        );

        let vpce = "vpce-07bb726a65816b33f-2nhnv7ig.vpce-svc-0493a3dfb1a2d0d6a.ap-northeast-1.vpce.amazonaws.com";
        for s in &c.sessions {
            assert_eq!(s.host, vpce);
            assert_eq!(s.credentials_ref, "PRD3");
        }
        assert_eq!(c.credentials["PRD3"].api_key_env, "CRYPTOCOM_PRD3_API_KEY");
        assert_eq!(
            c.credentials["PRD3"].api_secret_env,
            "CRYPTOCOM_PRD3_API_SECRET"
        );
        assert!(src.contains("31302"));
        assert!(src.contains("REPLACE_WITH_VENUE_ASSIGNED_"));
        assert!(!src.contains("api_key ="));
        assert_eq!(
            c.subscriptions.symbols,
            vec!["BTC_USDT", "BTC_USD", "BTCUSD-PERP"]
        );
        assert_eq!(c.connection.heartbeat_interval, 30);
        assert_eq!(c.connection.max_reconnect_attempts, 10);
        assert_eq!(c.connection.reconnect_backoff_ms, 1000);
    }

    fn usage_err(args: &[&str]) -> String {
        match parse_cli(args.iter().copied()) {
            Err(e) => e,
            Ok(_) => panic!("expected usage error for {args:?}"),
        }
    }

    #[test]
    fn parse_cli_help_wins_even_with_garbage() {
        match parse_cli(["--preflight", "--nope", "--help"]) {
            Ok(CliParse::Help) => {}
            other => panic!("expected Help, got {other:?}"),
        }
        match parse_cli(["-h", "--timeout-secs", "0"]) {
            Ok(CliParse::Help) => {}
            other => panic!("expected Help, got {other:?}"),
        }
    }

    #[test]
    fn parse_cli_preflight_defaults() {
        match parse_cli(["--preflight"]) {
            Ok(CliParse::Run(c)) => {
                assert!(c.preflight);
                assert_eq!(c.config_path, "config.toml");
                assert!(!c.logon_check);
                assert_eq!(c.timeout_secs, 5);
            }
            other => panic!("expected Run, got {other:?}"),
        }
    }

    #[test]
    fn parse_cli_preflight_all_flags() {
        match parse_cli([
            "--preflight",
            "--config",
            "config.prd3.toml",
            "--logon-check",
            "--timeout-secs",
            "7",
        ]) {
            Ok(CliParse::Run(c)) => {
                assert!(c.preflight);
                assert_eq!(c.config_path, "config.prd3.toml");
                assert!(c.logon_check);
                assert_eq!(c.timeout_secs, 7);
            }
            other => panic!("expected Run, got {other:?}"),
        }
    }

    #[test]
    fn parse_cli_live_config_path() {
        match parse_cli(["--config", "other.toml"]) {
            Ok(CliParse::Run(c)) => {
                assert!(!c.preflight);
                assert_eq!(c.config_path, "other.toml");
                assert!(!c.logon_check);
                assert_eq!(c.timeout_secs, 5);
            }
            other => panic!("expected Run, got {other:?}"),
        }
    }

    #[test]
    fn parse_cli_unknown_and_equals_form() {
        let err = usage_err(&["--preflight", "--nope"]);
        assert!(err.starts_with("fix_server: unknown argument: --nope"), "{err}");
        assert!(err.contains(PREFLIGHT_USAGE), "{err}");
        let err = usage_err(&["--config=foo"]);
        assert!(
            err.starts_with("fix_server: unknown argument: --config=foo"),
            "{err}"
        );
    }

    #[test]
    fn format_table_columns_left_align_without_trailing_pad() {
        let table = format_table(&[
            PreflightRow {
                session: "prd3_md_others_34405".into(),
                role: SessionRole::MarketData,
                addr: "127.0.0.1:34405".into(),
                creds_ok: true,
                reachable: true,
                logon: LogonCell::Skipped,
            },
            PreflightRow {
                session: "om".into(),
                role: SessionRole::OrderManagement,
                addr: "127.0.0.1:1".into(),
                creds_ok: false,
                reachable: false,
                logon: LogonCell::Fail,
            },
        ]);
        let mut lines = table.lines();
        let header = lines.next().expect("header");
        let row1 = lines.next().expect("row1");
        let row2 = lines.next().expect("row2");
        assert!(lines.next().is_none());
        assert!(!header.ends_with(' '));
        assert!(!row1.ends_with(' '));
        assert!(!row2.ends_with(' '));
        let role_at = header.find("role").expect("role header");
        let creds_at = header.find("creds").expect("creds header");
        let reachable_at = header.find("reachable").expect("reachable header");
        let logon_at = header.find("logon").expect("logon header");
        assert_eq!(&row1[role_at..role_at + 11], "market_data");
        assert_eq!(&row2[role_at..role_at + 16], "order_management");
        assert_eq!(&row1[creds_at..creds_at + 2], "ok");
        assert_eq!(&row2[creds_at..creds_at + 4], "fail");
        assert_eq!(&row1[reachable_at..reachable_at + 4], "true");
        assert_eq!(&row2[reachable_at..reachable_at + 5], "false");
        assert_eq!(&row1[logon_at..], "skipped");
        assert_eq!(&row2[logon_at..], "fail");
        assert!(header.contains("host:port"));
    }

    #[test]
    fn parse_cli_timeout_and_logon_require_preflight() {
        let err = usage_err(&["--logon-check"]);
        assert!(
            err.starts_with("fix_server: --logon-check requires --preflight"),
            "{err}"
        );
        let err = usage_err(&["--timeout-secs", "5"]);
        assert!(
            err.starts_with("fix_server: --timeout-secs requires --preflight"),
            "{err}"
        );
        let err = usage_err(&["--preflight", "--timeout-secs", "0"]);
        assert!(
            err.starts_with("fix_server: --timeout-secs must be a positive integer"),
            "{err}"
        );
        let err = usage_err(&["--preflight", "--timeout-secs"]);
        assert!(
            err.starts_with("fix_server: --timeout-secs must be a positive integer"),
            "{err}"
        );
        let err = usage_err(&["--preflight", "--config"]);
        assert!(
            err.starts_with("fix_server: --config requires a path"),
            "{err}"
        );
    }

    #[test]
    fn parse_rejects_symbol_routing_all_wrong_case() {
        let toml = skeleton_md().replace("symbol_routing = \"all\"", "symbol_routing = \"All\"");
        let err = parse_config(&toml).expect_err("parse should fail");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("Failed to parse config file:")
                && msg.contains("symbol_routing string must be exactly \"all\""),
            "got {msg:?}"
        );
    }

    #[test]
    fn validate_rejects_empty_sessions() {
        let toml = r#"
sessions = []

[connection]
heartbeat_interval = 30
max_reconnect_attempts = 10
reconnect_backoff_ms = 1000

[credentials.UAT]
api_key_env = "CRYPTOCOM_API_KEY"
api_secret_env = "CRYPTOCOM_API_SECRET"
"#;
        validate_err_contains(toml, "sessions must not be empty");
    }

    #[test]
    fn validate_rejects_duplicate_session_name() {
        let toml = skeleton_md()
            + r#"
[[sessions]]
name = "s1"
role = "market_data"
host = "fix-uat.crypto.com"
port = 34403
sender_comp_id = "S2"
target_comp_id = "T2"
credentials_ref = "UAT"
symbol_routing = "all"
"#;
        validate_err_contains(&toml, "duplicate session name: s1");
    }

    #[test]
    fn validate_rejects_two_order_management_sessions() {
        let toml = r#"
[connection]
heartbeat_interval = 30
max_reconnect_attempts = 10
reconnect_backoff_ms = 1000

[credentials.UAT]
api_key_env = "CRYPTOCOM_API_KEY"
api_secret_env = "CRYPTOCOM_API_SECRET"

[[sessions]]
name = "om1"
role = "order_management"
host = "fix-uat.crypto.com"
port = 34404
sender_comp_id = "S"
target_comp_id = "T"
credentials_ref = "UAT"

[[sessions]]
name = "om2"
role = "order_management"
host = "fix-uat.crypto.com"
port = 34405
sender_comp_id = "S2"
target_comp_id = "T2"
credentials_ref = "UAT"
"#;
        validate_err_contains(
            toml,
            "at most one session with role = order_management is allowed (found 2)",
        );
    }

    #[test]
    fn validate_rejects_empty_session_name() {
        let toml = skeleton_md().replace("name = \"s1\"", "name = \"   \"");
        validate_err_contains(&toml, "session name must not be empty");
    }

    #[test]
    fn validate_rejects_empty_credentials_ref() {
        let toml = skeleton_md().replace("credentials_ref = \"UAT\"", "credentials_ref = \"  \"");
        validate_err_contains(&toml, "credentials_ref must not be empty");
    }

    #[test]
    fn validate_rejects_unknown_credentials_ref() {
        let toml = skeleton_md().replace("credentials_ref = \"UAT\"", "credentials_ref = \"NOPE\"");
        validate_err_contains(&toml, "does not match any [credentials.*] block");
    }

    #[test]
    fn validate_rejects_empty_api_key_env_name() {
        let toml = skeleton_md().replace(
            "api_key_env = \"CRYPTOCOM_API_KEY\"",
            "api_key_env = \"  \"",
        );
        validate_err_contains(&toml, "credentials.UAT api_key_env must not be empty");
    }

    #[test]
    fn validate_rejects_empty_api_secret_env_name() {
        let toml = skeleton_md().replace(
            "api_secret_env = \"CRYPTOCOM_API_SECRET\"",
            "api_secret_env = \"\"",
        );
        validate_err_contains(&toml, "credentials.UAT api_secret_env must not be empty");
    }

    #[test]
    fn validate_rejects_md_missing_symbol_routing() {
        let toml = skeleton_md().replace("symbol_routing = \"all\"\n", "");
        validate_err_contains(&toml, "role = market_data requires symbol_routing");
    }

    #[test]
    fn validate_rejects_malformed_first_char_range() {
        let toml = skeleton_md().replace(
            "symbol_routing = \"all\"",
            "symbol_routing = { first_char_ranges = [\"not-a-range\"] }",
        );
        validate_err_contains(&toml, "is not X-Y");
    }

    #[test]
    fn validate_rejects_inverted_first_char_range() {
        let toml = skeleton_md().replace(
            "symbol_routing = \"all\"",
            "symbol_routing = { first_char_ranges = [\"D-A\"] }",
        );
        validate_err_contains(&toml, "is inverted (start > end after uppercase)");
    }

    #[test]
    fn validate_rejects_empty_first_char_ranges() {
        let toml = skeleton_md().replace(
            "symbol_routing = \"all\"",
            "symbol_routing = { first_char_ranges = [] }",
        );
        validate_err_contains(&toml, "first_char_ranges must not be empty");
    }

    #[test]
    fn validate_rejects_non_ascii_first_char_range() {
        let toml = skeleton_md().replace(
            "symbol_routing = \"all\"",
            "symbol_routing = { first_char_ranges = [\"Å-Ö\"] }",
        );
        validate_err_contains(&toml, "must be ASCII");
    }

    #[test]
    fn validate_rejects_host_with_scheme() {
        let toml = skeleton_md().replace(
            "host = \"fix-uat.crypto.com\"",
            "host = \"tcp://fix-uat.crypto.com\"",
        );
        validate_err_contains(&toml, "host must not contain '://'");
    }

    #[test]
    fn validate_rejects_host_with_whitespace() {
        let toml = skeleton_md().replace(
            "host = \"fix-uat.crypto.com\"",
            "host = \"fix-uat.crypto.com \"",
        );
        validate_err_contains(&toml, "host must not contain whitespace");
    }

    #[test]
    fn validate_accepts_zero_om_with_md() {
        let mut c = parse_config(&skeleton_md()).unwrap();
        validate_config(&mut c).unwrap();
        assert_eq!(c.sessions[0].compiled_routing, Some(CompiledSymbolRouting::All));
    }

    #[test]
    fn validate_accepts_om_with_zero_md() {
        let toml = r#"
[connection]
heartbeat_interval = 30
max_reconnect_attempts = 10
reconnect_backoff_ms = 1000

[credentials.UAT]
api_key_env = "CRYPTOCOM_API_KEY"
api_secret_env = "CRYPTOCOM_API_SECRET"

[[sessions]]
name = "om1"
role = "order_management"
host = "fix-uat.crypto.com"
port = 34404
sender_comp_id = "S"
target_comp_id = "T"
credentials_ref = "UAT"
"#;
        let mut c = parse_config(toml).unwrap();
        validate_config(&mut c).unwrap();
        assert!(c.sessions[0].compiled_routing.is_none());
    }

    #[test]
    fn validate_ignores_symbol_routing_on_om() {
        let toml = r#"
[connection]
heartbeat_interval = 30
max_reconnect_attempts = 10
reconnect_backoff_ms = 1000

[credentials.UAT]
api_key_env = "CRYPTOCOM_API_KEY"
api_secret_env = "CRYPTOCOM_API_SECRET"

[[sessions]]
name = "om1"
role = "order_management"
host = "fix-uat.crypto.com"
port = 34404
sender_comp_id = "S"
target_comp_id = "T"
credentials_ref = "UAT"
symbol_routing = "all"
"#;
        let mut c = parse_config(toml).unwrap();
        validate_config(&mut c).unwrap();
        assert!(c.sessions[0].compiled_routing.is_none());
    }

    fn parse_and_validate_with_env_names(key_env: &str, secret_env: &str) -> Config {
        let toml = format!(
            r#"
[connection]
heartbeat_interval = 30
max_reconnect_attempts = 10
reconnect_backoff_ms = 1000

[credentials.UAT]
api_key_env = "{key_env}"
api_secret_env = "{secret_env}"

[[sessions]]
name = "s1"
role = "market_data"
host = "fix-uat.crypto.com"
port = 34402
sender_comp_id = "S"
target_comp_id = "T"
credentials_ref = "UAT"
symbol_routing = "all"
"#
        );
        let mut c = parse_config(&toml).unwrap();
        validate_config(&mut c).unwrap();
        c
    }

    #[test]
    fn resolve_credentials_unset_is_err() {
        let key = "HFT_LLD_TEST_KEY_UNSET_9f3c";
        let secret = "HFT_LLD_TEST_SECRET_UNSET_9f3c";
        unsafe {
            std::env::remove_var(key);
            std::env::remove_var(secret);
        }
        let c = parse_and_validate_with_env_names(key, secret);
        let err = resolve_credentials(&c).expect_err("unset must fail");
        let msg = format!("{err:#}");
        assert!(msg.contains("is not set"), "got {msg:?}");
        assert!(msg.contains(key), "got {msg:?}");
        unsafe {
            std::env::remove_var(key);
            std::env::remove_var(secret);
        }
    }

    #[test]
    fn resolve_credentials_empty_is_err() {
        let key = "HFT_LLD_TEST_KEY_EMPTY";
        let secret = "HFT_LLD_TEST_SECRET_EMPTY";
        unsafe {
            std::env::set_var(key, "");
            std::env::set_var(secret, "not-empty");
        }
        let c = parse_and_validate_with_env_names(key, secret);
        let err = resolve_credentials(&c).expect_err("empty must fail");
        let msg = format!("{err:#}");
        assert!(msg.contains("is empty"), "got {msg:?}");
        unsafe {
            std::env::remove_var(key);
            std::env::remove_var(secret);
        }
    }

    #[test]
    fn resolve_credentials_whitespace_is_err() {
        let key = "HFT_LLD_TEST_KEY_WS";
        let secret = "HFT_LLD_TEST_SECRET_WS";
        unsafe {
            std::env::set_var(key, "   \t");
            std::env::set_var(secret, "not-empty");
        }
        let c = parse_and_validate_with_env_names(key, secret);
        let err = resolve_credentials(&c).expect_err("whitespace must fail");
        let msg = format!("{err:#}");
        assert!(msg.contains("is empty"), "got {msg:?}");
        assert!(!msg.contains("is not set"), "got {msg:?}");
        unsafe {
            std::env::remove_var(key);
            std::env::remove_var(secret);
        }
    }

    #[test]
    fn resolve_credentials_success_and_debug_redacted() {
        let key = "HFT_LLD_TEST_KEY_OK";
        let secret = "HFT_LLD_TEST_SECRET_OK";
        unsafe {
            std::env::set_var(key, "dummy-key-value-xyz");
            std::env::set_var(secret, "dummy-secret-value-xyz");
        }
        let c = parse_and_validate_with_env_names(key, secret);
        let map = resolve_credentials(&c).unwrap();
        let creds = map.get("UAT").expect("UAT ref");
        assert_eq!(creds.api_key, "dummy-key-value-xyz");
        assert_eq!(creds.api_secret, "dummy-secret-value-xyz");
        let dbg = format!("{:?}", creds);
        assert!(!dbg.contains("dummy-key-value-xyz"), "got {dbg:?}");
        assert!(!dbg.contains("dummy-secret-value-xyz"), "got {dbg:?}");
        assert!(dbg.contains("<redacted>"), "got {dbg:?}");
        unsafe {
            std::env::remove_var(key);
            std::env::remove_var(secret);
        }
    }

    #[test]
    fn resolve_credentials_skips_unused_block() {
        let key = "HFT_LLD_TEST_KEY_USED";
        let secret = "HFT_LLD_TEST_SECRET_USED";
        let unused = "HFT_LLD_TEST_UNUSED_KEY";
        unsafe {
            std::env::set_var(key, "used-key");
            std::env::set_var(secret, "used-secret");
            std::env::remove_var(unused);
        }
        let toml = format!(
            r#"
[connection]
heartbeat_interval = 30
max_reconnect_attempts = 10
reconnect_backoff_ms = 1000

[credentials.UAT]
api_key_env = "{key}"
api_secret_env = "{secret}"

[credentials.UNUSED]
api_key_env = "{unused}"
api_secret_env = "HFT_LLD_TEST_UNUSED_SECRET"

[[sessions]]
name = "s1"
role = "market_data"
host = "fix-uat.crypto.com"
port = 34402
sender_comp_id = "S"
target_comp_id = "T"
credentials_ref = "UAT"
symbol_routing = "all"
"#
        );
        let mut c = parse_config(&toml).unwrap();
        validate_config(&mut c).unwrap();
        let map = resolve_credentials(&c).unwrap();
        assert!(map.contains_key("UAT"));
        assert!(!map.contains_key("UNUSED"));
        unsafe {
            std::env::remove_var(key);
            std::env::remove_var(secret);
            std::env::remove_var(unused);
        }
    }

    #[test]
    fn missing_symbol_limits_and_risk_parse_as_defaults() {
        let mut c = parse_config(&skeleton_md()).unwrap();
        validate_config(&mut c).unwrap();
        assert!(c.symbol_limits.is_empty());
        assert!(c.risk.session_max_drawdown.is_none());
    }

    #[test]
    fn parse_config_uat_example_file_includes_risk_gates() {
        let src = include_str!("../config.example.toml");
        let mut c = parse_config(src).unwrap();
        validate_config(&mut c).unwrap();
        assert_eq!(c.symbol_limits.len(), 3);
        assert_eq!(c.symbol_limits[0].symbol, "BTC_USDT");
        assert_eq!(c.symbol_limits[1].symbol, "BTC_USD");
        assert_eq!(c.symbol_limits[2].symbol, "BTCUSD-PERP");
        assert_eq!(c.symbol_limits[0].max_position, 0.05);
        assert_eq!(c.symbol_limits[0].max_notional, 5000.0);
        assert_eq!(c.risk.session_max_drawdown, Some(0.0));
    }

    #[test]
    fn duplicate_symbol_limits_is_rejected() {
        let toml = skeleton_md()
            + r#"
[[symbol_limits]]
symbol = "BTC_USDT"
max_position = 0.05
max_notional = 5000.0

[[symbol_limits]]
symbol = " BTC_USDT "
max_position = 0.1
max_notional = 1000.0
"#;
        validate_err_contains(&toml, "duplicate symbol_limits symbol: BTC_USDT");
    }

    #[test]
    fn empty_symbol_limits_symbol_is_rejected() {
        let toml = skeleton_md()
            + r#"
[[symbol_limits]]
symbol = "   "
max_position = 0.05
max_notional = 5000.0
"#;
        validate_err_contains(&toml, "symbol_limits symbol must not be empty");
    }

    #[test]
    fn non_positive_or_non_finite_caps_are_rejected() {
        validate_err_contains(
            &(skeleton_md()
                + r#"
[[symbol_limits]]
symbol = "BTC_USDT"
max_position = 0.0
max_notional = 5000.0
"#),
            "max_position must be finite and > 0",
        );
        validate_err_contains(
            &(skeleton_md()
                + r#"
[[symbol_limits]]
symbol = "BTC_USDT"
max_position = 0.05
max_notional = -1.0
"#),
            "max_notional must be finite and > 0",
        );
        validate_err_contains(
            &(skeleton_md()
                + r#"
[[symbol_limits]]
symbol = "BTC_USDT"
max_position = nan
max_notional = 5000.0
"#),
            "max_position must be finite and > 0",
        );
        validate_err_contains(
            &(skeleton_md()
                + r#"
[[symbol_limits]]
symbol = "BTC_USDT"
max_position = 0.05
max_notional = inf
"#),
            "max_notional must be finite and > 0",
        );
    }

    #[test]
    fn nan_inf_session_max_drawdown_is_rejected() {
        validate_err_contains(
            &(skeleton_md() + "\n[risk]\nsession_max_drawdown = nan\n"),
            "risk.session_max_drawdown must be finite",
        );
        validate_err_contains(
            &(skeleton_md() + "\n[risk]\nsession_max_drawdown = inf\n"),
            "risk.session_max_drawdown must be finite",
        );
        validate_err_contains(
            &(skeleton_md() + "\n[risk]\nsession_max_drawdown = -inf\n"),
            "risk.session_max_drawdown must be finite",
        );
    }

    #[test]
    fn non_positive_session_max_drawdown_validates_as_off() {
        for snippet in [
            "\n[risk]\nsession_max_drawdown = 0.0\n",
            "\n[risk]\nsession_max_drawdown = -1.0\n",
        ] {
            let mut c = parse_config(&(skeleton_md() + snippet)).unwrap();
            validate_config(&mut c).unwrap();
            assert!(c.risk.session_max_drawdown.unwrap() <= 0.0);
        }
    }

    fn sample_order(symbol: &str, side: char, qty: f64, price: Option<f64>) -> OrderRequest {
        OrderRequest {
            cl_ord_id: "T1".into(),
            symbol: symbol.into(),
            side,
            qty,
            price,
            ord_type: None,
            time_in_force: None,
            currency: None,
            exec_inst: None,
            quote_id: None,
            cash_order_qty: None,
            cash_margin: None,
            trigger_price_type: None,
            self_match_prevention_scope: None,
            self_match_prevention_instruction: None,
            self_match_prevention_id: None,
            broker_id: None,
            receive_window: None,
        }
    }

    fn btc_limits() -> HashMap<String, SymbolLimit> {
        let mut m = HashMap::new();
        m.insert(
            "BTC_USDT".into(),
            SymbolLimit {
                max_position: 0.05,
                max_notional: 5000.0,
            },
        );
        m
    }

    #[test]
    fn check_exposure_empty_map_is_off() {
        let order = sample_order("ANY", '1', 99.0, Some(1.0));
        assert_eq!(check_exposure(&order, 0.0, &HashMap::new()), Ok(()));
    }

    #[test]
    fn check_exposure_unknown_symbol_fail_closed() {
        let order = sample_order("ETH_USDT", '1', 0.001, Some(100.0));
        assert_eq!(
            check_exposure(&order, 0.0, &btc_limits()),
            Err(ExposureReject::UnknownSymbol)
        );
    }

    #[test]
    fn check_exposure_position_and_notional_and_reduce() {
        let limits = btc_limits();
        let over_pos = sample_order("BTC_USDT", '1', 0.1, Some(100.0));
        assert_eq!(
            check_exposure(&over_pos, 0.0, &limits),
            Err(ExposureReject::Position)
        );

        let in_limit = sample_order("BTC_USDT", '1', 0.01, Some(100.0));
        assert_eq!(check_exposure(&in_limit, 0.0, &limits), Ok(()));

        // Already at/above cap: a sell that reduces |net| is not position-blocked.
        let reduce = sample_order("BTC_USDT", '2', 0.02, Some(100.0));
        assert_eq!(check_exposure(&reduce, 0.05, &limits), Ok(()));

        // Reverse-through-flat that grows |net| is capped.
        let reverse_grow = sample_order("BTC_USDT", '2', 0.12, Some(100.0));
        assert_eq!(
            check_exposure(&reverse_grow, 0.05, &limits),
            Err(ExposureReject::Position)
        );

        // Notional is not reduce-exempt.
        let reduce_fat_notional = sample_order("BTC_USDT", '2', 0.02, Some(300_000.0));
        assert_eq!(
            check_exposure(&reduce_fat_notional, 0.05, &limits),
            Err(ExposureReject::Notional)
        );
    }

    #[test]
    fn check_exposure_unpriced_market_skips_notional() {
        let mut order = sample_order("BTC_USDT", '1', 0.01, None);
        order.cash_order_qty = None;
        assert_eq!(check_exposure(&order, 0.0, &btc_limits()), Ok(()));

        order.cash_order_qty = Some(6000.0);
        assert_eq!(
            check_exposure(&order, 0.0, &btc_limits()),
            Err(ExposureReject::Notional)
        );
    }

    #[test]
    fn check_exposure_invalid_qty_side_price_cash() {
        let limits = btc_limits();
        let mut o = sample_order("BTC_USDT", '1', 0.0, Some(100.0));
        assert_eq!(check_exposure(&o, 0.0, &limits), Err(ExposureReject::InvalidQty));
        o.qty = f64::NAN;
        assert_eq!(check_exposure(&o, 0.0, &limits), Err(ExposureReject::InvalidQty));
        o.qty = 0.01;
        o.side = '3';
        assert_eq!(check_exposure(&o, 0.0, &limits), Err(ExposureReject::InvalidSide));
        o.side = '1';
        o.price = Some(0.0);
        assert_eq!(check_exposure(&o, 0.0, &limits), Err(ExposureReject::InvalidPrice));
        o.price = Some(100.0);
        o.cash_order_qty = Some(f64::INFINITY);
        assert_eq!(
            check_exposure(&o, 0.0, &limits),
            Err(ExposureReject::InvalidCashQty)
        );
    }

    fn skeleton_md_plus(extra: &str) -> String {
        format!("{}\n{extra}\n", skeleton_md())
    }

    #[test]
    fn validate_omitted_tls_defaults_off() {
        let mut c = parse_config(&skeleton_md()).unwrap();
        validate_config(&mut c).unwrap();
        assert_eq!(c.sessions[0].tls, TlsSetting::Off);
        assert!(c.sessions[0].tls_ca_path.is_none());
        assert!(c.sessions[0].tls_server_name.is_none());
    }

    #[test]
    fn validate_tls_false_is_ok() {
        let mut c = parse_config(&skeleton_md_plus("tls = false")).unwrap();
        validate_config(&mut c).unwrap();
        assert_eq!(c.sessions[0].tls, TlsSetting::Off);
    }

    #[test]
    fn validate_tls_true_without_ca_is_ok() {
        let mut c = parse_config(&skeleton_md_plus("tls = true")).unwrap();
        validate_config(&mut c).unwrap();
        assert!(c.sessions[0].tls.is_enabled());
        assert!(c.sessions[0].tls_ca_path.is_none());
    }

    #[test]
    fn validate_rejects_junk_tls_string() {
        validate_err_contains(
            &skeleton_md_plus("tls = \"yes\""),
            "session 's1' tls must be true or false",
        );
    }

    #[test]
    fn validate_rejects_junk_tls_integer() {
        validate_err_contains(
            &skeleton_md_plus("tls = 1"),
            "session 's1' tls must be true or false",
        );
    }

    #[test]
    fn validate_rejects_ca_path_when_tls_off() {
        validate_err_contains(
            &skeleton_md_plus("tls = false\ntls_ca_path = \"certs/venue-ca.pem\""),
            "session 's1' tls_ca_path is set but tls = false",
        );
    }

    #[test]
    fn validate_rejects_server_name_when_tls_off() {
        validate_err_contains(
            &skeleton_md_plus("tls = false\ntls_server_name = \"fix.example.com\""),
            "session 's1' tls_server_name is set but tls = false",
        );
    }

    #[test]
    fn validate_rejects_empty_tls_ca_path() {
        validate_err_contains(
            &skeleton_md_plus("tls = true\ntls_ca_path = \"\""),
            "session 's1' tls_ca_path must not be empty",
        );
    }

    #[test]
    fn validate_rejects_whitespace_tls_ca_path() {
        validate_err_contains(
            &skeleton_md_plus("tls = true\ntls_ca_path = \"   \""),
            "session 's1' tls_ca_path must not be empty",
        );
    }

    #[test]
    fn validate_rejects_bad_tls_server_name() {
        validate_err_contains(
            &skeleton_md_plus("tls = true\ntls_server_name = \"https://fix.example.com\""),
            "session 's1' tls_server_name must be a hostname (no whitespace, no scheme)",
        );
        validate_err_contains(
            &skeleton_md_plus("tls = true\ntls_server_name = \"fix example.com\""),
            "session 's1' tls_server_name must be a hostname (no whitespace, no scheme)",
        );
        validate_err_contains(
            &skeleton_md_plus("tls = true\ntls_server_name = \"\""),
            "session 's1' tls_server_name must be a hostname (no whitespace, no scheme)",
        );
    }

    #[test]
    fn validate_rejects_unreadable_tls_ca_path() {
        validate_err_contains(
            &skeleton_md_plus("tls = true\ntls_ca_path = \"/no/such/hft-tls-ca.pem\""),
            "session 's1' tls_ca_path '/no/such/hft-tls-ca.pem' is not a readable file",
        );
    }

    #[test]
    fn validate_rejects_invalid_pem_tls_ca_path() {
        let path = tls_test_listener::write_temp_bytes("hft-bad-ca", b"not-a-cert").unwrap();
        let extra = format!("tls = true\ntls_ca_path = \"{}\"", path.display());
        validate_err_contains(
            &skeleton_md_plus(&extra),
            &format!(
                "session 's1' tls_ca_path '{}' is not a valid PEM or DER certificate",
                path.display()
            ),
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn validate_accepts_readable_pem_tls_ca_path() {
        let pem = tls_test_listener::mint_self_signed_ca_pem("validate-ca").unwrap();
        let path = tls_test_listener::write_temp_bytes("hft-good-ca", &pem).unwrap();
        let extra = format!(
            "tls = true\ntls_ca_path = \"{}\"\ntls_server_name = \"fix-prd.crypto.com\"",
            path.display()
        );
        let mut c = parse_config(&skeleton_md_plus(&extra)).unwrap();
        validate_config(&mut c).unwrap();
        assert!(c.sessions[0].tls.is_enabled());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn example_configs_leave_tls_off() {
        for src in [
            include_str!("../config.example.toml"),
            include_str!("../config.prd3.example.toml"),
        ] {
            let c = parse_config(src).unwrap();
            for s in &c.sessions {
                assert_eq!(s.tls, TlsSetting::Off);
                assert!(s.tls_ca_path.is_none());
                assert!(s.tls_server_name.is_none());
            }
            // Comment blocks may mention the key; no [[sessions]] row may set it.
            for line in src.lines() {
                let trimmed = line.trim();
                if trimmed.starts_with('#') || trimmed.is_empty() {
                    continue;
                }
                assert!(
                    !trimmed.starts_with("tls ") && !trimmed.starts_with("tls="),
                    "example session rows must omit tls (got {trimmed:?})"
                );
            }
        }
    }
}


#[cfg(test)]
mod connectivity_tests {
    use super::*;
    use super::alerts::{self, AlertKind};
    use std::sync::atomic::Ordering;

    /// Connect, logon, and block until the Logon ACK (35=A) is processed —
    /// mirrors what `message_loop` does in production, trimmed to what a
    /// test needs.
    async fn connect_and_logon(session: &FIXSession, port: u16) -> Result<()> {
        session.connect_with_retry(&format!("127.0.0.1:{}", port), 5, 20).await?;
        session.logon(30).await?;

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if Instant::now() > deadline {
                return Err(anyhow!("Timed out waiting for Logon ACK"));
            }
            match tokio::time::timeout(Duration::from_millis(500), session.receive_message()).await {
                Ok(Ok(msg)) => {
                    let is_logon_ack = msg.msg_type == 'A';
                    session.handle_message(&msg).await?;
                    if is_logon_ack {
                        *session.connection_state.lock().await = ConnectionState::Ready;
                        return Ok(());
                    }
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => continue, // read timeout, keep waiting up to deadline
            }
        }
    }

    /// Drain and process inbound messages for up to `budget`, or until
    /// `stop_when` returns true after processing a message. Returns the
    /// message that satisfied `stop_when`, if any, so callers can assert on
    /// its raw fields (e.g. ExecType/OrdStatus) independent of how
    /// `handle_message` chooses to summarize it into other stores.
    async fn pump_messages(session: &FIXSession, budget: Duration, mut stop_when: impl FnMut(&FIXMessage) -> bool) -> Result<Option<FIXMessage>> {
        let deadline = Instant::now() + budget;
        while Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(200), session.receive_message()).await {
                Ok(Ok(msg)) => {
                    let stop = stop_when(&msg);
                    session.handle_message(&msg).await?;
                    if stop {
                        return Ok(Some(msg));
                    }
                }
                Ok(Err(_)) => return Ok(None), // connection closed
                Err(_) => continue,            // read timeout, keep pumping
            }
        }
        Ok(None)
    }

    async fn wait_for_alert(session: &FIXSession, kind: alerts::AlertKind, budget: Duration) -> bool {
        let deadline = Instant::now() + budget;
        while Instant::now() < deadline {
            let found = {
                let buf = session.alerts.lock().await;
                buf.iter().any(|a| a.kind == kind)
            };
            if found {
                return true;
            }
            sleep(Duration::from_millis(50)).await;
        }
        false
    }

    fn cleanup_processed(cl_ord_id: &str) {
        let _ = std::fs::remove_file(format!("orders/processed/{}.json", cl_ord_id));
    }

    /// AC6 (end-to-end smoke test) + AC3 (order gateway logs on, submits an
    /// order, correctly parses the resulting ExecutionReport into a typed
    /// event) + AC1 (feed handler connects/subscribes/publishes normalized
    /// ticks): a stub strategy subscribes to market data, receives a tick,
    /// submits an order through the gateway, and receives a matching
    /// execution report — all against the mock FIX counterparty.
    #[tokio::test]
    async fn e2e_market_data_and_order_roundtrip() -> Result<()> {
        let port = mock_gateway::start_mock_gateway(
            "127.0.0.1:0",
            mock_gateway::MockGatewayConfig { md_ticks_to_stream: 2, ..Default::default() },
        )
        .await?;

        let session = FIXSession::new("TEST.CLIENT", "MOCK.VENUE");
        connect_and_logon(&session, port).await?;

        session.market_data_request("BTC_USDT", 10).await?;
        pump_messages(&session, Duration::from_secs(3), |_| false).await?;

        let quote = session.get_best_quote("BTC_USDT").await;
        assert!(quote.is_some(), "expected a normalized tick/book update to have been published");
        let (bid, ask, _spread) = quote.unwrap();
        assert!(bid > 0.0 && ask > bid);

        let cl_ord_id = "E2E_TEST_ORDER_1";
        session.new_order_single(cl_ord_id, "BTC_USDT", '1', 1.0, 100.0, None, None, None).await?;
        pump_messages(&session, Duration::from_secs(3), |msg| msg.msg_type == '8').await?;

        let open_orders = session.open_orders.snapshot();
        let order = open_orders.get(cl_ord_id).expect("expected ExecutionReport to populate open_orders");
        assert_eq!(order.cl_ord_id, cl_ord_id);
        assert_eq!(order.exec_type, "0", "expected ExecType=New (0) for a fresh order");
        assert_eq!(
            order_events::map_exec_type(&order.exec_type, &order.ord_status),
            order_events::OrderEventType::New
        );

        cleanup_processed(cl_ord_id);
        Ok(())
    }

    /// A session whose instruments are quiet must NOT be torn down. The
    /// watchdog used to measure only market data, so on UAT -- where 1,114 of
    /// 1,118 snapshots were empty -- it killed healthy sessions every 30s
    /// while 1,210 heartbeats were arriving, ~20 reconnects an hour.
    #[test]
    fn quiet_market_data_is_not_a_stalled_stream() {
        let now = 1_000_000u64;
        let (stall, quiet) = (30_000u64, 60_000u64);

        // Heartbeats 1s ago, no market data for 5 minutes: alive, just quiet.
        assert_eq!(feed_health(now, now - 1_000, now - 300_000, stall, quiet),
                   FeedHealth::QuietFeed { md_silent_ms: 300_000 });

        // Nothing inbound at all for 45s: genuinely stalled.
        assert_eq!(feed_health(now, now - 45_000, now - 45_000, stall, quiet),
                   FeedHealth::Stalled { silent_ms: 45_000 });

        // Both flowing.
        assert_eq!(feed_health(now, now - 500, now - 500, stall, quiet), FeedHealth::Ok);

        // Nothing seen yet (fresh session): never force a reconnect.
        assert_eq!(feed_health(now, 0, 0, stall, quiet), FeedHealth::Ok);
    }

    /// Only configured symbols are subscribed to, and only if this port lists
    /// them. Subscribing to every listed symbol flooded the venue with ~2,200
    /// requests per reconnect and broke the socket, causing another reconnect.
    #[test]
    fn subscribes_only_to_configured_symbols_listed_on_the_port() {
        let listed: Vec<String> = ["BTC_USDT", "ATOM_USDT", "SXTUSD-PERP"].iter().map(|s| s.to_string()).collect();
        let allow: Vec<String> = ["BTC_USDT", "ETH_USD"].iter().map(|s| s.to_string()).collect();
        assert_eq!(subscriptions_for_port(&listed, &allow), vec!["BTC_USDT".to_string()],
                   "only the configured symbol that this port lists");
        assert!(subscriptions_for_port(&listed, &[]).is_empty(),
                "an empty allowlist must subscribe to nothing, never to everything");
    }

    /// Several messages arriving in one TCP read must be extracted one at a
    /// time. extract_complete_message used to locate the checksum with
    /// rfind("10="), i.e. the LAST checksum in the whole buffer, then drain and
    /// decode everything up to it as a single message -- so every message but
    /// one in a coalesced read vanished. Their MsgSeqNums then read as a gap,
    /// recovery triggered bursts that coalesced again, and the client looped in
    /// recovery dropping inbound messages, ExecutionReports included.
    #[tokio::test]
    async fn extracts_each_of_several_messages_from_one_read() -> Result<()> {
        let session = FIXSession::new("TEST.FRAMING", "MOCK.VENUE");
        let mut bytes = Vec::new();
        for seq in 1..=3u32 {
            let mut m = FIXMessage::new('0', "MOCK.VENUE", "TEST.FRAMING", seq);
            // Tag 110 contains "10=" -- the old parser could mistake it for a checksum.
            m.set_field(110, "5");
            bytes.extend(m.encode()?);
        }
        session.message_buffer.lock().await.extend_from_slice(&bytes);

        for expected_seq in 1..=3u32 {
            let msg = session.extract_complete_message().await?;
            assert_eq!(msg.seq_num, expected_seq, "messages must come out one at a time, in order");
        }
        assert!(session.message_buffer.lock().await.is_empty(), "nothing may be left over or lost");
        Ok(())
    }

    /// A message split across reads must wait for the rest, then extract whole.
    #[tokio::test]
    async fn waits_for_a_message_split_across_reads() -> Result<()> {
        let session = FIXSession::new("TEST.FRAMING", "MOCK.VENUE");
        let bytes = FIXMessage::new('0', "MOCK.VENUE", "TEST.FRAMING", 7).encode()?;
        let (head, tail) = bytes.split_at(bytes.len() - 5);
        session.message_buffer.lock().await.extend_from_slice(head);
        assert!(session.extract_complete_message().await.is_err(), "half a message must not extract");
        session.message_buffer.lock().await.extend_from_slice(tail);
        assert_eq!(session.extract_complete_message().await?.seq_num, 7);
        Ok(())
    }

    /// A TestRequest (35=1) must be answered with a Heartbeat echoing its
    /// TestReqID (112). The live venue sends these and hangs up when they go
    /// unanswered, which disconnected every session every 15-25 seconds for
    /// days. The mock reproduces that: it probes after Logon and drops the
    /// connection if the echo does not arrive.
    #[tokio::test]
    async fn answers_test_request_and_keeps_the_session_up() -> Result<()> {
        let port = mock_gateway::start_mock_gateway(
            "127.0.0.1:0",
            mock_gateway::MockGatewayConfig {
                test_request_after_logon: Some("PROBE-1".to_string()),
                ..Default::default()
            },
        )
        .await?;

        let session = FIXSession::new("TEST.CLIENT", "MOCK.VENUE");
        connect_and_logon(&session, port).await?;

        // Pump well past the mock's 2s grace period. If the TestRequest goes
        // unanswered the mock hangs up.
        pump_messages(&session, Duration::from_secs(4), |_| false).await?;

        // is_ready() is a cached flag and stays true across a peer-side close,
        // so prove liveness with traffic instead: an order can only be
        // acknowledged if the connection actually survived the probe.
        let cl_ord_id = "E2E_TEST_REQ_PROBE";
        session.new_order_single(cl_ord_id, "BTC_USDT", '1', 1.0, 100.0, None, None, None).await?;
        let report = pump_messages(&session, Duration::from_secs(3), |msg| msg.msg_type == '8').await?;

        assert!(
            report.is_some(),
            "no ExecutionReport after the probe window: the venue hung up because \
             the TestRequest (112) went unanswered"
        );

        cleanup_processed(cl_ord_id);
        Ok(())
    }

    /// AC3: cancel and cancel/replace round trips against the mock, verifying
    /// ExecutionReports map to Canceled and Replaced typed events.
    #[tokio::test]
    async fn cancel_and_replace_round_trip() -> Result<()> {
        let port = mock_gateway::start_mock_gateway("127.0.0.1:0", mock_gateway::MockGatewayConfig::default()).await?;
        let session = FIXSession::new("TEST.CLIENT", "MOCK.VENUE");
        connect_and_logon(&session, port).await?;

        let cl_ord_id = "E2E_TEST_ORDER_CXL";
        session.new_order_single(cl_ord_id, "BTC_USDT", '1', 1.0, 100.0, None, None, None).await?;
        pump_messages(&session, Duration::from_secs(3), |msg| msg.msg_type == '8').await?;

        let replace_id = "E2E_TEST_ORDER_REPL";
        session
            .order_cancel_replace_request(&ReplaceOrderRequest {
                cl_ord_id: replace_id.to_string(),
                orig_cl_ord_id: cl_ord_id.to_string(),
                symbol: "BTC_USDT".to_string(),
                side: '1',
                qty: Some(2.0),
                price: Some(101.0),
            })
            .await?;
        pump_messages(&session, Duration::from_secs(3), |msg| msg.msg_type == '8').await?;

        {
            let open_orders = session.open_orders.snapshot();
            let replaced = open_orders.get(replace_id).expect("expected Replaced ExecutionReport");
            assert_eq!(
                order_events::map_exec_type(&replaced.exec_type, &replaced.ord_status),
                order_events::OrderEventType::Replaced
            );
        }

        let cancel_req = CancelOrderRequest {
            cl_ord_id: "E2E_TEST_ORDER_CXL2".to_string(),
            orig_cl_ord_id: replace_id.to_string(),
            symbol: "BTC_USDT".to_string(),
            side: "1".to_string(),
        };
        session.cancel_order(&cancel_req).await?;
        let cancel_report = pump_messages(&session, Duration::from_secs(3), |msg| msg.msg_type == '8')
            .await?
            .expect("expected a Canceled ExecutionReport");
        assert_eq!(
            order_events::map_exec_type(
                cancel_report.get_field(150).unwrap_or_default(),
                cancel_report.get_field(tags::ORD_STATUS).unwrap_or_default(),
            ),
            order_events::OrderEventType::Canceled
        );
        // A fully canceled order (LeavesQty=0) is removed from the
        // open-orders store, since it's no longer open.
        assert!(session.open_orders.snapshot().get("E2E_TEST_ORDER_CXL2").is_none());

        cleanup_processed(cl_ord_id);
        cleanup_processed(replace_id);
        cleanup_processed("E2E_TEST_ORDER_CXL2");
        Ok(())
    }

    /// AC4: on disconnect, the session re-establishes (logon again) without
    /// a process restart — the mock closes the socket after one inbound
    /// application message, then the test reconnects the same `FIXSession`.
    #[tokio::test]
    async fn reconnects_and_relogs_on_after_disconnect() -> Result<()> {
        let port = mock_gateway::start_mock_gateway(
            "127.0.0.1:0",
            mock_gateway::MockGatewayConfig { disconnect_after_messages: Some(1), ..Default::default() },
        )
        .await?;

        let session = FIXSession::new("TEST.CLIENT", "MOCK.VENUE");
        connect_and_logon(&session, port).await?;
        assert!(session.is_ready().await);

        // Trip the mock's disconnect trigger and observe the connection drop.
        // (Heartbeats don't count toward `disconnect_after_messages` — same
        // as real FIX session-admin traffic not counting as "application
        // activity" — so use an application message here.) The mock answers
        // the request (a 'W' snapshot) before closing, so drain messages
        // until the connection actually drops.
        session.market_data_request("BTC_USDT", 10).await?;
        let mut saw_disconnect = false;
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(500), session.receive_message()).await {
                Ok(Ok(_msg)) => continue,
                Ok(Err(_)) => {
                    saw_disconnect = true;
                    break;
                }
                Err(_) => continue,
            }
        }
        assert!(saw_disconnect, "expected the mock to close the connection");

        // Reconnect + re-logon on the SAME session, no process restart.
        *session.connection_state.lock().await = ConnectionState::Disconnected;
        *session.is_connected.lock().await = false;
        connect_and_logon(&session, port).await?;
        assert!(session.is_ready().await, "session should be Ready again after reconnect + re-logon");

        Ok(())
    }

    /// Reconcile local open-order state with the venue: a ClOrdID the mock
    /// never saw (ghost, seeded only into the client store) is evicted after
    /// the rejecting ExecutionReport is applied, while a legitimately open
    /// order survives the same pass.
    #[tokio::test]
    async fn reconcile_open_orders_evicts_ghost_and_keeps_live() -> Result<()> {
        let port = mock_gateway::start_mock_gateway(
            "127.0.0.1:0",
            mock_gateway::MockGatewayConfig::default(),
        )
        .await?;

        let session = FIXSession::new("TEST.CLIENT", "MOCK.VENUE");
        connect_and_logon(&session, port).await?;

        let survivor = "RECON_SURVIVOR";
        session
            .new_order_single(survivor, "BTC_USDT", '1', 1.0, 100.0, None, None, None)
            .await?;
        pump_messages(&session, Duration::from_secs(3), |msg| msg.msg_type == '8').await?;
        assert!(
            session.open_orders.snapshot().get(survivor).is_some(),
            "survivor must be in the client store after the New ack"
        );

        let ghost = "RECON_GHOST";
        session.open_orders.update_order(
            ghost.to_string(),
            OpenOrder {
                cl_ord_id: ghost.to_string(),
                order_id: format!("SRV_{}", ghost),
                exec_id: format!("EXEC_{}", ghost),
                symbol: "BTC_USDT".to_string(),
                side: "1".to_string(),
                order_qty: "1.0".to_string(),
                price: "100.0".to_string(),
                ord_status: "0".to_string(),
                exec_type: "0".to_string(),
                cum_qty: "0".to_string(),
                leaves_qty: "1.0".to_string(),
                last_px: String::new(),
                last_qty: String::new(),
                avg_px: String::new(),
                transact_time: String::new(),
                order_create_time: String::new(),
                rejection_reason: String::new(),
            },
        );
        assert!(
            session.open_orders.snapshot().get(ghost).is_some(),
            "ghost must be in the client store before reconcile"
        );

        session.reconcile_open_orders().await?;
        let mut reports = 0u8;
        let pumped = pump_messages(&session, Duration::from_secs(3), |msg| {
            if msg.msg_type == '8' {
                reports += 1;
            }
            reports >= 2
        })
        .await?;
        assert!(
            pumped.is_some(),
            "expected both venue ExecutionReports (ghost reject + survivor status) after reconcile"
        );

        assert!(
            session.open_orders.snapshot().get(ghost).is_none(),
            "ghost ClOrdID unknown to the venue must be removed after reconcile"
        );
        assert!(
            session.open_orders.snapshot().get(survivor).is_some(),
            "legitimately open order must survive reconcile"
        );

        cleanup_processed(survivor);
        cleanup_processed(ghost);
        Ok(())
    }

    /// AC2, wired end-to-end (network-level): the mock silently drops one
    /// MarketDataIncrementalRefresh, inducing a real MsgSeqNum gap on the
    /// wire. `FIXSession::handle_message` must detect it, request recovery,
    /// and converge — proving the production gap-handling path (not just
    /// the standalone Sequencer unit tests in src/sequencer.rs) actually
    /// resolves and leaves the order book in a consistent state.
    #[tokio::test]
    async fn gap_recovery_converges_via_resubscribe_snapshot() -> Result<()> {
        let port = mock_gateway::start_mock_gateway(
            "127.0.0.1:0",
            mock_gateway::MockGatewayConfig {
                md_ticks_to_stream: 3,
                induce_gap_at_tick: Some(2),
                ..Default::default()
            },
        )
        .await?;

        let session = FIXSession::new("TEST.CLIENT", "MOCK.VENUE");
        connect_and_logon(&session, port).await?;

        session.market_data_request("BTC_USDT", 10).await?;

        // Observe: snapshot, tick1, (gap at tick2) -> recovery request ->
        // resubscribe -> fresh snapshot. Pump in slices and stop the moment
        // recovery converges rather than burning a fixed budget and asserting
        // afterwards -- every other test in this suite exits early on a
        // predicate; this one could not, so on a contended runner the 5s
        // expired mid-round-trip and failed a test that needs ~1s on an idle
        // box. The deadline keeps the assertion honest: genuine
        // non-convergence still fails, it just cannot fail spuriously.
        let recovery_deadline = Instant::now() + Duration::from_secs(30);
        loop {
            pump_messages(&session, Duration::from_millis(250), |_| false).await?;
            if !session.sequencer.lock().await.is_recovering("inbound") {
                break;
            }
            assert!(
                Instant::now() < recovery_deadline,
                "gap recovery should have converged within 30s"
            );
        }

        let quote = session.get_best_quote("BTC_USDT").await;
        assert!(quote.is_some(), "order book must still be in a valid, non-corrupted state after recovery");

        Ok(())
    }

    fn command_log_len(path: &str) -> usize {
        std::fs::read_to_string(path).map(|s| s.len()).unwrap_or(0)
    }

    /// Poll until every needle appears in content written *after* `before`.
    /// Needles must be unique to the stanza under test — matching a shared
    /// substring such as a bare ClOrdID can observe the earlier outbound
    /// NewOrderSingle (same 11=) before the inbound ExecutionReport is flushed.
    fn wait_for_new_log_contains(path: &str, before: usize, needles: &[&str]) -> String {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let contents = std::fs::read_to_string(path).unwrap_or_default();
            let new = if contents.len() >= before {
                &contents[before..]
            } else {
                contents.as_str()
            };
            if needles.iter().all(|n| new.contains(n)) {
                return contents;
            }
            if std::time::Instant::now() > deadline {
                panic!(
                    "timed out waiting for {:?} in {} after offset {}. new content:\n{}",
                    needles, path, before, new
                );
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }

    /// AC4 routing: `.UD` on sender *or* target (CompIDs swap on inbound) is OM;
    /// `.MD` only, or TEST.CLIENT isolation, is MD.
    #[test]
    fn is_om_command_log_is_direction_symmetric() {
        assert!(is_om_command_log("UAT1.WOZINGA.UD.00", "UAT1.CDC.UD"));
        assert!(is_om_command_log("UAT1.CDC.UD", "UAT1.WOZINGA.UD.00"));
        assert!(!is_om_command_log("UAT1.WOZINGA.MD.00", "UAT1.CDC.MD"));
        assert!(!is_om_command_log("UAT1.CDC.MD", "UAT1.WOZINGA.MD.00"));
        assert!(!is_om_command_log("TEST.CLIENT", "MOCK.VENUE"));
    }

    #[test]
    fn rotate_log_if_needed_over_threshold_creates_dot1_backup() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("hft_cmd_rot_{}.log", std::process::id()));
        let backup = format!("{}.1", path.as_os_str().to_string_lossy());
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&backup);

        std::fs::write(&path, vec![b'x'; 80]).unwrap();
        rotate_log_if_needed(&path, 64);
        assert!(
            Path::new(&backup).exists(),
            "backup must be {}.1, not a stripped-stem .1",
            path.file_name().unwrap().to_string_lossy()
        );
        assert_eq!(std::fs::read(&backup).unwrap(), vec![b'x'; 80]);
        assert!(!path.exists(), "active file is recreated on the next open, not here");

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&backup);
    }

    #[test]
    fn rotate_log_if_needed_equal_to_max_does_not_rotate() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("hft_cmd_eq_{}.log", std::process::id()));
        let backup = format!("{}.1", path.as_os_str().to_string_lossy());
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&backup);

        std::fs::write(&path, vec![b'y'; 64]).unwrap();
        rotate_log_if_needed(&path, 64);
        assert!(path.exists(), "len == max must not rotate");
        assert!(!Path::new(&backup).exists());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 64);

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(&backup);
    }

    /// AC1–3, AC6: inbound ExecutionReport / BusinessMessageReject / Logout
    /// land in OM_fix_command.log as full `|`-delimited IN stanzas. AC4: OM
    /// outbound NewOrderSingle still routes to the same OM file tagged OUT.
    #[tokio::test]
    async fn inbound_fix_commands_logged_in_full() -> Result<()> {
        let port = mock_gateway::start_mock_gateway(
            "127.0.0.1:0",
            mock_gateway::MockGatewayConfig {
                venue_comp_id: "UAT1.CDC.UD".into(),
                ..Default::default()
            },
        )
        .await?;

        let session = FIXSession::new("UAT1.WOZINGA.UD.00", "UAT1.CDC.UD");
        connect_and_logon(&session, port).await?;

        let cl_ord_id = format!("INLOG_{}_{}", std::process::id(), 1);
        let before_order = command_log_len(COMMAND_LOG_OM);
        session
            .new_order_single(&cl_ord_id, "BTC_USDT", '1', 1.0, 100.0, None, None, None)
            .await?;
        pump_messages(&session, Duration::from_secs(3), |msg| msg.msg_type == '8').await?;

        // Wait for both the outbound NewOrderSingle *and* the inbound
        // ExecutionReport. Logging is async in production; waiting only for
        // the IN stanza (or only for a bare ClOrdID shared with the OUT
        // stanza) is a release-mode race under `cargo test --release --locked`.
        let om_after_er = wait_for_new_log_contains(
            COMMAND_LOG_OM,
            before_order,
            &[
                "FIX OUT NewOrderSingle (35=D)",
                " UTC] FIX OUT NewOrderSingle",
                "FIX IN ExecutionReport (35=8)",
                &format!("11={}", cl_ord_id),
                "Sender=",
                "Target=CLIENT",
                "8=FIX.4.4|",
                "10=",
                " UTC] FIX IN ExecutionReport",
            ],
        );
        assert!(
            om_after_er.contains("FIX OUT NewOrderSingle (35=D)"),
            "AC4: OM outbound NewOrderSingle must still land in OM_fix_command.log"
        );
        assert!(om_after_er.contains("Sender=UAT1.WOZINGA.UD.00"));
        assert!(om_after_er.contains("Target=UAT1.CDC.UD"));
        let er_idx = om_after_er
            .rfind("FIX IN ExecutionReport (35=8)")
            .expect("IN ExecutionReport header");
        let er_slice = &om_after_er[er_idx..];
        assert!(
            er_slice.contains("8=FIX.4.4|") && er_slice.contains("|10="),
            "inbound ExecutionReport body must be a complete pipe-delimited frame, not hex/truncated: {}",
            &er_slice[..er_slice.len().min(400)]
        );
        assert!(
            !er_slice[..er_slice.find('\n').unwrap_or(er_slice.len())].contains("hex"),
            "header must not be a hex dump"
        );

        let before_status = command_log_len(COMMAND_LOG_OM);
        // 35=H is now handled by the mock (OrderStatusRequest → ExecutionReport),
        // so drive BMR coverage with still-unhandled SecurityListRequest (35=x).
        session.security_list_request("SEC_LIST_BMR").await?;
        pump_messages(&session, Duration::from_secs(3), |msg| msg.msg_type == 'j').await?;
        let om_after_j = wait_for_new_log_contains(
            COMMAND_LOG_OM,
            before_status,
            &[
                "FIX IN BusinessMessageReject (35=j)",
                "35=j|",
                "372=x|",
                "380=3|",
                "Sender=",
                "Target=CLIENT",
                " UTC] FIX IN BusinessMessageReject",
            ],
        );
        let j_idx = om_after_j
            .rfind("FIX IN BusinessMessageReject (35=j)")
            .expect("IN BusinessMessageReject header");
        let j_slice = &om_after_j[j_idx..];
        assert!(j_slice.contains("8=FIX.4.4|") && j_slice.contains("|10="));

        let before_logout = command_log_len(COMMAND_LOG_OM);
        session.logout().await?;
        // Pump immediately after logout — receive_message does not check
        // is_connected, but the mock closes after sending inbound 35=5.
        let _ = pump_messages(&session, Duration::from_secs(3), |msg| msg.msg_type == '5').await;
        let om_after_logout = wait_for_new_log_contains(
            COMMAND_LOG_OM,
            before_logout,
            &[
                "FIX IN Logout (35=5)",
                "Sender=",
                "Target=CLIENT",
                " UTC] FIX IN Logout",
            ],
        );
        let lo_idx = om_after_logout
            .rfind("FIX IN Logout (35=5)")
            .expect("IN Logout header");
        let lo_slice = &om_after_logout[lo_idx..];
        assert!(lo_slice.contains("8=FIX.4.4|") && lo_slice.contains("|10="));

        cleanup_processed(&cl_ord_id);
        Ok(())
    }

    /// AC4: MD CompIDs (`.MD` only, no `.UD`) still route outbound to
    /// `fix_commands.log`, not the OM file.
    #[test]
    fn md_outbound_still_routes_to_fix_commands_log() {
        let marker = format!("MD_ROUTE_{}", std::process::id());
        let mut msg = FIXMessage::new('V', "UAT1.WOZINGA.MD.00", "UAT1.CDC.MD", 1);
        msg.set_field(tags::MARKET_DATA_REQ_ID, &marker);
        let encoded = msg.encode().expect("encode MD request");
        let before = command_log_len(COMMAND_LOG_MD);
        FIXSession::log_fix_command(&msg, &encoded, Direction::Outbound);
        let contents = wait_for_new_log_contains(
            COMMAND_LOG_MD,
            before,
            &[
                "FIX OUT MarketDataRequest (35=V)",
                &marker,
                "Sender=UAT1.WOZINGA.MD.00",
                "Target=UAT1.CDC.MD",
            ],
        );
        assert!(contents.contains(&marker));
    }

    #[tokio::test]
    async fn config_driven_symbol_routing_records_only_admitted_symbols() -> Result<()> {
        // two mock gateways, recorder on
        let rec_all: Arc<tokio::sync::Mutex<Vec<String>>> =
            Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let rec_range: Arc<tokio::sync::Mutex<Vec<String>>> =
            Arc::new(tokio::sync::Mutex::new(Vec::new()));

        let port_all = mock_gateway::start_mock_gateway(
            "127.0.0.1:0",
            mock_gateway::MockGatewayConfig {
                md_requested_symbols: Some(Arc::clone(&rec_all)),
                ..Default::default()
            },
        )
        .await?;
        let port_range = mock_gateway::start_mock_gateway(
            "127.0.0.1:0",
            mock_gateway::MockGatewayConfig {
                md_requested_symbols: Some(Arc::clone(&rec_range)),
                ..Default::default()
            },
        )
        .await?;

        let sess_all = FIXSession::new("TEST.MD.ALL", "MOCK.VENUE");
        let sess_range = FIXSession::new("TEST.MD.RANGE", "MOCK.VENUE");
        connect_and_logon(&sess_all, port_all).await?;
        connect_and_logon(&sess_range, port_range).await?;

        let allowlist = ["BTC_USDT", "ETH_USDT"];
        let routing_all = CompiledSymbolRouting::All;
        let routing_em = compile_symbol_routing(
            "range",
            &SymbolRouting::FirstCharRanges(vec!["E-M".into()]),
        )?;

        for sym in allowlist {
            if session_admits_symbol(&routing_all, sym) {
                sess_all.market_data_request(sym, 10).await?;
            }
        }
        for sym in allowlist {
            if session_admits_symbol(&routing_em, sym) {
                sess_range.market_data_request(sym, 10).await?;
            }
        }

        pump_messages(&sess_all, Duration::from_secs(3), |_| false).await?;
        pump_messages(&sess_range, Duration::from_secs(3), |_| false).await?;

        let got_all: HashSet<String> = rec_all.lock().await.iter().cloned().collect();
        let got_range: HashSet<String> = rec_range.lock().await.iter().cloned().collect();
        let expect_all: HashSet<String> = allowlist
            .iter()
            .filter(|s| session_admits_symbol(&routing_all, s))
            .map(|s| s.to_string())
            .collect();
        let expect_range: HashSet<String> = allowlist
            .iter()
            .filter(|s| session_admits_symbol(&routing_em, s))
            .map(|s| s.to_string())
            .collect();

        assert_eq!(got_all, expect_all, "all-recorder must equal admitted set");
        assert_eq!(
            got_range, expect_range,
            "range-recorder must equal admitted set"
        );
        assert!(got_range.contains("ETH_USDT"), "overlap on ETH is required");
        assert!(got_all.contains("ETH_USDT") && got_all.contains("BTC_USDT"));
        assert!(!got_range.is_empty(), "range recorder must not be empty");
        // Do NOT assert got_all.is_disjoint(&got_range).

        // Optional corroboration
        assert!(sess_all.get_best_quote("ETH_USDT").await.is_some());
        assert!(sess_range.get_best_quote("ETH_USDT").await.is_some());
        Ok(())
    }

    /// Status helper must not share fate with a held `order_books` mutex.
    /// Exercises the lock/await layer directly (no FIX wire / mock_gateway).
    #[tokio::test]
    async fn market_data_session_status_returns_fast_when_order_books_lock_held() {
        let session = Arc::new(FIXSession::new_with_credentials(
            "TEST.STATUS",
            "MOCK.VENUE",
            "LEAK_API_KEY_XYZ",
            "LEAK_API_SECRET_XYZ",
        ));

        let (held_tx, held_rx) = tokio::sync::oneshot::channel();
        let holder_session = Arc::clone(&session);
        let holder = tokio::spawn(async move {
            let _g = holder_session.order_books.lock().await;
            let _ = held_tx.send(());
            sleep(Duration::from_secs(2)).await;
        });

        held_rx.await.expect("holder took order_books lock");

        let start = Instant::now();
        let value = market_data_session_status(&session, "BTC Market Data", 34402);
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_millis(100),
            "status helper blocked for {:?} (must be <100ms under a held order_books lock)",
            elapsed
        );
        assert_eq!(
            value.get("status_read").and_then(|v| v.as_str()),
            Some(STATUS_READ_SKIPPED_LOCK_CONTENDED)
        );
        assert_eq!(
            value.get("description").and_then(|v| v.as_str()),
            Some("BTC Market Data")
        );
        assert_eq!(value.get("port").and_then(|v| v.as_u64()), Some(34402));
        assert_eq!(
            value.get("sender_comp_id").and_then(|v| v.as_str()),
            Some("TEST.STATUS")
        );
        assert_eq!(
            value.get("target_comp_id").and_then(|v| v.as_str()),
            Some("MOCK.VENUE")
        );
        assert!(value.get("order_books").expect("order_books key").is_null());
        assert!(value.get("total_trades").expect("total_trades key").is_null());
        assert!(value.get("symbols").expect("symbols key").is_null());
        assert!(value.get("is_connected").expect("is_connected key").is_boolean());
        assert!(value.get("is_ready").expect("is_ready key").is_boolean());
        assert!(value.get("connection_state").expect("connection_state key").is_string());
        assert!(value.get("sequence_number").expect("sequence_number key").is_number());
        assert!(value.get("securities_count").expect("securities_count key").is_number());

        let dump = value.to_string();
        assert!(
            !dump.contains("LEAK_API_KEY_XYZ")
                && !dump.contains("LEAK_API_SECRET_XYZ")
                && !dump.contains("api_key")
                && !dump.contains("api_secret"),
            "status JSON must not leak credentials: {}",
            dump
        );

        holder.abort();
    }

    #[test]
    fn capture_symbols_unset_defaults_btc_usdt() {
        let set = parse_capture_symbols(None);
        assert_eq!(set, HashSet::from(["BTC_USDT".to_string()]));
    }

    #[test]
    fn capture_symbols_empty_is_kill_switch() {
        assert!(parse_capture_symbols(Some("")).is_empty());
    }

    #[test]
    fn capture_symbols_whitespace_is_kill_switch() {
        assert!(parse_capture_symbols(Some("  \t")).is_empty());
    }

    #[test]
    fn capture_symbols_comma_trim() {
        let set = parse_capture_symbols(Some("BTC_USDT, BTC_USD"));
        assert!(set.contains("BTC_USDT") && set.contains("BTC_USD"));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn capture_symbols_empty_token_not_kill() {
        let set = parse_capture_symbols(Some("BTC_USDT,,BTC_USD"));
        assert!(set.contains("BTC_USDT") && set.contains("BTC_USD"));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn capture_symbols_invalid_charset_dropped() {
        assert!(parse_capture_symbols(Some("BTC-USDT")).is_empty());
    }

    #[tokio::test]
    async fn replay_probes_seq_num_for_default_capture_symbol() -> Result<()> {
        let port = mock_gateway::start_mock_gateway(
            "127.0.0.1:0",
            mock_gateway::MockGatewayConfig { md_ticks_to_stream: 2, ..Default::default() },
        ).await?;
        let session = FIXSession::new("TEST.CLIENT", "MOCK.VENUE");
        let _ = session.drain_capture_probe();
        connect_and_logon(&session, port).await?;
        session.market_data_request("BTC_USDT", 10).await?;
        pump_messages(&session, Duration::from_secs(3), |_| false).await?;
        let evs = session.drain_capture_probe();
        assert!(
            evs.iter().any(|e| matches!(e,
                kdb_integration::ProbeEvent::PendingMdIo { kind: "snapshot", symbol, seq_num: Some(_) }
                if symbol == "BTC_USDT")),
            "expected snapshot probe with Some(seq) for BTC_USDT, got {:?}", evs
        );
        assert!(
            evs.iter().any(|e| matches!(e,
                kdb_integration::ProbeEvent::PendingMdIo { kind: "level", symbol, seq_num: Some(_) }
                if symbol == "BTC_USDT")),
            "expected level probe with Some(seq) for BTC_USDT, got {:?}", evs
        );
        Ok(())
    }

    #[tokio::test]
    async fn kill_switch_applies_book_but_does_not_probe_persist() -> Result<()> {
        let port = mock_gateway::start_mock_gateway(
            "127.0.0.1:0",
            mock_gateway::MockGatewayConfig { md_ticks_to_stream: 2, ..Default::default() },
        ).await?;
        let session = FIXSession::new("TEST.CLIENT", "MOCK.VENUE")
            .with_capture_symbols(HashSet::new());
        let _ = session.drain_capture_probe();
        connect_and_logon(&session, port).await?;
        session.market_data_request("BTC_USDT", 10).await?;
        pump_messages(&session, Duration::from_secs(3), |_| false).await?;
        assert!(session.get_best_quote("BTC_USDT").await.is_some(), "Live book independent of CAPTURE_SYMBOLS");
        let evs = session.drain_capture_probe();
        assert!(
            !evs.iter().any(|e| matches!(e, kdb_integration::ProbeEvent::PendingMdIo { .. })),
            "kill switch must not persist, got {:?}", evs
        );
        Ok(())
    }

    #[tokio::test]
    async fn symbol_not_in_allowlist_skips_persist() -> Result<()> {
        let port = mock_gateway::start_mock_gateway(
            "127.0.0.1:0",
            mock_gateway::MockGatewayConfig { md_ticks_to_stream: 2, ..Default::default() },
        ).await?;
        let session = FIXSession::new("TEST.CLIENT", "MOCK.VENUE")
            .with_capture_symbols(HashSet::from(["BTC_USDT".to_string()]));
        let _ = session.drain_capture_probe();
        connect_and_logon(&session, port).await?;
        session.market_data_request("ETH_USDT", 10).await?;
        pump_messages(&session, Duration::from_secs(3), |_| false).await?;
        assert!(session.get_best_quote("ETH_USDT").await.is_some(), "book for ETH_USDT must still apply");
        let evs = session.drain_capture_probe();
        assert!(
            !evs.iter().any(|e| matches!(e,
                kdb_integration::ProbeEvent::PendingMdIo { symbol, .. } if symbol == "ETH_USDT")),
            "ETH_USDT must not persist when allowlist is BTC_USDT, got {:?}", evs
        );
        Ok(())
    }

    #[tokio::test]
    async fn induce_gap_enqueues_capture_gap_probe() -> Result<()> {
        let port = mock_gateway::start_mock_gateway(
            "127.0.0.1:0",
            mock_gateway::MockGatewayConfig {
                md_ticks_to_stream: 3,
                induce_gap_at_tick: Some(2),
                ..Default::default()
            },
        ).await?;

        let session = FIXSession::new("TEST.CLIENT", "MOCK.VENUE");
        let _ = session.drain_capture_probe();
        connect_and_logon(&session, port).await?;
        session.market_data_request("BTC_USDT", 10).await?;

        let recovery_deadline = Instant::now() + Duration::from_secs(30);
        loop {
            pump_messages(&session, Duration::from_millis(250), |_| false).await?;
            if !session.sequencer.lock().await.is_recovering("inbound") {
                break;
            }
            assert!(Instant::now() < recovery_deadline, "gap recovery should have converged within 30s");
        }

        let evs = session.drain_capture_probe();
        let gaps: Vec<_> = evs.iter().filter_map(|e| match e {
            kdb_integration::ProbeEvent::CaptureGap { symbol, expected, received, gap_size } =>
                Some((symbol.clone(), *expected, *received, *gap_size)),
            _ => None,
        }).collect();
        assert!(
            gaps.iter().any(|(s, e, r, g)| s == "BTC_USDT" && *e == 4 && *r == 5 && *g == 1),
            "expected FIX gap probe (BTC_USDT, expected=4, received=5, gap_size=1), got {:?}",
            gaps
        );
        Ok(())
    }

    #[tokio::test]
    async fn capture_resumes_after_disconnect_with_per_session_seq_restart() -> Result<()> {
        let port = mock_gateway::start_mock_gateway(
            "127.0.0.1:0",
            mock_gateway::MockGatewayConfig {
                md_ticks_to_stream: 3,
                disconnect_after_messages: Some(1),
                ..Default::default()
            },
        ).await?;

        let session = FIXSession::new("TEST.CLIENT", "MOCK.VENUE");
        let _ = session.drain_capture_probe();
        connect_and_logon(&session, port).await?;
        session.market_data_request("BTC_USDT", 10).await?;

        let mut saw_disconnect = false;
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(500), session.receive_message()).await {
                Ok(Ok(msg)) => { session.handle_message(&msg).await?; }
                Ok(Err(_)) => { saw_disconnect = true; break; }
                Err(_) => continue,
            }
        }
        assert!(saw_disconnect, "expected the mock to close the connection");

        let session1 = session.drain_capture_probe();
        let seqs1: Vec<u32> = session1.iter().filter_map(|e| match e {
            kdb_integration::ProbeEvent::PendingMdIo { symbol, seq_num: Some(n), .. } if symbol == "BTC_USDT" => Some(*n),
            _ => None,
        }).collect();
        assert!(!seqs1.is_empty(), "session-1 ticks must have been enqueued (probe)");
        assert!(seqs1.iter().all(|n| *n >= 1), "FIX seqs start at 1, got {:?}", seqs1);
        let session1_count = seqs1.len();

        *session.connection_state.lock().await = ConnectionState::Disconnected;
        *session.is_connected.lock().await = false;
        connect_and_logon(&session, port).await?;
        assert!(session.is_ready().await);
        session.market_data_request("BTC_USDT", 10).await?;
        pump_messages(&session, Duration::from_secs(3), |_| false).await?;

        let session2 = session.drain_capture_probe();
        let seqs2: Vec<u32> = session2.iter().filter_map(|e| match e {
            kdb_integration::ProbeEvent::PendingMdIo { symbol, seq_num: Some(n), .. } if symbol == "BTC_USDT" => Some(*n),
            _ => None,
        }).collect();
        assert!(!seqs2.is_empty(), "session-2 ticks must be enqueued after re-logon");
        assert!(seqs2.iter().all(|n| *n >= 1), "ResetSeqNumFlag=Y restarts at 1, got {:?}", seqs2);
        let max1 = *seqs1.iter().max().unwrap();
        assert!(
            seqs2.iter().any(|n| *n <= max1),
            "per-session restart: expected some session-2 seq <= session-1 max {}, got {:?}",
            max1, seqs2
        );
        assert_eq!(session.drain_capture_probe().len(), 0, "drain is destructive; second drain empty");
        let _ = session1_count;
        Ok(())
    }

    /// Both shadow algos paper-fill against a two-sided mock book; neither ever
    /// emits 35=D (NewOrderSingle). MarketImbalance's aggressive-cross signals
    /// stay inside paper_step.
    #[tokio::test]
    async fn shadow_mode_both_strategies_never_send_orders() -> Result<()> {
        let orders_received = Arc::new(Mutex::new(0u64));
        let port = mock_gateway::start_mock_gateway(
            "127.0.0.1:0",
            mock_gateway::MockGatewayConfig {
                orders_received: Some(Arc::clone(&orders_received)),
                snapshot_bbo: Some(mock_gateway::MockBbo {
                    bid: 100.0,
                    ask: 100.5,
                    bid_size: 7.0,
                    ask_size: 3.0,
                }),
                followup_bbo: Some(mock_gateway::MockBbo {
                    bid: 100.49,
                    ask: 100.51,
                    bid_size: 7.0,
                    ask_size: 3.0,
                }),
                ..Default::default()
            },
        )
        .await?;

        let session = Arc::new(FIXSession::new("TEST.SHADOW", "MOCK.VENUE"));
        connect_and_logon(&session, port).await?;

        let pump_session = Arc::clone(&session);
        let pump = tokio::spawn(async move {
            let _ = pump_messages(&pump_session, Duration::from_secs(5), |_| false).await;
        });

        session.market_data_request("BTC_USDT", 10).await?;

        let deadline = Instant::now() + Duration::from_secs(2);
        loop {
            if Instant::now() > deadline {
                pump.abort();
                return Err(anyhow!("timed out waiting for two-sided mock book"));
            }
            if let Some((bid, ask, _)) = session.get_best_quote("BTC_USDT").await {
                if bid > 0.0 && ask > bid {
                    break;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let mut sc_cfg = algo::AlgoConfig::default();
        sc_cfg.enabled = true;
        sc_cfg.poll_interval_ms = 50;
        sc_cfg.max_book_staleness_ms = 0;
        let sc = Arc::new(algo::TradingAlgo::new(sc_cfg));

        let mut mi_cfg = algo::AlgoConfig::default();
        mi_cfg.strategy = algo::StrategyType::MarketImbalance;
        mi_cfg.enabled = true;
        mi_cfg.poll_interval_ms = 50;
        mi_cfg.max_book_staleness_ms = 0;
        let mi = Arc::new(algo::TradingAlgo::new(mi_cfg));

        let sc_run = {
            let a = Arc::clone(&sc);
            let om = Arc::clone(&session);
            let md = Arc::clone(&session);
            let pos = Arc::clone(&session.positions);
            tokio::spawn(async move { algo_runtime::run(a, om, md, pos).await })
        };
        let mi_run = {
            let a = Arc::clone(&mi);
            let om = Arc::clone(&session);
            let md = Arc::clone(&session);
            let pos = Arc::clone(&session.positions);
            tokio::spawn(async move { algo_runtime::run(a, om, md, pos).await })
        };

        tokio::time::sleep(Duration::from_millis(1500)).await;

        let sc_st = sc.status().await;
        let mi_st = mi.status().await;
        assert!(
            sc_st.paper_fills >= 1,
            "SpreadCapture paper_fills expected >= 1, got {}",
            sc_st.paper_fills
        );
        assert!(
            mi_st.paper_fills >= 1,
            "MarketImbalance paper_fills expected >= 1, got {}",
            mi_st.paper_fills
        );
        assert_eq!(sc_st.orders_placed, 0);
        assert_eq!(mi_st.orders_placed, 0);
        assert_eq!(*orders_received.lock().await, 0);

        sc_run.abort();
        mi_run.abort();
        pump.abort();
        Ok(())
    }

    /// Operator Kill Switch: freeze, flatten every resting order with 35=F,
    /// reject further 35=D until reset, then resume submission.
    #[tokio::test]
    async fn e2e_operator_halt_flattens_and_blocks_until_reset() -> Result<()> {
        let orders_received = Arc::new(Mutex::new(0u64));
        let port = mock_gateway::start_mock_gateway(
            "127.0.0.1:0",
            mock_gateway::MockGatewayConfig {
                orders_received: Some(Arc::clone(&orders_received)),
                ..Default::default()
            },
        )
        .await?;

        let session = FIXSession::new("TEST.CLIENT", "MOCK.VENUE");
        connect_and_logon(&session, port).await?;

        session.new_order_single("E2E_HALT_1", "BTC_USDT", '1', 1.0, 100.0, None, None, None).await?;
        pump_messages(&session, Duration::from_secs(3), |msg| msg.msg_type == '8').await?;
        session.new_order_single("E2E_HALT_2", "BTC_USDT", '2', 1.0, 101.0, None, None, None).await?;
        pump_messages(&session, Duration::from_secs(3), |msg| msg.msg_type == '8').await?;

        let resting = session.open_orders.get_open_orders();
        assert!(
            resting.len() >= 2,
            "expected >=2 resting orders before halt, got {}",
            resting.len()
        );
        let before_halt_orders = *orders_received.lock().await;
        assert_eq!(before_halt_orders, 2);

        let result = session.trigger_operator_halt().await;
        assert!(result.halted);
        assert!(
            result.cancel_attempted >= 2,
            "expected cancel_attempted >= 2, got {}",
            result.cancel_attempted
        );
        assert!(session.operator_halt_is_set());

        let mut canceled = 0u32;
        pump_messages(&session, Duration::from_secs(3), |msg| {
            if msg.msg_type == '8' && msg.get_field(150) == Some("4") {
                canceled += 1;
            }
            canceled >= 2
        })
        .await?;
        assert!(
            canceled >= 2,
            "expected a Canceled ExecutionReport for each resting order, got {}",
            canceled
        );

        let err = session
            .new_order_single("E2E_HALT_3", "BTC_USDT", '1', 1.0, 100.0, None, None, None)
            .await
            .expect_err("halted session must reject 35=D");
        let err_s = err.to_string();
        assert!(
            err_s.contains(OPERATOR_HALT_REJECT),
            "expected OPERATOR_HALT_REJECT, got {}",
            err_s
        );
        assert_eq!(
            *orders_received.lock().await,
            before_halt_orders,
            "rejected 35=D must not reach the mock gateway"
        );

        session.reset_operator_halt();
        assert!(!session.operator_halt_is_set());

        session.new_order_single("E2E_HALT_4", "BTC_USDT", '1', 1.0, 100.0, None, None, None).await?;
        pump_messages(&session, Duration::from_secs(3), |msg| msg.msg_type == '8').await?;
        assert_eq!(*orders_received.lock().await, before_halt_orders + 1);

        cleanup_processed("E2E_HALT_1");
        cleanup_processed("E2E_HALT_2");
        cleanup_processed("E2E_HALT_3");
        cleanup_processed("E2E_HALT_4");
        Ok(())
    }

    /// Inject a contiguous inbound ExecutionReport (ExecType=Fill) so the
    /// production fill path (`apply_fill` + `evaluate_session_drawdown`) runs.
    /// Seq must be last_applied("inbound")+1 or `handle_message` drops the ER.
    async fn inject_fill(
        session: &FIXSession,
        cl_ord_id: &str,
        symbol: &str,
        side: char,
        qty: f64,
        px: f64,
    ) -> Result<()> {
        let seq = session.sequencer.lock().await.last_applied("inbound") + 1;
        let mut msg = FIXMessage::new(
            '8',
            &session.target_comp_id,
            &session.sender_comp_id,
            seq,
        );
        msg.set_field(tags::CL_ORD_ID, cl_ord_id);
        msg.set_field(tags::SYMBOL, symbol);
        msg.set_field(tags::SIDE, &side.to_string());
        msg.set_field(tags::LAST_QTY, &qty.to_string());
        msg.set_field(tags::LAST_PX, &px.to_string());
        msg.set_field(tags::ORDER_ID, &format!("OID_{cl_ord_id}"));
        msg.set_field(150, "2");
        session.handle_message(&msg).await?;
        Ok(())
    }

    /// AC1–AC3: a 35=D that would breach configured position/notional is
    /// rejected locally with no venue send; in-limit and reducing orders send.
    #[tokio::test]
    async fn e2e_exposure_limit_rejects_without_sending() -> Result<()> {
        let orders_received = Arc::new(Mutex::new(0u64));
        let port = mock_gateway::start_mock_gateway(
            "127.0.0.1:0",
            mock_gateway::MockGatewayConfig {
                orders_received: Some(Arc::clone(&orders_received)),
                ..Default::default()
            },
        )
        .await?;

        let session = FIXSession::new("TEST.CLIENT", "MOCK.VENUE");
        connect_and_logon(&session, port).await?;

        // Flatten any CSV-replayed BTC_USDT inventory so this fixture starts flat.
        let existing = session.positions.net_qty_of("BTC_USDT");
        if existing.abs() > POSITION_FLAT_EPS {
            let side = if existing > 0.0 { "2" } else { "1" };
            session
                .positions
                .apply_fill("BTC_USDT", side, existing.abs(), 100.0);
        }

        let mut limits = HashMap::new();
        limits.insert(
            "BTC_USDT".to_string(),
            SymbolLimit {
                max_position: 0.01,
                max_notional: 50_000.0,
            },
        );
        session.apply_risk_config(limits, None);

        let before = *orders_received.lock().await;

        let err = session
            .new_order_single("E2E_EXP_OVER", "BTC_USDT", '1', 1.0, 100.0, None, None, None)
            .await
            .expect_err("oversized 35=D must be rejected locally");
        let err_s = err.to_string();
        assert!(
            err_s.contains(EXPOSURE_POSITION_REJECT) || err_s.contains(EXPOSURE_NOTIONAL_REJECT),
            "expected exposure reject, got {err_s}"
        );
        assert_eq!(
            *orders_received.lock().await,
            before,
            "rejected 35=D must not reach the mock gateway"
        );

        session
            .new_order_single("E2E_EXP_OK", "BTC_USDT", '1', 0.001, 100.0, None, None, None)
            .await?;
        pump_messages(&session, Duration::from_secs(3), |msg| msg.msg_type == '8').await?;
        assert_eq!(*orders_received.lock().await, before + 1);

        // Already above the cap: a reducing sell is not position-blocked (AC3).
        session.positions.apply_fill("BTC_USDT", "1", 1.0, 100.0);
        session
            .new_order_single("E2E_EXP_REDUCE", "BTC_USDT", '2', 0.4, 100.0, None, None, None)
            .await?;
        pump_messages(&session, Duration::from_secs(3), |msg| msg.msg_type == '8').await?;
        assert_eq!(*orders_received.lock().await, before + 2);

        cleanup_processed("E2E_EXP_OVER");
        cleanup_processed("E2E_EXP_OK");
        cleanup_processed("E2E_EXP_REDUCE");
        Ok(())
    }

    /// AC4–AC6: realized-PnL drawdown trips the existing operator halt;
    /// subsequent 35=D is rejected; reset rebases the peak and trading resumes.
    #[tokio::test]
    async fn e2e_session_drawdown_trips_halt_and_resumes_after_reset() -> Result<()> {
        let orders_received = Arc::new(Mutex::new(0u64));
        let port = mock_gateway::start_mock_gateway(
            "127.0.0.1:0",
            mock_gateway::MockGatewayConfig {
                orders_received: Some(Arc::clone(&orders_received)),
                ..Default::default()
            },
        )
        .await?;

        let session = FIXSession::new("TEST.CLIENT", "MOCK.VENUE");
        connect_and_logon(&session, port).await?;
        session.apply_risk_config(HashMap::new(), Some(5.0));

        // Dedicated symbol so CSV-replayed BTC_USDT inventory cannot change the PnL path.
        let peak_before = session.session_realized_pnl();
        inject_fill(&session, "E2E_DD_BUY", "DD_GUARD_SYM", '1', 1.0, 100.0).await?;
        inject_fill(&session, "E2E_DD_SELL", "DD_GUARD_SYM", '2', 1.0, 90.0).await?;
        assert!(
            session.operator_halt_is_set(),
            "buy@100 then sell@90 is −10 realized vs peak {:?}, threshold 5; now {}",
            peak_before,
            session.session_realized_pnl()
        );

        let before = *orders_received.lock().await;
        let err = session
            .new_order_single("E2E_DD_BLOCKED", "BTC_USDT", '1', 0.001, 100.0, None, None, None)
            .await
            .expect_err("drawdown halt must reject 35=D");
        assert!(
            err.to_string().contains(OPERATOR_HALT_REJECT),
            "expected OPERATOR_HALT_REJECT, got {err}"
        );
        assert_eq!(
            *orders_received.lock().await,
            before,
            "rejected 35=D must not reach the mock gateway"
        );

        session.reset_operator_halt();
        assert!(!session.operator_halt_is_set());

        // Rebase must hold: an opening fill that does not worsen realized P&L
        // must not immediately re-trip (peak is now the post-reset P&L).
        inject_fill(&session, "E2E_DD_REBASE", "DD_GUARD_SYM", '1', 1.0, 100.0).await?;
        assert!(
            !session.operator_halt_is_set(),
            "reset must rebase drawdown peak so the next fill does not re-halt"
        );

        session
            .new_order_single("E2E_DD_RESUME", "BTC_USDT", '1', 0.001, 100.0, None, None, None)
            .await?;
        pump_messages(&session, Duration::from_secs(3), |msg| msg.msg_type == '8').await?;
        assert_eq!(*orders_received.lock().await, before + 1);

        cleanup_processed("E2E_DD_BUY");
        cleanup_processed("E2E_DD_SELL");
        cleanup_processed("E2E_DD_BLOCKED");
        cleanup_processed("E2E_DD_REBASE");
        cleanup_processed("E2E_DD_RESUME");
        Ok(())
    }

    /// Drive `FIXSession::new_order_single` against a mock that records SMP tags
    /// on inbound 35=D. Does not start `algo_runtime::run` and does not flip
    /// `paper_mode`.
    async fn drive_nos_smp(
        cl_ord_id: &str,
        scope: Option<String>,
        instruction: Option<String>,
        id: Option<String>,
    ) -> Result<(std::result::Result<(), anyhow::Error>, Vec<mock_gateway::CapturedSmp>, u64)> {
        let rec: Arc<Mutex<Vec<mock_gateway::CapturedSmp>>> =
            Arc::new(Mutex::new(Vec::new()));
        let orders_received = Arc::new(Mutex::new(0u64));
        let port = mock_gateway::start_mock_gateway(
            "127.0.0.1:0",
            mock_gateway::MockGatewayConfig {
                nos_smp_tags: Some(Arc::clone(&rec)),
                orders_received: Some(Arc::clone(&orders_received)),
                ..Default::default()
            },
        )
        .await?;
        let session = FIXSession::new("TEST.CLIENT", "MOCK.VENUE");
        connect_and_logon(&session, port).await?;
        let send_result = session
            .new_order_single(
                cl_ord_id,
                "BTC_USDT",
                '1',
                1.0,
                100.0,
                scope,
                instruction,
                id,
            )
            .await;
        if send_result.is_ok() {
            pump_messages(&session, Duration::from_secs(3), |msg| msg.msg_type == '8').await?;
        }
        let captured = rec.lock().await.clone();
        let n = *orders_received.lock().await;
        cleanup_processed(cl_ord_id);
        Ok((send_result, captured, n))
    }

    #[tokio::test]
    async fn smp_armed_emits_all_three_tags() -> Result<()> {
        let (sent, rec, n) = drive_nos_smp(
            "SMP_ARMED",
            Some("S".to_string()),
            Some("1".to_string()),
            Some("42".to_string()),
        )
        .await?;
        sent.expect("armed SMP must send 35=D");
        assert_eq!(n, 1);
        assert_eq!(rec.len(), 1);
        assert_eq!(
            rec[0],
            mock_gateway::CapturedSmp {
                scope: Some("S".to_string()),
                instruction: Some("1".to_string()),
                id: Some("42".to_string()),
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn smp_inactive_omits_all_three_tags() -> Result<()> {
        let (sent, rec, n) = drive_nos_smp("SMP_INACTIVE", None, None, None).await?;
        sent.expect("unset SMP must still send 35=D");
        assert_eq!(n, 1);
        assert_eq!(rec.len(), 1);
        assert_eq!(
            rec[0],
            mock_gateway::CapturedSmp {
                scope: None,
                instruction: None,
                id: None,
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn smp_partial_emits_only_present_tags() -> Result<()> {
        let (sent, rec, n) = drive_nos_smp(
            "SMP_PARTIAL",
            None,
            None,
            Some("42".to_string()),
        )
        .await?;
        sent.expect("partial SMP must send 35=D");
        assert_eq!(n, 1);
        assert_eq!(rec.len(), 1);
        assert_eq!(
            rec[0],
            mock_gateway::CapturedSmp {
                scope: None,
                instruction: None,
                id: Some("42".to_string()),
            }
        );
        Ok(())
    }

    #[tokio::test]
    async fn smp_rejects_forbidden_delimiter_before_from_request() -> Result<()> {
        let (sent, rec, n) = drive_nos_smp(
            "SMP_DELIM",
            None,
            None,
            Some("42\x0138=999".to_string()),
        )
        .await?;
        let err = sent.expect_err("poisoned SMP id must be rejected");
        let err_s = err.to_string();
        assert!(
            err_s.contains("forbidden delimiter"),
            "expected forbidden delimiter, got {err_s}"
        );
        assert!(rec.is_empty(), "rejected SMP must not reach the mock gateway");
        assert_eq!(n, 0);
        Ok(())
    }

    #[tokio::test]
    async fn smp_rejects_oversize_before_from_request() -> Result<()> {
        let (sent, rec, n) = drive_nos_smp(
            "SMP_LEN",
            None,
            None,
            Some("x".repeat(33)),
        )
        .await?;
        let err = sent.expect_err("oversize SMP id must be rejected");
        let err_s = err.to_string();
        assert!(
            err_s.contains("exceeds max length"),
            "expected exceeds max length, got {err_s}"
        );
        assert!(rec.is_empty(), "rejected SMP must not reach the mock gateway");
        assert_eq!(n, 0);
        Ok(())
    }

    #[tokio::test]
    async fn smp_empty_string_is_emitted() -> Result<()> {
        let (sent, rec, n) = drive_nos_smp(
            "SMP_EMPTY",
            Some("".to_string()),
            Some("".to_string()),
            Some("".to_string()),
        )
        .await?;
        sent.expect("empty SMP strings must send 35=D");
        assert_eq!(n, 1);
        assert_eq!(rec.len(), 1);
        assert_eq!(
            rec[0],
            mock_gateway::CapturedSmp {
                scope: Some("".to_string()),
                instruction: Some("".to_string()),
                id: Some("".to_string()),
            }
        );
        Ok(())
    }

    /// AC1: a ready MD session whose inbound stream has gone silent past the
    /// 30s stall threshold emits StaleBook when the watchdog ticks. Poke
    /// `last_inbound_ms` rather than sleeping 30s. QuietFeed (`last_md_recv_ms`)
    /// is not this alert.
    #[tokio::test]
    async fn stale_book_alert_fires_on_stalled_inbound() -> Result<()> {
        let port = mock_gateway::start_mock_gateway(
            "127.0.0.1:0",
            mock_gateway::MockGatewayConfig::default(),
        )
        .await?;

        let session = FIXSession::new("TEST.CLIENT", "MOCK.VENUE");
        connect_and_logon(&session, port).await?;
        assert!(session.is_ready().await);

        let now = chrono::Utc::now().timestamp_millis() as u64;
        session
            .last_inbound_ms
            .store(now.saturating_sub(45_000), Ordering::Relaxed);
        let health = session.apply_feed_watchdog_tick(now, "test").await;
        assert!(
            matches!(health, FeedHealth::Stalled { silent_ms } if silent_ms >= 45_000),
            "watchdog tick must classify the backdated inbound as Stalled, got {:?}",
            health
        );
        assert!(
            wait_for_alert(&session, AlertKind::StaleBook, Duration::from_secs(60)).await,
            "StaleBook alert must appear in the session buffer within 60s"
        );
        Ok(())
    }

    /// AC2: ≥3 reconnects inside the rolling window emit ReconnectStorm.
    /// The mock re-accepts after `disconnect_after_messages`, so each
    /// application message trips a drop and the next `connect_with_retry`
    /// is a counted reconnect. The initial `connect_and_logon` is the
    /// uncounted first TCP.
    #[tokio::test]
    async fn reconnect_storm_alert_fires_on_repeated_disconnects() -> Result<()> {
        let port = mock_gateway::start_mock_gateway(
            "127.0.0.1:0",
            mock_gateway::MockGatewayConfig {
                disconnect_after_messages: Some(1),
                ..Default::default()
            },
        )
        .await?;

        let session = FIXSession::new("TEST.CLIENT", "MOCK.VENUE");
        connect_and_logon(&session, port).await?;

        for i in 1..=3 {
            session.market_data_request("BTC_USDT", 10).await?;
            let mut saw_disconnect = false;
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline {
                match tokio::time::timeout(Duration::from_millis(500), session.receive_message()).await {
                    Ok(Ok(_)) => continue,
                    Ok(Err(_)) => {
                        saw_disconnect = true;
                        break;
                    }
                    Err(_) => continue,
                }
            }
            assert!(
                saw_disconnect,
                "expected the mock to close the connection before reconnect {i}"
            );
            *session.connection_state.lock().await = ConnectionState::Disconnected;
            *session.is_connected.lock().await = false;
            connect_and_logon(&session, port).await?;
        }

        assert!(
            wait_for_alert(&session, AlertKind::ReconnectStorm, Duration::from_secs(60)).await,
            "ReconnectStorm alert must appear within 60s of the 3rd reconnect"
        );
        Ok(())
    }

    /// AC3: an induced inbound MsgSeqNum gap emits SequenceGap as soon as
    /// `handle_message` sees `GapDetected`. Same mock knobs as the existing
    /// capture-gap probe test.
    #[tokio::test]
    async fn sequence_gap_alert_fires_on_induced_gap() -> Result<()> {
        let port = mock_gateway::start_mock_gateway(
            "127.0.0.1:0",
            mock_gateway::MockGatewayConfig {
                md_ticks_to_stream: 3,
                induce_gap_at_tick: Some(2),
                ..Default::default()
            },
        )
        .await?;

        let session = FIXSession::new("TEST.CLIENT", "MOCK.VENUE");
        connect_and_logon(&session, port).await?;
        session.market_data_request("BTC_USDT", 10).await?;

        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            pump_messages(&session, Duration::from_millis(250), |_| false).await?;
            if wait_for_alert(&session, AlertKind::SequenceGap, Duration::from_millis(50)).await {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "SequenceGap alert should appear within 60s of the induced gap"
            );
        }
        assert!(
            wait_for_alert(&session, AlertKind::SequenceGap, Duration::from_secs(60)).await,
            "SequenceGap alert must remain in the session buffer"
        );
        Ok(())
    }
}

#[cfg(test)]
mod preflight_readonly_tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::Mutex;
    use std::time::{SystemTime, UNIX_EPOCH};

    static CSV_LOCK: Mutex<()> = Mutex::new(());

    const PLANT_SYMBOL: &str = "PF_LIVE_POS";
    const PLANT_QTY: f64 = 7.0;
    const PLANT_PX: f64 = 1.25;

    fn unique_comp(prefix: &str) -> String {
        format!(
            "{}_{}_{}",
            prefix,
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
    }

    fn seqnum_path(sender: &str) -> PathBuf {
        let safe = sender.replace('.', "_").replace(' ', "_");
        PathBuf::from(format!("data/seqnum_{}.txt", safe))
    }

    struct Ac6SeqnumFileGuard {
        path: PathBuf,
    }

    impl Ac6SeqnumFileGuard {
        fn new(sender: &str) -> Self {
            Self {
                path: seqnum_path(sender),
            }
        }
    }

    impl Drop for Ac6SeqnumFileGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    struct Ac6CsvGuard {
        path: PathBuf,
        previous: Option<Vec<u8>>,
        existed: bool,
    }

    impl Ac6CsvGuard {
        fn plant() -> Self {
            let path = PathBuf::from("data/trades.csv");
            let existed = path.exists();
            let previous = std::fs::read(&path).ok();
            let _ = std::fs::create_dir_all("data");
            let body = format!(
                "timestamp_ns,cl_ord_id,order_id,symbol,side,exec_type,last_qty,last_px,avg_px\n\
                 1,cl,oid,{PLANT_SYMBOL},1,F,{PLANT_QTY},{PLANT_PX},{PLANT_PX}\n"
            );
            let _ = std::fs::write(&path, body);
            Self {
                path,
                previous,
                existed,
            }
        }
    }

    impl Drop for Ac6CsvGuard {
        fn drop(&mut self) {
            if self.existed {
                if let Some(ref bytes) = self.previous {
                    let _ = std::fs::write(&self.path, bytes);
                }
            } else {
                let _ = std::fs::remove_file(&self.path);
            }
        }
    }

    async fn assert_live_loads_and_persists(session: &FIXSession, sender: &str) {
        assert_eq!(session.positions.net_qty_of(PLANT_SYMBOL), PLANT_QTY);
        assert!(session.order_books.lock().await.is_empty());
        let n = session.next_seq_num().await;
        let path = seqnum_path(sender);
        let written = std::fs::read_to_string(&path).unwrap_or_default();
        assert_eq!(written, n.to_string());
    }

    #[tokio::test]
    async fn ac6_live_new_loads_positions_and_persists_seqnum() {
        let _csv_lock = CSV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let sender = unique_comp("PF_LIVE");
        let _seq = Ac6SeqnumFileGuard::new(&sender);
        let _csv = Ac6CsvGuard::plant();
        let session = FIXSession::new(&sender, "PF_TGT");
        assert_live_loads_and_persists(&session, &sender).await;
    }

    #[tokio::test]
    async fn ac6_live_new_with_credentials_loads_positions_and_persists_seqnum() {
        let _csv_lock = CSV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let sender = unique_comp("PF_LIVE");
        let _seq = Ac6SeqnumFileGuard::new(&sender);
        let _csv = Ac6CsvGuard::plant();
        let session = FIXSession::new_with_credentials(&sender, "PF_TGT", "", "");
        assert_live_loads_and_persists(&session, &sender).await;
    }

    #[tokio::test]
    async fn ac6_live_new_with_credentials_and_kdb_loads_positions_and_persists_seqnum() {
        let _csv_lock = CSV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let sender = unique_comp("PF_LIVE");
        let _seq = Ac6SeqnumFileGuard::new(&sender);
        let _csv = Ac6CsvGuard::plant();
        let kdb = Arc::new(KdbWriter::new("localhost".to_string(), 5001));
        let session =
            FIXSession::new_with_credentials_and_kdb(&sender, "PF_TGT", "", "", kdb);
        assert_live_loads_and_persists(&session, &sender).await;
    }

    #[tokio::test]
    async fn ac6_preflight_ctor_reads_leftover_seqnum_without_writing() {
        let sender = unique_comp("PF_PRE");
        let _seq = Ac6SeqnumFileGuard::new(&sender);
        let path = seqnum_path(&sender);
        let _ = std::fs::create_dir_all("data");
        let _ = std::fs::write(&path, "42\n");
        let session = FIXSession::new_for_preflight(&sender, "PF_TGT", "", "");
        let n = session.next_seq_num().await;
        assert_eq!(n, 43);
        let leftover = std::fs::read_to_string(&path).unwrap_or_default();
        assert_eq!(leftover, "42\n");
        assert_eq!(session.positions.net_qty_of(PLANT_SYMBOL), 0.0);
        assert!(session.order_books.lock().await.is_empty());
    }
}

#[cfg(test)]
mod calibrate_http_tests {
    use super::*;

    #[test]
    fn sanitize_window_name_rejects_traversal_and_accepts_label() {
        for bad in [
            "..",
            "../etc/passwd",
            "foo/bar",
            "foo\\bar",
            "",
            ".",
            &"a".repeat(65),
        ] {
            assert!(
                sanitize_window_name(bad).is_err(),
                "expected reject for {:?}",
                bad
            );
        }
        assert_eq!(
            sanitize_window_name("uat_2026_03_01").expect("label"),
            "uat_2026_03_01"
        );
    }

    #[test]
    fn unix_ms_to_kdb_ns_checked_and_atom_is_typed() {
        let ms: i64 = 1_772_332_800_000; // 2026-03-01 UTC
        let ns = unix_ms_to_kdb_ns(ms).expect("2026 ms converts");
        let round_trip = (ns + kdb_integration::KDB_TIMESTAMP_OFFSET_NS) / 1_000_000;
        assert_eq!(round_trip, ms);

        assert!(matches!(
            unix_ms_to_kdb_ns(i64::MAX),
            Err(CalibrateHttpError::BoundsNotIntegers)
        ));

        let atom = kdb_ts_atom(ns);
        assert!(
            atom.starts_with("`timestamp$") && atom.ends_with('j'),
            "atom={atom}"
        );
        let inner = &atom["`timestamp$".len()..atom.len() - 1];
        assert!(
            !inner.is_empty() && inner.chars().all(|c| c.is_ascii_digit() || c == '-'),
            "atom inner must be digits only, got {atom}"
        );
        assert!(!atom.contains('"') && !atom.contains(';') && !atom.contains(' '));

        let injection = "1;system\"evil\"";
        assert!(
            injection.parse::<i64>().is_err(),
            "quoted injection must fail i64 parse before kdb_ts_atom"
        );
    }
}

#[cfg(test)]
mod tls_connector_tests {
    use super::*;

    #[test]
    fn helpers_never_disable_verification() {
        let src = include_str!("main.rs");
        let prod = src
            .split("mod config_tests")
            .next()
            .expect("production source precedes config_tests");
        assert!(
            !prod.contains("danger_accept_invalid_certs"),
            "verification must never be bypassable"
        );
        assert!(
            !prod.contains("danger_accept_invalid_hostnames"),
            "hostname verification must stay on"
        );
        assert!(!prod.contains("use_sni(false)"), "SNI must stay enabled");
        let disable = prod
            .find("disable_built_in_roots(true)")
            .expect("custom CA must replace system roots");
        let add = prod
            .find("add_root_certificate")
            .expect("custom CA must be installed");
        assert!(
            disable < add,
            "disable_built_in_roots must run before add_root_certificate"
        );
        assert!(
            !prod.contains("TlsConnector::new()"),
            "TlsConnector::new defaults min protocol to TLS 1.0"
        );
    }

    #[test]
    fn load_trust_anchors_parses_every_pem_block_and_der() {
        let pem_a = tls_test_listener::mint_self_signed_ca_pem("bundle-a").unwrap();
        let pem_b = tls_test_listener::mint_self_signed_ca_pem("bundle-b").unwrap();
        let mut bundle = pem_a.clone();
        bundle.extend_from_slice(&pem_b);
        let pem_path = tls_test_listener::write_temp_bytes("hft-ca-bundle", &bundle).unwrap();
        let certs = load_trust_anchors(pem_path.to_str().unwrap()).unwrap();
        assert_eq!(certs.len(), 2, "every CERTIFICATE block is a trust anchor");
        let _ = std::fs::remove_file(&pem_path);

        let der = tls_test_listener::mint_self_signed_ca_der("der-ca").unwrap();
        let der_path = tls_test_listener::write_temp_bytes("hft-ca-der", &der).unwrap();
        let der_certs = load_trust_anchors(der_path.to_str().unwrap()).unwrap();
        assert_eq!(der_certs.len(), 1);
        let _ = std::fs::remove_file(&der_path);
    }

    #[tokio::test]
    async fn exclusive_ca_rejects_untrusted_and_accepts_matching() {
        let trusted = tls_test_listener::start_trusted().await.unwrap();
        let untrusted = tls_test_listener::start_untrusted().await.unwrap();
        let ca = trusted.ca_pem_path.to_str().unwrap();
        let connector = build_tls_connector(Some(ca)).unwrap();

        let tcp = TcpStream::connect(("127.0.0.1", untrusted.port))
            .await
            .unwrap();
        assert!(
            connector.connect("localhost", tcp).await.is_err(),
            "exclusive CA store must reject a leaf signed by a different CA"
        );

        let tcp = TcpStream::connect(("127.0.0.1", trusted.port))
            .await
            .unwrap();
        connector
            .connect("localhost", tcp)
            .await
            .expect("matching CA must complete the handshake");
    }
}

#[cfg(test)]
mod tls_connectivity_tests {
    use super::*;

    async fn connect_and_logon(session: &FIXSession, port: u16) -> Result<()> {
        session
            .connect_with_retry(&format!("127.0.0.1:{}", port), 5, 20)
            .await?;
        session.logon(30).await?;

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if Instant::now() > deadline {
                return Err(anyhow!("Timed out waiting for Logon ACK"));
            }
            match tokio::time::timeout(Duration::from_millis(500), session.receive_message()).await {
                Ok(Ok(msg)) => {
                    let is_logon_ack = msg.msg_type == 'A';
                    session.handle_message(&msg).await?;
                    if is_logon_ack {
                        *session.connection_state.lock().await = ConnectionState::Ready;
                        return Ok(());
                    }
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => continue,
            }
        }
    }

    async fn wait_for_heartbeat(session: &FIXSession) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if Instant::now() > deadline {
                return Err(anyhow!("Timed out waiting for Heartbeat"));
            }
            match tokio::time::timeout(Duration::from_millis(500), session.receive_message()).await {
                Ok(Ok(msg)) => {
                    session.handle_message(&msg).await?;
                    if msg.msg_type == '0' {
                        return Ok(());
                    }
                }
                Ok(Err(e)) => return Err(e),
                Err(_) => continue,
            }
        }
    }

    #[tokio::test]
    async fn tls_false_logon_against_plain_mock_gateway() -> Result<()> {
        let port = mock_gateway::start_mock_gateway(
            "127.0.0.1:0",
            mock_gateway::MockGatewayConfig::default(),
        )
        .await?;
        let session = FIXSession::new("TEST.CLIENT", "MOCK.VENUE");
        assert!(!session.tls_enabled);
        connect_and_logon(&session, port).await?;
        Ok(())
    }

    #[tokio::test]
    async fn tls_true_logon_and_heartbeat_against_trusted_listener() -> Result<()> {
        let listener = tls_test_listener::start_trusted().await?;
        let ca = listener.ca_pem_path.to_string_lossy().into_owned();
        let session = FIXSession::new("TEST.CLIENT", "MOCK.VENUE").with_transport(
            true,
            Some(ca),
            "localhost".to_string(),
        );
        connect_and_logon(&session, listener.port).await?;
        session.heartbeat().await?;
        wait_for_heartbeat(&session).await?;
        Ok(())
    }

    #[tokio::test]
    async fn tls_true_without_matching_ca_fails_closed() -> Result<()> {
        let listener = tls_test_listener::start_untrusted().await?;
        let session = FIXSession::new("TEST.CLIENT", "MOCK.VENUE").with_transport(
            true,
            None,
            "localhost".to_string(),
        );
        let err = session
            .connect_with_retry(&format!("127.0.0.1:{}", listener.port), 1, 20)
            .await
            .expect_err("untrusted certificate must fail closed");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("TLS handshake failed") || msg.contains("Max reconnection"),
            "unexpected connect error: {msg}"
        );
        assert!(
            session.stream.lock().await.is_none(),
            "failed handshake must not store a stream"
        );
        Ok(())
    }

    #[tokio::test]
    async fn tls_true_with_matching_ca_path_succeeds() -> Result<()> {
        let listener = tls_test_listener::start_trusted().await?;
        let ca = listener.ca_pem_path.to_string_lossy().into_owned();
        let session = FIXSession::new("TEST.CLIENT", "MOCK.VENUE").with_transport(
            true,
            Some(ca),
            "localhost".to_string(),
        );
        connect_and_logon(&session, listener.port).await?;
        Ok(())
    }
}

#[cfg(test)]
mod credential_redaction {
    use super::*;

    /// The production API key was written to fix-server-error.log in clear on
    /// 2026-09-27 (tag 553), along with the full HMAC signature (554). Logs are
    /// 0644 and rotate rather than shred, so anything printed outlives the
    /// process — which defeats having moved secrets into a 0600 env file.
    #[test]
    fn logon_text_never_carries_the_key_or_signature() {
        let key = "GgCUxN3bnbudpmbvqRBK43";
        let sig = "571932fca5e7517184f54fb71bffb6652a59d63464717db8385e95aae169cc15";
        let raw = format!(
            "8=FIX.4.4|9=215|35=A|49=PRD3.WOZINGA_OTHERS.MD.00|56=PRD3.CDC.MD|34=1|\
             52=20260927-17:29:40.008|95=13|96=1790530180008|98=0|108=5|553={key}|554={sig}|141=Y|10=194|"
        );
        let out = redact_fix_text(&raw);
        assert!(!out.contains(key), "API key leaked: {out}");
        assert!(!out.contains(sig), "signature leaked: {out}");
        // Non-secret routing fields must survive, or the log stops being useful.
        assert!(out.contains("49=PRD3.WOZINGA_OTHERS.MD.00"), "sender lost: {out}");
        assert!(out.contains("96=1790530180008"), "nonce lost: {out}");
        assert!(out.contains("553=<redacted 22 chars>"), "553 not redacted: {out}");
    }

    /// A 16-character prefix of a secret is still a secret.
    #[test]
    fn redact_tag_hides_key_and_signature_but_not_others() {
        assert_eq!(redact_tag(553, "GgCUxN3bnbudpmbvqRBK43"), "<redacted 22 chars>");
        assert_eq!(redact_tag(554, "571932fca5e75171"), "<redacted 16 chars>");
        assert_eq!(redact_tag(49, "PRD3.CDC.MD"), "PRD3.CDC.MD");
        assert_eq!(redact_tag(96, "1790530180008"), "1790530180008");
    }
}
