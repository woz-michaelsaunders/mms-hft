// kdb+ Integration Module
// Handles writing order book data to kdb+ via direct IPC connection using kxkdb library
// Optimized for high-speed bulk inserts

use anyhow::{anyhow, Result};
use log::{debug, info, error, warn};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use chrono::Utc;
use kxkdb::ipc::*;
use kxkdb::qtype;
use std::collections::VecDeque;

/// kdb+ epoch offset in nanoseconds (nanoseconds from Unix epoch 1970-01-01 to kdb+ epoch 2000-01-01)
pub const KDB_TIMESTAMP_OFFSET_NS: i64 = 946_684_800_000_000_000_i64;

/// Maximum entries in each write buffer.  If this is exceeded (e.g. kdb+ is slow or
/// temporarily disconnected) the oldest half is dropped with a warning so we never
/// accumulate unbounded RAM.
const MAX_KDB_BUFFER: usize = 10_000;

const FLUSH_INTERVAL_MS: u64 = 100;
const RECONNECT_POLL_MS: u64 = 250;
const RECONNECT_BACKOFF_CAP_MS: u64 = 30_000;
const DEFAULT_RECONNECT_BACKOFF_MS: u64 = 1_000;
const LIVENESS_PROBE_INTERVAL_MS: u64 = 1000;
const LIVENESS_PROBE_TIMEOUT_MS: u64 = 2000;
const LIVENESS_PROBE_Q: &str = "1";  // file-local literal; never format!
const LEVELS_COUNT_Q: &str = "count orderbook_levels"; // file-local; never format!

/// Typed-scalar ensureCol — byte-identical with `kdb/start_kdb_server.q`.
/// Must use `` `long$0N `` (never `` `long$() ``) so `meta` stays `j` on a non-empty HDB.
const Q_ENSURE_COL_SEQ_NUM: &str = concat!(
    "ensureCol:{[t;c] if[not c in cols t; t set ![t;();0b;(enlist c)!enlist `long$0N]];};",
    "ensureCol[`orderbook_levels;`seq_num];",
    "ensureCol[`trades;`seq_num];",
    "ensureCol[`orderbook_snapshots;`seq_num];",
);

const Q_INIT_LEVELS: &str = "if[not `orderbook_levels in key `; orderbook_levels: ([] time:`timestamp$(); symbol:`symbol$(); side:`char$(); price:`float$(); size:`float$(); action:`char$(); seq_num:`long$()); ];";
const Q_INIT_TRADES: &str = "if[not `trades in key `; trades: ([] time:`timestamp$(); symbol:`symbol$(); price:`float$(); size:`float$(); seq_num:`long$()); ];";
const Q_INIT_SNAPSHOTS: &str = "if[not `orderbook_snapshots in key `; orderbook_snapshots: ([] time:`timestamp$(); symbol:`symbol$(); best_bid:`float$(); best_ask:`float$(); best_bid_size:`float$(); best_ask_size:`float$(); spread:`float$(); seq_num:`long$()); ];";
const Q_INIT_SECURITIES: &str = "if[not `securities in key `; securities: ([] time:`timestamp$(); symbol:`symbol$(); security_id:`symbol$(); exchange:`symbol$(); attributes_json:()); ];";
const Q_INIT_OWN_TRADES: &str = "if[not `own_trades in key `; own_trades: ([] time:`timestamp$(); symbol:`symbol$(); side:`char$(); last_px:`float$(); last_qty:`float$(); avg_px:`float$(); cl_ord_id:`symbol$(); order_id:`symbol$(); exec_type:`char$()); ];";
const Q_INIT_CAPTURE_GAPS: &str = "if[not `capture_gaps in key `; capture_gaps: ([] time:`timestamp$(); symbol:`symbol$(); expected_seq:`long$(); received_seq:`long$(); gap_size:`long$()); ];";

/// Escape a string for interpolation into a kdb `` `$"..." `` atom.
/// Module-level (not `impl KdbWriter`); same body as the former nested helper in main.rs.
pub(crate) fn escape_kdb_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Join `chars` into one kdb char-vector literal, including wrapping `"`.
/// Twin of `escape_kdb_string` per character. Empty iterator → `""`.
pub(crate) fn fmt_kdb_char_col<I: IntoIterator<Item = char>>(chars: I) -> String {
    let mut inner = String::new();
    for ch in chars {
        inner.push_str(&escape_kdb_string(&ch.to_string()));
    }
    format!("\"{}\"", inner)
}

#[allow(private_interfaces)]
pub(crate) fn levels_insert_q(batch: &[OrderBookLevel]) -> String {
    let times: Vec<String> = batch.iter().map(|l| format!("`timestamp${}", l.timestamp)).collect();
    let symbols: Vec<String> = batch.iter().map(|l| format!("`{}", l.symbol)).collect();
    let sides = fmt_kdb_char_col(batch.iter().map(|l| l.side));
    let prices: Vec<String> = batch.iter().map(|l| format!("{}f", l.price)).collect();
    let sizes: Vec<String> = batch.iter().map(|l| format!("{}f", l.size)).collect();
    let actions = fmt_kdb_char_col(batch.iter().map(|l| l.action));
    let seqs: Vec<String> = batch.iter().map(|l| fmt_seq(l.seq_num)).collect();
    format!(
        "`orderbook_levels insert (({});({});{};({});({});{};({}))",
        times.join(";"),
        symbols.join(";"),
        sides,
        prices.join(";"),
        sizes.join(";"),
        actions,
        seqs.join(";")
    )
}

#[allow(private_interfaces)]
pub(crate) fn own_trades_insert_q(batch: &[OwnTrade]) -> String {
    let times: Vec<String> = batch.iter().map(|t| format!("`timestamp${}", t.timestamp)).collect();
    let symbols: Vec<String> = batch.iter().map(|t| format!("`{}", t.symbol)).collect();
    let sides = fmt_kdb_char_col(batch.iter().map(|t| t.side));
    let last_pxs: Vec<String> = batch.iter().map(|t| format!("{}f", t.last_px)).collect();
    let last_qtys: Vec<String> = batch.iter().map(|t| format!("{}f", t.last_qty)).collect();
    let avg_pxs: Vec<String> = batch.iter().map(|t| format!("{}f", t.avg_px)).collect();
    let cl_ord_ids: Vec<String> = batch.iter().map(|t| format!("`{}", t.cl_ord_id)).collect();
    let order_ids: Vec<String> = batch.iter().map(|t| format!("`{}", t.order_id)).collect();
    let exec_types = fmt_kdb_char_col(batch.iter().map(|t| t.exec_type));
    format!(
        "`own_trades insert (({});({});{};({});({});({});({});({});{})",
        times.join(";"),
        symbols.join(";"),
        sides,
        last_pxs.join(";"),
        last_qtys.join(";"),
        avg_pxs.join(";"),
        cl_ord_ids.join(";"),
        order_ids.join(";"),
        exec_types
    )
}

fn fmt_seq(seq: Option<u32>) -> String {
    match seq {
        Some(n) => format!("{}j", n),
        None => "0Nj".to_string(),
    }
}

fn kdb_now_ns() -> i64 {
    Utc::now().timestamp_nanos_opt().unwrap_or(0) - KDB_TIMESTAMP_OFFSET_NS
}

// Batch buffer structures
#[derive(Clone)]
struct OwnTrade {
    timestamp: i64,
    symbol: String,
    side: char,
    last_px: f64,
    last_qty: f64,
    avg_px: f64,
    cl_ord_id: String,
    order_id: String,
    exec_type: char,
}

#[derive(Clone)]
struct OrderBookLevel {
    timestamp: i64,
    symbol: String,
    side: char,
    price: f64,
    size: f64,
    action: char,
    seq_num: Option<u32>,
}

#[derive(Clone)]
struct Trade {
    timestamp: i64,
    symbol: String,
    price: f64,
    size: f64,
    seq_num: Option<u32>,
}

#[derive(Clone)]
struct Snapshot {
    timestamp: i64,
    symbol: String,
    best_bid: f64,
    best_ask: f64,
    best_bid_size: f64,
    best_ask_size: f64,
    spread: f64,
    seq_num: Option<u32>,
}

#[derive(Clone)]
struct CaptureGap {
    timestamp: i64,
    symbol: String,
    expected_seq: u32,
    received_seq: u32,
    gap_size: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeEvent {
    PendingMdIo { kind: &'static str, symbol: String, seq_num: Option<u32> },
    CaptureGap { symbol: String, expected: u32, received: u32, gap_size: u32 },
    OverflowGap { symbol: String, drop_count: u32 },
    KdbDisconnected,
    KdbReconnected,
}

#[derive(Debug, Clone)]
enum ProbeOutcome {
    Alive,
    QError { message: String },
    Transport,
    Skipped,
}

/// kdb+ IPC connection manager using kxkdb library with batched writes for maximum performance
pub struct KdbWriter {
    stream: Arc<Mutex<Option<QStream>>>,
    host: String,
    port: u16,
    connected: Arc<tokio::sync::Mutex<bool>>,

    // Batch buffers
    level_buffer: Arc<tokio::sync::Mutex<VecDeque<OrderBookLevel>>>,
    trade_buffer: Arc<tokio::sync::Mutex<VecDeque<Trade>>>,
    snapshot_buffer: Arc<tokio::sync::Mutex<VecDeque<Snapshot>>>,
    own_trade_buffer: Arc<tokio::sync::Mutex<VecDeque<OwnTrade>>>,
    capture_gap_buffer: Arc<tokio::sync::Mutex<VecDeque<CaptureGap>>>,

    // Batch size thresholds
    batch_size: usize,

    probe: Arc<std::sync::Mutex<Vec<ProbeEvent>>>,

    /// One-shot: first non-empty levels insert Ok schedules a spawned count.
    levels_count_scheduled: Arc<AtomicBool>,
    /// True while the spawned count holds the writer QStream (skip-not-tear-down).
    levels_count_in_flight: Arc<AtomicBool>,
}

impl KdbWriter {
    pub fn new(host: String, port: u16) -> Self {
        KdbWriter {
            stream: Arc::new(Mutex::new(None)),
            host,
            port,
            connected: Arc::new(tokio::sync::Mutex::new(false)),
            level_buffer: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
            trade_buffer: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
            snapshot_buffer: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
            own_trade_buffer: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
            capture_gap_buffer: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
            batch_size: 100, // Flush when buffer reaches 100 items
            probe: Arc::new(std::sync::Mutex::new(Vec::new())),
            levels_count_scheduled: Arc::new(AtomicBool::new(false)),
            levels_count_in_flight: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Connect to kdb+ server via IPC. Fatal to the process if this returns Err
    /// (caller: main.rs). On connect failure neither flush nor reconnect tasks start.
    pub async fn initialize(&self) -> Result<()> {
        self.connect_ipc().await?;
        self.start_flush_task();
        self.start_reconnect_task();
        Ok(())
    }

    async fn connect_ipc(&self) -> Result<()> {
        Self::connect_ipc_with(&self.host, self.port, &self.stream, &self.connected, &self.probe).await
    }

    async fn connect_ipc_with(
        host: &str,
        port: u16,
        stream: &Arc<Mutex<Option<QStream>>>,
        connected: &Arc<tokio::sync::Mutex<bool>>,
        probe: &Arc<std::sync::Mutex<Vec<ProbeEvent>>>,
    ) -> Result<()> {
        #[cfg(not(test))]
        let _ = probe;
        let addr = format!("{}:{}", host, port);
        info!("🔌 Connecting to kdb+ at {}...", addr);

        match QStream::connect(ConnectionMethod::TCP, host, port, "").await {
            Ok(qstream) => {
                *stream.lock().await = Some(qstream);
                match Self::initialize_tables_with(stream, connected).await {
                    Ok(()) => {
                        *connected.lock().await = true;
                        info!("✅ Connected to kdb+ at {}", addr);
                        #[cfg(test)]
                        Self::record_probe_arc(probe, ProbeEvent::KdbReconnected);
                        Ok(())
                    }
                    Err(e) => {
                        *connected.lock().await = false;
                        let _ = stream.lock().await.take();
                        Err(e)
                    }
                }
            }
            Err(e) => {
                error!("Failed to connect to kdb+ at {}: {}", addr, e);
                Err(anyhow!("Connection failed: {}", e))
            }
        }
    }

    async fn probe_once(
        stream: &Arc<Mutex<Option<QStream>>>,
        in_flight: &AtomicBool,
    ) -> ProbeOutcome {
        if in_flight.load(Ordering::Acquire) {
            return ProbeOutcome::Skipped;
        }
        let mut stream_guard = stream.lock().await;
        let qstream = match stream_guard.as_mut() {
            Some(qstream) => qstream,
            None => return ProbeOutcome::Transport,
        };
        // ADR-072 scoped exception to ADR-060; drop stream on expiry — never reuse this QStream
        let result = tokio::time::timeout(
            std::time::Duration::from_millis(LIVENESS_PROBE_TIMEOUT_MS),
            qstream.send_sync_message(&LIVENESS_PROBE_Q),
        )
        .await;
        drop(stream_guard);
        match result {
            Ok(Ok(k)) if k.get_type() == qtype::ERROR => ProbeOutcome::QError {
                message: k.get_error_string().unwrap_or("unknown error").to_string(),
            },
            Ok(Ok(_)) => ProbeOutcome::Alive,
            Ok(Err(_)) => ProbeOutcome::Transport,
            Err(_elapsed) => ProbeOutcome::Transport,
        }
    }

    async fn mark_disconnected(
        connected: &Arc<tokio::sync::Mutex<bool>>,
        stream: &Arc<Mutex<Option<QStream>>>,
        probe: &Arc<std::sync::Mutex<Vec<ProbeEvent>>>,
    ) {
        let mut connected_guard = connected.lock().await;
        if !*connected_guard {
            return;
        }
        let _ = stream.lock().await.take();
        *connected_guard = false;
        drop(connected_guard);
        warn!("⚠️ kdb+ connection lost (peer closed or probe timeout) — entering reconnect backoff");
        #[cfg(test)]
        Self::record_probe_arc(probe, ProbeEvent::KdbDisconnected);
        #[cfg(not(test))]
        let _ = probe;
    }

    /// Start background task to periodically flush buffers
    fn start_flush_task(&self) {
        let level_buf = Arc::clone(&self.level_buffer);
        let trade_buf = Arc::clone(&self.trade_buffer);
        let snapshot_buf = Arc::clone(&self.snapshot_buffer);
        let own_trade_buf = Arc::clone(&self.own_trade_buffer);
        let gap_buf = Arc::clone(&self.capture_gap_buffer);
        let stream = Arc::clone(&self.stream);
        let connected = Arc::clone(&self.connected);
        let probe = Arc::clone(&self.probe);
        let scheduled = Arc::clone(&self.levels_count_scheduled);
        let in_flight = Arc::clone(&self.levels_count_in_flight);
        let batch_size = self.batch_size;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(FLUSH_INTERVAL_MS));
            let mut last_probe = std::time::Instant::now()
                .checked_sub(std::time::Duration::from_secs(1))
                .unwrap_or_else(std::time::Instant::now);
            loop {
                interval.tick().await;

                let up = *connected.lock().await;
                if up && !in_flight.load(Ordering::Acquire) {
                    let mut levels = level_buf.lock().await;
                    if !levels.is_empty() {
                        let batch: Vec<_> = levels.drain(..).collect();
                        drop(levels);
                        if let Err(e) = Self::flush_levels_batch(
                            &stream, &batch, &connected, &probe, &scheduled, &in_flight,
                        ).await {
                            Self::handle_flush_err(&connected, &stream, &level_buf, batch, "level", e, &probe).await;
                        }
                    }

                    let mut trades = trade_buf.lock().await;
                    if !trades.is_empty() {
                        let batch: Vec<_> = trades.drain(..).collect();
                        drop(trades);
                        if let Err(e) = Self::flush_trades_batch(&stream, &batch, &in_flight).await {
                            Self::handle_flush_err(&connected, &stream, &trade_buf, batch, "trade", e, &probe).await;
                        }
                    }

                    let mut snapshots = snapshot_buf.lock().await;
                    if !snapshots.is_empty() {
                        let batch: Vec<_> = snapshots.drain(..).collect();
                        drop(snapshots);
                        if let Err(e) = Self::flush_snapshots_batch(&stream, &batch, &in_flight).await {
                            Self::handle_flush_err(&connected, &stream, &snapshot_buf, batch, "snapshot", e, &probe).await;
                        }
                    }

                    let mut own_trades = own_trade_buf.lock().await;
                    if !own_trades.is_empty() {
                        let batch: Vec<_> = own_trades.drain(..).collect();
                        drop(own_trades);
                        if let Err(e) = Self::flush_own_trades_batch(&stream, &batch, &in_flight).await {
                            Self::handle_flush_err(&connected, &stream, &own_trade_buf, batch, "own_trade", e, &probe).await;
                        }
                    }

                    let mut gaps = gap_buf.lock().await;
                    if !gaps.is_empty() {
                        let batch: Vec<_> = gaps.drain(..).collect();
                        drop(gaps);
                        if let Err(e) = Self::flush_capture_gaps_batch(&stream, &batch, &in_flight).await {
                            Self::handle_flush_err(&connected, &stream, &gap_buf, batch, "capture_gap", e, &probe).await;
                        }
                    }
                }

                let up = *connected.lock().await;
                if up && last_probe.elapsed() >= std::time::Duration::from_millis(LIVENESS_PROBE_INTERVAL_MS) {
                    match Self::probe_once(&stream, &in_flight).await {
                        ProbeOutcome::Alive => {
                            last_probe = std::time::Instant::now();
                        }
                        ProbeOutcome::QError { message } => {
                            warn!("kdb+ liveness probe q-error (latch stays up): {}", message);
                            last_probe = std::time::Instant::now();
                        }
                        ProbeOutcome::Transport => {
                            Self::mark_disconnected(&connected, &stream, &probe).await;
                        }
                        ProbeOutcome::Skipped => {
                            // count holds the stream — do not tear down, do not refresh last_probe
                        }
                    }
                }
                let _ = batch_size; // suppress unused warning
            }
        });
    }

    fn start_reconnect_task(&self) {
        let host = self.host.clone();
        let port = self.port;
        let stream = Arc::clone(&self.stream);
        let connected = Arc::clone(&self.connected);
        let probe = Arc::clone(&self.probe);
        let initial = std::env::var("KDB_RECONNECT_BACKOFF_MS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(DEFAULT_RECONNECT_BACKOFF_MS);

        tokio::spawn(async move {
            let mut backoff = initial;
            loop {
                let up = *connected.lock().await;
                if up {
                    backoff = initial;
                    tokio::time::sleep(tokio::time::Duration::from_millis(RECONNECT_POLL_MS)).await;
                    continue;
                }
                warn!("kdb reconnect attempt (backoff {}ms) to {}:{}", backoff, host, port);
                match KdbWriter::connect_ipc_with(&host, port, &stream, &connected, &probe).await {
                    Ok(()) => {
                        info!("kdb reconnect success at {}:{}", host, port);
                        backoff = initial;
                    }
                    Err(e) => {
                        warn!("kdb reconnect failed: {}", e);
                        tokio::time::sleep(tokio::time::Duration::from_millis(backoff)).await;
                        backoff = backoff.saturating_mul(2).min(RECONNECT_BACKOFF_CAP_MS);
                    }
                }
            }
        });
    }

    async fn initialize_tables_with(
        stream: &Arc<Mutex<Option<QStream>>>,
        _connected: &Arc<tokio::sync::Mutex<bool>>,
    ) -> Result<()> {
        info!("📊 Initializing kdb+ tables...");

        Self::execute_q_on_stream(stream, Q_INIT_LEVELS).await?;
        Self::execute_q_on_stream(stream, Q_INIT_TRADES).await?;
        Self::execute_q_on_stream(stream, Q_INIT_SNAPSHOTS).await?;
        Self::execute_q_on_stream(stream, Q_INIT_SECURITIES).await?;
        Self::execute_q_on_stream(stream, Q_INIT_OWN_TRADES).await?;
        Self::execute_q_on_stream(stream, Q_INIT_CAPTURE_GAPS).await?;
        Self::execute_q_on_stream(stream, Q_ENSURE_COL_SEQ_NUM).await?;

        match Self::execute_q_on_stream(stream, "count orderbook_levels").await {
            Ok(_) => info!("✅ Verified orderbook_levels table exists"),
            Err(e) => warn!("⚠️  Could not verify orderbook_levels: {}", e),
        }

        info!("✅ kdb+ tables initialized");
        Ok(())
    }

    async fn execute_q_on_stream(stream: &Arc<Mutex<Option<QStream>>>, q_code: &str) -> Result<()> {
        let mut stream_guard = stream.lock().await;
        let qstream = stream_guard.as_mut()
            .ok_or_else(|| anyhow!("Stream not available"))?;

        match qstream.send_sync_message(&q_code).await {
            Ok(_) => Ok(()),
            Err(e) => {
                warn!("Failed to execute q code: {} - Error: {}", q_code, e);
                Err(anyhow!("kdb+ execution error: {}", e))
            }
        }
    }

    /// Fire-and-forget bulk insert — sends an async message (no response wait).
    /// Used by all flush_*_batch functions for high-throughput data writes.
    async fn execute_single_insert(
        stream: &Arc<Mutex<Option<QStream>>>,
        q_code: &str,
        in_flight: &AtomicBool,
    ) -> Result<()> {
        let mut stream_guard = stream.lock().await;
        match stream_guard.as_mut() {
            Some(qstream) => qstream
                .send_async_message(&q_code)
                .await
                .map_err(|e| anyhow!("Async insert failed: {}", e)),
            None if in_flight.load(Ordering::Acquire) => {
                Err(anyhow!("kdb levels-count in flight"))
            }
            None => Err(anyhow!("Stream not available")),
        }
    }

    /// Execute a q expression via IPC using kxkdb (sync for initialization)
    #[allow(dead_code)]
    async fn execute_q(&self, q_code: &str) -> Result<()> {
        if !*self.connected.lock().await {
            return Err(anyhow!("Not connected to kdb+"));
        }
        Self::execute_q_on_stream(&self.stream, q_code).await
    }

    async fn requeue_on_flush_err<T>(
        connected: &Arc<tokio::sync::Mutex<bool>>,
        stream: &Arc<Mutex<Option<QStream>>>,
        buf: &Arc<tokio::sync::Mutex<VecDeque<T>>>,
        batch: Vec<T>,
        what: &str,
        err: anyhow::Error,
        probe: &Arc<std::sync::Mutex<Vec<ProbeEvent>>>,
    ) {
        warn!("Failed to flush {} batch: {} — dropping kdb latch and re-queuing", what, err);
        Self::mark_disconnected(connected, stream, probe).await;
        let mut buf = buf.lock().await;
        for item in batch.into_iter().rev() {
            buf.push_front(item);
        }
    }

    async fn requeue_without_disconnect<T>(
        buf: &Arc<tokio::sync::Mutex<VecDeque<T>>>,
        batch: Vec<T>,
        what: &str,
    ) {
        warn!(
            "Failed to flush {} batch: kdb levels-count in flight — re-queuing without dropping latch",
            what
        );
        let mut buf = buf.lock().await;
        for item in batch.into_iter().rev() {
            buf.push_front(item);
        }
    }

    async fn handle_flush_err<T>(
        connected: &Arc<tokio::sync::Mutex<bool>>,
        stream: &Arc<Mutex<Option<QStream>>>,
        buf: &Arc<tokio::sync::Mutex<VecDeque<T>>>,
        batch: Vec<T>,
        what: &str,
        err: anyhow::Error,
        probe: &Arc<std::sync::Mutex<Vec<ProbeEvent>>>,
    ) {
        if err.to_string() == "kdb levels-count in flight" {
            Self::requeue_without_disconnect(buf, batch, what).await;
        } else {
            Self::requeue_on_flush_err(connected, stream, buf, batch, what, err, probe).await;
        }
    }

    fn spawn_levels_count_if_first(
        stream: Arc<Mutex<Option<QStream>>>,
        connected: Arc<tokio::sync::Mutex<bool>>,
        probe: Arc<std::sync::Mutex<Vec<ProbeEvent>>>,
        scheduled: Arc<AtomicBool>,
        in_flight: Arc<AtomicBool>,
        q_code: String,
    ) {
        if scheduled
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        tokio::spawn(async move {
            in_flight.store(true, Ordering::Release);
            let taken = {
                let mut guard = stream.lock().await;
                guard.take()
            };
            let Some(mut qstream) = taken else {
                in_flight.store(false, Ordering::Release);
                return;
            };

            let result = tokio::time::timeout(
                std::time::Duration::from_millis(LIVENESS_PROBE_TIMEOUT_MS),
                qstream.send_sync_message(&LEVELS_COUNT_Q),
            )
            .await;

            match result {
                Ok(Ok(k)) => {
                    Self::restore_count_stream(&stream, &connected, qstream).await;
                    in_flight.store(false, Ordering::Release);
                    if k.get_type() == qtype::ERROR {
                        let msg = k.get_error_string().unwrap_or("unknown error");
                        let msg_t: String = msg.chars().take(200).collect();
                        let q_t: String = q_code.chars().take(512).collect();
                        error!(
                            "kdb+ orderbook_levels count q-error: {} (truncated insert: {})",
                            msg_t, q_t
                        );
                    } else if k.get_type() == qtype::LONG_ATOM {
                        match k.get_long() {
                            Ok(n) if n > 0 => {
                                info!("orderbook_levels count={} after first flush", n);
                            }
                            Ok(n) => {
                                let q_t: String = q_code.chars().take(512).collect();
                                error!(
                                    "orderbook_levels count={} after first flush — kdb likely rejected the insert (type mismatch); truncated q: {}",
                                    n, q_t
                                );
                            }
                            Err(e) => {
                                error!("orderbook_levels count: LONG_ATOM get_long failed: {}", e);
                            }
                        }
                    } else {
                        error!(
                            "orderbook_levels count: unexpected qtype {}",
                            k.get_type()
                        );
                    }
                }
                Ok(Err(e)) => {
                    drop(qstream);
                    error!("orderbook_levels count did not complete (transport): {}", e);
                    Self::mark_disconnected(&connected, &stream, &probe).await;
                    in_flight.store(false, Ordering::Release);
                }
                Err(_elapsed) => {
                    drop(qstream);
                    error!("orderbook_levels count did not complete (timeout)");
                    Self::mark_disconnected(&connected, &stream, &probe).await;
                    in_flight.store(false, Ordering::Release);
                }
            }
        });
    }

    async fn restore_count_stream(
        stream: &Arc<Mutex<Option<QStream>>>,
        connected: &Arc<tokio::sync::Mutex<bool>>,
        qstream: QStream,
    ) {
        let connected_guard = connected.lock().await;
        if *connected_guard {
            *stream.lock().await = Some(qstream);
        }
        // else drop qstream — disconnect raced; never mark_disconnected from here
    }

    async fn emit_overflow_gap(&self, dropped_symbols: &[String]) {
        let drop_count = dropped_symbols.len() as u32;
        let symbol = if dropped_symbols.is_empty() {
            "*".to_string()
        } else if dropped_symbols.iter().all(|s| s == &dropped_symbols[0]) {
            dropped_symbols[0].clone()
        } else {
            "*".to_string()
        };
        self.enqueue_capture_gap(CaptureGap {
            timestamp: kdb_now_ns(),
            symbol: symbol.clone(),
            expected_seq: 0,
            received_seq: drop_count,
            gap_size: drop_count,
        }).await;
        #[cfg(test)]
        self.record_probe(ProbeEvent::OverflowGap { symbol, drop_count });
    }

    async fn enqueue_capture_gap(&self, gap: CaptureGap) {
        let mut buffer = self.capture_gap_buffer.lock().await;
        if buffer.len() >= MAX_KDB_BUFFER {
            let drop_count = buffer.len() / 2;
            buffer.drain(..drop_count);
            warn!(
                "kdb capture_gap buffer overflow — dropped {} oldest gap rows (not recorded as a nested gap)",
                drop_count
            );
        }
        buffer.push_back(gap);
        let buffer_len = buffer.len();
        drop(buffer);

        if buffer_len >= self.batch_size
            && *self.connected.lock().await
            && !self.levels_count_in_flight.load(Ordering::Acquire)
        {
            let mut buffer = self.capture_gap_buffer.lock().await;
            let batch: Vec<_> = buffer.drain(..).collect();
            drop(buffer);
            if let Err(e) = Self::flush_capture_gaps_batch(
                &self.stream,
                &batch,
                &self.levels_count_in_flight,
            )
            .await
            {
                Self::handle_flush_err(
                    &self.connected,
                    &self.stream,
                    &self.capture_gap_buffer,
                    batch,
                    "capture_gap",
                    e,
                    &self.probe,
                ).await;
            }
        }
    }

    /// Flush a batch of order book levels using a single bulk insert command
    async fn flush_levels_batch(
        stream: &Arc<Mutex<Option<QStream>>>,
        batch: &[OrderBookLevel],
        connected: &Arc<tokio::sync::Mutex<bool>>,
        probe: &Arc<std::sync::Mutex<Vec<ProbeEvent>>>,
        scheduled: &Arc<AtomicBool>,
        in_flight: &Arc<AtomicBool>,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        debug!("🔄 Flushing {} order book levels to kdb+", batch.len());

        let q_code = levels_insert_q(batch);

        match Self::execute_single_insert(stream, &q_code, in_flight).await {
            Ok(()) => {
                debug!("✅ Flushed {} order book levels", batch.len());
                if *connected.lock().await {
                    Self::spawn_levels_count_if_first(
                        Arc::clone(stream),
                        Arc::clone(connected),
                        Arc::clone(probe),
                        Arc::clone(scheduled),
                        Arc::clone(in_flight),
                        q_code,
                    );
                }
                Ok(())
            }
            Err(e) => {
                warn!("❌ Failed to bulk-insert order book levels: {}", e);
                Err(e)
            }
        }
    }

    /// Flush a batch of trades using a single bulk insert command
    async fn flush_trades_batch(
        stream: &Arc<Mutex<Option<QStream>>>,
        batch: &[Trade],
        in_flight: &AtomicBool,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        debug!("🔄 Flushing {} trades to kdb+", batch.len());

        let times: Vec<String> = batch.iter().map(|t| format!("`timestamp${}", t.timestamp)).collect();
        let symbols: Vec<String> = batch.iter().map(|t| format!("`{}", t.symbol)).collect();
        let prices: Vec<String> = batch.iter().map(|t| format!("{}f", t.price)).collect();
        let sizes: Vec<String> = batch.iter().map(|t| format!("{}f", t.size)).collect();
        let seqs: Vec<String> = batch.iter().map(|t| fmt_seq(t.seq_num)).collect();

        let q_code = format!(
            "`trades insert (({});({});({});({});({}))",
            times.join(";"), symbols.join(";"), prices.join(";"), sizes.join(";"),
            seqs.join(";")
        );

        match Self::execute_single_insert(stream, &q_code, in_flight).await {
            Ok(()) => {
                debug!("✅ Flushed {} trades", batch.len());
                Ok(())
            }
            Err(e) => {
                warn!("❌ Failed to bulk-insert trades: {}", e);
                Err(e)
            }
        }
    }

    /// Flush a batch of snapshots using a single bulk insert command
    async fn flush_snapshots_batch(
        stream: &Arc<Mutex<Option<QStream>>>,
        batch: &[Snapshot],
        in_flight: &AtomicBool,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        debug!("🔄 Flushing {} snapshots to kdb+", batch.len());

        let times: Vec<String> = batch.iter().map(|s| format!("`timestamp${}", s.timestamp)).collect();
        let symbols: Vec<String> = batch.iter().map(|s| format!("`{}", s.symbol)).collect();
        let best_bids: Vec<String> = batch.iter().map(|s| format!("{}f", s.best_bid)).collect();
        let best_asks: Vec<String> = batch.iter().map(|s| format!("{}f", s.best_ask)).collect();
        let best_bid_sizes: Vec<String> = batch.iter().map(|s| format!("{}f", s.best_bid_size)).collect();
        let best_ask_sizes: Vec<String> = batch.iter().map(|s| format!("{}f", s.best_ask_size)).collect();
        let spreads: Vec<String> = batch.iter().map(|s| format!("{}f", s.spread)).collect();
        let seqs: Vec<String> = batch.iter().map(|s| fmt_seq(s.seq_num)).collect();

        let q_code = format!(
            "`orderbook_snapshots insert (({});({});({});({});({});({});({});({}))",
            times.join(";"), symbols.join(";"), best_bids.join(";"),
            best_asks.join(";"), best_bid_sizes.join(";"),
            best_ask_sizes.join(";"), spreads.join(";"), seqs.join(";")
        );

        match Self::execute_single_insert(stream, &q_code, in_flight).await {
            Ok(()) => {
                debug!("✅ Flushed {} snapshots", batch.len());
                Ok(())
            }
            Err(e) => {
                warn!("❌ Failed to bulk-insert snapshots: {}", e);
                Err(e)
            }
        }
    }

    async fn flush_capture_gaps_batch(
        stream: &Arc<Mutex<Option<QStream>>>,
        batch: &[CaptureGap],
        in_flight: &AtomicBool,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        debug!("🔄 Flushing {} capture gaps to kdb+", batch.len());

        let times: Vec<String> = batch.iter()
            .map(|g| format!("`timestamp${}", g.timestamp)).collect();
        let symbols: Vec<String> = batch.iter().map(|g| {
            format!("`$\"{}\"", escape_kdb_string(&g.symbol))
        }).collect();
        let expected: Vec<String> = batch.iter().map(|g| format!("{}j", g.expected_seq)).collect();
        let received: Vec<String> = batch.iter().map(|g| format!("{}j", g.received_seq)).collect();
        let sizes: Vec<String> = batch.iter().map(|g| format!("{}j", g.gap_size)).collect();
        let q_code = format!(
            "`capture_gaps insert (({});({});({});({});({}))",
            times.join(";"), symbols.join(";"), expected.join(";"),
            received.join(";"), sizes.join(";")
        );

        match Self::execute_single_insert(stream, &q_code, in_flight).await {
            Ok(()) => {
                debug!("✅ Flushed {} capture gaps", batch.len());
                Ok(())
            }
            Err(e) => {
                warn!("❌ Failed to bulk-insert capture gaps: {}", e);
                Err(e)
            }
        }
    }

    /// Write order book level update to kdb+ (buffered, batched). Always enqueues
    /// even when disconnected; flush only happens while connected.
    pub async fn write_order_book_level(
        &self,
        symbol: &str,
        side: char,
        price: f64,
        size: f64,
        action: char,
        seq_num: Option<u32>,
    ) -> Result<()> {
        let timestamp = kdb_now_ns();

        let level = OrderBookLevel {
            timestamp,
            symbol: symbol.to_string(),
            side,
            price,
            size,
            action,
            seq_num,
        };

        let buffer_len = {
            let mut buffer = self.level_buffer.lock().await;
            if buffer.len() >= MAX_KDB_BUFFER {
                let drop_count = buffer.len() / 2;
                let dropped: Vec<_> = buffer.drain(..drop_count).collect();
                let symbols: Vec<String> = dropped.iter().map(|x| x.symbol.clone()).collect();
                warn!("⚠️  kdb level buffer overflow ({} entries) — dropped {} oldest entries", MAX_KDB_BUFFER, drop_count);
                drop(buffer);
                self.emit_overflow_gap(&symbols).await;
                let mut buffer = self.level_buffer.lock().await;
                buffer.push_back(level);
                buffer.len()
            } else {
                buffer.push_back(level);
                buffer.len()
            }
        };

        debug!("📦 Buffered order book level. Buffer size: {}", buffer_len);

        if buffer_len >= self.batch_size
            && *self.connected.lock().await
            && !self.levels_count_in_flight.load(Ordering::Acquire)
        {
            let mut buffer = self.level_buffer.lock().await;
            let batch: Vec<_> = buffer.drain(..).collect();
            drop(buffer);

            if let Err(e) = Self::flush_levels_batch(
                &self.stream,
                &batch,
                &self.connected,
                &self.probe,
                &self.levels_count_scheduled,
                &self.levels_count_in_flight,
            )
            .await
            {
                Self::handle_flush_err(
                    &self.connected,
                    &self.stream,
                    &self.level_buffer,
                    batch,
                    "level",
                    e,
                    &self.probe,
                )
                .await;
            }
        }

        Ok(())
    }

    /// Write trade to kdb+ (buffered, batched)
    pub async fn write_trade(
        &self,
        symbol: &str,
        price: f64,
        size: f64,
        timestamp: i64,
        seq_num: Option<u32>,
    ) -> Result<()> {
        let trade = Trade {
            timestamp: timestamp - KDB_TIMESTAMP_OFFSET_NS,
            symbol: symbol.to_string(),
            price,
            size,
            seq_num,
        };

        let buffer_len = {
            let mut buffer = self.trade_buffer.lock().await;
            if buffer.len() >= MAX_KDB_BUFFER {
                let drop_count = buffer.len() / 2;
                let dropped: Vec<_> = buffer.drain(..drop_count).collect();
                let symbols: Vec<String> = dropped.iter().map(|x| x.symbol.clone()).collect();
                warn!("⚠️  kdb trade buffer overflow — dropped {} oldest entries", drop_count);
                drop(buffer);
                self.emit_overflow_gap(&symbols).await;
                let mut buffer = self.trade_buffer.lock().await;
                buffer.push_back(trade);
                buffer.len()
            } else {
                buffer.push_back(trade);
                buffer.len()
            }
        };

        if buffer_len >= self.batch_size
            && *self.connected.lock().await
            && !self.levels_count_in_flight.load(Ordering::Acquire)
        {
            let mut buffer = self.trade_buffer.lock().await;
            let batch: Vec<_> = buffer.drain(..).collect();
            drop(buffer);

            if let Err(e) = Self::flush_trades_batch(
                &self.stream,
                &batch,
                &self.levels_count_in_flight,
            )
            .await
            {
                Self::handle_flush_err(
                    &self.connected,
                    &self.stream,
                    &self.trade_buffer,
                    batch,
                    "trade",
                    e,
                    &self.probe,
                )
                .await;
            }
        }

        Ok(())
    }

    /// Write best bid/ask snapshot to kdb+ (buffered, batched)
    pub async fn write_snapshot(
        &self,
        symbol: &str,
        best_bid: f64,
        best_ask: f64,
        best_bid_size: f64,
        best_ask_size: f64,
        spread: f64,
        seq_num: Option<u32>,
    ) -> Result<()> {
        let timestamp = kdb_now_ns();

        let snapshot = Snapshot {
            timestamp,
            symbol: symbol.to_string(),
            best_bid,
            best_ask,
            best_bid_size,
            best_ask_size,
            spread,
            seq_num,
        };

        let buffer_len = {
            let mut buffer = self.snapshot_buffer.lock().await;
            if buffer.len() >= MAX_KDB_BUFFER {
                let drop_count = buffer.len() / 2;
                let dropped: Vec<_> = buffer.drain(..drop_count).collect();
                let symbols: Vec<String> = dropped.iter().map(|x| x.symbol.clone()).collect();
                warn!("⚠️  kdb snapshot buffer overflow — dropped {} oldest entries", drop_count);
                drop(buffer);
                self.emit_overflow_gap(&symbols).await;
                let mut buffer = self.snapshot_buffer.lock().await;
                buffer.push_back(snapshot);
                buffer.len()
            } else {
                buffer.push_back(snapshot);
                buffer.len()
            }
        };

        if buffer_len >= self.batch_size
            && *self.connected.lock().await
            && !self.levels_count_in_flight.load(Ordering::Acquire)
        {
            let mut buffer = self.snapshot_buffer.lock().await;
            let batch: Vec<_> = buffer.drain(..).collect();
            drop(buffer);

            if let Err(e) = Self::flush_snapshots_batch(
                &self.stream,
                &batch,
                &self.levels_count_in_flight,
            )
            .await
            {
                Self::handle_flush_err(
                    &self.connected,
                    &self.stream,
                    &self.snapshot_buffer,
                    batch,
                    "snapshot",
                    e,
                    &self.probe,
                )
                .await;
            }
        }

        Ok(())
    }

    /// Always enqueue a capture-gap row. `gap_size` is computed in Rust.
    /// Never copies the MD `!connected` skip — a kdb-down GapDetected must still buffer.
    pub async fn write_capture_gap(
        &self,
        symbol: &str,
        expected_seq: u32,
        received_seq: u32,
    ) -> Result<()> {
        let gap_size = received_seq.saturating_sub(expected_seq);
        self.enqueue_capture_gap(CaptureGap {
            timestamp: kdb_now_ns(),
            symbol: symbol.to_string(),
            expected_seq,
            received_seq,
            gap_size,
        }).await;
        Ok(())
    }

    /// Flush a batch of own execution fills using a single bulk insert command
    async fn flush_own_trades_batch(
        stream: &Arc<Mutex<Option<QStream>>>,
        batch: &[OwnTrade],
        in_flight: &AtomicBool,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        debug!("🔄 Flushing {} own trades to kdb+", batch.len());

        let q_code = own_trades_insert_q(batch);

        match Self::execute_single_insert(stream, &q_code, in_flight).await {
            Ok(()) => {
                debug!("✅ Flushed {} own trades", batch.len());
                Ok(())
            }
            Err(e) => {
                warn!("❌ Failed to bulk-insert own trades: {}", e);
                Err(e)
            }
        }
    }

    /// Write an own execution fill to kdb+ (buffered)
    /// Call this from the FIX execution report handler when ExecType is F (partial) or 2 (full fill).
    pub async fn write_own_trade(
        &self,
        symbol: &str,
        side: char,
        last_px: f64,
        last_qty: f64,
        avg_px: f64,
        cl_ord_id: &str,
        order_id: &str,
        exec_type: char,
    ) -> Result<()> {
        let timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0) - KDB_TIMESTAMP_OFFSET_NS;

        let own_trade = OwnTrade {
            timestamp,
            symbol: symbol.to_string(),
            side,
            last_px,
            last_qty,
            avg_px,
            cl_ord_id: cl_ord_id.to_string(),
            order_id: order_id.to_string(),
            exec_type,
        };

        let buffer_len = {
            let mut buffer = self.own_trade_buffer.lock().await;

            if buffer.len() >= MAX_KDB_BUFFER {
                let drop_count = buffer.len() / 2;
                buffer.drain(..drop_count);
                warn!("⚠️  kdb own_trade buffer overflow — dropped {} oldest entries", drop_count);
            }

            buffer.push_back(own_trade);
            buffer.len()
        };

        let up = *self.connected.lock().await;
        if buffer_len >= self.batch_size
            && up
            && !self.levels_count_in_flight.load(Ordering::Acquire)
        {
            let mut buffer = self.own_trade_buffer.lock().await;
            let batch: Vec<_> = buffer.drain(..).collect();
            drop(buffer);
            if let Err(e) = Self::flush_own_trades_batch(
                &self.stream,
                &batch,
                &self.levels_count_in_flight,
            )
            .await
            {
                Self::handle_flush_err(
                    &self.connected,
                    &self.stream,
                    &self.own_trade_buffer,
                    batch,
                    "own_trade",
                    e,
                    &self.probe,
                ).await;
            }
        }

        Ok(())
    }

    /// Write securities to kdb+ (bulk insert, no batching needed as this is infrequent)
    pub async fn write_securities(&self, securities: &[(String, Option<String>, Option<String>, String)]) -> Result<()> {
        if !*self.connected.lock().await {
            warn!("⚠️  Not connected to kdb+, skipping securities write");
            return Ok(()); // Silently skip if not connected
        }

        if securities.is_empty() {
            return Ok(());
        }

        info!("📝 Writing {} securities to kdb+", securities.len());

        let timestamp = Utc::now().timestamp_nanos_opt().unwrap_or(0) - KDB_TIMESTAMP_OFFSET_NS;

        // Insert securities one at a time using proper table append syntax
        let mut success_count = 0;
        for (symbol, security_id, exchange, attributes_json) in securities {
            // Escape JSON string for kdb+
            let escaped_attrs = attributes_json.replace('\\', "\\\\").replace('"', "\\\"");

            // Format optional fields
            let sec_id_str = security_id.as_ref()
                .map(|s| format!("`{}", s))
                .unwrap_or_else(|| "`".to_string());
            let exch_str = exchange.as_ref()
                .map(|s| format!("`{}", s))
                .unwrap_or_else(|| "`".to_string());

            // Use direct table append (same pattern as orderbook_levels)
            let q_code = format!(
                "securities: securities, enlist (`timestamp${}; `{}; {}; {}; \"{}\")",
                timestamp, symbol, sec_id_str, exch_str, escaped_attrs
            );

            if let Ok(()) = Self::execute_single_insert(
                &self.stream,
                q_code.as_str(),
                &self.levels_count_in_flight,
            )
            .await
            {
                success_count += 1;
            } else {
                warn!("❌ Failed to insert security: {}", symbol);
            }
        }

        if success_count > 0 {
            info!("✅ Successfully wrote {} securities to kdb+", success_count);
        }

        Ok(())
    }

    /// Execute a kdb+ query and return the result serialised as a JSON string.
    /// Wraps `q_query` with `.j.j` automatically.
    pub async fn query_json(&self, q_query: &str) -> Result<String> {
        self.query_json_full(&format!(".j.j {}", q_query)).await
    }

    /// Execute a complete kdb+ expression and return the string result as-is.
    /// Use this when you need full control over the expression (e.g. to pre-convert types
    /// before calling `.j.j`), or when the expression already contains `.j.j`.
    pub async fn query_json_full(&self, full_expr: &str) -> Result<String> {
        if !*self.connected.lock().await {
            return Err(anyhow!("Not connected to kdb+"));
        }
        if self.levels_count_in_flight.load(Ordering::Acquire) {
            return Err(anyhow!("Stream not available"));
        }

        let mut stream_guard = self.stream.lock().await;
        let stream = stream_guard.as_mut()
            .ok_or_else(|| anyhow!("Stream not available"))?;

        match stream.send_sync_message(&full_expr).await {
            Ok(k) => {
                match k.as_string() {
                    Ok(s) => Ok(s.to_string()),
                    Err(_) => {
                        // K object is not a string — likely a kdb+ error object ('type, etc.)
                        // Return it via Display so the caller can detect and report it.
                        Ok(format!("{}", k))
                    }
                }
            }
            Err(e) => Err(anyhow!("kdb+ query error: {}", e)),
        }
    }

    /// Execute a kdb+ SELECT query and return the result as a JSON array of row objects.
    /// This bypasses `.j.j` entirely and converts the kdb+ binary TABLE response on the
    /// Rust side, avoiding serialization issues with timestamp$/symbol$ types in old kdb+.
    /// Returns a JSON string: `[{"col": val, ...}, ...]` or `[]` for empty tables.
    pub async fn query_table_to_json(&self, q_query: &str) -> Result<String> {
        if !*self.connected.lock().await {
            return Err(anyhow!("Not connected to kdb+"));
        }
        if self.levels_count_in_flight.load(Ordering::Acquire) {
            return Err(anyhow!("Stream not available"));
        }

        let mut stream_guard = self.stream.lock().await;
        let stream = stream_guard.as_mut()
            .ok_or_else(|| anyhow!("Stream not available"))?;

        let k = stream.send_sync_message(&q_query).await
            .map_err(|e| anyhow!("kdb+ query error: {}", e))?;

        // Detect kdb+ error response
        if k.get_type() == qtype::ERROR {
            let err_msg = k.get_error_string().unwrap_or("unknown error");
            return Err(anyhow!("kdb+ error: '{}", err_msg));
        }

        // Expect a TABLE response (type 98)
        if k.get_type() != qtype::TABLE {
            // Might be a long (count), atom, etc. — fall back to Display for debug
            return Err(anyhow!("kdb+ returned non-table type {}: {}", k.get_type(), k));
        }

        Self::k_table_to_json(&k)
    }

    fn record_probe_arc(probe: &Arc<std::sync::Mutex<Vec<ProbeEvent>>>, ev: ProbeEvent) {
        probe.lock().unwrap().push(ev.clone());
        log::info!("PROBE {:?}", ev);
    }

    pub fn record_probe(&self, ev: ProbeEvent) {
        Self::record_probe_arc(&self.probe, ev);
    }

    pub fn drain_probe(&self) -> Vec<ProbeEvent> {
        std::mem::take(&mut *self.probe.lock().unwrap())
    }

    /// Convert a kxkdb TABLE K object to a JSON array string.
    /// Handles columns of type: timestamp (12), float (9), symbol (11), long (7).
    pub(crate) fn k_table_to_json(k: &K) -> Result<String> {
        // A TABLE in kxkdb is a flip of a dictionary.
        // get_dictionary() returns the underlying [keys: symbol_list, values: compound_list]
        let dict_k = k.get_dictionary()
            .map_err(|e| anyhow!("Failed to get table dictionary: {}", e))?;

        let kv = dict_k.as_vec::<K>()
            .map_err(|e| anyhow!("Failed to get dict K-vec: {}", e))?;

        let col_names = kv[0].as_vec::<S>()
            .map_err(|e| anyhow!("Failed to get column names: {}", e))?;

        let col_data = kv[1].as_vec::<K>()
            .map_err(|e| anyhow!("Failed to get column data: {}", e))?;

        if col_names.is_empty() || col_data.is_empty() {
            return Ok("[]".to_string());
        }

        // Determine the number of rows from the first column
        let nrows = k.len();
        if nrows == 0 {
            return Ok("[]".to_string());
        }

        let ncols = col_names.len().min(col_data.len());

        // Build rows
        let mut rows: Vec<String> = Vec::with_capacity(nrows);
        for row_idx in 0..nrows {
            let mut fields: Vec<String> = Vec::with_capacity(ncols);
            for col_idx in 0..ncols {
                let col_name = &col_names[col_idx];
                let col = &col_data[col_idx];
                let json_val = Self::k_col_value_to_json(col, row_idx);
                fields.push(format!("\"{}\":{}", col_name, json_val));
            }
            rows.push(format!("{{{}}}", fields.join(",")));
        }

        Ok(format!("[{}]", rows.join(",")))
    }

    /// Extract a single value from a typed kdb+ column K object and return it as a JSON token.
    fn k_col_value_to_json(col: &K, idx: usize) -> String {
        match col.get_type() {
            qtype::TIMESTAMP_LIST => {
                // i64 nanoseconds since kdb+ epoch (2000-01-01). Convert to Unix milliseconds.
                if let Ok(v) = col.as_vec::<J>() {
                    if let Some(&ns) = v.get(idx) {
                        let unix_ms = (ns + KDB_TIMESTAMP_OFFSET_NS) / 1_000_000;
                        return format!("{}", unix_ms);
                    }
                }
                "null".to_string()
            }
            qtype::FLOAT_LIST => {
                if let Ok(v) = col.as_vec::<F>() {
                    if let Some(&f) = v.get(idx) {
                        if f.is_finite() {
                            return format!("{}", f);
                        }
                    }
                }
                "null".to_string()
            }
            qtype::SYMBOL_LIST => {
                if let Ok(v) = col.as_vec::<S>() {
                    if let Some(s) = v.get(idx) {
                        let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
                        return format!("\"{}\"", escaped);
                    }
                }
                "\"\"".to_string()
            }
            qtype::LONG_LIST => {
                if let Ok(v) = col.as_vec::<J>() {
                    if let Some(&j) = v.get(idx) {
                        return format!("{}", j);
                    }
                }
                "null".to_string()
            }
            qtype::INT_LIST => {
                if let Ok(v) = col.as_vec::<I>() {
                    if let Some(&i) = v.get(idx) {
                        return format!("{}", i);
                    }
                }
                "null".to_string()
            }
            _ => {
                // Fallback: use Display on the whole column and extract the idx-th element
                "null".to_string()
            }
        }
    }

    /// Disconnect from kdb+ and flush any remaining buffers
    pub async fn disconnect(&self) {
        *self.connected.lock().await = false;

        // Flush remaining buffers
        let levels: Vec<_> = self.level_buffer.lock().await.drain(..).collect();
        let trades: Vec<_> = self.trade_buffer.lock().await.drain(..).collect();
        let snapshots: Vec<_> = self.snapshot_buffer.lock().await.drain(..).collect();
        let own_trades: Vec<_> = self.own_trade_buffer.lock().await.drain(..).collect();
        let gaps: Vec<_> = self.capture_gap_buffer.lock().await.drain(..).collect();

        if !levels.is_empty() {
            let _ = Self::flush_levels_batch(
                &self.stream,
                &levels,
                &self.connected,
                &self.probe,
                &self.levels_count_scheduled,
                &self.levels_count_in_flight,
            )
            .await;
        }
        if !trades.is_empty() {
            let _ = Self::flush_trades_batch(
                &self.stream,
                &trades,
                &self.levels_count_in_flight,
            )
            .await;
        }
        if !snapshots.is_empty() {
            let _ = Self::flush_snapshots_batch(
                &self.stream,
                &snapshots,
                &self.levels_count_in_flight,
            )
            .await;
        }
        if !own_trades.is_empty() {
            let _ = Self::flush_own_trades_batch(
                &self.stream,
                &own_trades,
                &self.levels_count_in_flight,
            )
            .await;
        }
        if !gaps.is_empty() {
            let _ = Self::flush_capture_gaps_batch(
                &self.stream,
                &gaps,
                &self.levels_count_in_flight,
            )
            .await;
        }

        if let Some(mut stream) = self.stream.lock().await.take() {
            if let Err(e) = stream.shutdown().await {
                warn!("Error shutting down kdb+ connection: {}", e);
            }
        }
        info!("🔌 Disconnected from kdb+");
    }
}

/// Connect, run one SELECT, convert via `k_table_to_json`, drop the stream.
/// MUST NOT call initialize_tables / ensureCol / start_flush_task / start_reconnect_task.
pub async fn query_table_once(host: &str, port: u16, q: &str) -> anyhow::Result<String> {
    let mut stream = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        QStream::connect(ConnectionMethod::TCP, host, port, ""),
    )
    .await
    .map_err(|_| anyhow!("kdb connect timed out after 5s"))?
    .map_err(|e| anyhow!("kdb connect failed: {}", e))?;

    let k = tokio::time::timeout(
        std::time::Duration::from_secs(120),
        stream.send_sync_message(&q),
    )
    .await
    .map_err(|_| anyhow!("kdb query timed out after 120s"))?
    .map_err(|e| anyhow!("kdb query error: {}", e))?;

    if k.get_type() == qtype::ERROR {
        let err_msg = k.get_error_string().unwrap_or("unknown error");
        return Err(anyhow!("kdb+ error: '{}", err_msg));
    }
    if k.get_type() != qtype::TABLE {
        return Err(anyhow!("kdb+ returned non-table type {}: {}", k.get_type(), k));
    }
    KdbWriter::k_table_to_json(&k)
}

#[cfg(test)]
impl KdbWriter {
    pub(crate) async fn level_buffer_len(&self) -> usize {
        self.level_buffer.lock().await.len()
    }

    pub(crate) async fn own_trade_buffer_len(&self) -> usize {
        self.own_trade_buffer.lock().await.len()
    }

    pub(crate) async fn is_connected(&self) -> bool {
        *self.connected.lock().await
    }

    pub(crate) async fn capture_gap_buffer_len(&self) -> usize {
        self.capture_gap_buffer.lock().await.len()
    }

    pub(crate) async fn capture_gap_tuples(&self) -> Vec<(String, u32, u32, u32)> {
        self.capture_gap_buffer
            .lock()
            .await
            .iter()
            .map(|g| (g.symbol.clone(), g.expected_seq, g.received_seq, g.gap_size))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kxkdb::ipc::{qmsg_type, Auth, ConnectionMethod, K, QStream};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[test]
    fn escape_kdb_string_quotes_and_backslashes() {
        assert_eq!(escape_kdb_string("a\"b\\c"), "a\\\"b\\\\c");
    }

    #[test]
    fn fmt_kdb_char_col_ba_nc() {
        assert_eq!(fmt_kdb_char_col(['B', 'A']), "\"BA\"");
        assert_eq!(fmt_kdb_char_col(['N', 'C']), "\"NC\"");
        assert_eq!(fmt_kdb_char_col(std::iter::empty()), "\"\"");
    }

    #[test]
    fn fmt_kdb_char_col_escapes_quote_backslash_semicolon() {
        assert_eq!(fmt_kdb_char_col(['"']), "\"\\\"\"");
        assert_eq!(fmt_kdb_char_col(['\\']), "\"\\\\\"");
        assert_eq!(fmt_kdb_char_col([';']), "\";\"");
    }

    fn sample_level(symbol: &str, side: char, action: char, ts: i64, seq: u32) -> OrderBookLevel {
        OrderBookLevel {
            timestamp: ts,
            symbol: symbol.to_string(),
            side,
            price: 1.0,
            size: 2.0,
            action,
            seq_num: Some(seq),
        }
    }

    #[test]
    fn levels_insert_q_two_row_bare_char_vectors() {
        let batch = vec![
            sample_level("XYZ", 'B', 'N', 1, 7),
            sample_level("XYZ", 'A', 'C', 2, 8),
        ];
        let q = levels_insert_q(&batch);
        assert!(q.contains(";\"BA\";"), "expected bare char vector ;\"BA\"; got {}", q);
        assert!(q.contains(";\"NC\";"), "expected bare char vector ;\"NC\"; got {}", q);
        assert!(!q.contains("(\"BA\")"), "must not enlist (\"BA\"): {}", q);
        assert!(!q.contains("(\"NC\")"), "must not enlist (\"NC\"): {}", q);
        assert!(!q.contains("(\"B\";\"A\")"), "must not emit mixed-list (\"B\";\"A\"): {}", q);
        assert!(!q.contains("`B"), "leftover symbol atom `B would false-fail on BTC_USDT; fixture is XYZ: {}", q);
        assert!(q.contains("`XYZ"), "symbol column stays symbol atoms: {}", q);
        assert!(q.contains("7j"));
        assert!(q.contains("8j"));
    }

    #[test]
    fn levels_insert_q_one_row_char_atom() {
        let batch = vec![sample_level("XYZ", 'B', 'N', 1, 7)];
        let q = levels_insert_q(&batch);
        assert!(q.contains(";\"B\";"), "expected char atom ;\"B\"; got {}", q);
        assert!(q.contains(";\"N\";"), "expected char atom ;\"N\"; got {}", q);
        assert!(!q.contains("(\"B\")"), "must not enlist (\"B\"): {}", q);
        assert!(!q.contains("(\"N\")"), "must not enlist (\"N\"): {}", q);
        assert!(!q.contains("`B"));
        assert!(!q.contains("`N"));
    }

    #[test]
    fn own_trades_insert_q_char_slots_unparenthesized() {
        let batch = vec![OwnTrade {
            timestamp: 1,
            symbol: "XYZ".into(),
            side: '1',
            last_px: 1.0,
            last_qty: 1.0,
            avg_px: 1.0,
            cl_ord_id: "c1".into(),
            order_id: "o1".into(),
            exec_type: 'F',
        }];
        let q = own_trades_insert_q(&batch);
        assert!(q.contains(";\"1\";"), "side slot unparenthesized ;\"1\"; got {}", q);
        assert!(q.contains(";\"F\")"), "exec_type is last column so delimiter is ): {}", q);
        assert!(!q.contains("(\"1\")"));
        assert!(!q.contains("(\"F\")"));
        assert!(q.contains("`c1"), "ids stay symbol atoms: {}", q);
        assert!(q.contains("`o1"), "ids stay symbol atoms: {}", q);
    }

    #[test]
    fn flush_levels_batch_no_symbol_atoms_for_side_action() {
        let batch = vec![
            sample_level("XYZ", 'B', 'N', 1, 7),
            sample_level("XYZ", 'A', 'D', 2, 8),
        ];
        let q = levels_insert_q(&batch);
        assert!(!q.contains("`B"), "{}", q);
        assert!(!q.contains("`A"), "{}", q);
        assert!(!q.contains("`N"), "{}", q);
        assert!(!q.contains("`D"), "{}", q);
        assert!(q.contains("\"BA\""));
        assert!(q.contains("\"ND\""));
        let ot = own_trades_insert_q(&[OwnTrade {
            timestamp: 1,
            symbol: "XYZ".into(),
            side: '1',
            last_px: 1.0,
            last_qty: 1.0,
            avg_px: 1.0,
            cl_ord_id: "c1".into(),
            order_id: "o1".into(),
            exec_type: 'F',
        }]);
        assert!(!ot.contains("`1"), "own_trades side must not be a symbol atom: {}", ot);
        assert!(!ot.contains("`F"), "own_trades exec_type must not be a symbol atom: {}", ot);
    }

    #[test]
    fn k_table_to_json_long_null_is_still_array() {
        // LONG_LIST path uses format!("{}", j); extra keys cannot fail Array.isArray.
        let n = format!("{}", i64::MIN);
        assert!(n.parse::<i64>().is_ok());
        let arr = format!("[{{\"seq_num\":{}}}]", n);
        let v: serde_json::Value = serde_json::from_str(&arr).unwrap();
        assert!(v.is_array());
    }

    #[tokio::test]
    async fn write_level_enqueues_when_disconnected() {
        let w = KdbWriter::new("127.0.0.1".into(), 1);
        w.write_order_book_level("BTC_USDT", 'B', 1.0, 1.0, 'N', Some(7)).await.unwrap();
        assert!(w.drain_probe().is_empty(), "probe is recorded at replay, not write_*");
        assert_eq!(w.level_buffer_len().await, 1);
    }

    #[tokio::test]
    async fn overflow_emits_expected_seq_0_homogeneous() {
        let w = KdbWriter::new("127.0.0.1".into(), 1);
        for _ in 0..10_001 {
            w.write_order_book_level("BTC_USDT", 'B', 1.0, 1.0, 'N', Some(1)).await.unwrap();
        }
        let evs = w.drain_probe();
        let overflow = evs.iter().rev().find_map(|e| match e {
            ProbeEvent::OverflowGap { symbol, drop_count } => Some((symbol.as_str(), *drop_count)),
            _ => None,
        });
        assert_eq!(overflow, Some(("BTC_USDT", 5000)));
        let gaps = w.capture_gap_tuples().await;
        assert!(
            gaps.iter().any(|(s, e, r, g)| s == "BTC_USDT" && *e == 0 && *r == 5000 && *g == 5000),
            "expected overflow capture_gap (BTC_USDT, 0, 5000, 5000), got {:?}",
            gaps
        );
    }

    #[tokio::test]
    async fn overflow_mixed_symbol_star() {
        let w = KdbWriter::new("127.0.0.1".into(), 1);
        for i in 0..10_000 {
            let sym = if i % 2 == 0 { "BTC_USDT" } else { "BTC_USD" };
            w.write_order_book_level(sym, 'B', 1.0, 1.0, 'N', Some(1)).await.unwrap();
        }
        w.write_order_book_level("BTC_USDT", 'B', 1.0, 1.0, 'N', Some(1)).await.unwrap();
        let evs = w.drain_probe();
        let overflow = evs.iter().rev().find_map(|e| match e {
            ProbeEvent::OverflowGap { symbol, drop_count } => Some((symbol.clone(), *drop_count)),
            _ => None,
        });
        assert_eq!(overflow, Some(("*".to_string(), 5000)));
    }

    #[tokio::test]
    async fn gap_buffer_overflow_does_not_recurse() {
        let w = KdbWriter::new("127.0.0.1".into(), 1);
        for i in 0..10_001u32 {
            w.write_capture_gap("BTC_USDT", i, i + 1).await.unwrap();
        }
        assert!(w.capture_gap_buffer_len().await <= 10_000);
        let evs = w.drain_probe();
        assert!(
            !evs.iter().any(|e| matches!(e, ProbeEvent::OverflowGap { .. })),
            "gap-buffer overflow must not emit OverflowGap, got {:?}",
            evs
        );
    }

    struct PermissiveAuth;

    impl Auth for PermissiveAuth {
        fn authorize<'life0, 'life1, 'async_trait>(
            &'life0 mut self,
            _credential: &'life1 str,
        ) -> std::pin::Pin<
            Box<dyn std::future::Future<Output = kxkdb::ipc::Result<()>> + Send + 'async_trait>,
        >
        where
            'life0: 'async_trait,
            'life1: 'async_trait,
            Self: 'async_trait,
        {
            Box::pin(async { Ok(()) })
        }
    }

    fn sample_own_trade() -> OwnTrade {
        OwnTrade {
            timestamp: 1,
            symbol: "BTC_USDT".into(),
            side: 'B',
            last_px: 1.0,
            last_qty: 1.0,
            avg_px: 1.0,
            cl_ord_id: "c1".into(),
            order_id: "o1".into(),
            exec_type: 'F',
        }
    }

    async fn ack_until_err_or_drop(
        qs: &mut QStream,
        mut drop_rx: Option<&mut tokio::sync::oneshot::Receiver<()>>,
    ) {
        loop {
            let recv = qs.receive_message();
            let msg = if let Some(rx) = drop_rx.as_mut() {
                tokio::select! {
                    _ = &mut **rx => return,
                    msg = recv => msg,
                }
            } else {
                recv.await
            };
            match msg {
                Ok((typ, _)) if typ == qmsg_type::synchronous => {
                    let _ = qs.send_message(&K::new_long(1), qmsg_type::response).await;
                }
                Ok(_) => {}
                Err(_) => return,
            }
        }
    }

    #[tokio::test]
    async fn write_own_trade_enqueues_when_disconnected() {
        let w = KdbWriter::new("127.0.0.1".into(), 1);
        w.write_own_trade("BTC_USDT", 'B', 1.0, 1.0, 1.0, "c1", "o1", 'F').await.unwrap();
        assert_eq!(w.own_trade_buffer_len().await, 1);
        assert!(
            !w.drain_probe().iter().any(|e| matches!(e, ProbeEvent::OverflowGap { .. })),
            "own_trade overflow must not emit OverflowGap"
        );
    }

    #[tokio::test]
    async fn flush_own_trades_batch_err_requeues_and_drops_latch() {
        let w = KdbWriter::new("127.0.0.1".into(), 1);
        *w.connected.lock().await = true;
        w.own_trade_buffer.lock().await.push_back(sample_own_trade());
        let batch: Vec<_> = w.own_trade_buffer.lock().await.drain(..).collect();
        let err = KdbWriter::flush_own_trades_batch(&w.stream, &batch, &w.levels_count_in_flight)
            .await
            .unwrap_err();
        KdbWriter::requeue_on_flush_err(
            &w.connected,
            &w.stream,
            &w.own_trade_buffer,
            batch,
            "own_trade",
            err,
            &w.probe,
        )
        .await;
        assert_eq!(w.own_trade_buffer_len().await, 1);
        assert!(!w.is_connected().await);
        let disc = w
            .drain_probe()
            .into_iter()
            .filter(|e| matches!(e, ProbeEvent::KdbDisconnected))
            .count();
        assert_eq!(disc, 1);
    }

    #[tokio::test]
    async fn write_own_trade_flush_err_requeues_and_drops_latch() {
        let w = KdbWriter::new("127.0.0.1".into(), 1);
        *w.connected.lock().await = true;
        for _ in 0..100 {
            w.write_own_trade("BTC_USDT", 'B', 1.0, 1.0, 1.0, "c1", "o1", 'F')
                .await
                .unwrap();
        }
        assert_eq!(w.own_trade_buffer_len().await, 100);
        assert!(!w.is_connected().await);
        let disc = w
            .drain_probe()
            .into_iter()
            .filter(|e| matches!(e, ProbeEvent::KdbDisconnected))
            .count();
        assert_eq!(disc, 1);
    }

    #[tokio::test]
    async fn probe_once_stream_none_is_transport() {
        let stream: Arc<Mutex<Option<QStream>>> = Arc::new(Mutex::new(None));
        let outcome = KdbWriter::probe_once(&stream, &AtomicBool::new(false)).await;
        assert!(matches!(outcome, ProbeOutcome::Transport));
    }

    #[tokio::test]
    async fn levels_count_in_flight_does_not_mark_disconnected() {
        let w = KdbWriter::new("127.0.0.1".into(), 1);
        *w.connected.lock().await = true;
        w.levels_count_in_flight.store(true, Ordering::Release);

        let outcome = KdbWriter::probe_once(&w.stream, &w.levels_count_in_flight).await;
        assert!(
            matches!(outcome, ProbeOutcome::Skipped),
            "in_flight probe must skip, not Transport: {:?}",
            outcome
        );
        assert!(w.is_connected().await, "skip must not tear down connected");
        assert!(
            !w.drain_probe()
                .iter()
                .any(|e| matches!(e, ProbeEvent::KdbDisconnected)),
            "in_flight skip must not record KdbDisconnected"
        );

        let err = KdbWriter::execute_single_insert(&w.stream, "1", &w.levels_count_in_flight)
            .await
            .unwrap_err();
        assert_eq!(err.to_string(), "kdb levels-count in flight");
        assert!(w.is_connected().await);

        let batch = vec![sample_own_trade()];
        let flush_err = KdbWriter::flush_own_trades_batch(
            &w.stream,
            &batch,
            &w.levels_count_in_flight,
        )
        .await
        .unwrap_err();
        KdbWriter::handle_flush_err(
            &w.connected,
            &w.stream,
            &w.own_trade_buffer,
            batch,
            "own_trade",
            flush_err,
            &w.probe,
        )
        .await;
        assert!(
            w.is_connected().await,
            "in-flight flush defer must requeue without dropping latch"
        );
        assert_eq!(w.own_trade_buffer_len().await, 1);
    }

    #[tokio::test]
    async fn peer_close_reconnects_without_restart() {
        // kxkdb `accept_auth` takes `&mut dyn Auth` which is !Send; keep the stub
        // on a LocalSet so we do not add async-trait / Send bounds.
        let local = tokio::task::LocalSet::new();
        local
            .run_until(async {
                let port = {
                    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let port = listener.local_addr().unwrap().port();
                    drop(listener);
                    port
                };

                let (drop_tx, drop_rx) = tokio::sync::oneshot::channel::<()>();
                tokio::task::spawn_local(async move {
                    let mut drop_rx = Some(drop_rx);
                    let mut first = true;
                    loop {
                        let mut auth = PermissiveAuth;
                        match QStream::accept_auth(
                            ConnectionMethod::TCP,
                            "127.0.0.1",
                            port,
                            &mut auth,
                        )
                        .await
                        {
                            Ok(mut qs) => {
                                if first {
                                    first = false;
                                    let mut rx = drop_rx.take();
                                    ack_until_err_or_drop(&mut qs, rx.as_mut()).await;
                                    drop(qs);
                                } else {
                                    ack_until_err_or_drop(&mut qs, None).await;
                                }
                            }
                            Err(_) => {
                                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                            }
                        }
                    }
                });

                let w = KdbWriter::new("127.0.0.1".into(), port);
                tokio::time::timeout(std::time::Duration::from_secs(10), async {
                    loop {
                        if w.initialize().await.is_ok() {
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                    }
                })
                .await
                .expect("initialize should succeed within 10s");

                assert!(w.is_connected().await);
                let first_events = w.drain_probe();
                assert!(
                    first_events
                        .iter()
                        .any(|e| matches!(e, ProbeEvent::KdbReconnected)),
                    "first connect must record KdbReconnected, got {:?}",
                    first_events
                );
                assert!(
                    !first_events
                        .iter()
                        .any(|e| matches!(e, ProbeEvent::KdbDisconnected)),
                    "first connect must not record KdbDisconnected, got {:?}",
                    first_events
                );

                let _ = drop_tx.send(());

                tokio::time::timeout(std::time::Duration::from_secs(10), async {
                    let mut saw_disc = false;
                    loop {
                        for ev in w.drain_probe() {
                            match ev {
                                ProbeEvent::KdbDisconnected => saw_disc = true,
                                ProbeEvent::KdbReconnected if saw_disc => return,
                                _ => {}
                            }
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    }
                })
                .await
                .expect("idle peer-close must reconnect within 10s without reconstructing KdbWriter");

                assert!(w.is_connected().await);
            })
            .await;
    }
}
