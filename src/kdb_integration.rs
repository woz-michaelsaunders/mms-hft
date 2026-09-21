// kdb+ Integration Module
// Handles writing order book data to kdb+ via direct IPC connection using kxkdb library
// Optimized for high-speed bulk inserts

use anyhow::{anyhow, Result};
use log::{debug, info, error, warn};
use std::sync::Arc;
use tokio::sync::Mutex;
use chrono::Utc;
use kxkdb::ipc::*;
use kxkdb::qtype;
use std::collections::VecDeque;

/// kdb+ epoch offset in nanoseconds (nanoseconds from Unix epoch 1970-01-01 to kdb+ epoch 2000-01-01)
pub(crate) const KDB_TIMESTAMP_OFFSET_NS: i64 = 946_684_800_000_000_000_i64;

/// Maximum entries in each write buffer.  If this is exceeded (e.g. kdb+ is slow or
/// temporarily disconnected) the oldest half is dropped with a warning so we never
/// accumulate unbounded RAM.
const MAX_KDB_BUFFER: usize = 10_000;

const FLUSH_INTERVAL_MS: u64 = 100;
const RECONNECT_POLL_MS: u64 = 250;
const RECONNECT_BACKOFF_CAP_MS: u64 = 30_000;
const DEFAULT_RECONNECT_BACKOFF_MS: u64 = 1_000;

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

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProbeEvent {
    PendingMdIo { kind: &'static str, symbol: String, seq_num: Option<u32> },
    CaptureGap { symbol: String, expected: u32, received: u32, gap_size: u32 },
    OverflowGap { symbol: String, drop_count: u32 },
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

    #[cfg(test)]
    probe: Arc<std::sync::Mutex<Vec<ProbeEvent>>>,
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
            #[cfg(test)]
            probe: Arc::new(std::sync::Mutex::new(Vec::new())),
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
        Self::connect_ipc_with(&self.host, self.port, &self.stream, &self.connected).await
    }

    async fn connect_ipc_with(
        host: &str,
        port: u16,
        stream: &Arc<Mutex<Option<QStream>>>,
        connected: &Arc<tokio::sync::Mutex<bool>>,
    ) -> Result<()> {
        let addr = format!("{}:{}", host, port);
        info!("🔌 Connecting to kdb+ at {}...", addr);

        match QStream::connect(ConnectionMethod::TCP, host, port, "").await {
            Ok(qstream) => {
                *stream.lock().await = Some(qstream);
                match Self::initialize_tables_with(stream, connected).await {
                    Ok(()) => {
                        *connected.lock().await = true;
                        info!("✅ Connected to kdb+ at {}", addr);
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

    /// Start background task to periodically flush buffers
    fn start_flush_task(&self) {
        let level_buf = Arc::clone(&self.level_buffer);
        let trade_buf = Arc::clone(&self.trade_buffer);
        let snapshot_buf = Arc::clone(&self.snapshot_buffer);
        let own_trade_buf = Arc::clone(&self.own_trade_buffer);
        let gap_buf = Arc::clone(&self.capture_gap_buffer);
        let stream = Arc::clone(&self.stream);
        let connected = Arc::clone(&self.connected);
        let batch_size = self.batch_size;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(FLUSH_INTERVAL_MS));
            loop {
                interval.tick().await;

                if *connected.lock().await {
                    let mut levels = level_buf.lock().await;
                    if !levels.is_empty() {
                        let batch: Vec<_> = levels.drain(..).collect();
                        drop(levels);
                        if let Err(e) = Self::flush_levels_batch(&stream, &batch).await {
                            Self::requeue_on_flush_err(&connected, &stream, &level_buf, batch, "level", e).await;
                        }
                    }

                    let mut trades = trade_buf.lock().await;
                    if !trades.is_empty() {
                        let batch: Vec<_> = trades.drain(..).collect();
                        drop(trades);
                        if let Err(e) = Self::flush_trades_batch(&stream, &batch).await {
                            Self::requeue_on_flush_err(&connected, &stream, &trade_buf, batch, "trade", e).await;
                        }
                    }

                    let mut snapshots = snapshot_buf.lock().await;
                    if !snapshots.is_empty() {
                        let batch: Vec<_> = snapshots.drain(..).collect();
                        drop(snapshots);
                        if let Err(e) = Self::flush_snapshots_batch(&stream, &batch).await {
                            Self::requeue_on_flush_err(&connected, &stream, &snapshot_buf, batch, "snapshot", e).await;
                        }
                    }

                    let mut own_trades = own_trade_buf.lock().await;
                    if !own_trades.is_empty() {
                        let batch: Vec<_> = own_trades.drain(..).collect();
                        drop(own_trades);
                        if let Err(e) = Self::flush_own_trades_batch(&stream, &batch).await {
                            warn!("Failed to flush own_trades batch: {}", e);
                        }
                    }

                    let mut gaps = gap_buf.lock().await;
                    if !gaps.is_empty() {
                        let batch: Vec<_> = gaps.drain(..).collect();
                        drop(gaps);
                        if let Err(e) = Self::flush_capture_gaps_batch(&stream, &batch).await {
                            Self::requeue_on_flush_err(&connected, &stream, &gap_buf, batch, "capture_gap", e).await;
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
        let initial = std::env::var("KDB_RECONNECT_BACKOFF_MS")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|&v| v > 0)
            .unwrap_or(DEFAULT_RECONNECT_BACKOFF_MS);

        tokio::spawn(async move {
            let mut backoff = initial;
            loop {
                if *connected.lock().await {
                    backoff = initial;
                    tokio::time::sleep(tokio::time::Duration::from_millis(RECONNECT_POLL_MS)).await;
                    continue;
                }
                warn!("kdb reconnect attempt (backoff {}ms) to {}:{}", backoff, host, port);
                match KdbWriter::connect_ipc_with(&host, port, &stream, &connected).await {
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
    async fn execute_single_insert(stream: &Arc<Mutex<Option<QStream>>>, q_code: &str) -> Result<()> {
        let mut stream_guard = stream.lock().await;
        let qstream = stream_guard.as_mut()
            .ok_or_else(|| anyhow!("Stream not available"))?;
        qstream.send_async_message(&q_code).await
            .map_err(|e| anyhow!("Async insert failed: {}", e))
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
    ) {
        warn!("Failed to flush {} batch: {} — dropping kdb latch and re-queuing", what, err);
        *connected.lock().await = false;
        let _ = stream.lock().await.take();
        let mut buf = buf.lock().await;
        for item in batch.into_iter().rev() {
            buf.push_front(item);
        }
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

        if buffer_len >= self.batch_size && *self.connected.lock().await {
            let mut buffer = self.capture_gap_buffer.lock().await;
            let batch: Vec<_> = buffer.drain(..).collect();
            drop(buffer);
            if let Err(e) = Self::flush_capture_gaps_batch(&self.stream, &batch).await {
                Self::requeue_on_flush_err(
                    &self.connected,
                    &self.stream,
                    &self.capture_gap_buffer,
                    batch,
                    "capture_gap",
                    e,
                ).await;
            }
        }
    }

    /// Flush a batch of order book levels using a single bulk insert command
    async fn flush_levels_batch(stream: &Arc<Mutex<Option<QStream>>>, batch: &[OrderBookLevel]) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        debug!("🔄 Flushing {} order book levels to kdb+", batch.len());

        let times: Vec<String> = batch.iter().map(|l| format!("`timestamp${}", l.timestamp)).collect();
        let symbols: Vec<String> = batch.iter().map(|l| format!("`{}", l.symbol)).collect();
        let sides: Vec<String> = batch.iter().map(|l| format!("`{}", l.side)).collect();
        let prices: Vec<String> = batch.iter().map(|l| format!("{}f", l.price)).collect();
        let sizes: Vec<String> = batch.iter().map(|l| format!("{}f", l.size)).collect();
        let actions: Vec<String> = batch.iter().map(|l| format!("`{}", l.action)).collect();
        let seqs: Vec<String> = batch.iter().map(|l| fmt_seq(l.seq_num)).collect();

        let q_code = format!(
            "`orderbook_levels insert (({});({});({});({});({});({});({}))",
            times.join(";"), symbols.join(";"), sides.join(";"),
            prices.join(";"), sizes.join(";"), actions.join(";"),
            seqs.join(";")
        );

        match Self::execute_single_insert(stream, &q_code).await {
            Ok(()) => {
                debug!("✅ Flushed {} order book levels", batch.len());
                Ok(())
            }
            Err(e) => {
                warn!("❌ Failed to bulk-insert order book levels: {}", e);
                Err(e)
            }
        }
    }

    /// Flush a batch of trades using a single bulk insert command
    async fn flush_trades_batch(stream: &Arc<Mutex<Option<QStream>>>, batch: &[Trade]) -> Result<()> {
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

        match Self::execute_single_insert(stream, &q_code).await {
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
    async fn flush_snapshots_batch(stream: &Arc<Mutex<Option<QStream>>>, batch: &[Snapshot]) -> Result<()> {
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

        match Self::execute_single_insert(stream, &q_code).await {
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

        match Self::execute_single_insert(stream, &q_code).await {
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

        if buffer_len >= self.batch_size && *self.connected.lock().await {
            let mut buffer = self.level_buffer.lock().await;
            let batch: Vec<_> = buffer.drain(..).collect();
            drop(buffer);

            if let Err(e) = Self::flush_levels_batch(&self.stream, &batch).await {
                Self::requeue_on_flush_err(&self.connected, &self.stream, &self.level_buffer, batch, "level", e).await;
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

        if buffer_len >= self.batch_size && *self.connected.lock().await {
            let mut buffer = self.trade_buffer.lock().await;
            let batch: Vec<_> = buffer.drain(..).collect();
            drop(buffer);

            if let Err(e) = Self::flush_trades_batch(&self.stream, &batch).await {
                Self::requeue_on_flush_err(&self.connected, &self.stream, &self.trade_buffer, batch, "trade", e).await;
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

        if buffer_len >= self.batch_size && *self.connected.lock().await {
            let mut buffer = self.snapshot_buffer.lock().await;
            let batch: Vec<_> = buffer.drain(..).collect();
            drop(buffer);

            if let Err(e) = Self::flush_snapshots_batch(&self.stream, &batch).await {
                Self::requeue_on_flush_err(&self.connected, &self.stream, &self.snapshot_buffer, batch, "snapshot", e).await;
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
    async fn flush_own_trades_batch(stream: &Arc<Mutex<Option<QStream>>>, batch: &[OwnTrade]) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        debug!("🔄 Flushing {} own trades to kdb+", batch.len());

        let times: Vec<String> = batch.iter().map(|t| format!("`timestamp${}", t.timestamp)).collect();
        let symbols: Vec<String> = batch.iter().map(|t| format!("`{}", t.symbol)).collect();
        let sides: Vec<String> = batch.iter().map(|t| format!("`{}", t.side)).collect();
        let last_pxs: Vec<String> = batch.iter().map(|t| format!("{}f", t.last_px)).collect();
        let last_qtys: Vec<String> = batch.iter().map(|t| format!("{}f", t.last_qty)).collect();
        let avg_pxs: Vec<String> = batch.iter().map(|t| format!("{}f", t.avg_px)).collect();
        let cl_ord_ids: Vec<String> = batch.iter().map(|t| format!("`{}", t.cl_ord_id)).collect();
        let order_ids: Vec<String> = batch.iter().map(|t| format!("`{}", t.order_id)).collect();
        let exec_types: Vec<String> = batch.iter().map(|t| format!("`{}", t.exec_type)).collect();

        let q_code = format!(
            "`own_trades insert (({});({});({});({});({});({});({});({});({}))",
            times.join(";"), symbols.join(";"), sides.join(";"),
            last_pxs.join(";"), last_qtys.join(";"), avg_pxs.join(";"),
            cl_ord_ids.join(";"), order_ids.join(";"), exec_types.join(";")
        );

        match Self::execute_single_insert(stream, &q_code).await {
            Ok(()) => debug!("✅ Flushed {} own trades", batch.len()),
            Err(e) => warn!("❌ Failed to bulk-insert own trades: {}", e),
        }
        Ok(())
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
        if !*self.connected.lock().await {
            return Ok(());
        }

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

        let mut buffer = self.own_trade_buffer.lock().await;

        if buffer.len() >= MAX_KDB_BUFFER {
            let drop_count = buffer.len() / 2;
            buffer.drain(..drop_count);
            warn!("⚠️  kdb own_trade buffer overflow — dropped {} oldest entries", drop_count);
        }

        buffer.push_back(own_trade);

        if buffer.len() >= self.batch_size {
            let batch: Vec<_> = buffer.drain(..).collect();
            drop(buffer);
            if let Err(e) = Self::flush_own_trades_batch(&self.stream, &batch).await {
                warn!("Failed to flush own_trades batch: {}", e);
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

            if let Ok(()) = Self::execute_single_insert(&self.stream, q_code.as_str()).await {
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

    /// Convert a kxkdb TABLE K object to a JSON array string.
    /// Handles columns of type: timestamp (12), float (9), symbol (11), long (7).
    fn k_table_to_json(k: &K) -> Result<String> {
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
            let _ = Self::flush_levels_batch(&self.stream, &levels).await;
        }
        if !trades.is_empty() {
            let _ = Self::flush_trades_batch(&self.stream, &trades).await;
        }
        if !snapshots.is_empty() {
            let _ = Self::flush_snapshots_batch(&self.stream, &snapshots).await;
        }
        if !own_trades.is_empty() {
            let _ = Self::flush_own_trades_batch(&self.stream, &own_trades).await;
        }
        if !gaps.is_empty() {
            let _ = Self::flush_capture_gaps_batch(&self.stream, &gaps).await;
        }

        if let Some(mut stream) = self.stream.lock().await.take() {
            if let Err(e) = stream.shutdown().await {
                warn!("Error shutting down kdb+ connection: {}", e);
            }
        }
        info!("🔌 Disconnected from kdb+");
    }
}

#[cfg(test)]
impl KdbWriter {
    pub fn record_probe(&self, ev: ProbeEvent) {
        self.probe.lock().unwrap().push(ev.clone());
        log::info!("PROBE {:?}", ev);
    }

    pub fn drain_probe(&self) -> Vec<ProbeEvent> {
        std::mem::take(&mut *self.probe.lock().unwrap())
    }

    pub(crate) async fn level_buffer_len(&self) -> usize {
        self.level_buffer.lock().await.len()
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

    #[test]
    fn escape_kdb_string_quotes_and_backslashes() {
        assert_eq!(escape_kdb_string("a\"b\\c"), "a\\\"b\\\\c");
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
}
