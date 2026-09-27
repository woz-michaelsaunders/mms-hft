// kdb+ Integration Module
// Handles writing order book data to kdb+ via direct IPC connection using kxkdb library
// Optimized for high-speed bulk inserts

use anyhow::{anyhow, Result};
use log::{debug, error, info, warn};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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

fn fmt_f64(x: f64) -> String {
    format!("{}f", x)
}

fn kdb_now_ns() -> i64 {
    Utc::now().timestamp_nanos_opt().unwrap_or(0) - KDB_TIMESTAMP_OFFSET_NS
}

fn truncate_q_snippet(q: &str) -> String {
    if q.len() <= 200 {
        q.to_string()
    } else {
        format!("{}...", &q[..197])
    }
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
pub(crate) struct OrderBookLevel {
    timestamp: i64,
    symbol: String,
    side: char,
    price: f64,
    size: f64,
    action: char,
    seq_num: Option<u32>,
}

#[derive(Clone)]
pub(crate) struct Trade {
    timestamp: i64,
    symbol: String,
    price: f64,
    size: f64,
    seq_num: Option<u32>,
}

#[derive(Clone)]
pub(crate) struct Snapshot {
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
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MdStats {
    pub levels_enqueued: u64,
    pub levels_flushed: u64,
    pub levels_replay_errors: u64,
    pub levels_flush_errors: u64,
    pub trades_enqueued: u64,
    pub trades_flushed: u64,
    pub trades_replay_errors: u64,
    pub trades_flush_errors: u64,
    pub snapshots_enqueued: u64,
    pub snapshots_flushed: u64,
    pub snapshots_replay_errors: u64,
    pub snapshots_flush_errors: u64,
    pub flush_skips: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MdKind { Levels, Trades, Snapshots, OwnTrades, CaptureGaps }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CharCol { Side, Action }

#[derive(Debug)]
pub(crate) struct FormatSkip;

#[derive(Clone, Copy, Debug)]
enum LogKind {
    ReplayEnqueue = 0,
    ReplayLevel,
    ReplayTrade,
    ReplaySnapshot,
    FlushQLevels,
    FlushQTrades,
    FlushQSnapshots,
    FlushQOwnTrades,
    FlushQGaps,
    FlushTransportLevels,
    FlushTransportTrades,
    FlushTransportSnapshots,
    FlushTransportOwnTrades,
    FlushTransportGaps,
    ClosedAlphabet,
}
const LOG_KIND_LEN: usize = 15;

fn log_kind_for(kind: MdKind, q: bool) -> LogKind {
    match (kind, q) {
        (MdKind::Levels, true) => LogKind::FlushQLevels,
        (MdKind::Trades, true) => LogKind::FlushQTrades,
        (MdKind::Snapshots, true) => LogKind::FlushQSnapshots,
        (MdKind::OwnTrades, true) => LogKind::FlushQOwnTrades,
        (MdKind::CaptureGaps, true) => LogKind::FlushQGaps,
        (MdKind::Levels, false) => LogKind::FlushTransportLevels,
        (MdKind::Trades, false) => LogKind::FlushTransportTrades,
        (MdKind::Snapshots, false) => LogKind::FlushTransportSnapshots,
        (MdKind::OwnTrades, false) => LogKind::FlushTransportOwnTrades,
        (MdKind::CaptureGaps, false) => LogKind::FlushTransportGaps,
    }
}

struct MdCounters {
    levels_enqueued: AtomicU64,
    levels_flushed: AtomicU64,
    levels_replay_errors: AtomicU64,
    levels_flush_errors: AtomicU64,
    trades_enqueued: AtomicU64,
    trades_flushed: AtomicU64,
    trades_replay_errors: AtomicU64,
    trades_flush_errors: AtomicU64,
    snapshots_enqueued: AtomicU64,
    snapshots_flushed: AtomicU64,
    snapshots_replay_errors: AtomicU64,
    snapshots_flush_errors: AtomicU64,
    flush_skips: AtomicU64,
    last_log_ms: [AtomicU64; LOG_KIND_LEN],
}

impl MdCounters {
    fn new() -> Self {
        Self {
            levels_enqueued: AtomicU64::new(0),
            levels_flushed: AtomicU64::new(0),
            levels_replay_errors: AtomicU64::new(0),
            levels_flush_errors: AtomicU64::new(0),
            trades_enqueued: AtomicU64::new(0),
            trades_flushed: AtomicU64::new(0),
            trades_replay_errors: AtomicU64::new(0),
            trades_flush_errors: AtomicU64::new(0),
            snapshots_enqueued: AtomicU64::new(0),
            snapshots_flushed: AtomicU64::new(0),
            snapshots_replay_errors: AtomicU64::new(0),
            snapshots_flush_errors: AtomicU64::new(0),
            flush_skips: AtomicU64::new(0),
            last_log_ms: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    fn should_log(&self, kind: LogKind) -> bool {
        let now = Utc::now().timestamp_millis().max(0) as u64;
        let slot = kind as usize;
        let last = self.last_log_ms[slot].load(Ordering::Relaxed);
        if last == 0 || now.saturating_sub(last) >= 1000 {
            self.last_log_ms[slot].store(now, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    fn sampled_error(&self, kind: LogKind, msg: String) {
        if self.should_log(kind) {
            error!("{}", msg);
        }
    }

    fn sampled_info(&self, kind: LogKind, msg: String) {
        if self.should_log(kind) {
            info!("{}", msg);
        }
    }

    fn snapshot(&self) -> MdStats {
        MdStats {
            levels_enqueued: self.levels_enqueued.load(Ordering::Relaxed),
            levels_flushed: self.levels_flushed.load(Ordering::Relaxed),
            levels_replay_errors: self.levels_replay_errors.load(Ordering::Relaxed),
            levels_flush_errors: self.levels_flush_errors.load(Ordering::Relaxed),
            trades_enqueued: self.trades_enqueued.load(Ordering::Relaxed),
            trades_flushed: self.trades_flushed.load(Ordering::Relaxed),
            trades_replay_errors: self.trades_replay_errors.load(Ordering::Relaxed),
            trades_flush_errors: self.trades_flush_errors.load(Ordering::Relaxed),
            snapshots_enqueued: self.snapshots_enqueued.load(Ordering::Relaxed),
            snapshots_flushed: self.snapshots_flushed.load(Ordering::Relaxed),
            snapshots_replay_errors: self.snapshots_replay_errors.load(Ordering::Relaxed),
            snapshots_flush_errors: self.snapshots_flush_errors.load(Ordering::Relaxed),
            flush_skips: self.flush_skips.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
pub(crate) enum FlushErr {
    Transport(anyhow::Error),
    Q { message: String, q_snippet: String },
}

impl std::fmt::Display for FlushErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlushErr::Transport(e) => write!(f, "transport: {e}"),
            FlushErr::Q { message, q_snippet } => {
                write!(f, "q error '{message}' q={q_snippet}")
            }
        }
    }
}

fn flush_err_q(message: impl Into<String>, q: &str) -> FlushErr {
    FlushErr::Q {
        message: message.into(),
        q_snippet: truncate_q_snippet(q),
    }
}

fn level_row_ok(l: &OrderBookLevel) -> bool {
    matches!(l.side, 'B' | 'A') && matches!(l.action, 'N' | 'C' | 'D')
}

pub(crate) fn format_char_column(chars: &[char], kind: CharCol) -> Result<String, FormatSkip> {
    if chars.is_empty() {
        return Err(FormatSkip);
    }
    let ok = |c: char| match kind {
        CharCol::Side => matches!(c, 'B' | 'A'),
        CharCol::Action => matches!(c, 'N' | 'C' | 'D'),
    };
    if chars.iter().any(|&c| !ok(c)) {
        return Err(FormatSkip);
    }
    if chars.len() == 1 {
        Ok(format!("*\"{}\"", chars[0]))
    } else {
        Ok(format!("\"{}\"", chars.iter().collect::<String>()))
    }
}

pub(crate) fn format_levels_insert_q(batch: &[OrderBookLevel]) -> Option<String> {
    let kept: Vec<&OrderBookLevel> = batch.iter().filter(|l| level_row_ok(l)).collect();
    if kept.is_empty() {
        return None;
    }
    let times: Vec<String> = kept.iter().map(|l| format!("`timestamp${}", l.timestamp)).collect();
    let symbols: Vec<String> = kept.iter().map(|l| format!("`{}", l.symbol)).collect();
    let sides: Vec<char> = kept.iter().map(|l| l.side).collect();
    let actions: Vec<char> = kept.iter().map(|l| l.action).collect();
    let prices: Vec<String> = kept.iter().map(|l| fmt_f64(l.price)).collect();
    let sizes: Vec<String> = kept.iter().map(|l| fmt_f64(l.size)).collect();
    let seqs: Vec<String> = kept.iter().map(|l| fmt_seq(l.seq_num)).collect();
    let side_col = format_char_column(&sides, CharCol::Side)
        .expect("kept rows are closed-alphabet; FormatSkip is a bug");
    let action_col = format_char_column(&actions, CharCol::Action)
        .expect("kept rows are closed-alphabet; FormatSkip is a bug");
    let (sides_q, actions_q) = if kept.len() == 1 {
        (format!("({})", side_col), format!("({})", action_col))
    } else {
        (side_col, action_col)
    };
    Some(format!(
        "`orderbook_levels insert (({});({});{};({});({});{};({}))",
        times.join(";"),
        symbols.join(";"),
        sides_q,
        prices.join(";"),
        sizes.join(";"),
        actions_q,
        seqs.join(";")
    ))
}

pub(crate) fn format_trades_insert_q(batch: &[Trade]) -> Option<String> {
    if batch.is_empty() {
        return None;
    }
    let times: Vec<String> = batch.iter().map(|t| format!("`timestamp${}", t.timestamp)).collect();
    let symbols: Vec<String> = batch.iter().map(|t| format!("`{}", t.symbol)).collect();
    let prices: Vec<String> = batch.iter().map(|t| fmt_f64(t.price)).collect();
    let sizes: Vec<String> = batch.iter().map(|t| fmt_f64(t.size)).collect();
    let seqs: Vec<String> = batch.iter().map(|t| fmt_seq(t.seq_num)).collect();
    Some(format!(
        "`trades insert (({});({});({});({});({}))",
        times.join(";"),
        symbols.join(";"),
        prices.join(";"),
        sizes.join(";"),
        seqs.join(";")
    ))
}

pub(crate) fn format_snapshots_insert_q(batch: &[Snapshot]) -> Option<String> {
    if batch.is_empty() {
        return None;
    }
    let times: Vec<String> = batch.iter().map(|s| format!("`timestamp${}", s.timestamp)).collect();
    let symbols: Vec<String> = batch.iter().map(|s| format!("`{}", s.symbol)).collect();
    let best_bids: Vec<String> = batch.iter().map(|s| fmt_f64(s.best_bid)).collect();
    let best_asks: Vec<String> = batch.iter().map(|s| fmt_f64(s.best_ask)).collect();
    let best_bid_sizes: Vec<String> = batch.iter().map(|s| fmt_f64(s.best_bid_size)).collect();
    let best_ask_sizes: Vec<String> = batch.iter().map(|s| fmt_f64(s.best_ask_size)).collect();
    let spreads: Vec<String> = batch.iter().map(|s| fmt_f64(s.spread)).collect();
    let seqs: Vec<String> = batch.iter().map(|s| fmt_seq(s.seq_num)).collect();
    Some(format!(
        "`orderbook_snapshots insert (({});({});({});({});({});({});({});({}))",
        times.join(";"),
        symbols.join(";"),
        best_bids.join(";"),
        best_asks.join(";"),
        best_bid_sizes.join(";"),
        best_ask_sizes.join(";"),
        spreads.join(";"),
        seqs.join(";")
    ))
}

fn format_own_trades_insert_q(batch: &[OwnTrade]) -> Option<String> {
    if batch.is_empty() {
        return None;
    }
    let times: Vec<String> = batch.iter().map(|t| format!("`timestamp${}", t.timestamp)).collect();
    let symbols: Vec<String> = batch.iter().map(|t| format!("`{}", t.symbol)).collect();
    let sides: Vec<String> = batch.iter().map(|t| format!("`{}", t.side)).collect();
    let last_pxs: Vec<String> = batch.iter().map(|t| fmt_f64(t.last_px)).collect();
    let last_qtys: Vec<String> = batch.iter().map(|t| fmt_f64(t.last_qty)).collect();
    let avg_pxs: Vec<String> = batch.iter().map(|t| fmt_f64(t.avg_px)).collect();
    let cl_ord_ids: Vec<String> = batch.iter().map(|t| format!("`{}", t.cl_ord_id)).collect();
    let order_ids: Vec<String> = batch.iter().map(|t| format!("`{}", t.order_id)).collect();
    let exec_types: Vec<String> = batch.iter().map(|t| format!("`{}", t.exec_type)).collect();
    Some(format!(
        "`own_trades insert (({});({});({});({});({});({});({});({});({}))",
        times.join(";"),
        symbols.join(";"),
        sides.join(";"),
        last_pxs.join(";"),
        last_qtys.join(";"),
        avg_pxs.join(";"),
        cl_ord_ids.join(";"),
        order_ids.join(";"),
        exec_types.join(";")
    ))
}

fn format_capture_gaps_insert_q(batch: &[CaptureGap]) -> Option<String> {
    if batch.is_empty() {
        return None;
    }
    let times: Vec<String> = batch.iter().map(|g| format!("`timestamp${}", g.timestamp)).collect();
    let symbols: Vec<String> = batch.iter().map(|g| {
        format!("`$\"{}\"", escape_kdb_string(&g.symbol))
    }).collect();
    let expected: Vec<String> = batch.iter().map(|g| format!("{}j", g.expected_seq)).collect();
    let received: Vec<String> = batch.iter().map(|g| format!("{}j", g.received_seq)).collect();
    let sizes: Vec<String> = batch.iter().map(|g| format!("{}j", g.gap_size)).collect();
    Some(format!(
        "`capture_gaps insert (({});({});({});({});({}))",
        times.join(";"),
        symbols.join(";"),
        expected.join(";"),
        received.join(";"),
        sizes.join(";")
    ))
}

pub(crate) async fn execute_insert_and_check(
    stream: &mut QStream,
    q: &str,
) -> Result<(), FlushErr> {
    match stream.send_sync_message(&q).await {
        Ok(k) if k.get_type() == qtype::ERROR => {
            Err(flush_err_q(k.get_error_string().unwrap_or("unknown").to_string(), q))
        }
        Ok(_) => Ok(()),
        Err(e) => Err(FlushErr::Transport(anyhow!("sync insert failed: {e}"))),
    }
}

pub(crate) async fn execute_single_insert_locked(
    stream: &mut QStream,
    q: &str,
) -> Result<(), FlushErr> {
    stream
        .send_async_message(&q)
        .await
        .map_err(|e| FlushErr::Transport(anyhow!("async insert failed: {e}")))
}

pub(crate) async fn bind_and_drop_connected(
    connected: &tokio::sync::Mutex<bool>,
) -> bool {
    let g = connected.lock().await;
    let v = *g;
    drop(g);
    v
}

async fn requeue_front<T>(buf: &Arc<tokio::sync::Mutex<VecDeque<T>>>, batch: Vec<T>) {
    let mut g = buf.lock().await;
    for item in batch.into_iter().rev() {
        g.push_front(item);
    }
}

fn bump_flush_errors(counters: &MdCounters, kind: MdKind) {
    match kind {
        MdKind::Levels => {
            counters.levels_flush_errors.fetch_add(1, Ordering::Relaxed);
        }
        MdKind::Trades => {
            counters.trades_flush_errors.fetch_add(1, Ordering::Relaxed);
        }
        MdKind::Snapshots => {
            counters.snapshots_flush_errors.fetch_add(1, Ordering::Relaxed);
        }
        MdKind::OwnTrades | MdKind::CaptureGaps => {}
    }
}

async fn apply_flush_result<T: Send>(
    connected: &Arc<tokio::sync::Mutex<bool>>,
    stream: &Arc<Mutex<Option<QStream>>>,
    buf: &Arc<tokio::sync::Mutex<VecDeque<T>>>,
    batch: Vec<T>,
    sent_len: usize,
    kind: MdKind,
    result: Result<(), FlushErr>,
    counters: &MdCounters,
) {
    match result {
        Ok(()) => {
            match kind {
                MdKind::Levels => {
                    counters.levels_flushed.fetch_add(sent_len as u64, Ordering::Relaxed);
                }
                MdKind::Trades => {
                    counters.trades_flushed.fetch_add(sent_len as u64, Ordering::Relaxed);
                }
                MdKind::Snapshots => {
                    counters.snapshots_flushed.fetch_add(sent_len as u64, Ordering::Relaxed);
                }
                MdKind::OwnTrades | MdKind::CaptureGaps => {}
            }
            debug!("flushed kind={:?} sent_len={}", kind, sent_len);
        }
        Err(FlushErr::Transport(e)) => {
            *connected.lock().await = false;
            let _ = stream.lock().await.take();
            requeue_front(buf, batch).await;
            bump_flush_errors(counters, kind);
            counters.sampled_error(
                log_kind_for(kind, false),
                format!("kdb flush transport kind={:?} err={}", kind, e),
            );
        }
        Err(FlushErr::Q { message, q_snippet }) => {
            let batch_len = batch.len();
            drop(batch);
            bump_flush_errors(counters, kind);
            counters.sampled_error(
                log_kind_for(kind, true),
                format!(
                    "kdb flush Q kind={:?} batch_len={} message={} q={}",
                    kind, batch_len, message, q_snippet
                ),
            );
        }
    }
}

struct StreamAbortGuard {
    connected: Arc<tokio::sync::Mutex<bool>>,
    stream: Arc<Mutex<Option<QStream>>>,
    armed: bool,
}

impl StreamAbortGuard {
    fn disarmed(
        connected: &Arc<tokio::sync::Mutex<bool>>,
        stream: &Arc<Mutex<Option<QStream>>>,
    ) -> Self {
        Self {
            connected: Arc::clone(connected),
            stream: Arc::clone(stream),
            armed: false,
        }
    }

    fn arm(&mut self) {
        self.armed = true;
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for StreamAbortGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Ok(mut c) = self.connected.try_lock() {
            *c = false;
        }
        if let Ok(mut s) = self.stream.try_lock() {
            let _ = s.take();
        }
    }
}

async fn timer_flush_sync<T: Send>(
    connected: &Arc<tokio::sync::Mutex<bool>>,
    stream: &Arc<Mutex<Option<QStream>>>,
    buf: &Arc<tokio::sync::Mutex<VecDeque<T>>>,
    format: fn(&[T]) -> Option<String>,
    kind: MdKind,
    counters: &MdCounters,
) {
    let mut abort_guard = StreamAbortGuard::disarmed(connected, stream);
    let mut stream_guard = stream.lock().await;
    let qstream = match stream_guard.as_mut() {
        Some(s) => s,
        None => return,
    };
    let mut buf_g = buf.lock().await;
    if buf_g.is_empty() {
        return;
    }
    let batch: Vec<_> = buf_g.drain(..).collect();
    drop(buf_g);
    let Some(q) = format(&batch) else {
        return;
    };
    let sent_len = batch.len();
    abort_guard.arm();
    let result = execute_insert_and_check(qstream, &q).await;
    abort_guard.disarm();
    drop(stream_guard);
    apply_flush_result(connected, stream, buf, batch, sent_len, kind, result, counters).await;
}

async fn timer_flush_levels(
    connected: &Arc<tokio::sync::Mutex<bool>>,
    stream: &Arc<Mutex<Option<QStream>>>,
    buf: &Arc<tokio::sync::Mutex<VecDeque<OrderBookLevel>>>,
    counters: &MdCounters,
) {
    let mut abort_guard = StreamAbortGuard::disarmed(connected, stream);
    let mut stream_guard = stream.lock().await;
    let qstream = match stream_guard.as_mut() {
        Some(s) => s,
        None => return,
    };
    let mut buf_g = buf.lock().await;
    if buf_g.is_empty() {
        return;
    }
    let batch: Vec<_> = buf_g.drain(..).collect();
    drop(buf_g);
    let skipped = batch.iter().filter(|l| !level_row_ok(l)).count();
    if skipped > 0 {
        counters.levels_flush_errors.fetch_add(skipped as u64, Ordering::Relaxed);
        counters.sampled_error(
            LogKind::ClosedAlphabet,
            format!(
                "kdb closed-alphabet skip kind=levels skipped={} batch_len={}",
                skipped,
                batch.len()
            ),
        );
    }
    let Some(q) = format_levels_insert_q(&batch) else {
        return;
    };
    let sent_len = batch.len() - skipped;
    abort_guard.arm();
    let result = execute_insert_and_check(qstream, &q).await;
    abort_guard.disarm();
    drop(stream_guard);
    apply_flush_result(
        connected,
        stream,
        buf,
        batch,
        sent_len,
        MdKind::Levels,
        result,
        counters,
    )
    .await;
}

async fn timer_flush_async<T: Send>(
    connected: &Arc<tokio::sync::Mutex<bool>>,
    stream: &Arc<Mutex<Option<QStream>>>,
    buf: &Arc<tokio::sync::Mutex<VecDeque<T>>>,
    format: fn(&[T]) -> Option<String>,
    kind: MdKind,
    counters: &MdCounters,
) {
    let mut abort_guard = StreamAbortGuard::disarmed(connected, stream);
    let mut stream_guard = stream.lock().await;
    let qstream = match stream_guard.as_mut() {
        Some(s) => s,
        None => return,
    };
    let mut buf_g = buf.lock().await;
    if buf_g.is_empty() {
        return;
    }
    let batch: Vec<_> = buf_g.drain(..).collect();
    drop(buf_g);
    let Some(q) = format(&batch) else {
        return;
    };
    let sent_len = batch.len();
    abort_guard.arm();
    let result = execute_single_insert_locked(qstream, &q).await;
    abort_guard.disarm();
    drop(stream_guard);
    apply_flush_result(connected, stream, buf, batch, sent_len, kind, result, counters).await;
}

async fn try_flush_write_path<T: Send>(
    connected: &Arc<tokio::sync::Mutex<bool>>,
    stream: &Arc<Mutex<Option<QStream>>>,
    buf: &Arc<tokio::sync::Mutex<VecDeque<T>>>,
    format: fn(&[T]) -> Option<String>,
    kind: MdKind,
    counters: &MdCounters,
    increment_skip_on_disconnected: bool,
) {
    let mut abort_guard = StreamAbortGuard::disarmed(connected, stream);
    let conn_g = match connected.try_lock() {
        Ok(g) => g,
        Err(_) => {
            counters.flush_skips.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    if !*conn_g {
        drop(conn_g);
        if increment_skip_on_disconnected {
            counters.flush_skips.fetch_add(1, Ordering::Relaxed);
        }
        return;
    }
    let mut stream_g = match stream.try_lock() {
        Ok(g) => g,
        Err(_) => {
            counters.flush_skips.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    drop(conn_g);
    let qstream = match stream_g.as_mut() {
        Some(s) => s,
        None => {
            counters.flush_skips.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };
    let mut buf_g = buf.lock().await;
    if buf_g.is_empty() {
        return;
    }
    let batch: Vec<_> = buf_g.drain(..).collect();
    drop(buf_g);
    let Some(q) = format(&batch) else {
        return;
    };
    abort_guard.arm();
    let result = execute_single_insert_locked(qstream, &q).await;
    abort_guard.disarm();
    drop(stream_g);
    let sent_len = batch.len();
    apply_flush_result(connected, stream, buf, batch, sent_len, kind, result, counters).await;
}

#[cfg(test)]
async fn flush_send_sync(
    connected: &Arc<tokio::sync::Mutex<bool>>,
    stream: &Arc<Mutex<Option<QStream>>>,
    q: &str,
) -> Result<(), FlushErr> {
    let mut abort_guard = StreamAbortGuard::disarmed(connected, stream);
    let mut stream_guard = stream.lock().await;
    let qstream = match stream_guard.as_mut() {
        Some(s) => s,
        None => return Err(FlushErr::Transport(anyhow!("stream not available"))),
    };
    abort_guard.arm();
    let result = execute_insert_and_check(qstream, q).await;
    abort_guard.disarm();
    result
}

#[cfg(test)]
async fn flush_send_async(
    connected: &Arc<tokio::sync::Mutex<bool>>,
    stream: &Arc<Mutex<Option<QStream>>>,
    q: &str,
) -> Result<(), FlushErr> {
    let mut abort_guard = StreamAbortGuard::disarmed(connected, stream);
    let mut stream_guard = stream.lock().await;
    let qstream = match stream_guard.as_mut() {
        Some(s) => s,
        None => return Err(FlushErr::Transport(anyhow!("stream not available"))),
    };
    abort_guard.arm();
    let result = execute_single_insert_locked(qstream, q).await;
    abort_guard.disarm();
    result
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
    counters: Arc<MdCounters>,
    fail_next_write: AtomicBool,
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
            counters: Arc::new(MdCounters::new()),
            fail_next_write: AtomicBool::new(false),
        }
    }

    pub fn on_replay_write_err(
        &self,
        kind: &'static str,
        symbol: &str,
        seq: Option<u32>,
        err: &anyhow::Error,
    ) {
        let log_kind = match kind {
            "level" => {
                self.counters.levels_replay_errors.fetch_add(1, Ordering::Relaxed);
                LogKind::ReplayLevel
            }
            "trade" => {
                self.counters.trades_replay_errors.fetch_add(1, Ordering::Relaxed);
                LogKind::ReplayTrade
            }
            "snapshot" => {
                self.counters.snapshots_replay_errors.fetch_add(1, Ordering::Relaxed);
                LogKind::ReplaySnapshot
            }
            _ => LogKind::ReplayEnqueue,
        };
        self.counters.sampled_error(
            log_kind,
            format!(
                "kdb replay write error symbol={} kind={} seq={:?} err={}",
                symbol, kind, seq, err
            ),
        );
    }

    pub fn sampled_replay_enqueue(&self, symbol: &str, n_level: u64, n_trade: u64, n_snap: u64) {
        self.counters.sampled_info(
            LogKind::ReplayEnqueue,
            format!(
                "kdb replay enqueue symbol={} levels={} trades={} snapshots={}",
                symbol, n_level, n_trade, n_snap
            ),
        );
    }

    /// Next `write_order_book_level` / `write_trade` / `write_snapshot` returns
    /// `Err(anyhow!("injected write error"))` without enqueue, then clears the flag.
    pub fn inject_next_write_err(&self) {
        self.fail_next_write.store(true, Ordering::SeqCst);
    }

    pub async fn level_buffer_len(&self) -> usize {
        self.level_buffer.lock().await.len()
    }

    pub async fn trade_buffer_len(&self) -> usize {
        self.trade_buffer.lock().await.len()
    }

    pub async fn snapshot_buffer_len(&self) -> usize {
        self.snapshot_buffer.lock().await.len()
    }

    pub fn md_stats(&self) -> MdStats {
        self.counters.snapshot()
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

    /// Start background task to periodically flush buffers.
    /// Snapshots/own_trades/gaps async first, then levels/trades sync.
    fn start_flush_task(&self) {
        let level_buf = Arc::clone(&self.level_buffer);
        let trade_buf = Arc::clone(&self.trade_buffer);
        let snapshot_buf = Arc::clone(&self.snapshot_buffer);
        let own_trade_buf = Arc::clone(&self.own_trade_buffer);
        let gap_buf = Arc::clone(&self.capture_gap_buffer);
        let stream = Arc::clone(&self.stream);
        let connected = Arc::clone(&self.connected);
        let counters = Arc::clone(&self.counters);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(FLUSH_INTERVAL_MS));
            loop {
                interval.tick().await;
                let is_conn = bind_and_drop_connected(connected.as_ref()).await;
                if !is_conn {
                    counters.flush_skips.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                timer_flush_async(
                    &connected,
                    &stream,
                    &snapshot_buf,
                    format_snapshots_insert_q,
                    MdKind::Snapshots,
                    counters.as_ref(),
                )
                .await;
                timer_flush_async(
                    &connected,
                    &stream,
                    &own_trade_buf,
                    format_own_trades_insert_q,
                    MdKind::OwnTrades,
                    counters.as_ref(),
                )
                .await;
                timer_flush_async(
                    &connected,
                    &stream,
                    &gap_buf,
                    format_capture_gaps_insert_q,
                    MdKind::CaptureGaps,
                    counters.as_ref(),
                )
                .await;
                timer_flush_levels(&connected, &stream, &level_buf, counters.as_ref()).await;
                timer_flush_sync(
                    &connected,
                    &stream,
                    &trade_buf,
                    format_trades_insert_q,
                    MdKind::Trades,
                    counters.as_ref(),
                )
                .await;
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
                let is_conn = bind_and_drop_connected(connected.as_ref()).await;
                if is_conn {
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

    /// Execute a q expression via IPC using kxkdb (sync for initialization)
    #[allow(dead_code)]
    async fn execute_q(&self, q_code: &str) -> Result<()> {
        if !*self.connected.lock().await {
            return Err(anyhow!("Not connected to kdb+"));
        }
        Self::execute_q_on_stream(&self.stream, q_code).await
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

        if buffer_len >= self.batch_size {
            try_flush_write_path(
                &self.connected,
                &self.stream,
                &self.capture_gap_buffer,
                format_capture_gaps_insert_q,
                MdKind::CaptureGaps,
                self.counters.as_ref(),
                false,
            )
            .await;
        }
    }

    /// Write order book level update to kdb+ (buffered). Always enqueues even when
    /// disconnected. Drain is timer-only (sync) so `'type` is observable.
    pub async fn write_order_book_level(
        &self,
        symbol: &str,
        side: char,
        price: f64,
        size: f64,
        action: char,
        seq_num: Option<u32>,
    ) -> Result<()> {
        if self.fail_next_write.swap(false, Ordering::SeqCst) {
            return Err(anyhow!("injected write error"));
        }
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
        self.counters.levels_enqueued.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Write trade to kdb+ (buffered). Drain is timer-only (sync).
    pub async fn write_trade(
        &self,
        symbol: &str,
        price: f64,
        size: f64,
        timestamp: i64,
        seq_num: Option<u32>,
    ) -> Result<()> {
        if self.fail_next_write.swap(false, Ordering::SeqCst) {
            return Err(anyhow!("injected write error"));
        }
        let trade = Trade {
            timestamp: timestamp - KDB_TIMESTAMP_OFFSET_NS,
            symbol: symbol.to_string(),
            price,
            size,
            seq_num,
        };

        {
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
            } else {
                buffer.push_back(trade);
            }
        }

        self.counters.trades_enqueued.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Write best bid/ask snapshot to kdb+ (buffered, batched). Threshold flush is
    /// async + try_lock; contention skip is success.
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
        if self.fail_next_write.swap(false, Ordering::SeqCst) {
            return Err(anyhow!("injected write error"));
        }
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

        self.counters.snapshots_enqueued.fetch_add(1, Ordering::Relaxed);

        if buffer_len >= self.batch_size {
            try_flush_write_path(
                &self.connected,
                &self.stream,
                &self.snapshot_buffer,
                format_snapshots_insert_q,
                MdKind::Snapshots,
                self.counters.as_ref(),
                true,
            )
            .await;
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

    /// Write an own execution fill to kdb+ (buffered).
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
        let connected = match self.connected.try_lock() {
            Ok(g) => *g,
            Err(_) => return Ok(()),
        };
        if !connected {
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

        if buffer_len >= self.batch_size {
            try_flush_write_path(
                &self.connected,
                &self.stream,
                &self.own_trade_buffer,
                format_own_trades_insert_q,
                MdKind::OwnTrades,
                self.counters.as_ref(),
                true,
            )
            .await;
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

            let mut stream_guard = self.stream.lock().await;
            match stream_guard.as_mut() {
                Some(qs) => {
                    if execute_single_insert_locked(qs, q_code.as_str()).await.is_ok() {
                        success_count += 1;
                    } else {
                        warn!("❌ Failed to insert security: {}", symbol);
                    }
                }
                None => warn!("❌ Failed to insert security: {}", symbol),
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

    pub fn record_probe(&self, ev: ProbeEvent) {
        self.probe.lock().unwrap().push(ev.clone());
        log::info!("PROBE {:?}", ev);
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

        let levels: Vec<_> = self.level_buffer.lock().await.drain(..).collect();
        let trades: Vec<_> = self.trade_buffer.lock().await.drain(..).collect();
        let snapshots: Vec<_> = self.snapshot_buffer.lock().await.drain(..).collect();
        let own_trades: Vec<_> = self.own_trade_buffer.lock().await.drain(..).collect();
        let gaps: Vec<_> = self.capture_gap_buffer.lock().await.drain(..).collect();

        {
            let mut stream_guard = self.stream.lock().await;
            if let Some(qs) = stream_guard.as_mut() {
                if let Some(q) = format_levels_insert_q(&levels) {
                    let _ = execute_single_insert_locked(qs, &q).await;
                }
                if let Some(q) = format_trades_insert_q(&trades) {
                    let _ = execute_single_insert_locked(qs, &q).await;
                }
                if let Some(q) = format_snapshots_insert_q(&snapshots) {
                    let _ = execute_single_insert_locked(qs, &q).await;
                }
                if let Some(q) = format_own_trades_insert_q(&own_trades) {
                    let _ = execute_single_insert_locked(qs, &q).await;
                }
                if let Some(q) = format_capture_gaps_insert_q(&gaps) {
                    let _ = execute_single_insert_locked(qs, &q).await;
                }
            }
            if let Some(stream) = stream_guard.take() {
                if let Err(e) = stream.shutdown().await {
                    warn!("Error shutting down kdb+ connection: {}", e);
                }
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

    pub(crate) async fn connected_for_test(&self) -> bool {
        *self.connected.lock().await
    }

    pub(crate) async fn set_connected_for_test(&self, v: bool) {
        *self.connected.lock().await = v;
    }

    pub(crate) async fn stream_is_some_for_test(&self) -> bool {
        self.stream.lock().await.is_some()
    }

    pub(crate) async fn install_stream_for_test(&self, s: QStream) {
        *self.stream.lock().await = Some(s);
    }

    pub(crate) async fn apply_flush_result_levels_for_test(
        &self,
        batch: Vec<OrderBookLevel>,
        result: Result<(), FlushErr>,
    ) {
        let sent_len = batch.len();
        apply_flush_result(
            &self.connected,
            &self.stream,
            &self.level_buffer,
            batch,
            sent_len,
            MdKind::Levels,
            result,
            self.counters.as_ref(),
        )
        .await;
    }

    pub(crate) async fn send_sync_with_abort_guard_for_test(&self, q: &str) -> Result<(), FlushErr> {
        flush_send_sync(&self.connected, &self.stream, q).await
    }

    pub(crate) async fn send_async_with_abort_guard_for_test(&self, q: &str) -> Result<(), FlushErr> {
        flush_send_async(&self.connected, &self.stream, q).await
    }
}

#[cfg(test)]
async fn spawn_kdb_handshake_then_hang() -> u16 {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let Ok((mut sock, _)) = listener.accept().await else {
            return;
        };
        let mut buf = [0u8; 2];
        if sock.read_exact(&mut buf).await.is_err() {
            return;
        }
        let _ = sock.write_all(&[0x03]).await;
        std::future::pending::<()>().await;
    });
    port
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn sample_level(symbol: &str, side: char, action: char) -> OrderBookLevel {
        OrderBookLevel {
            timestamp: 1,
            symbol: symbol.to_string(),
            side,
            price: 1.0,
            size: 1.0,
            action,
            seq_num: Some(7),
        }
    }

    fn sample_trade(symbol: &str) -> Trade {
        Trade {
            timestamp: 1,
            symbol: symbol.to_string(),
            price: 1.0,
            size: 1.0,
            seq_num: Some(7),
        }
    }

    fn sample_snapshot(symbol: &str, n: f64) -> Snapshot {
        Snapshot {
            timestamp: 1,
            symbol: symbol.to_string(),
            best_bid: n,
            best_ask: n + 1.0,
            best_bid_size: 3.0,
            best_ask_size: 4.0,
            spread: 1.0,
            seq_num: Some(7),
        }
    }

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

    // --- PR-A / PR-B ---

    #[tokio::test]
    async fn storm_gate_q_err_does_not_drop_latch_or_requeue() {
        let w = KdbWriter::new("127.0.0.1".into(), 1);
        w.set_connected_for_test(true).await;
        w.write_order_book_level("BTC_USDT", 'B', 1.0, 1.0, 'N', Some(7)).await.unwrap();
        let batch: Vec<_> = w.level_buffer.lock().await.drain(..).collect();
        w.apply_flush_result_levels_for_test(
            batch,
            Err(FlushErr::Q {
                message: "type".into(),
                q_snippet: "`orderbook_levels insert ...".into(),
            }),
        )
        .await;
        assert!(w.connected_for_test().await);
        assert!(!w.stream_is_some_for_test().await);
        assert_eq!(w.level_buffer_len().await, 0);
        assert!(w.md_stats().levels_flush_errors >= 1);
        assert_eq!(w.md_stats().levels_flushed, 0);
    }

    #[tokio::test]
    async fn transport_err_drops_latch_and_requeues() {
        let w = KdbWriter::new("127.0.0.1".into(), 1);
        w.set_connected_for_test(true).await;
        w.write_order_book_level("BTC_USDT", 'B', 1.0, 1.0, 'N', Some(7)).await.unwrap();
        let batch: Vec<_> = w.level_buffer.lock().await.drain(..).collect();
        w.apply_flush_result_levels_for_test(
            batch,
            Err(FlushErr::Transport(anyhow!("tcp reset"))),
        )
        .await;
        assert!(!w.connected_for_test().await);
        assert_eq!(w.level_buffer_len().await, 1);
        assert!(w.md_stats().levels_flush_errors >= 1);
        assert_eq!(w.md_stats().levels_flushed, 0);
    }

    #[tokio::test]
    async fn write_path_try_lock_stream_held_does_not_await_buffer_stays() {
        tokio::time::timeout(Duration::from_secs(2), async {
            let w = KdbWriter::new("127.0.0.1".into(), 1);
            w.set_connected_for_test(true).await;
            let _hold = w.stream.lock().await;
            for _ in 0..100 {
                w.write_snapshot("BTC_USDT", 1.0, 2.0, 3.0, 4.0, 1.0, Some(1))
                    .await
                    .unwrap();
            }
            assert_eq!(w.snapshot_buffer_len().await, 100);
            assert!(w.md_stats().flush_skips >= 1);
        })
        .await
        .expect("try_lock skip must not await the held stream");
    }

    #[tokio::test]
    async fn write_path_try_lock_connected_held_does_not_await_buffer_stays() {
        tokio::time::timeout(Duration::from_secs(2), async {
            let w = KdbWriter::new("127.0.0.1".into(), 1);
            w.set_connected_for_test(true).await;
            let _hold = w.connected.lock().await;
            for _ in 0..100 {
                w.write_snapshot("BTC_USDT", 1.0, 2.0, 3.0, 4.0, 1.0, Some(1))
                    .await
                    .unwrap();
            }
            assert_eq!(w.snapshot_buffer_len().await, 100);
            assert!(w.md_stats().flush_skips >= 1);
        })
        .await
        .expect("try_lock skip must not await the held connected mutex");
    }

    #[tokio::test]
    async fn cancelled_send_does_not_reuse_stream_with_latch_up() {
        let port = spawn_kdb_handshake_then_hang().await;
        let qs = QStream::connect(ConnectionMethod::TCP, "127.0.0.1", port, "")
            .await
            .expect("handshake stub must complete QStream::connect");
        let w = Arc::new(KdbWriter::new("127.0.0.1".into(), port));
        w.install_stream_for_test(qs).await;
        w.set_connected_for_test(true).await;
        let w2 = Arc::clone(&w);
        let handle = tokio::spawn(async move {
            w2.send_sync_with_abort_guard_for_test("`1").await
        });
        tokio::time::sleep(Duration::from_millis(80)).await;
        handle.abort();
        let _ = handle.await;
        assert!(!w.connected_for_test().await, "armed abort must drop the latch");
        assert!(
            !w.stream_is_some_for_test().await,
            "armed abort must take the stream"
        );
    }

    #[tokio::test]
    async fn write_path_cancelled_send_does_not_reuse_stream_with_latch_up() {
        let port = spawn_kdb_handshake_then_hang().await;
        let qs = QStream::connect(ConnectionMethod::TCP, "127.0.0.1", port, "")
            .await
            .expect("handshake stub must complete QStream::connect");
        let w = Arc::new(KdbWriter::new("127.0.0.1".into(), port));
        w.install_stream_for_test(qs).await;
        w.set_connected_for_test(true).await;
        let w2 = Arc::clone(&w);
        // send_async is write-only; a small payload completes before abort.
        // Fill the TCP window so write_all stays armed while the peer never reads.
        let huge = "a".repeat(8 * 1024 * 1024);
        let handle = tokio::spawn(async move {
            w2.send_async_with_abort_guard_for_test(&huge).await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        handle.abort();
        let _ = handle.await;
        assert!(!w.connected_for_test().await);
        assert!(!w.stream_is_some_for_test().await);
    }

    #[tokio::test]
    async fn timer_connected_bind_and_drop_releases_lock() {
        let m = tokio::sync::Mutex::new(true);
        assert!(bind_and_drop_connected(&m).await);
        assert!(m.try_lock().is_ok());
    }

    #[tokio::test]
    async fn reconnect_connected_bind_and_drop_releases_lock() {
        // start_reconnect_task uses bind_and_drop_connected and must not hold
        // the guard across sleep. Same helper as the timer path.
        let m = tokio::sync::Mutex::new(false);
        assert!(!bind_and_drop_connected(&m).await);
        assert!(m.try_lock().is_ok());
    }

    #[tokio::test]
    async fn write_snapshot_enqueues_when_disconnected() {
        let w = KdbWriter::new("127.0.0.1".into(), 1);
        w.write_snapshot("BTC_USDT", 1.0, 2.0, 3.0, 4.0, 1.0, Some(7))
            .await
            .unwrap();
        assert!(w.drain_probe().is_empty(), "probe is recorded at replay, not write_*");
        assert_eq!(w.snapshot_buffer_len().await, 1);
    }

    #[tokio::test]
    async fn write_path_levels_threshold_does_not_drain() {
        let w = KdbWriter::new("127.0.0.1".into(), 1);
        w.set_connected_for_test(true).await;
        for _ in 0..100 {
            w.write_order_book_level("BTC_USDT", 'B', 1.0, 1.0, 'N', Some(1))
                .await
                .unwrap();
        }
        for _ in 0..100 {
            w.write_trade("BTC_USDT", 1.0, 1.0, 0, Some(1)).await.unwrap();
        }
        assert_eq!(w.level_buffer_len().await, 100);
        assert_eq!(w.trade_buffer_len().await, 100);
        assert_eq!(w.md_stats().levels_flushed, 0);
        assert_eq!(w.md_stats().trades_flushed, 0);
        assert_eq!(w.md_stats().flush_skips, 0);
    }

    #[test]
    fn truncate_q_snippet_201_byte_ascii() {
        let q = "a".repeat(201);
        let t = truncate_q_snippet(&q);
        assert_eq!(t.len(), 200);
        assert!(t.ends_with("..."));
    }

    #[test]
    fn format_levels_insert_q_n1_emits_char_atom_star_quote() {
        let q = format_levels_insert_q(&[sample_level("ETH_USDT", 'B', 'N')]).unwrap();
        assert!(q.contains("*\""), "n=1 must emit char atom");
        assert!(q.contains("*\"B\""), "got {q}");
        assert!(q.contains("*\"N\""), "got {q}");
        assert!(!q.contains("`B"), "must not emit kdb symbol for side, got {q}");
        assert!(!q.contains("(\"B\")"), "must not emit 10h char without star, got {q}");
    }

    #[test]
    fn format_levels_insert_q_n2_emits_char_vector() {
        let q = format_levels_insert_q(&[
            sample_level("S1", 'B', 'N'),
            sample_level("S2", 'A', 'C'),
        ])
        .unwrap();
        assert!(q.contains("\"BA\""), "got {q}");
        assert!(q.contains("\"NC\""), "got {q}");
        assert!(!q.contains("`B"), "got {q}");
        assert!(!q.contains("*\"B\""), "got {q}");
    }

    #[test]
    fn format_trades_insert_q_n1_type_aligned() {
        let q = format_trades_insert_q(&[sample_trade("ETH_USDT")]).unwrap();
        assert!(q.starts_with("`trades insert "));
        assert_eq!(q.matches('(').count(), 6, "five parenthesized columns plus outer, got {q}");
        assert!(!q.contains("*\""), "trades have no char columns, got {q}");
    }

    #[test]
    fn format_levels_insert_q_closed_alphabet_skips_bad_row() {
        let q = format_levels_insert_q(&[
            sample_level("ETH_USDT", 'X', 'N'),
            sample_level("ETH_USDT", 'B', 'N'),
        ]);
        let q = q.expect("mixed batch with a valid sibling must still format");
        assert!(q.contains("*\"B\""), "got {q}");
        assert!(!q.contains('X'), "must not interpolate unexpected side, got {q}");
    }

    #[test]
    fn format_snapshots_insert_q_still_valid_insert() {
        let q1 = format_snapshots_insert_q(&[sample_snapshot("ETH_USDT", 1.0)]).unwrap();
        assert!(q1.starts_with("`orderbook_snapshots insert "));
        assert_eq!(q1.matches('(').count(), 9, "8 parenthesized columns plus outer, got {q1}");
        let q2 = format_snapshots_insert_q(&[
            sample_snapshot("ETH_USDT", 1.0),
            sample_snapshot("ETH_USDT", 2.0),
        ])
        .unwrap();
        assert!(q2.starts_with("`orderbook_snapshots insert "));
        assert_eq!(q2.matches('(').count(), 9, "got {q2}");
    }
}
