/// Trading Algorithm Module
///
/// Provides a configurable trading algorithm skeleton that:
///   1. Polls the live order book every `poll_interval_ms` milliseconds
///   2. Evaluates a signal function against the current market state
///   3. Places orders via the FIX order session when the signal fires
///   4. Enforces position limits so the strategy never exceeds `max_position`
///
/// The default signal is a simple spread-capture / passive-quote strategy:
///   - If spread >= `min_spread_bps` basis points, place a passive bid one
///     tick inside the best ask (and a passive offer one tick inside the
///     best bid) to capture the spread.
///   - All orders are limit orders at IOC (configurable) so they do not
///     accumulate in the book indefinitely.
///
/// To implement your own strategy, replace the body of `evaluate_signal`.

use std::sync::Arc;
use tokio::sync::Mutex;
use log::{warn, debug};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use chrono::Utc;
use std::io::Write;
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::OnceLock;

/// Selects which signal-generation strategy the algo uses.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum StrategyType {
    /// Passive spread-capture: place limit orders one tick inside the spread.
    SpreadCapture,
    /// Market imbalance: cross the spread aggressively when order-book volume
    /// is skewed beyond `imbalance_threshold` on one side.
    MarketImbalance,
}

impl Default for StrategyType {
    fn default() -> Self {
        StrategyType::SpreadCapture
    }
}

/// Configuration for the trading algorithm (hot-swappable at runtime).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlgoConfig {
    /// Whether the algo is actively trading
    pub enabled: bool,
    /// Symbol to trade (e.g. "BTC_USDT")
    pub symbol: String,
    /// Order size in base currency units
    pub order_qty: f64,
    /// Minimum spread required before placing orders (in basis points)
    pub min_spread_bps: f64,
    /// Maximum absolute net position before suppressing new entries
    pub max_position: f64,
    /// Polling interval in milliseconds
    pub poll_interval_ms: u64,
    /// Tick size used for price improvement (price step)
    pub tick_size: f64,
    /// Time-in-force: "0"=Day, "3"=IOC, "4"=FOK
    pub time_in_force: String,
    /// Which strategy generates signals
    #[serde(default)]
    pub strategy: StrategyType,
    /// Fraction of total book volume that must be on one side to trigger an
    /// imbalance signal (range 0.5–1.0; default 0.6 means 60%).
    #[serde(default = "default_imbalance_threshold")]
    pub imbalance_threshold: f64,
    /// Number of price levels to sum when computing book imbalance.
    #[serde(default = "default_imbalance_depth_levels")]
    pub imbalance_depth_levels: usize,
    /// Estimated market impact per side in basis points (applied to fill price in backtest).
    #[serde(default = "default_slippage_bps")]
    pub slippage_bps: f64,
    /// Taker fee in basis points (Crypto.com default ~4 bps = 0.04%).
    #[serde(default = "default_fee_bps")]
    pub fee_bps: f64,
    /// Maker fee in bps — NEGATIVE means a rebate. Crypto.com MM Program pays a
    /// maker rebate, so passive (SpreadCapture) fills should use this. Set to your
    /// actual program rate via /api/algo/config.
    #[serde(default = "default_maker_fee_bps")]
    pub maker_fee_bps: f64,
    /// Taker fee in bps (positive cost) for liquidity-removing (MarketImbalance) fills.
    #[serde(default = "default_taker_fee_bps")]
    pub taker_fee_bps: f64,
    /// Drop snapshots whose quoted spread exceeds this fraction of best_bid (default 5%).
    #[serde(default = "default_max_spread_frac")]
    pub max_spread_frac: f64,
    /// Drop snapshots whose mid jumps by more than this fraction vs the previous kept mid (default 10%).
    #[serde(default = "default_max_mid_jump_frac")]
    pub max_mid_jump_frac: f64,
    /// Minimum milliseconds between consecutive signals — prevents order-quota burn
    /// and reduces adverse selection on stale book updates.
    #[serde(default = "default_min_signal_interval_ms")]
    pub min_signal_interval_ms: u64,
    /// Suppress entries if the order book has not updated within this many ms
    /// (guards against quoting on a frozen/stale feed). 0 = disabled.
    #[serde(default = "default_max_book_staleness_ms")]
    pub max_book_staleness_ms: u64,
    /// Hard cap on orders placed per process run (circuit breaker). 0 = unlimited.
    #[serde(default = "default_max_orders_session")]
    pub max_orders_session: u64,
    /// Tag 12362 SelfMatchPreventionScope. Comments on OrderRequest list M, S.
    /// None (default / omitted JSON) → encoder omits the tag. Not validated here.
    #[serde(default)]
    pub self_match_prevention_scope: Option<String>,
    /// Tag 2964 SelfMatchPreventionInstruction. Comments list 1, 2, 3.
    #[serde(default)]
    pub self_match_prevention_instruction: Option<String>,
    /// Tag 2362 SelfMatchPreventionID. Static per-strategy group id.
    /// Comments list 0-32767. Not generated per order. Not validated here.
    #[serde(default)]
    pub self_match_prevention_id: Option<String>,
}

pub fn default_imbalance_threshold() -> f64 { 0.6 }
pub fn default_imbalance_depth_levels() -> usize { 5 }
pub fn default_slippage_bps() -> f64 { 1.0 }
pub fn default_fee_bps() -> f64 { 4.0 }
pub fn default_maker_fee_bps() -> f64 { -1.0 }  // placeholder MM-program rebate; set real rate
pub fn default_taker_fee_bps() -> f64 { 4.0 }
pub fn default_max_spread_frac() -> f64 { 0.05 }
pub fn default_max_mid_jump_frac() -> f64 { 0.10 }
pub fn default_min_signal_interval_ms() -> u64 { 500 }
pub fn default_max_book_staleness_ms() -> u64 { 2000 }
/// Orders per process run before the circuit breaker halts placement.
///
/// This defaulted to 0, and 0 means UNLIMITED -- so the circuit breaker was
/// off unless someone remembered to set it, on both UAT and production. A
/// safety limit that defaults to absent is not a safety limit.
///
/// 100 is a floor, not a recommendation: enough to exercise the order path,
/// small enough that a runaway loop stops. Set it deliberately per deployment.
pub fn default_max_orders_session() -> u64 { 100 }

impl Default for AlgoConfig {
    fn default() -> Self {
        AlgoConfig {
            enabled: false,
            symbol: "BTC_USDT".to_string(),
            order_qty: 0.001,
            min_spread_bps: 5.0,
            max_position: 0.01,
            poll_interval_ms: 1000,
            tick_size: 0.01,
            time_in_force: "3".to_string(), // IOC by default — safe for testing
            strategy: StrategyType::default(),
            imbalance_threshold: default_imbalance_threshold(),
            imbalance_depth_levels: default_imbalance_depth_levels(),
            slippage_bps: default_slippage_bps(),
            fee_bps: default_fee_bps(),
            maker_fee_bps: default_maker_fee_bps(),
            taker_fee_bps: default_taker_fee_bps(),
            max_spread_frac: default_max_spread_frac(),
            max_mid_jump_frac: default_max_mid_jump_frac(),
            min_signal_interval_ms: default_min_signal_interval_ms(),
            max_book_staleness_ms: default_max_book_staleness_ms(),
            max_orders_session: default_max_orders_session(),
            self_match_prevention_scope: None,
            self_match_prevention_instruction: None,
            self_match_prevention_id: None,
        }
    }
}

/// A single signal decision produced by `evaluate_signal`.
#[derive(Debug, Clone, Serialize)]
pub struct Signal {
    pub timestamp_ms: i64,
    pub symbol: String,
    /// "1" = buy, "2" = sell
    pub side: String,
    pub price: f64,
    pub qty: f64,
    pub reason: String,
}

/// Ring buffer of recent signals for the status API.
#[derive(Debug)]
pub struct SignalLog {
    entries: VecDeque<Signal>,
    capacity: usize,
}

impl SignalLog {
    pub fn new(capacity: usize) -> Self {
        SignalLog {
            entries: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    pub fn push(&mut self, sig: Signal) {
        if self.entries.len() >= self.capacity {
            self.entries.pop_front();
        }
        self.entries.push_back(sig);
    }

    pub fn recent(&self, n: usize) -> Vec<Signal> {
        self.entries.iter().rev().take(n).cloned().collect()
    }
}

/// Algo state exposed by the status API.
#[derive(Debug, Clone, Serialize)]
pub struct AlgoStatus {
    pub config: AlgoConfig,
    pub signals_fired: u64,
    pub orders_placed: u64,
    pub recent_signals: Vec<Signal>,
    pub paper_mode: bool,
    pub paper_fills: u64,
    pub paper_position: f64,
    pub paper_realized_pnl: f64,
    pub paper_total_pnl: f64,
}

pub const SHADOW_FILLS_PATH: &str = "data/shadow_fills.jsonl";

/// One simulated paper fill. Serde field order is the JSONL key order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShadowFill {
    pub ts_ms: i64,
    pub strategy: StrategyType,
    pub symbol: String,
    pub side: String,
    pub price: f64,
    pub qty: f64,
    pub pnl: f64,
    pub cumulative_pnl: f64,
    pub net_qty_after: f64,
}

/// HTTP 200 body for `GET /api/shadow/status`. Both keys always present.
#[derive(Debug, Clone, Serialize)]
pub struct ShadowStatus {
    pub spread_capture: AlgoStatus,
    pub market_imbalance: AlgoStatus,
}

/// Shadow-trading (paper) state: simulated inventory and P&L from passive quotes
/// filled against the LIVE book. Never sends a real order. Mirrors the backtest
/// fill model (fill only on a real cross; mark at the new mid => adverse selection).
#[derive(Default)]
pub(crate) struct PaperState {
    net_qty: f64,
    avg_entry: f64,
    realized: f64,
    fills: u64,
    quote_buy: Option<f64>,
    quote_sell: Option<f64>,
    last_mid: f64,
}

impl PaperState {
    /// Unchanged inventory math. Returns None when qty after room cap <= 1e-12.
    /// Some((qty, pnl_increment, net_qty_after)) when a fill is booked.
    /// pnl_increment is 0.0 on a pure opening leg.
    fn apply_fill(&mut self, side: &str, fill: f64, cfg: &AlgoConfig) -> Option<(f64, f64, f64)> {
        let room = if side == "1" {
            (cfg.max_position - self.net_qty).max(0.0)
        } else {
            (cfg.max_position + self.net_qty).max(0.0)
        };
        let qty = cfg.order_qty.min(room);
        if qty <= 1e-12 {
            return None;
        }
        let signed = if side == "1" { qty } else { -qty };
        let prev = self.net_qty;
        let new_net = prev + signed;
        let mut pnl_increment = 0.0;
        if prev.abs() < 1e-12 {
            self.avg_entry = fill;
        } else if prev.signum() == signed.signum() {
            self.avg_entry = (self.avg_entry * prev.abs() + fill * qty) / (prev.abs() + qty);
        } else {
            let closing = qty.min(prev.abs());
            let pnl = if prev > 0.0 {
                closing * (fill - self.avg_entry)
            } else {
                closing * (self.avg_entry - fill)
            };
            self.realized += pnl;
            pnl_increment = pnl;
            if new_net.abs() < 1e-12 {
                self.avg_entry = 0.0;
            } else if new_net.signum() != prev.signum() {
                self.avg_entry = fill;
            }
        }
        self.net_qty = new_net;
        self.fills += 1;
        Some((qty, pnl_increment, new_net))
    }

    fn total_pnl(&self) -> f64 {
        self.realized + self.net_qty * (self.last_mid - self.avg_entry)
    }
}

/// The trading algorithm.
pub struct TradingAlgo {
    pub config: Arc<Mutex<AlgoConfig>>,
    pub signals_fired: Arc<Mutex<u64>>,
    pub orders_placed: Arc<Mutex<u64>>,
    pub signal_log: Arc<Mutex<SignalLog>>,
    /// Shadow/paper mode. Default TRUE; while true the algo NEVER sends a real
    /// order — it only simulates fills against the live book. (No toggle is
    /// exposed yet, so the live-order path is currently unreachable by design.)
    ///
    /// The field is `pub` so binary tests can *read* it after the lib split.
    /// There is no setter and no `AlgoConfig` field: callers MUST NOT `.store()`
    /// this AtomicBool. That rule is convention/review, not the type system.
    pub paper_mode: Arc<std::sync::atomic::AtomicBool>,
    paper: Arc<Mutex<PaperState>>,
}

impl TradingAlgo {
    pub fn new(config: AlgoConfig) -> Self {
        TradingAlgo {
            config: Arc::new(Mutex::new(config)),
            signals_fired: Arc::new(Mutex::new(0)),
            orders_placed: Arc::new(Mutex::new(0)),
            signal_log: Arc::new(Mutex::new(SignalLog::new(50))),
            paper_mode: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            paper: Arc::new(Mutex::new(PaperState::default())),
        }
    }

    /// Return a snapshot of current algo status.
    pub async fn status(&self) -> AlgoStatus {
        let p = self.paper.lock().await;
        AlgoStatus {
            config: self.config.lock().await.clone(),
            signals_fired: *self.signals_fired.lock().await,
            orders_placed: *self.orders_placed.lock().await,
            recent_signals: self.signal_log.lock().await.recent(20),
            paper_mode: self.paper_mode.load(std::sync::atomic::Ordering::Relaxed),
            paper_fills: p.fills,
            paper_position: p.net_qty,
            paper_realized_pnl: p.realized,
            paper_total_pnl: p.total_pnl(),
        }
    }

    /// One shadow-trading step. Locks paper, runs `paper_step_inner`, then
    /// appends fills without holding the paper mutex.
    pub async fn paper_step(
        &self,
        best_bid: f64,
        best_ask: f64,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        cfg: &AlgoConfig,
    ) {
        let now_ms = Utc::now().timestamp_millis();
        let fills = {
            let mut paper = self.paper.lock().await;
            paper_step_inner(&mut paper, cfg, best_bid, best_ask, bids, asks, now_ms)
        };
        for fill in fills {
            append_shadow_fill(&fill);
        }
    }

    /// Hot-swap the algo configuration without restarting the task.
    pub async fn update_config(&self, new_cfg: AlgoConfig) {
        *self.config.lock().await = new_cfg;
    }

    /// Evaluate signal given current market state.
    ///
    /// `bids` and `asks` are the top-N depth levels from the live order book
    /// snapshot, sorted price-descending and price-ascending respectively.
    /// Pass empty slices when depth is unavailable (e.g. in backtests) — the
    /// MarketImbalance strategy will return no signals in that case.
    pub fn evaluate_signal(
        cfg: &AlgoConfig,
        best_bid: f64,
        best_ask: f64,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        net_position: f64,
    ) -> Vec<Signal> {
        if best_bid <= 0.0 || best_ask <= 0.0 || best_ask <= best_bid {
            return vec![];
        }

        let ts = Utc::now().timestamp_millis();

        match cfg.strategy {
            StrategyType::SpreadCapture => {
                let spread = best_ask - best_bid;
                let spread_bps = (spread / best_bid) * 10_000.0;
                if spread_bps < cfg.min_spread_bps {
                    debug!("Spread {:.2} bps < min {:.2} bps — no signal", spread_bps, cfg.min_spread_bps);
                    return vec![];
                }
                Self::build_spread_capture_signals(cfg, best_bid, best_ask, net_position, spread_bps, ts)
            }
            StrategyType::MarketImbalance => {
                Self::build_imbalance_signals(cfg, best_bid, best_ask, bids, asks, net_position, ts)
            }
        }
    }

    /// Spread-capture signal builder (shared between live and backtest paths).
    pub fn build_spread_capture_signals(
        cfg: &AlgoConfig,
        best_bid: f64,
        best_ask: f64,
        net_position: f64,
        spread_bps: f64,
        timestamp_ms: i64,
    ) -> Vec<Signal> {
        let mut signals = Vec::new();

        // Keep quotes one tick inside the spread. If we cannot quote passively, skip.
        let buy_px = best_bid + cfg.tick_size;
        let sell_px = best_ask - cfg.tick_size;
        let can_place_buy = buy_px < best_ask;
        let can_place_sell = sell_px > best_bid;
        if !can_place_buy && !can_place_sell {
            return signals;
        }

        // Enforce hard position limits by capping order size to remaining room.
        let buy_room = (cfg.max_position - net_position).max(0.0);
        let sell_room = (cfg.max_position + net_position).max(0.0);
        let buy_qty = cfg.order_qty.min(buy_room);
        let sell_qty = cfg.order_qty.min(sell_room);

        // Single-action policy: at most one fill opportunity per snapshot.
        // Prefer inventory-reducing side; when flat, alternate by timestamp parity.
        let preferred_side = if net_position > 0.0 {
            "2"
        } else if net_position < 0.0 {
            "1"
        } else if timestamp_ms % 2 == 0 {
            "1"
        } else {
            "2"
        };

        let make_signal = |side: &str| -> Option<Signal> {
            match side {
                "1" if can_place_buy && buy_qty > 0.0 => Some(Signal {
                    timestamp_ms,
                    symbol: cfg.symbol.clone(),
                    side: "1".to_string(),
                    price: (buy_px * 100.0).round() / 100.0,
                    qty: buy_qty,
                    reason: format!(
                        "spread={:.2}bps >= min={:.2}bps, side=BUY, net_pos={:.6}, room={:.6}",
                        spread_bps, cfg.min_spread_bps, net_position, buy_room
                    ),
                }),
                "2" if can_place_sell && sell_qty > 0.0 => Some(Signal {
                    timestamp_ms,
                    symbol: cfg.symbol.clone(),
                    side: "2".to_string(),
                    price: (sell_px * 100.0).round() / 100.0,
                    qty: sell_qty,
                    reason: format!(
                        "spread={:.2}bps >= min={:.2}bps, side=SELL, net_pos={:.6}, room={:.6}",
                        spread_bps, cfg.min_spread_bps, net_position, sell_room
                    ),
                }),
                _ => None,
            }
        };

        if let Some(sig) = make_signal(preferred_side) {
            signals.push(sig);
            return signals;
        }

        let fallback_side = if preferred_side == "1" { "2" } else { "1" };
        if let Some(sig) = make_signal(fallback_side) {
            signals.push(sig);
        }

        signals
    }

    /// Market-imbalance signal builder.
    ///
    /// Sums volume across the top `imbalance_depth_levels` price levels on each
    /// side. If bid volume / total volume >= `imbalance_threshold`, buy
    /// aggressively at `best_ask`. If ask volume dominates by the same ratio,
    /// sell aggressively at `best_bid`.
    pub fn build_imbalance_signals(
        cfg: &AlgoConfig,
        best_bid: f64,
        best_ask: f64,
        bids: &[(f64, f64)],
        asks: &[(f64, f64)],
        net_position: f64,
        timestamp_ms: i64,
    ) -> Vec<Signal> {
        let depth = cfg.imbalance_depth_levels;

        let bid_vol: f64 = bids.iter().take(depth).map(|(_, sz)| sz).sum();
        let ask_vol: f64 = asks.iter().take(depth).map(|(_, sz)| sz).sum();
        let total_vol = bid_vol + ask_vol;

        if total_vol <= 0.0 {
            debug!("Imbalance: no depth data available — no signal");
            return vec![];
        }

        let bid_ratio = bid_vol / total_vol;

        debug!(
            "Imbalance: bid_vol={:.4} ask_vol={:.4} bid_ratio={:.4} threshold={:.4}",
            bid_vol, ask_vol, bid_ratio, cfg.imbalance_threshold
        );

        let buy_room  = (cfg.max_position - net_position).max(0.0);
        let sell_room = (cfg.max_position + net_position).max(0.0);

        if bid_ratio >= cfg.imbalance_threshold && buy_room > 0.0 {
            let qty = cfg.order_qty.min(buy_room);
            return vec![Signal {
                timestamp_ms,
                symbol: cfg.symbol.clone(),
                side: "1".to_string(),
                price: best_ask,
                qty,
                reason: format!(
                    "imbalance BUY: bid_ratio={:.3} >= threshold={:.3}, bid_vol={:.4}, ask_vol={:.4}, net_pos={:.6}",
                    bid_ratio, cfg.imbalance_threshold, bid_vol, ask_vol, net_position
                ),
            }];
        }

        let ask_ratio = 1.0 - bid_ratio;
        if ask_ratio >= cfg.imbalance_threshold && sell_room > 0.0 {
            let qty = cfg.order_qty.min(sell_room);
            return vec![Signal {
                timestamp_ms,
                symbol: cfg.symbol.clone(),
                side: "2".to_string(),
                price: best_bid,
                qty,
                reason: format!(
                    "imbalance SELL: ask_ratio={:.3} >= threshold={:.3}, bid_vol={:.4}, ask_vol={:.4}, net_pos={:.6}",
                    ask_ratio, cfg.imbalance_threshold, bid_vol, ask_vol, net_position
                ),
            }];
        }

        vec![]
    }
}

/// Sync paper step. No tokio, no file I/O. Tests drive this.
pub(crate) fn paper_step_inner(
    paper: &mut PaperState,
    cfg: &AlgoConfig,
    best_bid: f64,
    best_ask: f64,
    bids: &[(f64, f64)],
    asks: &[(f64, f64)],
    now_ms: i64,
) -> Vec<ShadowFill> {
    if best_bid <= 0.0 || best_ask <= best_bid {
        return vec![]; // do not clear quotes; match today's early return
    }
    paper.last_mid = 0.5 * (best_bid + best_ask);
    let mut out = Vec::new();

    match cfg.strategy {
        StrategyType::SpreadCapture => {
            if let Some(pb) = paper.quote_buy.take() {
                if best_ask <= pb {
                    let fill = apply_costs(pb, "1", cfg.slippage_bps, cfg.maker_fee_bps);
                    if let Some((qty, pnl, net)) = paper.apply_fill("1", fill, cfg) {
                        out.push(ShadowFill {
                            ts_ms: now_ms,
                            strategy: cfg.strategy.clone(),
                            symbol: cfg.symbol.clone(),
                            side: "1".to_string(),
                            price: fill,
                            qty,
                            pnl,
                            cumulative_pnl: paper.total_pnl(),
                            net_qty_after: net,
                        });
                    }
                }
            }
            if let Some(ps) = paper.quote_sell.take() {
                if best_bid >= ps {
                    let fill = apply_costs(ps, "2", cfg.slippage_bps, cfg.maker_fee_bps);
                    if let Some((qty, pnl, net)) = paper.apply_fill("2", fill, cfg) {
                        out.push(ShadowFill {
                            ts_ms: now_ms,
                            strategy: cfg.strategy.clone(),
                            symbol: cfg.symbol.clone(),
                            side: "2".to_string(),
                            price: fill,
                            qty,
                            pnl,
                            cumulative_pnl: paper.total_pnl(),
                            net_qty_after: net,
                        });
                    }
                }
            }

            let spread_bps = (best_ask - best_bid) / best_bid * 10_000.0;
            if spread_bps >= cfg.min_spread_bps {
                if (cfg.max_position - paper.net_qty) > 1e-12 {
                    paper.quote_buy = Some(best_bid + cfg.tick_size);
                }
                if (cfg.max_position + paper.net_qty) > 1e-12 {
                    paper.quote_sell = Some(best_ask - cfg.tick_size);
                }
            }
        }
        StrategyType::MarketImbalance => {
            // Quotes stay None. Inventory is PaperState.net_qty, never PositionsStore.
            let signals = TradingAlgo::build_imbalance_signals(
                cfg,
                best_bid,
                best_ask,
                bids,
                asks,
                paper.net_qty,
                now_ms,
            );
            for signal in signals {
                let fill = apply_costs(
                    signal.price,
                    signal.side.as_str(),
                    cfg.slippage_bps,
                    cfg.taker_fee_bps,
                );
                if let Some((qty, pnl, net)) = paper.apply_fill(signal.side.as_str(), fill, cfg) {
                    out.push(ShadowFill {
                        ts_ms: now_ms,
                        strategy: cfg.strategy.clone(),
                        symbol: cfg.symbol.clone(),
                        side: signal.side,
                        price: fill,
                        qty,
                        pnl,
                        cumulative_pnl: paper.total_pnl(),
                        net_qty_after: net,
                    });
                }
            }
        }
    }
    out
}

fn shadow_fills_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: OnceLock<std::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

/// Process-wide lock; create_dir_all on parent; append + flush. I/O errors warn and return.
pub(crate) fn append_shadow_fill(fill: &ShadowFill) {
    append_shadow_fill_to(Path::new(SHADOW_FILLS_PATH), fill);
}

fn append_shadow_fill_to(path: &Path, fill: &ShadowFill) {
    let _guard = match shadow_fills_lock().lock() {
        Ok(g) => g,
        Err(p) => p.into_inner(),
    };
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                warn!("shadow fills: create_dir_all failed: {}", e);
                return;
            }
        }
    }
    let line = match serde_json::to_string(fill) {
        Ok(s) => s,
        Err(e) => {
            warn!("shadow fills: serialize failed: {}", e);
            return;
        }
    };
    let mut file = match OpenOptions::new().create(true).append(true).open(path) {
        Ok(f) => f,
        Err(e) => {
            warn!("shadow fills: open failed: {}", e);
            return;
        }
    };
    if let Err(e) = writeln!(file, "{}", line) {
        warn!("shadow fills: write failed: {}", e);
        return;
    }
    if let Err(e) = file.flush() {
        warn!("shadow fills: flush failed: {}", e);
    }
}

// ─── Backtesting Engine ───────────────────────────────────────────────────────

/// A single market data tick used as input to the backtest.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Snapshot {
    pub timestamp_ms: i64,
    pub best_bid: f64,
    pub best_ask: f64,
    /// Top-of-book bid size (0.0 when unavailable, e.g. old CSV rows)
    #[serde(default)]
    pub best_bid_size: f64,
    /// Top-of-book ask size (0.0 when unavailable, e.g. old CSV rows)
    #[serde(default)]
    pub best_ask_size: f64,
    /// Full depth bid levels (price descending). Populated from kdb+ or ZMQ snapshots;
    /// falls back to single top-of-book entry when not available.
    #[serde(default)]
    pub bids: Vec<(f64, f64)>,
    /// Full depth ask levels (price ascending).
    #[serde(default)]
    pub asks: Vec<(f64, f64)>,
}

/// One simulated fill produced by the backtest.
#[derive(Debug, Clone, Serialize)]
pub struct BacktestTrade {
    pub timestamp_ms: i64,
    /// "1" = buy, "2" = sell
    pub side: String,
    pub price: f64,
    pub qty: f64,
    /// Realised P&L contributed by this fill (0 for a pure opening leg)
    pub pnl: f64,
    pub cumulative_pnl: f64,
    pub net_qty_after: f64,
}

/// Aggregate statistics over all simulated fills.
#[derive(Debug, Clone, Serialize)]
pub struct BacktestSummary {
    pub total_trades: usize,
    pub winning_trades: usize,
    pub losing_trades: usize,
    pub win_rate_pct: f64,
    pub total_pnl: f64,
    pub max_drawdown: f64,
    /// Annualised Sharpe ratio (assumes ~252 trading days, simplified).
    pub sharpe_ratio: f64,
    /// Sortino ratio — like Sharpe but only penalises downside volatility.
    pub sortino_ratio: f64,
    /// Gross profit / gross loss. f64::INFINITY serialises as null in JSON; use 0 when no trades.
    pub profit_factor: f64,
    /// Mean milliseconds between position open and close (round-trip duration).
    pub avg_trade_duration_ms: i64,
    /// Longest streak of consecutive losing trades.
    pub max_consecutive_losses: u32,
    pub final_position: f64,
    pub snapshots_processed: usize,
}

/// Full result returned by `run_backtest`.
#[derive(Debug, Clone, Serialize)]
pub struct BacktestResult {
    pub summary: BacktestSummary,
    pub trades: Vec<BacktestTrade>,
    /// (timestamp_ms, cumulative_pnl) — one point per fill
    pub equity_curve: Vec<[f64; 2]>,
    /// Downsampled price series for chart rendering: (timestamp_ms, best_bid, best_ask)
    pub price_series: Vec<[f64; 3]>,
}

/// Apply slippage and fee costs to a fill price.
///
/// For buys the cost increases the effective price; for sells it decreases it.
/// `side` is "1" for buy, "2" for sell.
#[inline]
fn apply_costs(price: f64, side: &str, slippage_bps: f64, fee_bps: f64) -> f64 {
    let total_bps = slippage_bps + fee_bps;
    if side == "1" {
        price * (1.0 + total_bps / 10_000.0)
    } else {
        price * (1.0 - total_bps / 10_000.0)
    }
}

/// If kdb BBO rows have empty depth vectors, synthesize a 1-level book from
/// top-of-book price and size so MarketImbalance can still fire.
fn synthesize_bbo_book(s: &Snapshot) -> (Vec<(f64, f64)>, Vec<(f64, f64)>) {
    let bids = if s.bids.is_empty() {
        vec![(s.best_bid, s.best_bid_size)]
    } else {
        s.bids.clone()
    };
    let asks = if s.asks.is_empty() {
        vec![(s.best_ask, s.best_ask_size)]
    } else {
        s.asks.clone()
    };
    (bids, asks)
}

/// Shared inventory / trade / equity accounting used by BOTH fill branches.
/// Arithmetic is frozen — SpreadCapture numeric results must stay identical.
fn record_fill(
    net_qty: &mut f64,
    avg_entry: &mut f64,
    realized: &mut f64,
    trades: &mut Vec<BacktestTrade>,
    pnl_increments: &mut Vec<f64>,
    consecutive_losses: &mut u32,
    max_consecutive_losses: &mut u32,
    position_open_time: &mut Option<i64>,
    round_trip_durations_ms: &mut Vec<i64>,
    side: &str,
    fill: f64,
    qty: f64,
    cur_ts: i64,
    mid: f64,
) {
    let signed = if side == "1" { qty } else { -qty };
    let prev_net = *net_qty;
    let new_net = prev_net + signed;
    let mut trade_pnl = 0.0;

    if prev_net.abs() < 1e-12 {
        *avg_entry = fill;
        *position_open_time = Some(cur_ts);
    } else if prev_net.signum() == signed.signum() {
        *avg_entry = (*avg_entry * prev_net.abs() + fill * qty) / (prev_net.abs() + qty);
    } else {
        let closing = qty.min(prev_net.abs());
        trade_pnl = if prev_net > 0.0 {
            closing * (fill - *avg_entry)
        } else {
            closing * (*avg_entry - fill)
        };
        *realized += trade_pnl;
        if new_net.abs() < 1e-12 {
            *avg_entry = 0.0;
            if let Some(open) = position_open_time.take() {
                round_trip_durations_ms.push(cur_ts - open);
            }
        } else if new_net.signum() != prev_net.signum() {
            *avg_entry = fill;
            *position_open_time = Some(cur_ts);
        }
    }

    *net_qty = new_net;
    let cum = *realized + *net_qty * (mid - *avg_entry);
    pnl_increments.push(trade_pnl);
    if trade_pnl < 0.0 {
        *consecutive_losses += 1;
        *max_consecutive_losses = (*max_consecutive_losses).max(*consecutive_losses);
    } else if trade_pnl > 0.0 {
        *consecutive_losses = 0;
    }
    trades.push(BacktestTrade {
        timestamp_ms: cur_ts,
        side: side.to_string(),
        price: fill,
        qty,
        pnl: trade_pnl,
        cumulative_pnl: cum,
        net_qty_after: *net_qty,
    });
}

/// Run a passive market-making simulation over historical snapshots.
///
/// Two things make this honest where the old version was not:
///  1. SANITY FILTER — one-sided, crossed, absurd-spread (`max_spread_frac`,
///     default 5%) or spike (`max_mid_jump_frac`, default 10% mid jump) ticks
///     are dropped. Corrupt feed ticks otherwise produce garbage
///     mark-to-market (e.g. a bogus best_ask blew P&L up to ~1e10).
///  2. ADVERSE SELECTION — a passive quote posted at snapshot[i] only fills at
///     snapshot[i+1] if the market actually trades THROUGH it (best_ask<=our bid,
///     or best_bid>=our offer). The resulting inventory is then marked at the NEW
///     mid, so a buy that fills *because price fell* is immediately marked down.
///     That captures adverse selection — the term that actually decides whether
///     market-making is profitable. The maker rebate is applied to the fill via
///     `apply_costs` (a negative fee improves the fill).
///
/// MarketImbalance crosses the spread at the signal price and is charged
/// `taker_fee_bps` rather than the maker rebate.
///
/// Conservative by design: one-requote-interval quote life, no queue-jumping, no
/// partial fills. It under-counts fills, not edge.
pub fn run_backtest(cfg: &AlgoConfig, snapshots: &[Snapshot]) -> BacktestResult {
    // ── 1. Sanity-filter snapshots ────────────────────────────────────────
    let mut clean: Vec<Snapshot> = Vec::with_capacity(snapshots.len());
    let mut prev_mid: Option<f64> = None;
    for s in snapshots {
        if s.best_bid <= 0.0 || s.best_ask <= s.best_bid {
            continue;
        }
        let mid = 0.5 * (s.best_bid + s.best_ask);
        if (s.best_ask - s.best_bid) / s.best_bid > cfg.max_spread_frac {
            continue; // spread wider than configured sanity threshold
        }
        if let Some(pm) = prev_mid {
            if (mid - pm).abs() / pm > cfg.max_mid_jump_frac {
                continue; // mid jump larger than configured sanity threshold
            }
        }
        prev_mid = Some(mid);
        clean.push(s.clone());
    }

    let mut net_qty: f64 = 0.0;
    let mut avg_entry: f64 = 0.0;
    let mut realized: f64 = 0.0;

    let mut trades: Vec<BacktestTrade> = Vec::new();
    let mut equity_curve: Vec<[f64; 2]> = Vec::new();
    let mut pnl_increments: Vec<f64> = Vec::new();
    let mut peak_equity: f64 = 0.0;
    let mut max_drawdown: f64 = 0.0;
    let mut position_open_time: Option<i64> = None;
    let mut round_trip_durations_ms: Vec<i64> = Vec::new();
    let mut consecutive_losses: u32 = 0;
    let mut max_consecutive_losses: u32 = 0;
    let mut last_mid: Option<f64> = None;

    let price_step = (clean.len() / 2000).max(1);
    let price_series: Vec<[f64; 3]> = clean
        .iter()
        .enumerate()
        .filter(|(i, _)| i % price_step == 0)
        .map(|(_, s)| [s.timestamp_ms as f64, s.best_bid, s.best_ask])
        .collect();

    // ── 2. Walk consecutive snapshots; quotes from prev, fills at cur ──────
    for w in clean.windows(2) {
        let prev = &w[0];
        let cur = &w[1];
        let mid = 0.5 * (cur.best_bid + cur.best_ask);
        last_mid = Some(mid);

        match cfg.strategy {
            StrategyType::SpreadCapture => {
                let spread_bps = (prev.best_ask - prev.best_bid) / prev.best_bid * 10_000.0;
                if spread_bps >= cfg.min_spread_bps {
                    let p_buy = prev.best_bid + cfg.tick_size;
                    let p_sell = prev.best_ask - cfg.tick_size;

                    let mut fills: Vec<(&str, f64)> = Vec::new();
                    if cur.best_ask <= p_buy {
                        fills.push(("1", p_buy));
                    }
                    if cur.best_bid >= p_sell {
                        fills.push(("2", p_sell));
                    }

                    for (side, quote_px) in fills {
                        let room = if side == "1" {
                            (cfg.max_position - net_qty).max(0.0)
                        } else {
                            (cfg.max_position + net_qty).max(0.0)
                        };
                        let qty = cfg.order_qty.min(room);
                        if qty <= 1e-12 {
                            continue;
                        }

                        let fill = apply_costs(quote_px, side, cfg.slippage_bps, cfg.maker_fee_bps);
                        record_fill(
                            &mut net_qty,
                            &mut avg_entry,
                            &mut realized,
                            &mut trades,
                            &mut pnl_increments,
                            &mut consecutive_losses,
                            &mut max_consecutive_losses,
                            &mut position_open_time,
                            &mut round_trip_durations_ms,
                            side,
                            fill,
                            qty,
                            cur.timestamp_ms,
                            mid,
                        );
                    }
                }
            }
            StrategyType::MarketImbalance => {
                let (bids, asks) = synthesize_bbo_book(prev);
                let signals = TradingAlgo::build_imbalance_signals(
                    cfg,
                    prev.best_bid,
                    prev.best_ask,
                    &bids,
                    &asks,
                    net_qty,
                    prev.timestamp_ms,
                );
                for signal in signals {
                    let fill = apply_costs(
                        signal.price,
                        signal.side.as_str(),
                        cfg.slippage_bps,
                        cfg.taker_fee_bps,
                    );
                    let qty = signal.qty;
                    record_fill(
                        &mut net_qty,
                        &mut avg_entry,
                        &mut realized,
                        &mut trades,
                        &mut pnl_increments,
                        &mut consecutive_losses,
                        &mut max_consecutive_losses,
                        &mut position_open_time,
                        &mut round_trip_durations_ms,
                        signal.side.as_str(),
                        fill,
                        qty,
                        cur.timestamp_ms,
                        mid,
                    );
                }
            }
        }

        let equity = realized + net_qty * (mid - avg_entry);
        peak_equity = peak_equity.max(equity);
        max_drawdown = max_drawdown.max(peak_equity - equity);
        equity_curve.push([cur.timestamp_ms as f64, equity]);
    }

    // ── 3. Summary statistics ─────────────────────────────────────────────
    let winning_trades = trades.iter().filter(|t| t.pnl > 0.0).count();
    let losing_trades = trades.iter().filter(|t| t.pnl < 0.0).count();
    let total_trades = trades.len();
    let win_rate_pct = if total_trades > 0 {
        winning_trades as f64 / total_trades as f64 * 100.0
    } else {
        0.0
    };

    let nonzero: Vec<f64> = pnl_increments.iter().cloned().filter(|&v| v != 0.0).collect();
    let sharpe_ratio = if nonzero.len() >= 2 {
        let mean = nonzero.iter().sum::<f64>() / nonzero.len() as f64;
        let var = nonzero.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / nonzero.len() as f64;
        let sd = var.sqrt();
        if sd > 0.0 { mean / sd * (252_f64.sqrt()) } else { 0.0 }
    } else {
        0.0
    };
    let sortino_ratio = if nonzero.len() >= 2 {
        let mean = nonzero.iter().sum::<f64>() / nonzero.len() as f64;
        let neg: Vec<f64> = nonzero.iter().cloned().filter(|&v| v < 0.0).collect();
        if neg.len() >= 2 {
            let dvar = neg.iter().map(|v| v.powi(2)).sum::<f64>() / neg.len() as f64;
            let dsd = dvar.sqrt();
            if dsd > 0.0 { mean / dsd * (252_f64.sqrt()) } else { 0.0 }
        } else if mean > 0.0 {
            f64::INFINITY
        } else {
            0.0
        }
    } else {
        0.0
    };
    let gross_profit: f64 = pnl_increments.iter().filter(|&&v| v > 0.0).sum();
    let gross_loss: f64 = pnl_increments.iter().filter(|&&v| v < 0.0).map(|v| v.abs()).sum();
    let profit_factor = if gross_loss > 0.0 {
        gross_profit / gross_loss
    } else if gross_profit > 0.0 {
        f64::INFINITY
    } else {
        0.0
    };
    let avg_trade_duration_ms = if !round_trip_durations_ms.is_empty() {
        round_trip_durations_ms.iter().sum::<i64>() / round_trip_durations_ms.len() as i64
    } else {
        0
    };
    let total_pnl = match last_mid {
        Some(m) => realized + net_qty * (m - avg_entry),
        None => realized,
    };

    BacktestResult {
        summary: BacktestSummary {
            total_trades,
            winning_trades,
            losing_trades,
            win_rate_pct,
            total_pnl,
            max_drawdown,
            sharpe_ratio,
            sortino_ratio,
            profit_factor,
            avg_trade_duration_ms,
            max_consecutive_losses,
            final_position: net_qty,
            snapshots_processed: clean.len(),
        },
        trades,
        equity_curve,
        price_series,
    }
}

/// Four axes searched by `calibrate()`. Field declaration order is the serde JSON key order.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CalibratedParams {
    pub maker_fee_bps: f64,
    pub taker_fee_bps: f64,
    pub max_spread_frac: f64,
    pub max_mid_jump_frac: f64,
}

/// AlgoConfig numeric fields held constant across the calibration grid.
#[derive(Debug, Clone, Serialize)]
pub struct HeldConstant {
    pub order_qty: f64,
    pub max_position: f64,
    pub tick_size: f64,
    pub min_spread_bps: f64,
    pub imbalance_threshold: f64,
    pub imbalance_depth_levels: usize,
    pub slippage_bps: f64,
}

/// Pure calibration output. Determinism AC serializes this struct.
#[derive(Debug, Clone, Serialize)]
pub struct CalibrationResult {
    pub window_name: String,
    pub old_params: CalibratedParams,
    pub new_params: CalibratedParams,
    pub old_spread_capture: BacktestSummary,
    pub old_market_imbalance: BacktestSummary,
    pub new_spread_capture: BacktestSummary,
    pub new_market_imbalance: BacktestSummary,
    pub grid_product: usize,
    pub held_constant: HeldConstant,
}

#[derive(Debug, thiserror::Error)]
pub enum CalibrateError {
    #[error("grid too large: {product} combinations")]
    GridTooLarge { product: usize },
    #[error("empty calibration axis")]
    EmptyAxis,
    #[error("non-finite calibration axis value")]
    NonFiniteAxis,
}

const CALIBRATE_GRID_CELL_CAP: usize = 200;

/// Dual-strategy grid search ranked by mean Sharpe. Pure: no fs, kdb, or clock.
pub fn calibrate(
    snapshots: &[Snapshot],
    window_name: &str,
    maker_fee_bps: &[f64],
    taker_fee_bps: &[f64],
    max_spread_frac: &[f64],
    max_mid_jump_frac: &[f64],
    baseline: &AlgoConfig,
) -> Result<CalibrationResult, CalibrateError> {
    if maker_fee_bps.is_empty()
        || taker_fee_bps.is_empty()
        || max_spread_frac.is_empty()
        || max_mid_jump_frac.is_empty()
    {
        return Err(CalibrateError::EmptyAxis);
    }
    let any_non_finite = maker_fee_bps
        .iter()
        .chain(taker_fee_bps)
        .chain(max_spread_frac)
        .chain(max_mid_jump_frac)
        .any(|v| !v.is_finite());
    if any_non_finite {
        return Err(CalibrateError::NonFiniteAxis);
    }

    let product = maker_fee_bps
        .len()
        .checked_mul(taker_fee_bps.len())
        .and_then(|x| x.checked_mul(max_spread_frac.len()))
        .and_then(|x| x.checked_mul(max_mid_jump_frac.len()));
    let grid_product = match product {
        None => {
            return Err(CalibrateError::GridTooLarge {
                product: usize::MAX,
            })
        }
        Some(p) if p > CALIBRATE_GRID_CELL_CAP => {
            return Err(CalibrateError::GridTooLarge { product: p })
        }
        Some(p) => p,
    };

    let mut best_key = f64::NEG_INFINITY;
    let mut best_params: Option<CalibratedParams> = None;
    let mut best_sc: Option<BacktestSummary> = None;
    let mut best_mi: Option<BacktestSummary> = None;

    for &maker in maker_fee_bps {
        for &taker in taker_fee_bps {
            for &spread in max_spread_frac {
                for &jump in max_mid_jump_frac {
                    let mut cfg = baseline.clone();
                    cfg.maker_fee_bps = maker;
                    cfg.taker_fee_bps = taker;
                    cfg.max_spread_frac = spread;
                    cfg.max_mid_jump_frac = jump;
                    cfg.strategy = StrategyType::SpreadCapture;
                    let sc = run_backtest(&cfg, snapshots);
                    cfg.strategy = StrategyType::MarketImbalance;
                    let mi = run_backtest(&cfg, snapshots);
                    let mean = (sc.summary.sharpe_ratio + mi.summary.sharpe_ratio) / 2.0;
                    let key = if mean.is_finite() {
                        mean
                    } else {
                        f64::NEG_INFINITY
                    };
                    if best_params.is_none() || key > best_key {
                        best_key = key;
                        best_params = Some(CalibratedParams {
                            maker_fee_bps: maker,
                            taker_fee_bps: taker,
                            max_spread_frac: spread,
                            max_mid_jump_frac: jump,
                        });
                        best_sc = Some(sc.summary);
                        best_mi = Some(mi.summary);
                    }
                }
            }
        }
    }

    let old_params = CalibratedParams {
        maker_fee_bps: default_maker_fee_bps(),
        taker_fee_bps: default_taker_fee_bps(),
        max_spread_frac: default_max_spread_frac(),
        max_mid_jump_frac: default_max_mid_jump_frac(),
    };
    let mut old_cfg = baseline.clone();
    old_cfg.maker_fee_bps = old_params.maker_fee_bps;
    old_cfg.taker_fee_bps = old_params.taker_fee_bps;
    old_cfg.max_spread_frac = old_params.max_spread_frac;
    old_cfg.max_mid_jump_frac = old_params.max_mid_jump_frac;
    old_cfg.strategy = StrategyType::SpreadCapture;
    let old_spread_capture = run_backtest(&old_cfg, snapshots).summary;
    old_cfg.strategy = StrategyType::MarketImbalance;
    let old_market_imbalance = run_backtest(&old_cfg, snapshots).summary;

    Ok(CalibrationResult {
        window_name: window_name.to_string(),
        old_params,
        new_params: best_params.expect("grid is non-empty"),
        old_spread_capture,
        old_market_imbalance,
        new_spread_capture: best_sc.expect("grid is non-empty"),
        new_market_imbalance: best_mi.expect("grid is non-empty"),
        grid_product,
        held_constant: HeldConstant {
            order_qty: baseline.order_qty,
            max_position: baseline.max_position,
            tick_size: baseline.tick_size,
            min_spread_bps: baseline.min_spread_bps,
            imbalance_threshold: baseline.imbalance_threshold,
            imbalance_depth_levels: baseline.imbalance_depth_levels,
            slippage_bps: baseline.slippage_bps,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_cfg() -> AlgoConfig {
        AlgoConfig {
            enabled: false,
            symbol: "BTC_USDT".to_string(),
            order_qty: 1.0,
            min_spread_bps: 5.0,
            max_position: 2.0,
            poll_interval_ms: 1000,
            tick_size: 0.5,
            time_in_force: "3".to_string(),
            strategy: StrategyType::SpreadCapture,
            imbalance_threshold: 0.6,
            imbalance_depth_levels: 5,
            // Zero costs so existing numeric assertions remain valid
            slippage_bps: 0.0,
            fee_bps: 0.0,
            maker_fee_bps: 0.0,
            taker_fee_bps: 0.0,
            max_spread_frac: 0.05,
            max_mid_jump_frac: 0.10,
            min_signal_interval_ms: 0,
            max_book_staleness_ms: 0,
            max_orders_session: 0,
            self_match_prevention_scope: None,
            self_match_prevention_instruction: None,
            self_match_prevention_id: None,
        }
    }

    #[test]
    fn build_signals_single_side_and_correct_price_levels() {
        let cfg = test_cfg();
        let spread_bps = ((102.0 - 100.0) / 100.0) * 10_000.0;

        let even_ts = TradingAlgo::build_spread_capture_signals(&cfg, 100.0, 102.0, 0.0, spread_bps, 2);
        assert_eq!(even_ts.len(), 1);
        assert_eq!(even_ts[0].side, "1");
        assert!((even_ts[0].price - 100.5).abs() < 1e-9);

        let odd_ts = TradingAlgo::build_spread_capture_signals(&cfg, 100.0, 102.0, 0.0, spread_bps, 3);
        assert_eq!(odd_ts.len(), 1);
        assert_eq!(odd_ts[0].side, "2");
        assert!((odd_ts[0].price - 101.5).abs() < 1e-9);
    }

    #[test]
    fn build_signals_caps_qty_to_remaining_room() {
        let mut cfg = test_cfg();
        cfg.order_qty = 0.5;
        cfg.max_position = 0.1;
        let spread_bps = ((102.0 - 100.0) / 100.0) * 10_000.0;

        // Flat inventory on even timestamp => BUY side selected and qty capped by max_position room.
        let sigs = TradingAlgo::build_spread_capture_signals(&cfg, 100.0, 102.0, 0.0, spread_bps, 2);
        assert_eq!(sigs.len(), 1);
        assert_eq!(sigs[0].side, "1");
        assert!((sigs[0].qty - 0.1).abs() < 1e-9);
    }

    #[test]
    fn build_imbalance_signals_fires_buy_on_bid_heavy_book() {
        let cfg = test_cfg();
        // 70% bid volume => bid_ratio=0.7 >= threshold=0.6 => BUY at best_ask
        let bids = vec![(100.0, 7.0), (99.5, 0.0)];
        let asks = vec![(101.0, 3.0)];
        let ts = 1_000i64;
        let sigs = TradingAlgo::build_imbalance_signals(
            &cfg, 100.0, 101.0, &bids, &asks, 0.0, ts,
        );
        assert_eq!(sigs.len(), 1);
        assert_eq!(sigs[0].side, "1");
        assert!((sigs[0].price - 101.0).abs() < 1e-9);
    }

    #[test]
    fn build_imbalance_signals_fires_sell_on_ask_heavy_book() {
        let cfg = test_cfg();
        // 70% ask volume => ask_ratio=0.7 >= threshold=0.6 => SELL at best_bid
        let bids = vec![(100.0, 3.0)];
        let asks = vec![(101.0, 7.0)];
        let ts = 1_000i64;
        let sigs = TradingAlgo::build_imbalance_signals(
            &cfg, 100.0, 101.0, &bids, &asks, 0.0, ts,
        );
        assert_eq!(sigs.len(), 1);
        assert_eq!(sigs[0].side, "2");
        assert!((sigs[0].price - 100.0).abs() < 1e-9);
    }

    #[test]
    fn build_imbalance_signals_no_signal_when_balanced() {
        let cfg = test_cfg();
        // 50/50 split — neither side reaches threshold
        let bids = vec![(100.0, 5.0)];
        let asks = vec![(101.0, 5.0)];
        let ts = 1_000i64;
        let sigs = TradingAlgo::build_imbalance_signals(
            &cfg, 100.0, 101.0, &bids, &asks, 0.0, ts,
        );
        assert!(sigs.is_empty());
    }

    #[test]
    fn build_imbalance_signals_no_signal_on_empty_depth() {
        let cfg = test_cfg();
        let sigs = TradingAlgo::build_imbalance_signals(
            &cfg, 100.0, 101.0, &[], &[], 0.0, 1_000,
        );
        assert!(sigs.is_empty());
    }

    #[test]
    fn run_backtest_fills_buy_when_market_crosses_down() {
        let cfg = test_cfg(); // tick=0.5, qty=1, fees 0
        // s0 posts a passive buy at 100.5; s1 best_ask=100.5 trades through it.
        let snaps = vec![
            Snapshot { timestamp_ms: 1, best_bid: 100.0, best_ask: 102.0, ..Default::default() },
            Snapshot { timestamp_ms: 2, best_bid: 100.0, best_ask: 100.5, ..Default::default() },
        ];
        let r = run_backtest(&cfg, &snaps);
        assert_eq!(r.summary.total_trades, 1);
        assert_eq!(r.trades[0].side, "1");
        assert!((r.trades[0].price - 100.5).abs() < 1e-9);
        assert!((r.summary.final_position - 1.0).abs() < 1e-9);
        assert!(r.summary.total_pnl.is_finite());
    }

    #[test]
    fn run_backtest_no_fill_without_a_cross() {
        let cfg = test_cfg();
        // s1 ask=101.6 > buy quote 100.5, bid=100 < sell quote 101.5 -> no cross.
        let snaps = vec![
            Snapshot { timestamp_ms: 1, best_bid: 100.0, best_ask: 102.0, ..Default::default() },
            Snapshot { timestamp_ms: 2, best_bid: 100.0, best_ask: 101.6, ..Default::default() },
        ];
        let r = run_backtest(&cfg, &snaps);
        assert_eq!(r.summary.total_trades, 0);
        assert!(r.summary.total_pnl.abs() < 1e-9);
    }

    #[test]
    fn run_backtest_filters_bad_tick_and_stays_finite() {
        let cfg = test_cfg();
        // Middle snapshot is a corrupt tick (absurd ask): must be dropped and must
        // NOT blow up P&L (this is exactly the old 1e10 failure mode).
        let snaps = vec![
            Snapshot { timestamp_ms: 1, best_bid: 100.0, best_ask: 102.0, ..Default::default() },
            Snapshot { timestamp_ms: 2, best_bid: 100.0, best_ask: 1.0e13, ..Default::default() },
            Snapshot { timestamp_ms: 3, best_bid: 100.0, best_ask: 100.5, ..Default::default() },
        ];
        let r = run_backtest(&cfg, &snaps);
        assert_eq!(r.summary.snapshots_processed, 2); // bad tick dropped
        assert!(r.summary.total_pnl.is_finite());
        assert!(r.summary.total_pnl.abs() < 1.0e6);
    }

    #[test]
    fn run_backtest_maker_rebate_improves_buy_fill() {
        let mut cfg = test_cfg();
        cfg.maker_fee_bps = -10.0; // 10bps rebate -> buy fills BELOW the quote
        let snaps = vec![
            Snapshot { timestamp_ms: 1, best_bid: 100.0, best_ask: 102.0, ..Default::default() },
            Snapshot { timestamp_ms: 2, best_bid: 100.0, best_ask: 100.5, ..Default::default() },
        ];
        let r = run_backtest(&cfg, &snaps);
        assert_eq!(r.summary.total_trades, 1);
        assert!(r.trades[0].price < 100.5);
    }

    #[test]
    fn run_backtest_market_imbalance_charges_taker_fee_bps() {
        let mut cfg = test_cfg();
        cfg.strategy = StrategyType::MarketImbalance;
        cfg.maker_fee_bps = -10.0; // rebate would *lower* a buy fill if wrongly used
        cfg.taker_fee_bps = 10.0; // positive taker fee raises a buy fill above the cross
        cfg.slippage_bps = 0.0;
        // Bid-heavy BBO sizes so synthesize_bbo_book + build_imbalance_signals fires a BUY
        // at prev.best_ask (101.0). Cur is only used for mark-to-market / trade timestamp.
        let snaps = vec![
            Snapshot {
                timestamp_ms: 1,
                best_bid: 100.0,
                best_ask: 101.0,
                best_bid_size: 7.0,
                best_ask_size: 3.0,
                ..Default::default()
            },
            Snapshot {
                timestamp_ms: 2,
                best_bid: 100.0,
                best_ask: 101.0,
                best_bid_size: 7.0,
                best_ask_size: 3.0,
                ..Default::default()
            },
        ];
        let r = run_backtest(&cfg, &snaps);
        assert_eq!(r.summary.total_trades, 1);
        assert_eq!(r.trades[0].side, "1");
        let cross = 101.0;
        let taker_px = cross * (1.0 + cfg.taker_fee_bps / 10_000.0);
        let maker_px = cross * (1.0 + cfg.maker_fee_bps / 10_000.0);
        assert!(
            r.trades[0].price > cross,
            "taker fee must raise the buy fill above the cross {}, got {}",
            cross,
            r.trades[0].price
        );
        assert!(
            (r.trades[0].price - taker_px).abs() < 1e-9,
            "fill must equal taker-fee path {}, got {}",
            taker_px,
            r.trades[0].price
        );
        assert!(
            (r.trades[0].price - maker_px).abs() > 1e-9,
            "fill must not equal the maker-fee path {}",
            maker_px
        );
    }

    #[test]
    fn calibrate_is_deterministic_byte_identical() {
        let snaps = vec![
            Snapshot {
                timestamp_ms: 1,
                best_bid: 100.0,
                best_ask: 101.0,
                best_bid_size: 7.0,
                best_ask_size: 3.0,
                ..Default::default()
            },
            Snapshot {
                timestamp_ms: 2,
                best_bid: 100.0,
                best_ask: 100.5,
                best_bid_size: 7.0,
                best_ask_size: 3.0,
                ..Default::default()
            },
        ];
        let baseline = test_cfg();
        let makers = [-1.0, 0.0];
        let takers = [4.0, 5.0];
        let spreads = [0.05];
        let jumps = [0.10];
        let a = calibrate(
            &snaps,
            "uat_2026_03_01",
            &makers,
            &takers,
            &spreads,
            &jumps,
            &baseline,
        )
        .expect("calibrate a");
        let b = calibrate(
            &snaps,
            "uat_2026_03_01",
            &makers,
            &takers,
            &spreads,
            &jumps,
            &baseline,
        )
        .expect("calibrate b");
        assert_eq!(
            serde_json::to_vec(&a).expect("serialize a"),
            serde_json::to_vec(&b).expect("serialize b")
        );
    }

    #[test]
    fn paper_step_inner_mi_bid_heavy_immediate_buy_taker_no_quotes() {
        let mut cfg = test_cfg();
        cfg.strategy = StrategyType::MarketImbalance;
        cfg.slippage_bps = 1.0;
        cfg.taker_fee_bps = 4.0;
        cfg.maker_fee_bps = -1.0;
        cfg.order_qty = 0.001;
        cfg.max_position = 0.01;
        let mut paper = PaperState::default();
        let bids = vec![(100.0, 7.0)];
        let asks = vec![(100.5, 3.0)];
        let fills = paper_step_inner(&mut paper, &cfg, 100.0, 100.5, &bids, &asks, 1_000);
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].side, "1");
        assert_eq!(fills[0].strategy, StrategyType::MarketImbalance);
        let expected = 100.5 * (1.0 + (cfg.slippage_bps + cfg.taker_fee_bps) / 10_000.0);
        assert!(
            (fills[0].price - expected).abs() < 1e-9,
            "taker-costed ask {}, got {}",
            expected,
            fills[0].price
        );
        assert!(paper.quote_buy.is_none());
        assert!(paper.quote_sell.is_none());
        assert!(paper.fills >= 1);
    }

    #[test]
    fn paper_step_inner_mi_uses_taker_even_when_maker_rebate_nonzero() {
        let mut cfg = test_cfg();
        cfg.strategy = StrategyType::MarketImbalance;
        cfg.slippage_bps = 0.0;
        cfg.taker_fee_bps = 10.0;
        cfg.maker_fee_bps = -10.0;
        let mut paper = PaperState::default();
        let bids = vec![(100.0, 7.0)];
        let asks = vec![(101.0, 3.0)];
        let fills = paper_step_inner(&mut paper, &cfg, 100.0, 101.0, &bids, &asks, 2_000);
        assert_eq!(fills.len(), 1);
        let taker_px = 101.0 * (1.0 + cfg.taker_fee_bps / 10_000.0);
        let maker_px = 101.0 * (1.0 + cfg.maker_fee_bps / 10_000.0);
        assert!((fills[0].price - taker_px).abs() < 1e-9);
        assert!((fills[0].price - maker_px).abs() > 1e-9);
        assert!(paper.quote_buy.is_none() && paper.quote_sell.is_none());
    }

    #[test]
    fn paper_step_inner_sc_rests_then_fills_on_cross_with_maker_fee() {
        let mut cfg = test_cfg();
        cfg.strategy = StrategyType::SpreadCapture;
        cfg.tick_size = 0.01;
        cfg.min_spread_bps = 5.0;
        cfg.slippage_bps = 0.0;
        cfg.maker_fee_bps = -10.0;
        cfg.order_qty = 0.001;
        let mut paper = PaperState::default();
        let first = paper_step_inner(&mut paper, &cfg, 100.0, 100.5, &[], &[], 1);
        assert!(first.is_empty());
        assert!(paper.quote_buy.is_some());
        assert!(paper.quote_sell.is_some());
        let quote_buy = paper.quote_buy.unwrap();
        assert!((quote_buy - 100.01).abs() < 1e-9);
        let fills = paper_step_inner(&mut paper, &cfg, 100.0, 100.01, &[], &[], 2);
        assert_eq!(fills.len(), 1);
        assert_eq!(fills[0].side, "1");
        assert_eq!(fills[0].strategy, StrategyType::SpreadCapture);
        let maker_px = quote_buy * (1.0 + cfg.maker_fee_bps / 10_000.0);
        assert!(
            (fills[0].price - maker_px).abs() < 1e-9,
            "maker-fee fill {}, got {}",
            maker_px,
            fills[0].price
        );
        assert!(fills[0].price < quote_buy);
    }

    #[test]
    fn append_shadow_fill_to_writes_one_compact_object_and_newline() {
        let fill = ShadowFill {
            ts_ms: 1_773_964_800_123,
            strategy: StrategyType::MarketImbalance,
            symbol: "BTC_USDT".to_string(),
            side: "1".to_string(),
            price: 101.0505,
            qty: 0.001,
            pnl: 0.0,
            cumulative_pnl: -0.00005,
            net_qty_after: 0.001,
        };
        let dir = std::env::temp_dir().join(format!(
            "shadow_fill_test_{}_{}",
            std::process::id(),
            Utc::now().timestamp_nanos_opt().unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("shadow_fills.jsonl");
        append_shadow_fill_to(&path, &fill);
        let s = std::fs::read_to_string(&path).expect("read jsonl");
        assert!(s.ends_with('\n'));
        assert_eq!(s.matches('\n').count(), 1);
        let line = s.trim_end();
        assert!(line.starts_with('{') && line.ends_with('}'));
        assert!(s.contains("\"strategy\":\"market_imbalance\""));
        assert!(!s.contains("MarketImbalance"));
        assert!(!line.contains('\n'));
        let parsed: serde_json::Value = serde_json::from_str(line).expect("json");
        assert!(parsed.is_object());
        // Compact serde_json::to_string preserves struct field order (BTreeMap
        // of serde_json::Value does not). Check the raw line.
        let expected_order = [
            "\"ts_ms\":",
            "\"strategy\":",
            "\"symbol\":",
            "\"side\":",
            "\"price\":",
            "\"qty\":",
            "\"pnl\":",
            "\"cumulative_pnl\":",
            "\"net_qty_after\":",
        ];
        let mut last = 0usize;
        for key in expected_order {
            let pos = line.find(key).expect(key);
            assert!(pos >= last, "key order broken at {}", key);
            last = pos;
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}

