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
use log::{info, warn, debug};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use chrono::Utc;

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
}

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
}

/// The trading algorithm.
pub struct TradingAlgo {
    pub config: Arc<Mutex<AlgoConfig>>,
    signals_fired: Arc<Mutex<u64>>,
    orders_placed: Arc<Mutex<u64>>,
    signal_log: Arc<Mutex<SignalLog>>,
}

impl TradingAlgo {
    pub fn new(config: AlgoConfig) -> Self {
        TradingAlgo {
            config: Arc::new(Mutex::new(config)),
            signals_fired: Arc::new(Mutex::new(0)),
            orders_placed: Arc::new(Mutex::new(0)),
            signal_log: Arc::new(Mutex::new(SignalLog::new(50))),
        }
    }

    /// Return a snapshot of current algo status.
    pub async fn status(&self) -> AlgoStatus {
        AlgoStatus {
            config: self.config.lock().await.clone(),
            signals_fired: *self.signals_fired.lock().await,
            orders_placed: *self.orders_placed.lock().await,
            recent_signals: self.signal_log.lock().await.recent(20),
        }
    }

    /// Hot-swap the algo configuration without restarting the task.
    pub async fn update_config(&self, new_cfg: AlgoConfig) {
        *self.config.lock().await = new_cfg;
    }

    /// Evaluate signal given current market state.
    ///
    /// Returns a list of orders to place (typically 0 or 1 per tick).
    /// Replace or extend this function with your own strategy logic.
    pub fn evaluate_signal(
        cfg: &AlgoConfig,
        best_bid: f64,
        best_ask: f64,
        net_position: f64,
    ) -> Vec<Signal> {
        if best_bid <= 0.0 || best_ask <= 0.0 || best_ask <= best_bid {
            return vec![];
        }

        let spread = best_ask - best_bid;
        let spread_bps = (spread / best_bid) * 10_000.0;

        if spread_bps < cfg.min_spread_bps {
            debug!("Spread {:.2} bps < min {:.2} bps — no signal", spread_bps, cfg.min_spread_bps);
            return vec![];
        }

        let ts = Utc::now().timestamp_millis();
        Self::build_signals(cfg, best_bid, best_ask, net_position, spread_bps, ts)
    }

    /// Inner signal builder shared between live and backtest paths.
    fn build_signals(
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

    /// Run the algo loop.  Spawned as a long-lived Tokio task in main().
    ///
    /// `order_session`  — FIXSession used to place orders (order management port)
    /// `market_session` — FIXSession used to read live order books (market data port)
    /// `positions`      — shared PositionsStore so we can check current net position
    pub async fn run(
        self: Arc<Self>,
        order_session: Arc<crate::FIXSession>,
        market_session: Arc<crate::FIXSession>,
        positions: Arc<crate::PositionsStore>,
    ) {
        info!("🤖 Trading algo task started");

        loop {
            let cfg = self.config.lock().await.clone();
            let interval = tokio::time::Duration::from_millis(cfg.poll_interval_ms);

            if !cfg.enabled {
                tokio::time::sleep(interval).await;
                continue;
            }

            // Read live order book for the configured symbol
            let (best_bid, best_ask) = {
                let books = market_session.order_books.lock().await;
                books.get(&cfg.symbol)
                    .map(|db| {
                        let snap = db.get_snapshot();
                        (snap.best_bid, snap.best_ask)
                    })
                    .unwrap_or((0.0, 0.0))
            };

            // Get current net position
            let net_position = positions
                .snapshot()
                .into_iter()
                .find(|p| p.symbol == cfg.symbol)
                .map(|p| p.net_qty)
                .unwrap_or(0.0);

            let signals = Self::evaluate_signal(&cfg, best_bid, best_ask, net_position);

            for signal in signals {
                info!(
                    "🤖 ALGO SIGNAL: {} {} qty={} px={:.4} | {}",
                    if signal.side == "1" { "BUY" } else { "SELL" },
                    signal.symbol,
                    signal.qty,
                    signal.price,
                    signal.reason
                );

                *self.signals_fired.lock().await += 1;
                self.signal_log.lock().await.push(signal.clone());

                // Place the order via the FIX order session
                let side_char = signal.side.chars().next().unwrap_or('1');
                let cl_ord_id = format!("ALGO_{}", uuid::Uuid::new_v4().simple());
                match order_session
                    .new_order_single(&cl_ord_id, &signal.symbol, side_char, signal.qty, signal.price)
                    .await
                {
                    Ok(()) => {
                        *self.orders_placed.lock().await += 1;
                        info!("🤖 ALGO ORDER SENT: {}", cl_ord_id);
                    }
                    Err(e) => {
                        warn!("🤖 ALGO ORDER FAILED: {}", e);
                    }
                }
            }

            tokio::time::sleep(interval).await;
        }
    }
}

// ─── Backtesting Engine ───────────────────────────────────────────────────────

/// A single market data tick used as input to the backtest.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Snapshot {
    pub timestamp_ms: i64,
    pub best_bid: f64,
    pub best_ask: f64,
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

/// Run the algo signal logic against a slice of historical snapshots.
///
/// This is a pure simulation: no network calls, no real orders. Fills are
/// assumed to occur at the signal price (zero slippage model).
pub fn run_backtest(cfg: &AlgoConfig, snapshots: &[Snapshot]) -> BacktestResult {
    let mut trades: Vec<BacktestTrade> = Vec::new();
    let mut equity_curve: Vec<[f64; 2]> = Vec::new();

    let mut net_qty: f64 = 0.0;
    let mut avg_entry_px: f64 = 0.0;
    let mut realized_pnl: f64 = 0.0;
    let mut last_mid_px: Option<f64> = None;

    // For Sharpe: collect per-trade P&L increments
    let mut pnl_increments: Vec<f64> = Vec::new();

    // For max drawdown: track running peak equity
    let mut peak_equity: f64 = 0.0;
    let mut max_drawdown: f64 = 0.0;

    // Downsample price series: at most 2000 points to keep payload small
    let price_step = (snapshots.len() / 2000).max(1);
    let price_series: Vec<[f64; 3]> = snapshots
        .iter()
        .enumerate()
        .filter(|(i, _)| i % price_step == 0)
        .map(|(_, s)| [s.timestamp_ms as f64, s.best_bid, s.best_ask])
        .collect();

    for snap in snapshots {
        if snap.best_bid <= 0.0 || snap.best_ask <= 0.0 || snap.best_ask <= snap.best_bid {
            continue;
        }
        let mid_px = (snap.best_bid + snap.best_ask) / 2.0;
        last_mid_px = Some(mid_px);

        let spread = snap.best_ask - snap.best_bid;
        let spread_bps = (spread / snap.best_bid) * 10_000.0;

        if spread_bps < cfg.min_spread_bps {
            let total_equity = realized_pnl + net_qty * (mid_px - avg_entry_px);
            if total_equity > peak_equity {
                peak_equity = total_equity;
            }
            let drawdown = peak_equity - total_equity;
            if drawdown > max_drawdown {
                max_drawdown = drawdown;
            }
            equity_curve.push([snap.timestamp_ms as f64, total_equity]);
            continue;
        }

        let signals = TradingAlgo::build_signals(
            cfg,
            snap.best_bid,
            snap.best_ask,
            net_qty,
            spread_bps,
            snap.timestamp_ms,
        );

        for sig in signals {
            let fill_price = sig.price;
            let fill_qty = sig.qty;
            let signed_qty = if sig.side == "1" { fill_qty } else { -fill_qty };
            let prev_net = net_qty;
            let new_net = prev_net + signed_qty;
            let mut trade_pnl: f64 = 0.0;

            if prev_net == 0.0 {
                // Opening a new position
                avg_entry_px = fill_price;
            } else if prev_net > 0.0 && signed_qty > 0.0 {
                // Adding to long
                let total_cost = avg_entry_px * prev_net + fill_price * fill_qty;
                avg_entry_px = total_cost / new_net;
            } else if prev_net < 0.0 && signed_qty < 0.0 {
                // Adding to short
                let total_cost = avg_entry_px * prev_net.abs() + fill_price * fill_qty;
                avg_entry_px = total_cost / new_net.abs();
            } else {
                // Closing or reversing
                let closing_qty = fill_qty.min(prev_net.abs());
                trade_pnl = if prev_net > 0.0 {
                    closing_qty * (fill_price - avg_entry_px)
                } else {
                    closing_qty * (avg_entry_px - fill_price)
                };
                realized_pnl += trade_pnl;
                if new_net.abs() < 1e-10 {
                    avg_entry_px = 0.0;
                } else if new_net.signum() != prev_net.signum() {
                    avg_entry_px = fill_price;
                }
            }

            net_qty = new_net;
            let cumulative_pnl = realized_pnl + net_qty * (mid_px - avg_entry_px);

            pnl_increments.push(trade_pnl);

            // Update drawdown
            if cumulative_pnl > peak_equity {
                peak_equity = cumulative_pnl;
            }
            let drawdown = peak_equity - cumulative_pnl;
            if drawdown > max_drawdown {
                max_drawdown = drawdown;
            }

            trades.push(BacktestTrade {
                timestamp_ms: snap.timestamp_ms,
                side: sig.side,
                price: fill_price,
                qty: fill_qty,
                pnl: trade_pnl,
                cumulative_pnl,
                net_qty_after: net_qty,
            });
        }

        // One equity point per snapshot, including unrealised mark-to-market.
        let total_equity = realized_pnl + net_qty * (mid_px - avg_entry_px);
        if total_equity > peak_equity {
            peak_equity = total_equity;
        }
        let drawdown = peak_equity - total_equity;
        if drawdown > max_drawdown {
            max_drawdown = drawdown;
        }
        equity_curve.push([snap.timestamp_ms as f64, total_equity]);
    }

    // Compute summary stats
    let winning_trades = trades.iter().filter(|t| t.pnl > 0.0).count();
    let losing_trades = trades.iter().filter(|t| t.pnl < 0.0).count();
    let total_trades = trades.len();
    let win_rate_pct = if total_trades > 0 {
        winning_trades as f64 / total_trades as f64 * 100.0
    } else {
        0.0
    };

    // Sharpe: mean / stddev of per-trade P&L increments (non-zero only)
    let nonzero: Vec<f64> = pnl_increments.iter().cloned().filter(|&v| v != 0.0).collect();
    let sharpe_ratio = if nonzero.len() >= 2 {
        let mean = nonzero.iter().sum::<f64>() / nonzero.len() as f64;
        let variance = nonzero.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / nonzero.len() as f64;
        let std_dev = variance.sqrt();
        if std_dev > 0.0 { mean / std_dev * (252_f64.sqrt()) } else { 0.0 }
    } else {
        0.0
    };

    let final_total_pnl = match last_mid_px {
        Some(mid_px) => realized_pnl + net_qty * (mid_px - avg_entry_px),
        None => realized_pnl,
    };

    BacktestResult {
        summary: BacktestSummary {
            total_trades,
            winning_trades,
            losing_trades,
            win_rate_pct,
            total_pnl: final_total_pnl,
            max_drawdown,
            sharpe_ratio,
            final_position: net_qty,
            snapshots_processed: snapshots.len(),
        },
        trades,
        equity_curve,
        price_series,
    }
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
        }
    }

    #[test]
    fn build_signals_single_side_and_correct_price_levels() {
        let cfg = test_cfg();
        let spread_bps = ((102.0 - 100.0) / 100.0) * 10_000.0;

        let even_ts = TradingAlgo::build_signals(&cfg, 100.0, 102.0, 0.0, spread_bps, 2);
        assert_eq!(even_ts.len(), 1);
        assert_eq!(even_ts[0].side, "1");
        assert!((even_ts[0].price - 100.5).abs() < 1e-9);

        let odd_ts = TradingAlgo::build_signals(&cfg, 100.0, 102.0, 0.0, spread_bps, 3);
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
        let sigs = TradingAlgo::build_signals(&cfg, 100.0, 102.0, 0.0, spread_bps, 2);
        assert_eq!(sigs.len(), 1);
        assert_eq!(sigs[0].side, "1");
        assert!((sigs[0].qty - 0.1).abs() < 1e-9);
    }

    #[test]
    fn run_backtest_marks_open_inventory_to_market() {
        let mut cfg = test_cfg();
        cfg.order_qty = 1.0;
        cfg.max_position = 1.0;

        // Single even timestamp => one BUY at 100.5, end mid is 102.0.
        let snapshots = vec![Snapshot {
            timestamp_ms: 2,
            best_bid: 100.0,
            best_ask: 104.0,
        }];

        let result = run_backtest(&cfg, &snapshots);
        assert_eq!(result.summary.total_trades, 1);
        assert!((result.summary.final_position - 1.0).abs() < 1e-9);
        assert!((result.summary.total_pnl - 1.5).abs() < 1e-9);
        assert_eq!(result.equity_curve.len(), 1);
    }

    #[test]
    fn run_backtest_has_at_most_one_trade_per_snapshot() {
        let cfg = test_cfg();
        let snapshots = vec![
            Snapshot { timestamp_ms: 2, best_bid: 100.0, best_ask: 102.0 },
            Snapshot { timestamp_ms: 3, best_bid: 100.0, best_ask: 102.0 },
            Snapshot { timestamp_ms: 4, best_bid: 100.0, best_ask: 102.0 },
        ];

        let result = run_backtest(&cfg, &snapshots);
        assert!(result.summary.total_trades <= snapshots.len());
        assert_eq!(result.equity_curve.len(), snapshots.len());
    }
}
