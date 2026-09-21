//! Binary-only poll loop. Lives here because `FIXSession` is a binary type.
//! `algo.rs` must not name `FIXSession`, `ResolvedCredentials`, or `new_order_single`.

use std::sync::Arc;
use chrono::Utc;
use log::{info, warn};
use fix_server::algo;
use crate::{FIXSession, PositionsStore};

pub async fn run(
    algo: Arc<algo::TradingAlgo>,
    order_session: Arc<FIXSession>,
    market_session: Arc<FIXSession>,
    positions: Arc<PositionsStore>,
) {
    info!("🤖 Trading algo task started");

    let mut last_signal_ms: i64 = 0;

    loop {
        let cfg = algo.config.lock().await.clone();
        let interval = tokio::time::Duration::from_millis(cfg.poll_interval_ms);

        if !cfg.enabled {
            tokio::time::sleep(interval).await;
            continue;
        }

        // Read live order book for the configured symbol
        let (best_bid, best_ask, bids, asks) = {
            let books = market_session.order_books.lock().await;
            books.get(&cfg.symbol)
                .map(|db| {
                    let snap = db.get_snapshot();
                    let depth = cfg.imbalance_depth_levels.max(1);
                    let bids = snap.bids.iter().take(depth).cloned().collect::<Vec<_>>();
                    let asks = snap.asks.iter().take(depth).cloned().collect::<Vec<_>>();
                    (snap.best_bid, snap.best_ask, bids, asks)
                })
                .unwrap_or_default()
        };

        // Get current net position
        let net_position = positions
            .snapshot()
            .into_iter()
            .find(|p| p.symbol == cfg.symbol)
            .map(|p| p.net_qty)
            .unwrap_or(0.0);

        // Stale-feed guard: never quote on a frozen/stale order book.
        let now_ms = Utc::now().timestamp_millis();
        if cfg.max_book_staleness_ms > 0 {
            let last_md = market_session
                .last_md_recv_ms
                .load(std::sync::atomic::Ordering::Relaxed) as i64;
            let age = if last_md == 0 { i64::MAX } else { now_ms - last_md };
            if age > cfg.max_book_staleness_ms as i64 {
                warn!("Order book STALE ({} ms) - suppressing algo entries", age);
                tokio::time::sleep(interval).await;
                continue;
            }
        }

        // PAPER/SHADOW MODE: simulate fills against the live book; never send
        // a real order. Produces a live paper P&L the moment a two-sided
        // (production) feed is connected.
        if algo.paper_mode.load(std::sync::atomic::Ordering::Relaxed) {
            algo.paper_step(best_bid, best_ask, &bids, &asks, &cfg).await;
            tokio::time::sleep(interval).await;
            continue;
        }

        // Enforce signal cooldown — skip evaluation if fired too recently
        if cfg.min_signal_interval_ms > 0
            && (now_ms - last_signal_ms) < cfg.min_signal_interval_ms as i64
        {
            tokio::time::sleep(interval).await;
            continue;
        }

        let signals = algo::TradingAlgo::evaluate_signal(&cfg, best_bid, best_ask, &bids, &asks, net_position);

        for signal in signals {
            info!(
                "🤖 ALGO SIGNAL: {} {} qty={} px={:.4} | {}",
                if signal.side == "1" { "BUY" } else { "SELL" },
                signal.symbol,
                signal.qty,
                signal.price,
                signal.reason
            );

            last_signal_ms = Utc::now().timestamp_millis();
            *algo.signals_fired.lock().await += 1;
            algo.signal_log.lock().await.push(signal.clone());

            // Circuit breaker: stop placing once max_orders_session is hit.
            if cfg.max_orders_session > 0
                && *algo.orders_placed.lock().await >= cfg.max_orders_session
            {
                warn!(
                    "max_orders_session ({}) reached - halting order placement",
                    cfg.max_orders_session
                );
                continue;
            }

            // Place the order via the FIX order session
            let side_char = signal.side.chars().next().unwrap_or('1');
            let cl_ord_id = format!("ALGO_{}", uuid::Uuid::new_v4().simple());
            match order_session
                .new_order_single(
                    &cl_ord_id,
                    &signal.symbol,
                    side_char,
                    signal.qty,
                    signal.price,
                    cfg.self_match_prevention_scope.clone(),
                    cfg.self_match_prevention_instruction.clone(),
                    cfg.self_match_prevention_id.clone(),
                )
                .await
            {
                Ok(()) => {
                    *algo.orders_placed.lock().await += 1;
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
