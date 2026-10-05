//! Binary-only poll loop. Lives here because `FIXSession` is a binary type.
//! `algo.rs` must not name `FIXSession`, `ResolvedCredentials`, or `new_order_single`.

use std::sync::Arc;
use chrono::Utc;
use log::{info, warn};
use fix_server::algo;
use crate::{FIXSession, PositionsStore};

/// What the stale-feed guard decided about the order book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BookFreshness {
    /// Updated within the limit — safe to quote.
    Fresh,
    /// No market data has EVER arrived on this session. `last_md_recv_ms` is
    /// also reset to 0 on reconnect, so this is the normal state immediately
    /// after a restart, not an error.
    NeverSeen,
    /// Last update is older than the limit, by this many ms.
    Stale(i64),
}

/// Classify the book without logging, so the decision can be tested.
///
/// Fail-closed on NeverSeen: an unknown book must never be quoted on. The
/// previous code expressed that by setting age to i64::MAX, which was correct
/// but rendered as "9223372036854775807 ms" in the log and read like a bug.
pub(crate) fn classify_book(last_md_ms: i64, now_ms: i64, limit_ms: u64) -> BookFreshness {
    if last_md_ms == 0 {
        return BookFreshness::NeverSeen;
    }
    let age = now_ms - last_md_ms;
    if age > limit_ms as i64 {
        BookFreshness::Stale(age)
    } else {
        BookFreshness::Fresh
    }
}

/// Latch that turns a per-poll verdict into state CHANGES only.
///
/// The guard runs every poll_interval_ms (default 1000) and a sparse venue is
/// stale most of the time, so logging each verdict produced 26,262 identical
/// warnings in one UAT log — on a box that rotates logs every few minutes.
/// That volume destroys the evidence around real incidents.
#[derive(Debug, Default)]
pub(crate) struct StaleLatch {
    stale: bool,
}

impl StaleLatch {
    /// Returns (suppress_entries, message_to_log_if_any).
    pub(crate) fn step(&mut self, state: BookFreshness) -> (bool, Option<String>) {
        match state {
            BookFreshness::Fresh => {
                if self.stale {
                    self.stale = false;
                    (false, Some("Order book no longer stale - resuming algo entries".to_string()))
                } else {
                    (false, None)
                }
            }
            BookFreshness::NeverSeen => {
                if self.stale {
                    (true, None)
                } else {
                    self.stale = true;
                    (true, Some(
                        "Order book STALE: no market data received yet - suppressing algo entries"
                            .to_string(),
                    ))
                }
            }
            BookFreshness::Stale(age) => {
                if self.stale {
                    (true, None)
                } else {
                    self.stale = true;
                    (true, Some(format!(
                        "Order book STALE: last update {age} ms ago - suppressing algo entries"
                    )))
                }
            }
        }
    }
}

pub async fn run(
    algo: Arc<algo::TradingAlgo>,
    order_session: Arc<FIXSession>,
    market_session: Arc<FIXSession>,
    positions: Arc<PositionsStore>,
) {
    info!("🤖 Trading algo task started");

    let mut last_signal_ms: i64 = 0;

    // Latch so the stale-book guard logs state CHANGES, not every poll.
    let mut stale_latch = StaleLatch::default();


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
            let verdict = classify_book(last_md, now_ms, cfg.max_book_staleness_ms);
            let (suppress, msg) = stale_latch.step(verdict);
            if let Some(m) = msg {
                if suppress {
                    warn!("{}", m);
                } else {
                    info!("{}", m);
                }
            }
            if suppress {
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

#[cfg(test)]
mod stale_book_guard {
    use super::*;

    /// Fail-closed when nothing has arrived. `last_md_recv_ms` is reset to 0 on
    /// reconnect, so this is also the normal state right after a restart.
    #[test]
    fn no_data_yet_is_never_seen_not_a_huge_age() {
        assert_eq!(classify_book(0, 1_000_000, 2000), BookFreshness::NeverSeen);
    }

    #[test]
    fn fresh_within_the_limit_and_stale_beyond_it() {
        assert_eq!(classify_book(999_000, 1_000_000, 2000), BookFreshness::Fresh);
        assert_eq!(classify_book(990_000, 1_000_000, 2000), BookFreshness::Stale(10_000));
        // Exactly at the limit is still fresh; the guard uses `>`.
        assert_eq!(classify_book(998_000, 1_000_000, 2000), BookFreshness::Fresh);
    }

    /// THE POINT OF THIS CHANGE. The guard runs every poll_interval_ms (default
    /// 1000) and a sparse venue is stale most of the time, so logging each
    /// verdict produced 26,262 identical warnings in one UAT log — on a box
    /// that rotates logs every few minutes, destroying the evidence around real
    /// incidents. Only transitions may log.
    #[test]
    fn logs_the_transition_not_every_poll() {
        let mut l = StaleLatch::default();

        // Going stale logs once.
        let (sup, msg) = l.step(BookFreshness::Stale(5000));
        assert!(sup, "must suppress entries while stale");
        assert!(msg.is_some(), "the transition into stale must log");

        // Staying stale logs nothing, however long it lasts.
        for _ in 0..1000 {
            let (sup, msg) = l.step(BookFreshness::Stale(9999));
            assert!(sup);
            assert!(msg.is_none(), "repeated stale polls must not log");
        }

        // Recovering logs once.
        let (sup, msg) = l.step(BookFreshness::Fresh);
        assert!(!sup, "must allow entries once fresh");
        assert!(msg.is_some(), "recovery must log");

        // Staying fresh logs nothing.
        for _ in 0..1000 {
            let (sup, msg) = l.step(BookFreshness::Fresh);
            assert!(!sup);
            assert!(msg.is_none(), "repeated fresh polls must not log");
        }
    }

    /// The sentinel must never reach a log line as a duration.
    #[test]
    fn never_seen_message_does_not_print_a_sentinel() {
        let mut l = StaleLatch::default();
        let (sup, msg) = l.step(BookFreshness::NeverSeen);
        assert!(sup, "an unknown book must never be quoted on");
        let m = msg.expect("first NeverSeen must log");
        assert!(m.contains("no market data received yet"), "got: {m}");
        assert!(!m.contains("9223372036854775807"), "sentinel leaked into the log: {m}");
        assert!(!m.contains(&i64::MAX.to_string()), "sentinel leaked into the log: {m}");
    }

    /// A flapping feed logs each genuine change, not each poll.
    #[test]
    fn flapping_logs_each_change_only() {
        let mut l = StaleLatch::default();
        let mut logs = 0;
        for state in [
            BookFreshness::Fresh,            // no log (already fresh)
            BookFreshness::Stale(3000),      // log
            BookFreshness::Stale(4000),      // no log
            BookFreshness::Fresh,            // log
            BookFreshness::Fresh,            // no log
            BookFreshness::NeverSeen,        // log
            BookFreshness::NeverSeen,        // no log
        ] {
            if l.step(state).1.is_some() {
                logs += 1;
            }
        }
        assert_eq!(logs, 3, "expected one line per state change");
    }
}
