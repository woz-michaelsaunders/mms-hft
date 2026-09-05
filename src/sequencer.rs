// Sequence-gap detection and recovery state machine.
//
// FIX MsgSeqNum (34) is strictly increasing per session; the exchange
// resets both sides to 1 on every Logon (ResetSeqNumFlag=Y, see
// `FIXSession::logon_with_auth`). Every inbound message — market data
// incremental refreshes included — flows through here before its payload is
// allowed to touch an `OrderBook` or an order's state machine. This is the
// mandatory choke point: there is no path from "message decoded" to "book
// mutated" that skips a `Sequencer::check` call, so a dropped/out-of-order
// packet can never silently corrupt book state — it can only be dropped
// loudly (and counted) while recovery is requested.
//
// Keyed by an arbitrary session/stream identifier so one `Sequencer`
// instance can track multiple independent FIX sessions (order-entry session,
// market-data session) or, within a market-data session, multiple symbols if
// a venue provides per-symbol sequencing instead of session-level.

use std::collections::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceResult {
    /// seq == last_applied + 1: contiguous, safe to apply.
    Accepted,
    /// seq <= last_applied: already-seen message, drop without mutating state.
    DuplicateIgnored,
    /// seq > last_applied + 1: a gap opened. Recovery (ResendRequest /
    /// re-subscribe snapshot) must be requested by the caller; this key is
    /// now `recovering` and further `check` calls return
    /// `StaleDuringRecovery` until `resolve_recovery` is called.
    GapDetected { expected: u32, received: u32 },
    /// Already recovering from a prior gap on this key — this message
    /// arrived on top of a known-stale base and must be dropped, not
    /// applied, even if its own seq number looks locally contiguous.
    StaleDuringRecovery,
}

#[derive(Debug, Clone, Copy)]
struct SeqState {
    last_applied: u32,
    recovering: bool,
}

impl Default for SeqState {
    fn default() -> Self {
        SeqState { last_applied: 0, recovering: false }
    }
}

pub struct Sequencer {
    state: HashMap<String, SeqState>,
}

impl Sequencer {
    pub fn new() -> Self {
        Sequencer { state: HashMap::new() }
    }

    /// (Re-)initialize tracking for `key`, e.g. right after a fresh Logon
    /// ACK, since this venue resets MsgSeqNum to 1 on every session.
    pub fn reset(&mut self, key: &str) {
        self.state.insert(key.to_string(), SeqState::default());
    }

    pub fn is_recovering(&self, key: &str) -> bool {
        self.state.get(key).map(|s| s.recovering).unwrap_or(false)
    }

    pub fn last_applied(&self, key: &str) -> u32 {
        self.state.get(key).map(|s| s.last_applied).unwrap_or(0)
    }

    /// Validate the sequence number of an inbound message for `key`.
    /// Only ever advances `last_applied` on `Accepted` — a gap or a stale
    /// message during recovery leaves state untouched so the eventual
    /// `resolve_recovery` call has an unambiguous starting point.
    pub fn check(&mut self, key: &str, seq: u32) -> SequenceResult {
        let state = self.state.entry(key.to_string()).or_default();

        if state.recovering {
            return SequenceResult::StaleDuringRecovery;
        }

        if seq == state.last_applied + 1 {
            state.last_applied = seq;
            SequenceResult::Accepted
        } else if seq <= state.last_applied {
            SequenceResult::DuplicateIgnored
        } else {
            state.recovering = true;
            SequenceResult::GapDetected { expected: state.last_applied + 1, received: seq }
        }
    }

    /// Resolve an in-progress recovery: a full snapshot (or a
    /// SequenceReset-GapFill covering the missing range) has brought `key`
    /// up to date as of `resync_seq`. This is the only way `recovering`
    /// clears, so normal processing can never silently resume on a stale
    /// base.
    pub fn resolve_recovery(&mut self, key: &str, resync_seq: u32) {
        let state = self.state.entry(key.to_string()).or_default();
        state.last_applied = resync_seq;
        state.recovering = false;
    }
}

impl Default for Sequencer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::order_book::{OrderBook, Side, UpdateAction};

    #[test]
    fn accepts_contiguous_sequence() {
        let mut seq = Sequencer::new();
        assert_eq!(seq.check("md", 1), SequenceResult::Accepted);
        assert_eq!(seq.check("md", 2), SequenceResult::Accepted);
        assert_eq!(seq.check("md", 3), SequenceResult::Accepted);
        assert_eq!(seq.last_applied("md"), 3);
        assert!(!seq.is_recovering("md"));
    }

    #[test]
    fn duplicate_is_ignored_without_mutating_state() {
        let mut seq = Sequencer::new();
        seq.check("md", 1);
        seq.check("md", 2);
        assert_eq!(seq.check("md", 2), SequenceResult::DuplicateIgnored);
        assert_eq!(seq.check("md", 1), SequenceResult::DuplicateIgnored);
        assert_eq!(seq.last_applied("md"), 2);
    }

    #[test]
    fn gap_triggers_recovery_and_drops_subsequent_stale_messages() {
        let mut seq = Sequencer::new();
        seq.check("md", 1);
        seq.check("md", 2);

        // seq 3, 4 dropped on the wire; seq 5 arrives next.
        assert_eq!(seq.check("md", 5), SequenceResult::GapDetected { expected: 3, received: 5 });
        assert!(seq.is_recovering("md"));

        // Everything else arriving while recovering is stale, even if it
        // looks locally sane (6 directly follows the 5 that opened the gap).
        assert_eq!(seq.check("md", 6), SequenceResult::StaleDuringRecovery);
        assert_eq!(seq.check("md", 7), SequenceResult::StaleDuringRecovery);
        // last_applied must NOT have advanced past the last good message.
        assert_eq!(seq.last_applied("md"), 2);

        // A snapshot resync arrives covering up through seq 7.
        seq.resolve_recovery("md", 7);
        assert!(!seq.is_recovering("md"));
        assert_eq!(seq.check("md", 8), SequenceResult::Accepted);
    }

    #[test]
    fn out_of_order_delivery_does_not_silently_corrupt_the_order_book() {
        // Full pipeline: decode -> sequencer -> (maybe) OrderBook::apply.
        // Only Accepted results are allowed to reach the book; everything
        // else must be dropped by the caller. This proves that property
        // holds even when messages are re-ordered/duplicated/dropped on the
        // wire, which is the actual failure mode AC2 guards against.
        let mut seq = Sequencer::new();
        let mut book = OrderBook::new("BTC_USDT".to_string(), 1_000_000);

        let wire = [
            (1u32, 100.0, 1.0),
            (3, 102.0, 1.0), // out of order / gap: 2 missing -> recovery
            (2, 101.0, 1.0), // late arrival of the "missing" one, now stale
            (4, 103.0, 1.0), // also stale, recovery still open
        ];

        for (n, price, size) in wire {
            match seq.check("md:BTC_USDT", n) {
                SequenceResult::Accepted => {
                    book.update_level(Side::Bid, price, size, UpdateAction::New);
                }
                SequenceResult::GapDetected { .. } | SequenceResult::StaleDuringRecovery | SequenceResult::DuplicateIgnored => {
                    // Dropped by design — must not touch the book.
                }
            }
        }

        // Only seq 1 was ever applied; the book must reflect exactly that,
        // not any interleaving of the out-of-order/stale updates.
        assert_eq!(book.best_bid(), 100.0);
        assert!(seq.is_recovering("md:BTC_USDT"));

        // Recovery resolves via a fresh full snapshot (the real recovery
        // path: re-subscribe / MarketDataSnapshotFullRefresh), which clears
        // and repopulates the book atomically.
        book.clear();
        book.update_level(Side::Bid, 103.0, 2.0, UpdateAction::New);
        seq.resolve_recovery("md:BTC_USDT", 4);

        assert_eq!(book.best_bid(), 103.0);
        assert!(!seq.is_recovering("md:BTC_USDT"));
        assert_eq!(seq.check("md:BTC_USDT", 5), SequenceResult::Accepted);
    }
}
