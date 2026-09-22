// In-process alerting for stale books, reconnect storms, and sequence gaps.
//
// Operator-visible channel is a greppable `error!("ALERT[...]")` line emitted
// by `FIXSession::emit_alert`. This module owns the closed kinds, the bounded
// per-session buffer helpers, allowlisted detail builders (SEC-002), and the
// reconnect-storm rolling-window FSM (ADR-005). No transport, no HTTP, no kdb.

use std::collections::VecDeque;
use std::fmt;

pub const ALERT_BUFFER_CAP: usize = 200;
pub const RECONNECT_STORM_THRESHOLD: usize = 3;
pub const RECONNECT_STORM_WINDOW_MS: u64 = 300_000; // 5 minutes

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertKind {
    StaleBook,
    ReconnectStorm,
    SequenceGap,
}

impl fmt::Display for AlertKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AlertKind::StaleBook => f.write_str("StaleBook"),
            AlertKind::ReconnectStorm => f.write_str("ReconnectStorm"),
            AlertKind::SequenceGap => f.write_str("SequenceGap"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Alert {
    pub kind: AlertKind,
    pub session: String,
    pub detail: String,
    pub at_ms: u64,
}

pub fn push_bounded(buf: &mut VecDeque<Alert>, alert: Alert) {
    if buf.len() >= ALERT_BUFFER_CAP {
        let _ = buf.pop_front();
    }
    buf.push_back(alert);
}

pub fn sanitize_alert_text(s: &str) -> String {
    s.replace(['\n', '\r'], " ")
}

pub fn stale_book_detail(silent_ms: u64) -> String {
    format!("silent_ms={silent_ms}")
}

pub fn sequence_gap_detail(expected: u32, received: u32, msg_type: char) -> String {
    format!("expected={expected} received={received} msg_type={msg_type}")
}

pub fn reconnect_storm_detail(count: usize) -> String {
    format!(
        "count={count} threshold={RECONNECT_STORM_THRESHOLD} window_ms={RECONNECT_STORM_WINDOW_MS}"
    )
}

pub struct ReconnectStormTracker {
    ever_connected: bool,
    timestamps_ms: VecDeque<u64>,
}

impl ReconnectStormTracker {
    pub fn new() -> Self {
        ReconnectStormTracker {
            ever_connected: false,
            timestamps_ms: VecDeque::new(),
        }
    }

    /// Record a successful TCP connect. The first-ever connect is ignored.
    /// Subsequent connects are reconnects: prune the rolling window, push
    /// `now_ms`, and return `Some(len)` only on the edge that `len` equals
    /// `RECONNECT_STORM_THRESHOLD` (edge 3→hot). Sync, no I/O, no log.
    pub fn record(&mut self, now_ms: u64) -> Option<usize> {
        if !self.ever_connected {
            self.ever_connected = true;
            return None;
        }
        while let Some(&ts) = self.timestamps_ms.front() {
            if now_ms.saturating_sub(ts) > RECONNECT_STORM_WINDOW_MS {
                self.timestamps_ms.pop_front();
            } else {
                break;
            }
        }
        self.timestamps_ms.push_back(now_ms);
        let len = self.timestamps_ms.len();
        if len == RECONNECT_STORM_THRESHOLD {
            Some(len)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_is_exact_variant_name() {
        assert_eq!(AlertKind::StaleBook.to_string(), "StaleBook");
        assert_eq!(AlertKind::ReconnectStorm.to_string(), "ReconnectStorm");
        assert_eq!(AlertKind::SequenceGap.to_string(), "SequenceGap");
    }

    #[test]
    fn sanitize_replaces_cr_lf_with_space() {
        assert_eq!(sanitize_alert_text("a\nb\rc"), "a b c");
        assert_eq!(sanitize_alert_text("clean"), "clean");
        assert_eq!(sanitize_alert_text("line\n"), "line ");
    }

    #[test]
    fn builder_strings_are_allowlisted() {
        assert_eq!(stale_book_detail(45_000), "silent_ms=45000");
        assert_eq!(
            sequence_gap_detail(3, 5, 'X'),
            "expected=3 received=5 msg_type=X"
        );
        assert_eq!(
            reconnect_storm_detail(3),
            "count=3 threshold=3 window_ms=300000"
        );
    }

    #[test]
    fn builders_never_emit_secret_substrings() {
        let details = [
            stale_book_detail(553),
            sequence_gap_detail(553, 554, 'X'),
            reconnect_storm_detail(3),
        ];
        for s in details {
            assert!(!s.contains("api_key"), "detail leaked api_key: {s}");
            assert!(!s.contains("api_secret"), "detail leaked api_secret: {s}");
            assert!(!s.contains("553="), "detail leaked 553=: {s}");
            assert!(!s.contains("554="), "detail leaked 554=: {s}");
            assert!(!s.contains("signature"), "detail leaked signature: {s}");
        }
    }

    fn dummy_alert(n: u64) -> Alert {
        Alert {
            kind: AlertKind::StaleBook,
            session: "s".to_string(),
            detail: n.to_string(),
            at_ms: n,
        }
    }

    #[test]
    fn push_bounded_drops_oldest_at_cap_201() {
        let mut buf = VecDeque::new();
        for i in 0..201u64 {
            push_bounded(&mut buf, dummy_alert(i));
        }
        assert_eq!(buf.len(), ALERT_BUFFER_CAP);
        assert_eq!(buf.front().unwrap().detail, "1");
        assert_eq!(buf.back().unwrap().detail, "200");
    }

    #[test]
    fn record_fsm_first_tcp_ignored_third_reconnect_fires_fourth_does_not() {
        let mut tracker = ReconnectStormTracker::new();
        assert_eq!(tracker.record(0), None, "first TCP must not count");
        assert_eq!(tracker.record(10), None, "reconnect 1");
        assert_eq!(tracker.record(20), None, "reconnect 2");
        assert_eq!(tracker.record(30), Some(3), "reconnect 3 fires");
        assert_eq!(tracker.record(40), None, "reconnect 4 stays hot, no re-fire");
    }

    #[test]
    fn record_fsm_prune_then_recross_fires_again() {
        let mut tracker = ReconnectStormTracker::new();
        assert_eq!(tracker.record(0), None);
        assert_eq!(tracker.record(10), None);
        assert_eq!(tracker.record(20), None);
        assert_eq!(tracker.record(30), Some(3));
        assert_eq!(tracker.record(40), None);

        let later = 40 + RECONNECT_STORM_WINDOW_MS + 1;
        assert_eq!(tracker.record(later), None, "window pruned to 1");
        assert_eq!(tracker.record(later), None, "reconnect 2 in new window");
        assert_eq!(
            tracker.record(later),
            Some(3),
            "re-crossing threshold fires again"
        );
    }
}
