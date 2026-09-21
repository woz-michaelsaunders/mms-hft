//! kdb query helpers shared by the calibrate HTTP path and `shadow_report`.
//! Timestamp offset is owned by `kdb_integration` — do not reimplement it.

use crate::algo;
use crate::kdb_integration;

/// Checked conversion. `None` on i64 overflow.
pub fn unix_ms_to_kdb_ns(ms: i64) -> Option<i64> {
    ms.checked_mul(1_000_000)
        .and_then(|ns| ns.checked_sub(kdb_integration::KDB_TIMESTAMP_OFFSET_NS))
}

/// The ONLY q timestamp fragment.
pub fn kdb_ts_atom(n: i64) -> String {
    format!("`timestamp${}j", n)
}

/// Sibling of the nested helper in `order_book_web_server`. Never concatenate a raw symbol.
pub fn kdb_symbol_predicate(symbol: &str) -> String {
    format!(
        "symbol=`$\"{}\"",
        kdb_integration::escape_kdb_string(symbol)
    )
}

/// `^[A-Za-z0-9_]+$` (BTC_USDT passes). Used on JSONL rows AND the spawn symbol before q.
pub fn is_allowlisted_symbol(symbol: &str) -> bool {
    !symbol.is_empty() && symbol.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Same mapper as today's `main.rs` snapshots_from_kdb_json.
pub fn snapshots_from_kdb_json(raw_json: &str) -> Vec<algo::Snapshot> {
    serde_json::from_str::<serde_json::Value>(raw_json)
        .ok()
        .and_then(|v| v.as_array().cloned())
        .unwrap_or_default()
        .iter()
        .filter_map(|row| {
            let timestamp_ms = row.get("time").and_then(|v| v.as_i64())?;
            let best_bid = row.get("best_bid").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let best_ask = row.get("best_ask").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let best_bid_size = row.get("best_bid_size").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let best_ask_size = row.get("best_ask_size").and_then(|v| v.as_f64()).unwrap_or(0.0);
            if best_bid > 0.0 && best_ask > 0.0 {
                Some(algo::Snapshot {
                    timestamp_ms,
                    best_bid,
                    best_ask,
                    best_bid_size,
                    best_ask_size,
                    ..Default::default()
                })
            } else {
                None
            }
        })
        .collect()
}
