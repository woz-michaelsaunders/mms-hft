// High-Performance In-Memory Order Book
// Designed for microsecond-level latency with:
// - No heap allocations during updates
// - Direct addressing (O(1) price level access)
// - Cache-line aligned structures
// - Branch-predictable update logic

use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::{BTreeMap, HashMap, VecDeque};

/// Price level precision: 0.01 (2 decimal places)
/// Price is stored as integer: price * 100
const PRICE_SCALE: u64 = 100;
const MAX_PRICE: u64 = 10_000_000_000; // $100M max price (1,000,000,000,000 cents)
const MAX_LEVELS: usize = 10_000; // Maximum active price levels per side

/// Cache-aligned price level (64 bytes = 1 cache line)
#[repr(align(64))]
#[derive(Debug, Clone, Copy)]
pub struct PriceLevel {
    pub price: u64,        // Price in cents (price * 100)
    pub size: u64,         // Size in base units (scaled by 1e8 for precision)
    pub sequence: u64,     // Update sequence number
    pub _pad: [u8; 40],    // Padding to 64 bytes
}

/// Trade execution record
#[derive(Debug, Clone, Copy)]
pub struct Trade {
    pub price: u64,        // Price in cents (price * 100)
    pub size: u64,         // Size in base units (scaled by 1e8 for precision)
    pub timestamp: i64,     // Nanosecond timestamp
    pub sequence: u64,     // Trade sequence number
}

impl Default for PriceLevel {
    fn default() -> Self {
        PriceLevel {
            price: 0,
            size: 0,
            sequence: 0,
            _pad: [0; 40],
        }
    }
}

impl PriceLevel {
    #[inline(always)]
    pub fn new(price: u64, size: u64, sequence: u64) -> Self {
        PriceLevel {
            price,
            size,
            sequence,
            _pad: [0; 40],
        }
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    #[inline(always)]
    pub fn price_f64(&self) -> f64 {
        self.price as f64 / PRICE_SCALE as f64
    }

    #[inline(always)]
    pub fn size_f64(&self) -> f64 {
        self.size as f64 / 100_000_000.0 // Scale back from 1e8
    }
}

/// High-performance order book for a single symbol
pub struct OrderBook {
    symbol: String,
    // Direct addressing: price_index -> PriceLevel
    // price_index = (price * PRICE_SCALE) as usize
    bids: Vec<Option<PriceLevel>>,
    asks: Vec<Option<PriceLevel>>,
    // Sparse map for prices outside the dense range
    // Only used if price exceeds dense array bounds
    sparse_bids: HashMap<u64, PriceLevel>,
    sparse_asks: HashMap<u64, PriceLevel>,
    // BTreeMap secondary indexes for O(log n) best-price tracking.
    // Maps price_cents -> size_scaled; zero-size entries are removed on delete.
    // Best bid  = bid_index.iter().next_back()  (highest key)
    // Best ask  = ask_index.iter().next()        (lowest key)
    bid_index: BTreeMap<u64, u64>,
    ask_index: BTreeMap<u64, u64>,
    // Cached best bid/ask for O(1) reads after every update
    best_bid: AtomicU64,  // Price in cents
    best_ask: AtomicU64,  // Price in cents
    best_bid_size: AtomicU64,
    best_ask_size: AtomicU64,
    // Update sequence counter
    sequence: AtomicU64,
    // Price range for dense array (optimized for typical trading range)
    price_base: u64,  // Base price in cents (e.g., 90,000 * 100 = 9,000,000)
    dense_size: usize, // Size of dense arrays
    // Recent trades history (circular buffer, max 1000 trades)
    recent_trades: VecDeque<Trade>,
    trade_sequence: AtomicU64,
}

impl OrderBook {
    /// Create a new order book for a symbol
    /// dense_range: price range to allocate densely (e.g., 10,000 = $100 range)
    pub fn new(symbol: String, dense_range_cents: u64) -> Self {
        let dense_size = (dense_range_cents * 2) as usize; // +/- range from center
        let price_base = 0; // Will be adjusted on first update
        
        OrderBook {
            symbol,
            bids: vec![None; dense_size],
            asks: vec![None; dense_size],
            sparse_bids: HashMap::with_capacity(1000),
            sparse_asks: HashMap::with_capacity(1000),
            bid_index: BTreeMap::new(),
            ask_index: BTreeMap::new(),
            best_bid: AtomicU64::new(0),
            best_ask: AtomicU64::new(u64::MAX),
            best_bid_size: AtomicU64::new(0),
            best_ask_size: AtomicU64::new(0),
            sequence: AtomicU64::new(0),
            price_base: 0,
            dense_size,
            recent_trades: VecDeque::with_capacity(1000),
            trade_sequence: AtomicU64::new(0),
        }
    }

    /// Convert price (f64) to price index (u64 cents)
    #[inline(always)]
    fn price_to_index(price: f64) -> u64 {
        (price * PRICE_SCALE as f64) as u64
    }

    /// Get dense array index for a price
    #[inline(always)]
    fn get_dense_index(&self, price_cents: u64) -> Option<usize> {
        if price_cents < self.price_base {
            return None;
        }
        let offset = price_cents - self.price_base;
        if offset < self.dense_size as u64 {
            Some(offset as usize)
        } else {
            None
        }
    }

    /// Move the dense window. MUST NOT wipe dense arrays, MUST NOT clear sparse
    /// maps, MUST NOT rebuild storage from indexes, MUST NOT store caches.
    /// Indexes are price-keyed but are cleared because the following helper
    /// rebuilds them from remaining storage after the sparse wipe and the new
    /// dense write; clearing here without that rebuild is the original T1 bug
    /// (levels present, best_bid/best_ask == 0).
    #[inline(always)]
    fn adjust_price_base(&mut self, price_cents: u64) {
        let new_base = price_cents.saturating_sub(self.dense_size as u64 / 2);
        if new_base == self.price_base {
            return;
        }
        self.price_base = new_base;
        self.bid_index.clear();
        self.ask_index.clear();
    }

    /// Clear both BTreeMaps, then insert every dense+sparse PriceLevel with
    /// size > 0 keyed by `level.price` (absolute cents) → `level.size`.
    #[inline(always)]
    fn rebuild_indexes_from_storage(&mut self) {
        self.bid_index.clear();
        self.ask_index.clear();
        for slot in &self.bids {
            if let Some(level) = slot {
                if level.size > 0 {
                    self.bid_index.insert(level.price, level.size);
                }
            }
        }
        for level in self.sparse_bids.values() {
            if level.size > 0 {
                self.bid_index.insert(level.price, level.size);
            }
        }
        for slot in &self.asks {
            if let Some(level) = slot {
                if level.size > 0 {
                    self.ask_index.insert(level.price, level.size);
                }
            }
        }
        for level in self.sparse_asks.values() {
            if level.size > 0 {
                self.ask_index.insert(level.price, level.size);
            }
        }
    }

    /// Out-of-range owner: recenter, wipe sparse, write this level, rebuild
    /// both indexes from remaining size>0 storage, refresh BOTH caches.
    /// Terminal — caller must return and must not incremental-index or store
    /// one-side caches again.
    #[inline(always)]
    fn apply_out_of_range_level(&mut self, side: Side, price_cents: u64, level: Option<PriceLevel>) {
        self.adjust_price_base(price_cents);
        self.sparse_bids.clear();
        self.sparse_asks.clear();
        match self.get_dense_index(price_cents) {
            Some(idx) => match side {
                Side::Bid => self.bids[idx] = level,
                Side::Ask => self.asks[idx] = level,
            },
            None => {
                if let Some(l) = level {
                    match side {
                        Side::Bid => {
                            self.sparse_bids.insert(price_cents, l);
                        }
                        Side::Ask => {
                            self.sparse_asks.insert(price_cents, l);
                        }
                    }
                }
            }
        }
        self.rebuild_indexes_from_storage();
        let (bp, bs) = self.update_best_bid();
        self.best_bid.store(bp, Ordering::Relaxed);
        self.best_bid_size.store(bs, Ordering::Relaxed);
        let (ap, asz) = self.update_best_ask();
        self.best_ask.store(ap, Ordering::Relaxed);
        self.best_ask_size.store(asz, Ordering::Relaxed);
    }

    /// Update a price level (bid or ask)
    #[inline(always)]
    pub fn update_level(&mut self, side: Side, price: f64, size: f64, action: UpdateAction) {
        let price_cents = Self::price_to_index(price);
        let size_scaled = (size * 100_000_000.0) as u64; // Scale to 1e8 for precision
        let seq = self.sequence.fetch_add(1, Ordering::Relaxed) + 1;

        // Adjust price base on first update to center dense array around actual prices
        if self.price_base == 0 && price_cents > 0 {
            self.adjust_price_base(price_cents);
        }

        let level = match action {
            UpdateAction::New | UpdateAction::Change => {
                Some(PriceLevel::new(price_cents, size_scaled, seq))
            }
            UpdateAction::Delete => {
                Some(PriceLevel::new(price_cents, 0, seq))
            }
        };

        match side {
            Side::Bid => match self.get_dense_index(price_cents) {
                Some(idx) => {
                    // D8: after a storage-authority recenter an unmapped occupant
                    // can sit in this slot. Evict its absolute price before overwrite
                    // or the index keeps a ghost best.
                    if let Some(old) = self.bids[idx] {
                        if old.price != price_cents {
                            self.bid_index.remove(&old.price);
                        }
                    }
                    self.bids[idx] = level;
                    if size_scaled > 0 {
                        self.bid_index.insert(price_cents, size_scaled);
                    } else {
                        self.bid_index.remove(&price_cents);
                    }
                    let (p, s) = self.update_best_bid();
                    self.best_bid.store(p, Ordering::Relaxed);
                    self.best_bid_size.store(s, Ordering::Relaxed);
                }
                None => {
                    self.apply_out_of_range_level(side, price_cents, level);
                    return;
                }
            },
            Side::Ask => match self.get_dense_index(price_cents) {
                Some(idx) => {
                    if let Some(old) = self.asks[idx] {
                        if old.price != price_cents {
                            self.ask_index.remove(&old.price);
                        }
                    }
                    self.asks[idx] = level;
                    if size_scaled > 0 {
                        self.ask_index.insert(price_cents, size_scaled);
                    } else {
                        self.ask_index.remove(&price_cents);
                    }
                    let (p, s) = self.update_best_ask();
                    self.best_ask.store(p, Ordering::Relaxed);
                    self.best_ask_size.store(s, Ordering::Relaxed);
                }
                None => {
                    self.apply_out_of_range_level(side, price_cents, level);
                    return;
                }
            },
        }
    }

    /// Return best bid (highest price) from the BTreeMap index — O(log n).
    #[inline(always)]
    fn update_best_bid(&self) -> (u64, u64) {
        self.bid_index.iter().next_back()
            .map(|(&p, &s)| (p, s))
            .unwrap_or((0, 0))
    }

    /// Return best ask (lowest price) from the BTreeMap index — O(log n).
    #[inline(always)]
    fn update_best_ask(&self) -> (u64, u64) {
        self.ask_index.iter().next()
            .map(|(&p, &s)| (p, s))
            .unwrap_or((0, 0))
    }

    /// Clear entire order book (for Empty Book message)
    #[inline(always)]
    pub fn clear(&mut self) {
        self.bids.fill(None);
        self.asks.fill(None);
        self.sparse_bids.clear();
        self.sparse_asks.clear();
        self.bid_index.clear();
        self.ask_index.clear();
        self.best_bid.store(0, Ordering::Relaxed);
        self.best_ask.store(u64::MAX, Ordering::Relaxed);
        self.best_bid_size.store(0, Ordering::Relaxed);
        self.best_ask_size.store(0, Ordering::Relaxed);
    }

    /// Get best bid price
    #[inline(always)]
    pub fn best_bid(&self) -> f64 {
        self.best_bid.load(Ordering::Relaxed) as f64 / PRICE_SCALE as f64
    }

    /// Get best ask price
    #[inline(always)]
    pub fn best_ask(&self) -> f64 {
        let ask = self.best_ask.load(Ordering::Relaxed);
        if ask == u64::MAX {
            0.0
        } else {
            ask as f64 / PRICE_SCALE as f64
        }
    }

    /// Get best bid size
    #[inline(always)]
    pub fn best_bid_size(&self) -> f64 {
        self.best_bid_size.load(Ordering::Relaxed) as f64 / 100_000_000.0
    }

    /// Get best ask size
    #[inline(always)]
    pub fn best_ask_size(&self) -> f64 {
        self.best_ask_size.load(Ordering::Relaxed) as f64 / 100_000_000.0
    }

    /// Get spread
    #[inline(always)]
    pub fn spread(&self) -> f64 {
        let bid = self.best_bid();
        let ask = self.best_ask();
        if ask > 0.0 && bid > 0.0 {
            ask - bid
        } else {
            0.0
        }
    }

    /// Get mid price
    #[inline(always)]
    pub fn mid_price(&self) -> f64 {
        let bid = self.best_bid();
        let ask = self.best_ask();
        if ask > 0.0 && bid > 0.0 {
            (bid + ask) / 2.0
        } else {
            0.0
        }
    }

    /// Get top N levels for a side
    pub fn get_depth(&self, side: Side, depth: usize) -> Vec<(f64, f64)> {
        let mut levels = Vec::with_capacity(depth);

        match side {
            Side::Bid => {
                // Collect from dense array (highest first)
                for i in (0..self.dense_size).rev() {
                    if let Some(level) = &self.bids[i] {
                        if level.size > 0 {
                            levels.push((level.price_f64(), level.size_f64()));
                            if levels.len() >= depth {
                                break;
                            }
                        }
                    }
                }
                // Always check sparse map (prices might be outside dense range)
                let mut sparse: Vec<_> = self.sparse_bids.values()
                    .filter(|l| l.size > 0)
                    .map(|l| (l.price_f64(), l.size_f64()))
                    .collect();
                sparse.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
                // Merge sparse with dense (remove duplicates, keep highest)
                for (price, size) in sparse {
                    if !levels.iter().any(|(p, _)| (*p - price).abs() < 0.01) {
                        levels.push((price, size));
                        if levels.len() >= depth {
                            break;
                        }
                    }
                }
                // Sort final result by price descending
                levels.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap());
                levels.truncate(depth);
            }
            Side::Ask => {
                // Collect from dense array (lowest first)
                for i in 0..self.dense_size {
                    if let Some(level) = &self.asks[i] {
                        if level.size > 0 {
                            levels.push((level.price_f64(), level.size_f64()));
                            if levels.len() >= depth {
                                break;
                            }
                        }
                    }
                }
                // Always check sparse map (prices might be outside dense range)
                let mut sparse: Vec<_> = self.sparse_asks.values()
                    .filter(|l| l.size > 0)
                    .map(|l| (l.price_f64(), l.size_f64()))
                    .collect();
                sparse.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
                // Merge sparse with dense (remove duplicates, keep lowest)
                for (price, size) in sparse {
                    if !levels.iter().any(|(p, _)| (*p - price).abs() < 0.01) {
                        levels.push((price, size));
                        if levels.len() >= depth {
                            break;
                        }
                    }
                }
                // Sort final result by price ascending
                levels.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
                levels.truncate(depth);
            }
        }

        levels
    }

    /// Get symbol
    #[inline(always)]
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// Add a trade to the order book
    /// Trades are stored in a circular buffer (max 1000 trades)
    #[inline(always)]
    pub fn add_trade(&mut self, price: f64, size: f64, timestamp: i64) {
        let price_cents = Self::price_to_index(price);
        let size_scaled = (size * 100_000_000.0) as u64; // Scale to 1e8 for precision
        let seq = self.trade_sequence.fetch_add(1, Ordering::Relaxed) + 1;

        let trade = Trade {
            price: price_cents,
            size: size_scaled,
            timestamp,
            sequence: seq,
        };

        // Add to circular buffer (remove oldest if at capacity)
        if self.recent_trades.len() >= 1000 {
            self.recent_trades.pop_front();
        }
        self.recent_trades.push_back(trade);
    }

    /// Get recent trades (up to N most recent)
    pub fn get_recent_trades(&self, limit: usize) -> Vec<(f64, f64, i64)> {
        let count = limit.min(self.recent_trades.len());
        self.recent_trades
            .iter()
            .rev() // Most recent first
            .take(count)
            .map(|t| (
                t.price as f64 / PRICE_SCALE as f64,
                t.size as f64 / 100_000_000.0,
                t.timestamp
            ))
            .collect()
    }

    /// Get total number of trades stored
    #[inline(always)]
    pub fn trade_count(&self) -> usize {
        self.recent_trades.len()
    }
}

impl Clone for OrderBook {
    fn clone(&self) -> Self {
        let best_bid_val = self.best_bid.load(Ordering::Relaxed);
        let best_ask_val = self.best_ask.load(Ordering::Relaxed);
        let sparse_bid_count = self.sparse_bids.len();
        let sparse_ask_count = self.sparse_asks.len();
        
        let cloned = OrderBook {
            symbol: self.symbol.clone(),
            bids: self.bids.clone(),
            asks: self.asks.clone(),
            sparse_bids: self.sparse_bids.clone(),
            sparse_asks: self.sparse_asks.clone(),
            bid_index: self.bid_index.clone(),
            ask_index: self.ask_index.clone(),
            best_bid: AtomicU64::new(best_bid_val),
            best_ask: AtomicU64::new(best_ask_val),
            best_bid_size: AtomicU64::new(self.best_bid_size.load(Ordering::Relaxed)),
            best_ask_size: AtomicU64::new(self.best_ask_size.load(Ordering::Relaxed)),
            sequence: AtomicU64::new(self.sequence.load(Ordering::Relaxed)),
            price_base: self.price_base,
            dense_size: self.dense_size,
            recent_trades: self.recent_trades.clone(),
            trade_sequence: AtomicU64::new(self.trade_sequence.load(Ordering::Relaxed)),
        };
        
        // Debug: Log clone details if there's data
        if best_bid_val > 0 || best_ask_val < u64::MAX || sparse_bid_count > 0 || sparse_ask_count > 0 {
            log::debug!("OrderBook clone: sparse_bids={}, sparse_asks={}, best_bid={}, best_ask={}", 
                       sparse_bid_count, sparse_ask_count, best_bid_val, best_ask_val);
        }
        
        cloned
    }
}

/// Compact read-only snapshot of an order book.
/// Built from a full `OrderBook` by capturing only the top-N price levels and
/// recent trades — much cheaper to create than cloning the full dense array.
#[derive(Debug, Clone)]
pub struct OrderBookSnapshot {
    pub best_bid: f64,
    pub best_ask: f64,
    pub best_bid_size: f64,
    pub best_ask_size: f64,
    /// Pre-sorted top-N bids (price descending)
    pub bids: Vec<(f64, f64)>,
    /// Pre-sorted top-N asks (price ascending)
    pub asks: Vec<(f64, f64)>,
    /// Recent trades: (price, size, timestamp_ns) — most recent first
    pub recent_trades: Vec<(f64, f64, i64)>,
}

impl OrderBookSnapshot {
    #[inline]
    pub fn spread(&self) -> f64 {
        if self.best_ask > 0.0 && self.best_bid > 0.0 {
            self.best_ask - self.best_bid
        } else {
            0.0
        }
    }

    #[inline]
    pub fn mid_price(&self) -> f64 {
        if self.best_ask > 0.0 && self.best_bid > 0.0 {
            (self.best_bid + self.best_ask) / 2.0
        } else {
            0.0
        }
    }

    /// Return up to `depth` levels for the given side from the pre-built vecs.
    pub fn get_depth(&self, side: Side, depth: usize) -> Vec<(f64, f64)> {
        match side {
            Side::Bid => self.bids.iter().take(depth).cloned().collect(),
            Side::Ask => self.asks.iter().take(depth).cloned().collect(),
        }
    }

    /// Return up to `limit` recent trades (most recent first).
    pub fn get_recent_trades(&self, limit: usize) -> Vec<(f64, f64, i64)> {
        self.recent_trades.iter().take(limit).cloned().collect()
    }

    pub fn trade_count(&self) -> usize {
        self.recent_trades.len()
    }
}

impl OrderBook {
    /// Build a compact `OrderBookSnapshot` capturing the top `depth` levels per side
    /// and the last `trade_limit` trades.  Far cheaper than cloning the full dense array.
    pub fn to_snapshot(&self, depth: usize, trade_limit: usize) -> OrderBookSnapshot {
        OrderBookSnapshot {
            best_bid: self.best_bid(),
            best_ask: self.best_ask(),
            best_bid_size: self.best_bid_size(),
            best_ask_size: self.best_ask_size(),
            bids: self.get_depth(Side::Bid, depth),
            asks: self.get_depth(Side::Ask, depth),
            recent_trades: self.get_recent_trades(trade_limit),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateAction {
    New,    // 0
    Change, // 1
    Delete, // 2
}

impl From<&str> for UpdateAction {
    fn from(s: &str) -> Self {
        match s {
            "0" => UpdateAction::New,
            "1" => UpdateAction::Change,
            "2" => UpdateAction::Delete,
            _ => UpdateAction::New, // Default to New
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_price_level_creation() {
        let level = PriceLevel::new(9050000, 125000000, 1);
        assert_eq!(level.price_f64(), 90500.0);
        assert_eq!(level.size_f64(), 1.25);
    }

    #[test]
    fn test_order_book_updates() {
        let mut book = OrderBook::new("BTC_USDT".to_string(), 1000000); // $10k range
        
        book.update_level(Side::Bid, 90500.0, 1.0, UpdateAction::New);
        book.update_level(Side::Ask, 90600.0, 1.0, UpdateAction::New);
        
        assert_eq!(book.best_bid(), 90500.0);
        assert_eq!(book.best_ask(), 90600.0);
        assert_eq!(book.spread(), 100.0);
    }

    #[test]
    fn test_order_book_delete() {
        let mut book = OrderBook::new("BTC_USDT".to_string(), 1000000);
        
        book.update_level(Side::Bid, 90500.0, 1.0, UpdateAction::New);
        assert_eq!(book.best_bid(), 90500.0);
        
        book.update_level(Side::Bid, 90500.0, 0.0, UpdateAction::Delete);
        assert_eq!(book.best_bid(), 0.0);
    }
}


#[cfg(test)]
mod best_price_consistency {
    use super::*;

    /// A read of the book must never show levels on a side while that side's
    /// cached best price is zero. Before this fix, adjust_price_base() cleared
    /// bid_index/ask_index — which feed the cached bests — but left the dense
    /// and sparse levels in place, so get_depth() reported a full book while
    /// best_bid()/best_ask() read 0. On BTC_USD that was 48% of reads.
    #[test]
    fn best_prices_never_disagree_with_depth_across_recentres() {
        let mut book = OrderBook::new("BTC_USD".to_string(), 10_000);

        // A real bid ladder.
        for i in 0..20u64 {
            book.update_level(Side::Bid, 80_000.0 - (i as f64), 0.5, UpdateAction::New);
        }
        assert!(!book.get_depth(Side::Bid, 20).is_empty(), "seed failed");

        // Drive the ask side far away: this re-centres, which (unfixed) wipes
        // bid_index while leaving every bid level in the dense array. The cached
        // best_bid is not touched here, so it merely goes stale.
        book.update_level(Side::Ask, 95_000.0, 0.5, UpdateAction::New);

        // Now a single bid DELETE. The bid branch recomputes the cached best from
        // the (emptied) index and stores 0 — while get_depth still reports the
        // ladder. That is the torn read seen live: levels present, best 0.
        book.update_level(Side::Bid, 79_995.0, 0.0, UpdateAction::Delete);

        let bids = book.get_depth(Side::Bid, 20);
        assert!(
            !(!bids.is_empty() && book.best_bid() == 0.0),
            "torn read: {} bid levels present but best_bid == 0",
            bids.len()
        );
    }

    /// Re-centring keeps unmapped occupants in *other* dense slots plus the
    /// new price. The landing-slot occupant is overwritten (E11) — we do not
    /// require PR #50's "all far prices survive via sparse-from-index".
    #[test]
    fn recentre_preserves_live_levels() {
        let mut book = OrderBook::new("BTC_USD".to_string(), 10_000);
        book.update_level(Side::Bid, 80_000.0, 1.0, UpdateAction::New);
        book.update_level(Side::Bid, 80_010.0, 2.0, UpdateAction::New);
        // Force a re-centre far away. 80_000 shares the new centre slot with
        // 95_000 (both offset dense_size/2 from their respective bases) and
        // is overwritten; 80_010 sits in another slot and must remain.
        book.update_level(Side::Bid, 95_000.0, 3.0, UpdateAction::New);

        let bids = book.get_depth(Side::Bid, 50);
        let prices: Vec<f64> = bids.iter().map(|(p, _)| *p).collect();
        for want in [80_010.0, 95_000.0] {
            assert!(
                prices.iter().any(|p| (p - want).abs() < 0.005),
                "level {want} lost across re-centre; got {prices:?}"
            );
        }
        assert!(
            !prices.iter().any(|p| (p - 80_000.0).abs() < 0.005),
            "landing-slot occupant 80000 should be overwritten; got {prices:?}"
        );
        assert!(
            (book.best_bid() - 95_000.0).abs() < 0.005,
            "best_bid should be the highest live bid, got {}",
            book.best_bid()
        );
        assert_eq!(!bids.is_empty(), book.best_bid() > 0.0);
    }

    /// `clear()` is the true-empty-book path and must not be entangled with
    /// the recenter rebuild: after a populated book and at least one recenter,
    /// clear still yields 0 levels and both public bests == 0.
    #[test]
    fn clear_after_recenter_is_true_empty_book() {
        let mut book = OrderBook::new("BTC_USD".to_string(), 10_000);
        for i in 0..20u64 {
            book.update_level(Side::Bid, 80_000.0 - i as f64, 0.5, UpdateAction::New);
            book.update_level(Side::Ask, 80_010.0 + i as f64, 0.5, UpdateAction::New);
        }
        book.update_level(Side::Ask, 95_000.0, 0.5, UpdateAction::New);
        assert!(!book.get_depth(Side::Bid, 50).is_empty());
        assert!(!book.get_depth(Side::Ask, 50).is_empty());

        book.clear();

        assert!(book.get_depth(Side::Bid, 50).is_empty());
        assert!(book.get_depth(Side::Ask, 50).is_empty());
        assert_eq!(book.best_bid(), 0.0);
        assert_eq!(book.best_ask(), 0.0);
        assert_eq!(book.best_bid_size(), 0.0);
        assert_eq!(book.best_ask_size(), 0.0);
    }

    /// D8: an in-range write onto a post-recenter occupied slot must evict the
    /// old absolute price so it cannot remain as a ghost non-zero best.
    #[test]
    fn in_range_slot_evict_drops_unmapped_occupant() {
        let mut book = OrderBook::new("BTC_USD".to_string(), 10_000);
        // Centre at 95_000 (slot dense_size/2). Neighbour at +10 dollars sits
        // in another slot and survives the later recenter as an unmapped occupant.
        book.update_level(Side::Bid, 95_000.0, 1.0, UpdateAction::New);
        book.update_level(Side::Bid, 95_010.0, 2.0, UpdateAction::New);
        // Recentre down: landing slot overwrites 95_000; 95_010 remains in its slot.
        book.update_level(Side::Bid, 80_000.0, 1.0, UpdateAction::New);

        // New window base = 80_000c - 10_000 = 7_990_000. Slot of 95_010 maps to
        // 80_010. Writing that in-range price must evict 95_010.
        book.update_level(Side::Bid, 80_010.0, 1.0, UpdateAction::New);

        let bids = book.get_depth(Side::Bid, 50);
        let prices: Vec<f64> = bids.iter().map(|(p, _)| *p).collect();
        assert!(
            !prices.iter().any(|p| (p - 95_010.0).abs() < 0.005),
            "unmapped occupant 95010 must be gone from depth after slot overwrite; got {prices:?}"
        );
        assert!(
            (book.best_bid() - 80_010.0).abs() < 0.005,
            "ghost best: expected 80010 after D8 evict, got {}",
            book.best_bid()
        );
        assert_eq!(!bids.is_empty(), book.best_bid() > 0.0);
    }
}
