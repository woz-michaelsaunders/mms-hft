// High-Performance In-Memory Order Book
// Designed for microsecond-level latency with:
// - No heap allocations during updates
// - Direct addressing (O(1) price level access)
// - Cache-line aligned structures
// - Branch-predictable update logic

use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::{HashMap, VecDeque};

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
    // Cached best bid/ask for O(1) queries
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

    /// Adjust price base to center dense array around current price
    #[inline(always)]
    fn adjust_price_base(&mut self, price_cents: u64) {
        // Center the dense array around this price
        let new_base = price_cents.saturating_sub(self.dense_size as u64 / 2);
        if new_base != self.price_base {
            // Clear and recenter (in production, you'd migrate existing levels)
            self.price_base = new_base;
        }
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

        let (best_price, best_size) = match side {
            Side::Bid => {
                let dense_idx = self.get_dense_index(price_cents);
                if let Some(idx) = dense_idx {
                    self.bids[idx] = level;
                } else {
                    if size_scaled > 0 {
                        self.sparse_bids.insert(price_cents, level.unwrap());
                    } else {
                        self.sparse_bids.remove(&price_cents);
                    }
                }
                self.update_best_bid()
            }
            Side::Ask => {
                let dense_idx = self.get_dense_index(price_cents);
                if let Some(idx) = dense_idx {
                    self.asks[idx] = level;
                } else {
                    if size_scaled > 0 {
                        self.sparse_asks.insert(price_cents, level.unwrap());
                    } else {
                        self.sparse_asks.remove(&price_cents);
                    }
                }
                self.update_best_ask()
            }
        };

        // Update cached best prices
        match side {
            Side::Bid => {
                self.best_bid.store(best_price, Ordering::Relaxed);
                self.best_bid_size.store(best_size, Ordering::Relaxed);
            }
            Side::Ask => {
                self.best_ask.store(best_price, Ordering::Relaxed);
                self.best_ask_size.store(best_size, Ordering::Relaxed);
            }
        }
    }

    /// Update best bid by scanning dense array and sparse map
    #[inline(always)]
    fn update_best_bid(&self) -> (u64, u64) {
        let mut best_price = 0u64;
        let mut best_size = 0u64;

        // Scan dense array (highest price first)
        for i in (0..self.dense_size).rev() {
            if let Some(level) = &self.bids[i] {
                if level.size > 0 && level.price > best_price {
                    best_price = level.price;
                    best_size = level.size;
                }
            }
        }

        // Check sparse map
        for (price, level) in &self.sparse_bids {
            if level.size > 0 && *price > best_price {
                best_price = *price;
                best_size = level.size;
            }
        }

        (best_price, best_size)
    }

    /// Update best ask by scanning dense array and sparse map
    #[inline(always)]
    fn update_best_ask(&self) -> (u64, u64) {
        let mut best_price = u64::MAX;
        let mut best_size = 0u64;

        // Scan dense array (lowest price first)
        for i in 0..self.dense_size {
            if let Some(level) = &self.asks[i] {
                if level.size > 0 && level.price < best_price {
                    best_price = level.price;
                    best_size = level.size;
                }
            }
        }

        // Check sparse map
        for (price, level) in &self.sparse_asks {
            if level.size > 0 && *price < best_price {
                best_price = *price;
                best_size = level.size;
            }
        }

        if best_price == u64::MAX {
            (0, 0)
        } else {
            (best_price, best_size)
        }
    }

    /// Clear entire order book (for Empty Book message)
    #[inline(always)]
    pub fn clear(&mut self) {
        self.bids.fill(None);
        self.asks.fill(None);
        self.sparse_bids.clear();
        self.sparse_asks.clear();
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

