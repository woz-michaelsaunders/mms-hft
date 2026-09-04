// Lock-Free Double-Buffered Order Book
// HFT pattern: active book for writes, compact snapshot for reads
// Atomic Arc swapping for lock-free snapshotting

use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::ptr;
use crate::order_book::{OrderBook, OrderBookSnapshot};

/// Snapshot depth: number of bid/ask levels captured per swap.
/// Only these levels are ever consumed by the REST API and ZMQ publisher.
const SNAPSHOT_DEPTH: usize = 50;
/// Number of recent trades captured in each snapshot.
const SNAPSHOT_TRADE_LIMIT: usize = 100;

/// Double-buffered order book container.
///
/// Pattern:
/// - Active book (`*mut OrderBook`): only the market data thread writes (no locks in hot path).
/// - Snapshot (`Arc<AtomicPtr<OrderBookSnapshot>>`): readers get a lock-free stable view.
/// - Swap: builds a compact `OrderBookSnapshot` (top-50 levels + 100 trades) instead of
///   cloning the full dense array, keeping swap cost proportional to depth, not book size.
pub struct DoubleBufferedOrderBook {
    active: *mut OrderBook,
    snapshot: Arc<AtomicPtr<OrderBookSnapshot>>,
    symbol: String,
}

impl DoubleBufferedOrderBook {
    pub fn new(symbol: String, dense_range_cents: u64) -> Self {
        let active_box = Box::new(OrderBook::new(symbol.clone(), dense_range_cents));
        let active = Box::into_raw(active_box);

        let initial = unsafe { (*active).to_snapshot(SNAPSHOT_DEPTH, SNAPSHOT_TRADE_LIMIT) };
        let snapshot = Arc::new(AtomicPtr::new(
            Arc::into_raw(Arc::new(initial)) as *mut OrderBookSnapshot,
        ));

        DoubleBufferedOrderBook { active, snapshot, symbol }
    }

    /// Mutable reference to the active book (hot path — no locks).
    /// Safe because only the market data thread calls this.
    #[inline(always)]
    pub fn get_active_mut(&mut self) -> &mut OrderBook {
        unsafe { &mut *self.active }
    }

    /// Immutable reference to the active book.
    #[inline(always)]
    pub fn get_active(&self) -> &OrderBook {
        unsafe { &*self.active }
    }

    /// Build a compact snapshot from the current active book and atomically publish it.
    /// Cost is proportional to `SNAPSHOT_DEPTH`, not the size of the dense array.
    pub fn swap_snapshot(&mut self) {
        let active_ref = unsafe { &*self.active };
        let new_snap = Arc::new(active_ref.to_snapshot(SNAPSHOT_DEPTH, SNAPSHOT_TRADE_LIMIT));
        let new_ptr = Arc::into_raw(new_snap) as *mut OrderBookSnapshot;
        let old_ptr = self.snapshot.swap(new_ptr, Ordering::AcqRel);
        if !old_ptr.is_null() {
            unsafe { drop(Arc::from_raw(old_ptr as *const OrderBookSnapshot)) };
        }
    }

    /// Lock-free read of the current snapshot via Arc reference-count clone.
    pub fn get_snapshot(&self) -> Arc<OrderBookSnapshot> {
        let ptr = self.snapshot.load(Ordering::Acquire);
        unsafe {
            let arc = Arc::from_raw(ptr as *const OrderBookSnapshot);
            let cloned = arc.clone();
            std::mem::forget(arc);
            cloned
        }
    }

    pub fn symbol(&self) -> &str {
        &self.symbol
    }
}

impl Drop for DoubleBufferedOrderBook {
    fn drop(&mut self) {
        unsafe { drop(Box::from_raw(self.active)) };
        let ptr = self.snapshot.swap(ptr::null_mut(), Ordering::AcqRel);
        if !ptr.is_null() {
            unsafe { drop(Arc::from_raw(ptr as *const OrderBookSnapshot)) };
        }
    }
}

unsafe impl Send for DoubleBufferedOrderBook {}
unsafe impl Sync for DoubleBufferedOrderBook {}

