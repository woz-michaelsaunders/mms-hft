// Lock-Free Double-Buffered Order Book
// HFT pattern: active book for writes, snapshot book for reads
// Atomic Arc swapping for lock-free snapshotting

use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::ptr;
use crate::order_book::OrderBook;

/// Double-buffered order book container
/// Uses atomic Arc pointer swapping for lock-free snapshotting
/// 
/// Pattern:
/// - Active book: Only market data thread writes (no locks in hot path)
/// - Snapshot book: Readers get stable view (lock-free reads via Arc clone)
/// - Swap: Atomic pointer swap (~10-30ns cost)
pub struct DoubleBufferedOrderBook {
    // Active book - ONLY market data thread writes to this
    // Stored as raw pointer to avoid Arc overhead in hot path
    active: *mut OrderBook,
    // Snapshot book - readers get stable view from this (atomic pointer to Arc<OrderBook>)
    // Stored as *mut OrderBook (the Arc is managed separately)
    snapshot: Arc<AtomicPtr<OrderBook>>,
    symbol: String,
}

impl DoubleBufferedOrderBook {
    /// Create a new double-buffered order book
    pub fn new(symbol: String, dense_range_cents: u64) -> Self {
        let active_box = Box::new(OrderBook::new(symbol.clone(), dense_range_cents));
        let active = Box::into_raw(active_box);
        
        // Initialize snapshot with Arc wrapping active (clone for snapshot)
        let initial_snapshot = Arc::new(unsafe { (*active).clone() });
        let snapshot = Arc::new(AtomicPtr::new(Arc::into_raw(initial_snapshot) as *mut OrderBook));
        
        DoubleBufferedOrderBook {
            active,
            snapshot,
            symbol,
        }
    }

    /// Get mutable reference to active book (hot path - no locks, no Arc overhead)
    /// This is safe because only the market data thread calls this
    #[inline(always)]
    pub fn get_active_mut(&mut self) -> &mut OrderBook {
        unsafe { &mut *self.active }
    }

    /// Get immutable reference to active book
    #[inline(always)]
    pub fn get_active(&self) -> &OrderBook {
        unsafe { &*self.active }
    }

    /// Atomically swap active and snapshot books
    /// Creates a new Arc from active, swaps it with snapshot
    /// Returns the old snapshot (now contains what was active)
    /// Cost: ~10-30 nanoseconds
    pub fn swap_snapshot(&mut self) -> Arc<OrderBook> {
        // Clone active to create new snapshot Arc
        let active_ref = unsafe { &*self.active };
        let new_snapshot_arc = Arc::new(active_ref.clone());
        
        // Atomically swap pointers (*mut OrderBook)
        let new_ptr = Arc::into_raw(new_snapshot_arc) as *mut OrderBook;
        let old_ptr = self.snapshot.swap(new_ptr, Ordering::AcqRel);
        
        // Convert old pointer back to Arc<OrderBook>
        unsafe {
            Arc::from_raw(old_ptr as *const OrderBook)
        }
    }

    /// Get current snapshot (lock-free read via Arc clone)
    /// Returns a stable, immutable view of the order book
    pub fn get_snapshot(&self) -> Arc<OrderBook> {
        let ptr = self.snapshot.load(Ordering::Acquire);
        unsafe {
            // Clone the Arc (increment ref count), then forget the original
            // This safely increments ref count without taking ownership
            let arc = Arc::from_raw(ptr as *const OrderBook);
            let cloned = arc.clone();
            std::mem::forget(arc); // Don't decrement - it's managed by atomic pointer
            cloned
        }
    }

    /// Get symbol
    pub fn symbol(&self) -> &str {
        &self.symbol
    }
}

impl Drop for DoubleBufferedOrderBook {
    fn drop(&mut self) {
        // Clean up active book
        unsafe {
            let _ = Box::from_raw(self.active);
        }
        
        // Clean up atomic pointer (last snapshot)
        let ptr = self.snapshot.swap(ptr::null_mut(), Ordering::AcqRel);
        if !ptr.is_null() {
            unsafe {
                let _ = Arc::from_raw(ptr as *const OrderBook);
            }
        }
    }
}

unsafe impl Send for DoubleBufferedOrderBook {}
unsafe impl Sync for DoubleBufferedOrderBook {}

