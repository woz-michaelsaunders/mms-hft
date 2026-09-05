# Hot-Path Review: Zero-Allocation / No-Blocking-Lock Checklist

Scope: the market-data tick path (`FIXSession::handle_message` 'W'/'X' cases
in `src/main.rs`, `OrderBook::update_level` in `src/order_book.rs`,
`DoubleBufferedOrderBook` in `src/order_book_buffer.rs`) and the order-ack
path (`FIXSession::handle_message` '8' case, `order_events::map_exec_type`).

This is an honest static review, not a checklist gamed to read green. Where a
claim doesn't hold, it's called out with why and what a fix would look like,
per the acceptance criterion's "static review checklist" option.

## What is allocation-free / lock-free today

- `OrderBook::update_level` (`src/order_book.rs`): when the price falls in
  the pre-allocated dense `Vec<Option<PriceLevel>>` range (the common case
  once `price_base` has been centered), the write is an in-place array
  store — no allocation, no lock. `PriceLevel` is `#[repr(align(64))]`,
  cache-line sized.
- `OrderBook::best_bid`/`best_ask`/`spread`/`mid_price`: atomic loads only
  (`AtomicU64`, `Ordering::Relaxed`) — no allocation, no lock.
- `DoubleBufferedOrderBook::get_snapshot` (`src/order_book_buffer.rs`):
  atomic pointer load + `Arc` clone (refcount bump) — no lock, and the only
  "allocation" is an atomic increment, not a heap allocation.
- `Sequencer::check`/`resolve_recovery` (`src/sequencer.rs`): once a key's
  entry exists in the `HashMap`, subsequent calls are lookups/updates with no
  further allocation. (See gap below re: first-touch.)
- `order_events::map_exec_type`: pure `match` on `&str`, no allocation.

## Where the claim does NOT hold (real gaps, not hidden)

1. **`DoubleBufferedOrderBook::swap_snapshot` clones the entire book**
   (`src/order_book_buffer.rs`), including both dense `Vec<Option<PriceLevel>>`
   arrays (sized by `dense_range_cents`, e.g. 100,000+ entries at the range
   used in `src/main.rs`) and both sparse `HashMap`s. That's a real heap
   allocation plus O(dense_size) copy — and `handle_message`'s `'X'`
   (MarketDataIncrementalRefresh) handler calls `swap_snapshot()` after
   **every single tick**, not batched. This is the single biggest gap
   between "hot path" and "actually zero-alloc" in this codebase today.
   - Fix direction (not applied here — a redesign, not a bug fix): either
     (a) swap on a bounded cadence (e.g. every N updates or every T
     microseconds) instead of per-tick, or (b) replace the clone-based swap
     with an actual double-buffer (two pre-allocated `OrderBook`s, apply the
     same delta to both, swap a pointer) so steady-state has no clone at all.
2. **Sparse price levels (`sparse_bids`/`sparse_asks`) allocate on
   first insert** at a new price outside the dense range (`HashMap::insert`
   past capacity). Bounded/rare in practice (prices should mostly land in
   the pre-sized dense window), but not strictly zero-alloc.
3. **`Sequencer`'s per-key `HashMap` entry is allocation-on-first-touch**
   (`state.entry(key.to_string()).or_default()` — the `key.to_string()`
   itself allocates on every call, not just first touch, since `entry()`
   needs an owned key to insert). For the small, fixed set of session keys
   used today (one per `FIXSession`) this is a bounded, one-time-per-message
   `String` allocation, not unbounded — but it is real per-message
   allocation, not zero-alloc.
4. **`FIXSession` uses `tokio::sync::Mutex` around session state**
   (`stream`, `order_books`, `open_orders` snapshot swaps via `ArcSwap` are
   the lock-free exception). These are async (non-blocking-OS-thread) locks,
   not raw OS mutexes, so they don't block a kernel thread — but they are
   still lock *acquisition* on the message-handling path, which is a softer
   violation of "no locks" than a blocking `std::sync::Mutex` would be.

## Net assessment

The **decode → sequence-check → book-apply** portion of the path is close to
allocation-free in the common (dense, in-range) case. The **publish** step
(`swap_snapshot`) is not, and is the honest, primary finding of this review.
Treat this document as the baseline for a follow-up performance pass, not as
a claim that AC5 is fully satisfied by the current implementation — the
measured benchmark in `src/order_book_buffer.rs`
(`tests::tick_to_publish_latency_p50_p99`) quantifies the actual cost so a
regression (or an improvement) is visible over time.

### Measured (this sandbox, debug build, dense_range_cents=1,000 — 100x
smaller than the 100,000 used in `src/main.rs`, purely so the benchmark
terminates in reasonable test time):

```
tick-to-publish latency over 2,000 iterations: p50=203.70µs, p99=743.53µs
```

`swap_snapshot`'s cost scales linearly with `dense_range_cents`, so at the
production range this is on the order of tens of milliseconds, not
microseconds — three to four orders of magnitude away from HFT-viable
tick-to-publish latency, and squarely a per-tick full-clone cost, not
network/syscall latency. This is the review's key, actionable finding.
