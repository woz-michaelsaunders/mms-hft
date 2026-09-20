# kdb+ Order Book Integration

This directory contains the kdb+ integration for storing order book data from the FIX server via direct IPC connection.

## Overview

The FIX server connects directly to a kdb+ server via IPC and writes order book data in real-time:
- `orderbook_levels` - Individual order book level updates (bids/asks), with live `seq_num`
- `trades` - Trade executions, with live `seq_num`
- `orderbook_snapshots` - Best bid/ask snapshots, with live `seq_num`
- `capture_gaps` - Inbound FIX sequence gaps and kdb buffer-overflow drops
- `securities` / `own_trades` - unchanged

## Quick Start

### 1. Start kdb+ Server

Start a kdb+ server that will receive the data:

```bash
cd /home/ubuntu/fix_server
q kdb/start_kdb_server.q
```

This will:
- Bind **loopback** `127.0.0.1:5001` by default (opt-out: `KDB_LISTEN=0.0.0.0`)
- Initialize the tables (including `seq_num` and `capture_gaps`)
- Reload `kdb/hdb/` if present
- Display connection status

To listen beyond loopback under systemd, do **not** rewrite `kdb-server.service`; use a drop-in:

```bash
sudo systemctl edit kdb-server
# [Service]
# Environment=KDB_LISTEN=0.0.0.0
```

### 2. Configure FIX Server

The FIX server connects to kdb+ using environment variables:
- `KDB_HOST` - kdb+ server host (default: `localhost`)
- `KDB_PORT` - kdb+ server port (default: `5001`)
- `KDB_RECONNECT_BACKOFF_MS` - initial IPC reconnect backoff (default `1000`, doubles to 30s, unbounded)
- `CAPTURE_SYMBOLS` - comma-separated persistence allow-list (default `BTC_USDT`)

Example:
```bash
export KDB_HOST=localhost
export KDB_PORT=5001
export CAPTURE_SYMBOLS=BTC_USDT
cd /home/ubuntu/fix_server
cargo run --release
```

`CAPTURE_SYMBOLS` gates **persistence only**. Unset → capture `BTC_USDT`. Empty or whitespace-only → kill switch (capture nothing). The Live tab / in-memory book is independent of this filter; venue subscription is unchanged.

### 3. Verify Connection

When the FIX server starts, you should see:
```
✅ kdb+ writer initialized. Connected to localhost:5001
✅ kdb+ tables initialized
```

The **first** `initialize()` is fatal: if kdb+ is not running at process start, the FIX server will not come up. After a successful start, a later IPC drop is retried (backoff ×2, cap 30s) instead of silently disabling writes.

## Data Flow

1. FIX server receives market data from exchange
2. Order book updates are applied in memory, then written to kdb+ via IPC **after** the book lock is released
3. Only symbols in `CAPTURE_SYMBOLS` are persisted
4. Inbound FIX sequence gaps are written to `capture_gaps` (not filtered by the allow-list)

## Table Schemas

### orderbook_levels
- `time` (timestamp) - Update timestamp
- `symbol` (symbol) - Trading symbol
- `side` (char) - 'B' for bid, 'A' for ask
- `price` (float) - Price level
- `size` (float) - Size at this level
- `action` (char) - 'N' for new, 'C' for change, 'D' for delete
- `seq_num` (long, nullable) - FIX `MsgSeqNum` for live rows; `0N` for history

### trades
- `time` (timestamp) - Trade timestamp
- `symbol` (symbol) - Trading symbol
- `price` (float) - Trade price
- `size` (float) - Trade size
- `seq_num` (long, nullable) - live FIX seq; `0N` for history

### orderbook_snapshots
- `time` (timestamp) - Snapshot timestamp
- `symbol` (symbol) - Trading symbol
- `best_bid` (float) - Best bid price
- `best_ask` (float) - Best ask price
- `best_bid_size` (float) - Best bid size
- `best_ask_size` (float) - Best ask size
- `spread` (float) - Bid-ask spread
- `seq_num` (long, nullable) - live FIX seq; `0N` for history

### capture_gaps
- `time` (timestamp)
- `symbol` (symbol) - FIX tag 55, else sender CompID; overflow mixed-symbol `"*"`
- `expected_seq` (long) - FIX: `last_applied+1` (≥1). Overflow during kdb outage: `0`
- `received_seq` (long) - FIX: received MsgSeqNum. Overflow: drop count
- `gap_size` (long) - `received - expected` (Rust saturating_sub). Overflow: drop count

How to read a quiet window vs a lost gap vs history:

| Observation | Meaning |
|---|---|
| FIX `capture_gaps` row (`expected_seq ≥ 1`) | Inbound sequence gap; recovery was requested |
| Overflow `capture_gaps` row (`expected_seq = 0`) | MD buffer dropped oldest half during a kdb outage |
| No snapshot rows **and** no `capture_gaps` row | Quiet market, kill switch, or symbol not in `CAPTURE_SYMBOLS` |
| Snapshot/trade rows with `null seq_num` | History backfill (`load_ticks_history.q`), not live capture |
| Gap-buffer overflow (logged warning only) | Oldest half of `capture_gaps` dropped; **not** recorded as a nested gap |

There is no dashboard Gaps tab; use the queries below.

## History load (`data/ticks.csv`)

One-shot, attach-only. kdb must already be running. Does **not** bind a port.

```bash
cd /home/ubuntu/fix_server
q kdb/load_ticks_history.q
```

- Streams CSV in ~100k-row / ~8MB windows (does not slurp 28M rows into the client)
- Every CSV row → `orderbook_snapshots` (BBO from bid/ask/sizes; spread `0` if either side is 0)
- Rows with `last_size > 0` also → `trades`
- `seq_num` is `0Nj` on every backfilled row (no FIX sequence for replayed history)
- Does **not** write `orderbook_levels` or `capture_gaps`
- Re-run **duplicates** rows (no dedup)
- The early-September 2026 hole in `data/ticks.csv` is a **time gap** in the source, not an error and not a `capture_gaps` row
- `kdb/load_orderbook.q` is a different CSV dialect (`kdb_data/*.csv`) and is **not** the ticks loader; do not run it against live `kdb/hdb/`

## Example Queries

### Latest snapshot for each symbol
```q
select by symbol from `time xdesc orderbook_snapshots
```

### All trades for a symbol
```q
select from trades where symbol=`BTC_USDT
```

### Order book updates for a symbol
```q
select from orderbook_levels where symbol=`BTC_USDT
```

### Best bid/ask over time
```q
select time, best_bid, best_ask, spread from orderbook_snapshots where symbol=`BTC_USDT
```

### Trade volume by symbol
```q
select sum size by symbol from trades
```

### Recent order book activity
```q
select from orderbook_levels where time > .z.p - 0D00:01:00  // Last minute
```

### FIX sequence gaps vs overflow
```q
select from capture_gaps where expected_seq>0   / FIX
select from capture_gaps where expected_seq=0   / kdb buffer overflow
```

### Live vs history snapshots
```q
select from orderbook_snapshots where symbol=`BTC_USDT, not null seq_num  / live
select from orderbook_snapshots where symbol=`BTC_USDT, null seq_num       / history
```

See also `kdb/query_examples.q`.

## Deploy / rollback

Binary + q schema ship in **one** `v*` tag. `release.yml` copies binaries only; q rides the git checkout. On deploy:

1. Restart **kdb** (`kdb/start_kdb_server.q`) **before** the new `fix_server` binary so `ensureCol` / `capture_gaps` exist
2. Then restart `fix_server`

Rollback is the previous `v*` tag: binary **and** q **and** a pre-upgrade copy of `kdb/hdb/`. A binary-only rollback against a widened HDB is not supported.

## Performance Notes

- **Real-time**: Data is written immediately via IPC - no file I/O latency
- **Efficient**: Direct IPC connection is much faster than CSV file writes
- **Reconnect**: Transient kdb outages are retried; MD+gap rows stay buffered (cap 10_000, oldest half dropped and recorded as `expected_seq=0`)
- **Scalable**: kdb+ can handle millions of inserts per second

## Troubleshooting

### Connection Failed at startup
If you see:
```
❌ FATAL: Failed to initialize kdb+ writer (localhost:5001): ...
```

**Solution**: Make sure kdb+ server is running **before** starting `fix_server`:
```bash
q kdb/start_kdb_server.q
```

### Port Already in Use
If port 5001 is already in use, change the port:
```bash
export KDB_PORT=5002
q -p 5002
```

### No Data Appearing
1. Check kdb+ server is running and listening (`127.0.0.1:5001` by default)
2. Verify FIX server connection logs
3. Check `CAPTURE_SYMBOLS` (empty = kill switch)
4. Query tables in kdb+ to verify structure:
```q
meta orderbook_levels
meta trades
meta orderbook_snapshots
meta capture_gaps
```

## Advanced: Custom kdb+ Server

You can customize the kdb+ server script (`start_kdb_server.q`) to:
- Add custom processing logic
- Set up ticker plant patterns
- Configure persistence to disk
- Add real-time analytics

Example with persistence:
```q
// Save to disk every minute
.z.ts: {orderbook_levels upsert .z.p; trades upsert .z.p; orderbook_snapshots upsert .z.p; `:/data/orderbook set orderbook_levels; `:/data/trades set trades; `:/data/snapshots set orderbook_snapshots};
\t 60000
```
