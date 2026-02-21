# kdb+ Order Book Integration

This directory contains the kdb+ integration for storing order book data from the FIX server via direct IPC connection.

## Overview

The FIX server connects directly to a kdb+ server via IPC and writes order book data in real-time:
- `orderbook_levels` - Individual order book level updates (bids/asks)
- `trades` - Trade executions
- `orderbook_snapshots` - Best bid/ask snapshots

## Quick Start

### 1. Start kdb+ Server

Start a kdb+ server that will receive the data:

```bash
cd /home/ubuntu/fix_server
q kdb/start_kdb_server.q
```

This will:
- Start kdb+ listening on port 5001
- Initialize the three tables
- Display connection status

### 2. Configure FIX Server

The FIX server connects to kdb+ using environment variables:
- `KDB_HOST` - kdb+ server host (default: `localhost`)
- `KDB_PORT` - kdb+ server port (default: `5001`)

Example:
```bash
export KDB_HOST=localhost
export KDB_PORT=5001
cd /home/ubuntu/fix_server
cargo run --release
```

### 3. Verify Connection

When the FIX server starts, you should see:
```
✅ kdb+ writer initialized. Connected to localhost:5001
✅ kdb+ tables initialized
```

If kdb+ is not running, you'll see a warning but the server will continue without kdb+ integration.

## Data Flow

1. FIX server receives market data from exchange
2. Order book updates are written directly to kdb+ via IPC in real-time
3. Data appears immediately in kdb+ tables - no file I/O overhead

## Table Schemas

### orderbook_levels
- `time` (timestamp) - Update timestamp
- `symbol` (symbol) - Trading symbol
- `side` (char) - 'B' for bid, 'A' for ask
- `price` (float) - Price level
- `size` (float) - Size at this level
- `action` (char) - 'N' for new, 'C' for change, 'D' for delete

### trades
- `time` (timestamp) - Trade timestamp
- `symbol` (symbol) - Trading symbol
- `price` (float) - Trade price
- `size` (float) - Trade size

### orderbook_snapshots
- `time` (timestamp) - Snapshot timestamp
- `symbol` (symbol) - Trading symbol
- `best_bid` (float) - Best bid price
- `best_ask` (float) - Best ask price
- `best_bid_size` (float) - Best bid size
- `best_ask_size` (float) - Best ask size
- `spread` (float) - Bid-ask spread

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

## Performance Notes

- **Real-time**: Data is written immediately via IPC - no file I/O latency
- **Efficient**: Direct IPC connection is much faster than CSV file writes
- **Reliable**: Connection failures are handled gracefully (server continues without kdb+)
- **Scalable**: kdb+ can handle millions of inserts per second

## Troubleshooting

### Connection Failed
If you see:
```
Failed to initialize kdb+ writer (localhost:5001): ...
```

**Solution**: Make sure kdb+ server is running:
```bash
q -p 5001
```

### Port Already in Use
If port 5001 is already in use, change the port:
```bash
export KDB_PORT=5002
q -p 5002
```

### No Data Appearing
1. Check kdb+ server is running and listening
2. Verify FIX server connection logs
3. Check that market data is being received
4. Query tables in kdb+ to verify structure:
```q
meta orderbook_levels
meta trades
meta orderbook_snapshots
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
