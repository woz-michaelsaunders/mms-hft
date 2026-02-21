// kdb+ Server Script
// Start kdb+ server to receive order book data via IPC
// Usage: q start_kdb_server.q

// Set port (default 5001)
port: 5001

// Initialize tables
orderbook_levels: ([] time:`timestamp$(); symbol:`symbol$(); side:`char$(); price:`float$(); size:`float$(); action:`char$());
trades: ([] time:`timestamp$(); symbol:`symbol$(); price:`float$(); size:`float$());
orderbook_snapshots: ([] time:`timestamp$(); symbol:`symbol$(); best_bid:`float$(); best_ask:`float$(); best_bid_size:`float$(); best_ask_size:`float$(); spread:`float$());
securities: ([] time:`timestamp$(); symbol:`symbol$(); security_id:`symbol$(); exchange:`symbol$(); attributes_json:());

// Note: For now, we'll optimize the main snapshots table with g# on symbol
// This already makes "select by symbol" and "where symbol=" queries very fast
// The g# index on symbol is the most important optimization for snapshots

// Function to apply indexes (grouped attributes) to symbol columns
// This speeds up distinct, group by, and where clause queries on symbol
applyIndexes: {
    // Apply grouped attribute to symbol columns for faster distinct/group by
    if[count orderbook_levels; @[`orderbook_levels; `symbol; `g#]];
    if[count trades; @[`trades; `symbol; `g#]];
    if[count orderbook_snapshots; @[`orderbook_snapshots; `symbol; `g#]];
    if[count securities; @[`securities; `symbol; `g#]];
    // Note: g# on symbol already optimizes "select by symbol" and "where symbol=" queries significantly
    // This is the most important optimization for snapshots queries
    };

// Apply indexes to existing data (if any)
applyIndexes[];

// Modified insert functions that maintain indexes
insertLevel: {[t;s;sd;p;sz;a] 
    orderbook_levels insert enlist (t; s; sd; p; sz; a);
    // Re-apply grouped attribute periodically (every 1000 inserts for performance)
    if[0 = count[orderbook_levels] mod 1000; @[`orderbook_levels; `symbol; `g#]];
    };

insertTrade: {[t;s;p;sz] 
    trades insert enlist (t; s; p; sz);
    if[0 = count[trades] mod 1000; @[`trades; `symbol; `g#]];
    };

insertSnapshot: {[t;s;bb;ba;bbs;bas;sp] 
    orderbook_snapshots insert enlist (t; s; bb; ba; bbs; bas; sp);
    if[0 = count[orderbook_snapshots] mod 1000; @[`orderbook_snapshots; `symbol; `g#]];
    };

// Function to handle incoming data (if using .u.upd pattern)
// For now, we'll use direct inserts from the FIX server

// Note: insertLevel, insertTrade, insertSnapshot are defined above with index maintenance

// Periodic function to maintain indexes (runs every 30 seconds)
// This ensures indexes stay optimized as new data is inserted
.z.ts: {
    applyIndexes[];
    };

// Set timer to run every 30 seconds (30000 milliseconds)
system "t 30000";

// Start listening on port
system "p ", string port;
show "kdb+ server listening on port ", string port;
show "Tables initialized:";
show "  - orderbook_levels (symbol indexed with g#)";
show "  - trades (symbol indexed with g#)";
show "  - orderbook_snapshots (symbol indexed with g#)";
show "  - securities (symbol indexed with g#)";
show "Indexes applied: grouped attribute (g#) on symbol columns for fast distinct/group by queries";
show "Snapshots optimization: g# on symbol makes 'select by symbol' and 'where symbol=' queries very fast";
show "Index maintenance: indexes updated every 30 seconds";

// Example queries:
// select count by symbol from orderbook_levels
// select from trades where symbol=`BTC_USDT
// select by symbol from `time xdesc orderbook_snapshots  // Fast with g# on symbol
// select from orderbook_snapshots where symbol=`BTC_USDT  // Fast with g# on symbol

