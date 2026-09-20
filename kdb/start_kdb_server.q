// kdb+ Server Script
// Start kdb+ server to receive order book data via IPC
// Usage: q start_kdb_server.q

// Set port (default 5001)
port: 5001

// HDB (Historical Database) path for disk persistence
// Tables are saved here periodically and reloaded on startup
hdbPath: `:kdb/hdb

// Initialize tables (empty schemas — may be overwritten by loadTables below)
orderbook_levels: ([] time:`timestamp$(); symbol:`symbol$(); side:`char$(); price:`float$(); size:`float$(); action:`char$(); seq_num:`long$());
trades: ([] time:`timestamp$(); symbol:`symbol$(); price:`float$(); size:`float$(); seq_num:`long$());
orderbook_snapshots: ([] time:`timestamp$(); symbol:`symbol$(); best_bid:`float$(); best_ask:`float$(); best_bid_size:`float$(); best_ask_size:`float$(); spread:`float$(); seq_num:`long$());
securities: ([] time:`timestamp$(); symbol:`symbol$(); security_id:`symbol$(); exchange:`symbol$(); attributes_json:());
// Own execution fills — populated from FIX execution reports (ExecType F/2)
own_trades: ([] time:`timestamp$(); symbol:`symbol$(); side:`char$(); last_px:`float$(); last_qty:`float$(); avg_px:`float$(); cl_ord_id:`symbol$(); order_id:`symbol$(); exec_type:`char$());
capture_gaps: ([] time:`timestamp$(); symbol:`symbol$(); expected_seq:`long$(); received_seq:`long$(); gap_size:`long$());

// Load existing HDB data from disk (called at startup)
loadTables: {
    system "mkdir -p kdb/hdb";
    tableNames: `orderbook_levels`trades`orderbook_snapshots`securities`own_trades`capture_gaps;
    {[n]
        p: hdbPath,n;
        // key on a non-existent path returns a null in some kdb+ versions — test carefully
        if[not @[{count key x; 1b}; p; {0b}]; :[]];
        @[{[nm;pth] value (string nm), ": load `", (1_string pth)}; (n;p); {[e] show "Could not load table: ", e}]
    } each tableNames;
    };

loadTables[];

// Widen existing HDB tables with nullable seq_num. Typed scalar `long$0N (never `long$()).
// Byte-identical with Q_ENSURE_COL_SEQ_NUM in src/kdb_integration.rs.
ensureCol:{[t;c] if[not c in cols t; t set ![t;();0b;(enlist c)!enlist `long$0N]];};
ensureCol[`orderbook_levels;`seq_num];
ensureCol[`trades;`seq_num];
ensureCol[`orderbook_snapshots;`seq_num];

// Save all tables to HDB path
saveTables: {
    show "Saving tables to disk...";
    @[save; hdbPath,`orderbook_levels;     {[e] show "Error saving orderbook_levels: ",     e}];
    @[save; hdbPath,`trades;               {[e] show "Error saving trades: ",               e}];
    @[save; hdbPath,`orderbook_snapshots;  {[e] show "Error saving orderbook_snapshots: ",  e}];
    @[save; hdbPath,`securities;           {[e] show "Error saving securities: ",           e}];
    @[save; hdbPath,`own_trades;           {[e] show "Error saving own_trades: ",           e}];
    @[save; hdbPath,`capture_gaps;         {[e] show "Error saving capture_gaps: ",         e}];
    show "Tables saved to disk.";
    };

// Tick counter — .z.ts fires every 30 s; save every 2 ticks (1 minute)
saveTickCount: 0;
saveEveryNTicks: 2;

// Note: g# (grouped) attribute is intentionally NOT applied here.
// On large tables (millions of rows) g# reapplication is O(n) expensive and causes
// high CPU usage. Plain scans are fast enough for the query volumes here.
applyIndexes: {[]};  // no-op — kept for compatibility

// Modified insert functions that maintain indexes
insertLevel: {[t;s;sd;p;sz;a;seq]
    orderbook_levels insert enlist (t; s; sd; p; sz; a; seq);
    };

insertTrade: {[t;s;p;sz;seq]
    trades insert enlist (t; s; p; sz; seq);
    };

insertSnapshot: {[t;s;bb;ba;bbs;bas;sp;seq]
    orderbook_snapshots insert enlist (t; s; bb; ba; bbs; bas; sp; seq);
    };

// Function to handle incoming data (if using .u.upd pattern)
// For now, we'll use direct inserts from the FIX server

// Note: insertLevel, insertTrade, insertSnapshot are defined above with index maintenance

// Periodic function: maintain indexes every 30 s, save to disk every 5 min
.z.ts: {
    applyIndexes[];
    saveTickCount+:1;
    if[0=saveTickCount mod saveEveryNTicks; saveTables[]]
    };

// Set timer to run every 30 seconds (30000 milliseconds)
system "t 30000";

// Bind loopback by default. IPC password is empty (QStream::connect password "").
// kdb cannot read the FIX process's KDB_HOST. To listen beyond loopback
// (only when operators set Rust KDB_HOST to a remote address they control):
//   KDB_LISTEN=0.0.0.0 q kdb/start_kdb_server.q
// systemd: systemctl edit kdb-server  →  [Service] Environment=KDB_LISTEN=0.0.0.0
// Do not require .z.pw.
listenAddr: $[(l:getenv`KDB_LISTEN)~""; "127.0.0.1"; l]
system "p ", listenAddr, ":", string port
show "kdb+ server listening on ", listenAddr, ":", string port
show "Tables initialized:";
show "  - orderbook_levels (plain scan; g# intentionally disabled)";
show "  - trades (plain scan; g# intentionally disabled)";
show "  - orderbook_snapshots (plain scan; g# intentionally disabled)";
show "  - securities (plain scan; g# intentionally disabled)";
show "  - own_trades (own execution fills; plain scan)";
show "  - capture_gaps (FIX seq gaps expected_seq>=1; buffer overflow expected_seq=0)";
show "Persistence: tables saved to kdb/hdb/ every minute and reloaded on startup";

// Example queries:
// select count by symbol from own_trades
// select from own_trades where symbol=`BTC_USDT
// select sum last_qty, avg avg_px by symbol, side from own_trades  // position summary
// select from trades where symbol=`BTC_USDT
// select from orderbook_snapshots where symbol=`BTC_USDT

