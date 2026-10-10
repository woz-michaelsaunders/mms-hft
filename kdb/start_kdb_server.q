// kdb+ Server Script
// Start kdb+ server to receive order book data via IPC
// Usage: q start_kdb_server.q

// Listen port, from KDB_PORT so this file is IDENTICAL for every instance.
//
// It was a hardcoded literal, which forced one hand-edited copy per instance:
// this file said 5001 while production ran 5002. Two divergent copies of a
// script that two live services load at startup is the same drift surface that
// lost eight fixes between 2026-09-27 and 2026-10-07, and it kept the script
// out of scripts/check-drift.sh, because a byte comparison could not pass for
// both instances at once.
//
// The port now comes from the environment, set per instance in the systemd
// unit, so every copy matches the repository byte for byte.
port: $[count p: getenv `KDB_PORT; "I" $ p; 5001i]

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
        p: .Q.dd[hdbPath;n];
        // key on a non-existent path returns a null in some kdb+ versions — test carefully
        if[not @[{count key x; 1b}; p; {0b}]; :[]];
        // `load `:path` BOTH reads the file AND defines the global named after
        // it, then returns that name as a symbol. Two things therefore must not
        // be done here:
        //
        //   1_string pth  stripped the leading ":", so the expression became
        //                 `load `kdb/hdb/orderbook_levels` -> 'ERROR: /',
        //                 swallowed by the error handler below. Nothing was
        //                 ever restored, silently.
        //   nm ": " load  assigns load's RETURN VALUE -- the symbol -- over the
        //                 table it just read, leaving an atom behind.
        //
        // Measured on a 3-row table saved and cleared:
        //   mytab: load `:kdb/hdb/mytab  -> count=1 type=-11  (symbol atom)
        //   load `:kdb/hdb/mytab         -> count=3 type=98   (table)
        //
        // So: keep the colon, and do not assign.
        @[{[pth] value "load `", string pth}; p; {[e] show "Could not load table: ", e}]
    } each tableNames;
    };

loadTables[];

// Widen existing HDB tables with nullable seq_num. Typed scalar `long$0N (never `long$()).
// Byte-identical with Q_ENSURE_COL_SEQ_NUM in src/kdb_integration.rs.
ensureCol:{[t;c] if[not c in cols t; ![t;();0b;(enlist c)!enlist `long$0N]];};
ensureCol[`orderbook_levels;`seq_num];
ensureCol[`trades;`seq_num];
ensureCol[`orderbook_snapshots;`seq_num];

// Save all tables to HDB path
saveTables: {
    show "Saving tables to disk...";
    @[save; .Q.dd[hdbPath;`orderbook_levels];     {[e] show "Error saving orderbook_levels: ",     e}];
    @[save; .Q.dd[hdbPath;`trades];               {[e] show "Error saving trades: ",               e}];
    @[save; .Q.dd[hdbPath;`orderbook_snapshots];  {[e] show "Error saving orderbook_snapshots: ",  e}];
    @[save; .Q.dd[hdbPath;`securities];           {[e] show "Error saving securities: ",           e}];
    @[save; .Q.dd[hdbPath;`own_trades];           {[e] show "Error saving own_trades: ",           e}];
    @[save; .Q.dd[hdbPath;`capture_gaps];         {[e] show "Error saving capture_gaps: ",         e}];
    show "Tables saved to disk.";
    };

// Tick counter — .z.ts fires every 30 s; save every 2 ticks (1 minute)
// ---------------------------------------------------------------------------
// Partitioned, append-only persistence.
//
// Replaces saveTables' q `save`, which serialised the WHOLE in-memory table
// over one flat file. Production reached 60.8M rows / a 2.5GB file, so each
// save wrote 2,500 MB -- measured 1,003 MB per 15s -- which blocked the q
// event loop, timed out the writer's liveness probe and dropped market data.
// Appending costs only the new rows: 0.4 MB to add 10,000 rows to a 2,000,000
// row partition, 0.43% of a full rewrite.
//
// Bookkeeping is by POSITION, not by timestamp. persistedCount[t] is the
// number of leading rows already written. An earlier version used a
// `time > watermark` watermark and lost 150,000 of 400,000 rows in testing:
// the rows shared a timestamp, so the strict comparison treated unwritten rows
// as written and the trim discarded them. Positions have no ties.
//
// Rows are kept in memory for bufferRetain so health checks that look back
// 5 minutes keep working, and the trim never removes more than
// persistedCount[t] leading rows, so an unwritten row cannot be dropped.
//
// maxPersistRows bounds the copy per tick. On 2026-10-10 an unbounded select
// over a 60.8M-row backlog needed ~2.5GB on a box with ~400MB free; the OOM
// killer took q and 13 minutes of capture was lost. A bounded copy catches up
// over successive ticks instead.
// ---------------------------------------------------------------------------
pdbPath: `:kdb/pdb;
system "mkdir -p kdb/pdb";
persistedCount: (`symbol$())!(`long$());
bufferRetain: 00:15:00;
maxPersistRows: 250000;

persistTable: {[t]
  tab: value t;
  n: count tab;
  k: persistedCount[t];
  if[null k; k: 0];
  if[k > n; k: 0];                                   // table was replaced under us
  // 1. write the next slice of unwritten rows, by position
  m: min(maxPersistRows; n - k);
  if[m > 0;
    chunk: tab[k + til m];
    {[t;tb;d]
      (.Q.dd[pdbPath;(`$string d; t; `)]) upsert .Q.en[pdbPath; select from tb where (`date$time)=d]
    }[t;chunk;] each distinct `date$chunk`time;
    k: k + m;
  ];
  // 2. trim leading rows that are BOTH written and older than the retention
  //    window. Capped at k, so an unwritten row can never be dropped.
  keepFrom: .z.p - bufferRetain;
  old: sum (tab`time) < keepFrom;
  d2: min(k; old);
  if[d2 > 0; t set d2 _ tab; k: k - d2];
  persistedCount[t]: k;
  };

persistTables: {
  persistTable each `orderbook_levels`trades`orderbook_snapshots`securities`own_trades`capture_gaps;
  // Every table must exist in every partition or a query spanning dates
  // fails. A table with no rows on some date creates no directory, and the
  // whole DB then refuses to load:
  //   './2026.10.08/securities/time. OS reports: No such file or directory'
  // Found migrating the archive, where securities had no rows on 10.08.
  // .Q.chk fills the gaps with empty tables; it is a directory scan, cheap
  // next to the writes.
  @[.Q.chk; pdbPath; {[e] -1 "WARN .Q.chk failed: ", e}];
  };

// Count rows on disk without mapping the partitioned DB into this process.
capturedRows: {[t]
  ds: key pdbPath;
  ds: ds where ds <> `sym;
  sum {[t;d] p: .Q.dd[pdbPath;(d; t; `)]; $[() ~ key p; 0; count get .Q.dd[p;`time]]}[t;] each ds
  };

saveTickCount: 0;
// How many 30s ticks between saves, from the environment so it is per instance.
//
// saveTables uses q `save`, which serialises the WHOLE in-memory table over a
// single flat file. Production reached 60.7M rows / a 2.5GB orderbook_levels
// file, so saving every 2 ticks (60s) meant rewriting 2.5GB every minute --
// measured at 1,003 MB written per 15s. That blocked the q event loop, timed
// out the writer's liveness probe, and dropped live market data:
//
//   kdb+ connection lost (peer closed or probe timeout)
//   kdb level buffer overflow (10000 entries) - dropped 5000 oldest entries
//
// Raising it to 60 (30 min) on production took writes to 0 MB/30s and made the
// process responsive again. UAT keeps the 2-tick default; its HDB is 19MB.
//
// Mitigation, not a fix. The real fix is to stop rewriting the whole table --
// a date-partitioned HDB via .Q.dpft -- and to bound the in-memory table. At
// 830k rows/hour and ~48 bytes/row this box has roughly 30 hours of memory.
saveEveryNTicks: $[count s: getenv `KDB_SAVE_EVERY_N_TICKS; "I" $ s; 2];

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
    if[0=saveTickCount mod saveEveryNTicks; persistTables[]]   // append-only; saveTables[] is kept for manual use only
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

