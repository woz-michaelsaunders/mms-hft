// kdb/load_ticks_history.q
// One-shot bulk loader for data/ticks.csv into the already-running kdb+ server.
// Usage (from repo root / /home/ubuntu/fix_server, kdb already up):
//   q kdb/load_ticks_history.q
// Does NOT system "p". Does NOT value the CSV. Does NOT fabricate September ticks.
// Does NOT run from start_kdb_server.q.
// Re-running against non-empty tables DUPLICATES rows (no dedup).
//
// Stream from disk via .Q.fsn (~100k-row / ~8MB newline-aligned windows).
// Do NOT `0: csvPath` the whole ~28M-row file into this process.

h:hopen `:localhost:5001
if[null h; '`connection_failed]

KDB_TIMESTAMP_OFFSET_NS:946684800000000000j
csvPath:`:data/ticks.csv
if[()~key csvPath; -2 "data/ticks.csv not found (cwd must be /home/ubuntu/fix_server)"; exit 1]

// 8 CSV columns: timestamp,symbol,bid,ask,bid_size,ask_size,last_price,last_size
// "JSFFFFFF" = J + S + 6×F. A 7-char type string drops last_size.
expected:`timestamp`symbol`bid`ask`bid_size`ask_size`last_price`last_size

insertChunk:{[t]
  snaps: select
    time: `timestamp$ timestamp - KDB_TIMESTAMP_OFFSET_NS,
    symbol,
    best_bid: bid,
    best_ask: ask,
    best_bid_size: bid_size,
    best_ask_size: ask_size,
    spread: ?[(bid = 0) or ask = 0; 0f; ask - bid],
    seq_num: 0Nj
    from t;
  h(insert; `orderbook_snapshots; snaps);
  tr: select
    time: `timestamp$ timestamp - KDB_TIMESTAMP_OFFSET_NS,
    symbol,
    price: last_price,
    size: last_size,
    seq_num: 0Nj
    from t where last_size > 0;
  if[count tr; h(insert; `trades; tr)];
  count t
  }

firstChunk:1b
nInserted:0
// ~8 MB ≈ 100_000 rows of this CSV (LLD-1). .Q.fsn splits on newlines.
.Q.fsn[{[x]
  if[0=count x; :()];
  t:$[firstChunk;
    [
      c:("JSFFFFFF"; enlist ",") 0: x;
      if[not expected~cols c;
        -2 "bad_csv_shape expected ",(.Q.s1 expected)," got ",.Q.s1 cols c;
        '`bad_csv_shape];
      firstChunk::0b;
      c
    ];
    flip expected!("JSFFFFFF"; ",") 0: x];
  nInserted+: insertChunk t;
  show "inserted_through=", string nInserted;
  }; csvPath; 8000000]

show "rows_in_csv=", string nInserted
show "snapshots=", string h"count orderbook_snapshots"
show "trades=", string h"count trades"
show "symbols=", h"exec distinct symbol from orderbook_snapshots"
show "span=", h"select min time, max time by symbol from orderbook_snapshots"
show "history_null_seq=", string h"exec count i from orderbook_snapshots where null seq_num"
hclose h
show "Done!"
