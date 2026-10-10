// Regression tests for the partitioned, append-only persistence in
// kdb/start_kdb_server.q.
//
// Run standalone from a scratch directory:
//   cd $(mktemp -d) && mkdir -p kdb
//   QHOME=/home/ubuntu/q q /home/ubuntu/hft-deploy/test-persistence.q -q \
//     -script /home/ubuntu/fix_server_prod/kdb/start_kdb_server.q
//
// Exits non-zero if any case fails, so CI can gate on it.
//
// Every case here is a bug that actually happened on 2026-10-10 while this
// code was being written. Two of them lost data silently in testing and would
// have lost it in production:
//
//   whole_table_per_partition  `update ... from t where ...` returns the WHOLE
//                              table, so every date partition received the
//                              entire buffer (3x duplication).
//   watermark_tie_loss         A `time > watermark` watermark dropped 150,000
//                              of 400,000 rows when rows shared a timestamp:
//                              the strict comparison treated unwritten rows as
//                              written and the trim discarded them.
//   empty_partition_loads      A table with no rows on a date creates no
//                              directory, and the whole DB then refuses to
//                              load: './<date>/securities/time. OS reports: No
//                              such file or directory'.

args: .Q.opt .z.x;
scriptPath: first args`script;

failures: 0;
check: {[name; got; want]
  ok: got ~ want;
  if[not ok; failures+: 1];
  -1 "  ", ($[ok; "PASS"; "FAIL"]), "  ", name,
     $[ok; ""; "  got=", (.Q.s1 got), " want=", .Q.s1 want];
  };

// Fresh schemas + the persistence code under test, per case.
reset: {
  system "rm -rf kdb/pdb";
  system "mkdir -p kdb/pdb";
  `orderbook_levels set ([] time:`timestamp$(); symbol:`symbol$(); side:`char$();
     price:`float$(); size:`float$(); action:`char$(); seq_num:`long$());
  `trades set ([] time:`timestamp$(); symbol:`symbol$(); price:`float$();
     size:`float$(); seq_num:`long$());
  `orderbook_snapshots set ([] time:`timestamp$(); symbol:`symbol$(); best_bid:`float$();
     best_ask:`float$(); best_bid_size:`float$(); best_ask_size:`float$();
     spread:`float$(); seq_num:`long$());
  `securities set ([] time:`timestamp$(); symbol:`symbol$(); security_id:`symbol$();
     exchange:`symbol$(); attributes_json:());
  `own_trades set ([] time:`timestamp$(); symbol:`symbol$(); side:`char$();
     last_px:`float$(); last_qty:`float$(); avg_px:`float$(); cl_ord_id:`symbol$();
     order_id:`symbol$(); exec_type:`char$());
  `capture_gaps set ([] time:`timestamp$(); symbol:`symbol$(); expected_seq:`long$();
     received_seq:`long$(); gap_size:`long$());
  // Load only the persistence section; the full script opens a port and starts
  // a timer, neither of which belongs in a test.
  src: read0 hsym `$scriptPath;
  from: first where src like "pdbPath:*";
  to:   first where src like "capturedRows:*";
  if[(null from) or null to; -1 "FATAL: could not locate the persistence block in ", scriptPath; exit 2];
  // include capturedRows itself: it runs to the end of its lambda
  tail: to + first where (to _ src) like "  };*";
  value "\n" sv src from + til 1 + tail - from;
  };

mk: {[ts;n] `orderbook_levels insert
  (n#ts; n#`$"BTCUSD-PERP"; n#"B"; n#100.0; n#1.0; n#"0"; til n)};

// ---------------------------------------------------------------- case 1
// Rows land in the partition for their OWN date, not all of them in each.
-1 "case: date routing";
reset[];
mk[2026.10.07D12:00; 1000]; mk[2026.10.08D12:00; 2000]; mk[2026.10.09D12:00; 3000];
persistTable[`orderbook_levels];
check["total on disk"; capturedRows[`orderbook_levels]; 6000];
check["partitions present"; asc (key pdbPath) except `sym; asc `2026.10.07`2026.10.08`2026.10.09];

// ---------------------------------------------------------------- case 2
// Rows sharing a timestamp must not be lost when the per-tick cap splits them.
-1 "case: cap with tied timestamps loses nothing";
reset[];
maxPersistRows: 250000;
mk[.z.p - 0D01:00; 400000];                  // all identical timestamps
persistTable[`orderbook_levels];
check["written after tick 1"; capturedRows[`orderbook_levels]; 250000];
check["unwritten retained in buffer"; count orderbook_levels; 150000];
persistTable[`orderbook_levels];
check["written after tick 2"; capturedRows[`orderbook_levels]; 400000];
check["buffer drained"; count orderbook_levels; 0];

// ---------------------------------------------------------------- case 3
// Re-persisting must not duplicate.
-1 "case: idempotent";
persistTable[`orderbook_levels];
persistTable[`orderbook_levels];
check["still 400000, no duplicates"; capturedRows[`orderbook_levels]; 400000];

// ---------------------------------------------------------------- case 4
// Recent rows stay in memory so health checks that look back 5 minutes work.
-1 "case: retention window keeps recent rows queryable";
reset[];
maxPersistRows: 250000;
mk[.z.p - 0D00:02; 500];
persistTable[`orderbook_levels];
check["written"; capturedRows[`orderbook_levels]; 500];
check["retained for health checks"; count orderbook_levels; 500];
check["5-minute query sees them"; count select from orderbook_levels where time > .z.p - 00:05:00; 500];

// ---------------------------------------------------------------- case 5
// An empty table must not make the partitioned DB unloadable.
-1 "case: empty tables and .Q.chk";
reset[];
mk[2026.10.07D12:00; 10];
persistTables[];                              // own_trades etc. are empty
loadErr: @[{system "l kdb/pdb"; `ok}; ::; {`$"ERR ", x}];
check["partitioned DB loads with empty tables"; loadErr; `ok];

// ---------------------------------------------------------------- case 6
// A hyphenated symbol must survive enumeration (BTCUSD-PERP terminates a q
// symbol literal; this cost two outages before `$"..." was adopted).
-1 "case: hyphenated symbols round-trip";
reset[];
mk[2026.10.07D12:00; 5];
persistTable[`orderbook_levels];
// .Q.en enumerates symbol columns, so this column comes back as `sym$...
// rather than plain symbols. Compare the string form: it asserts the VALUE
// survived enumeration without asserting the enumeration's type.
syms: distinct string exec symbol from get .Q.dd[pdbPath;(`2026.10.07;`orderbook_levels;`)];
check["symbol preserved"; syms; enlist "BTCUSD-PERP"];

-1 "";
-1 $[failures = 0; "ALL PASS"; (string failures), " FAILURE(S)"];
exit $[failures = 0; 0; 1];
