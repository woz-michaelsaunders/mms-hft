// Migrate a flat-file HDB archive into a date-partitioned DB.
//
// Run OFFLINE, in its own process, never inside a live capture instance:
// doing this in the live process on 2026-10-10 needed a ~2.5GB copy on a box
// with ~400MB free and the OOM killer took q, losing 13 minutes of capture.
//
// Writes to its own root with its own sym file, so it cannot race the live
// instance's .Q.en enumeration or its partition directories.
//
// args: -src <flat archive dir> -dst <partitioned root> [-tables a,b,c]
args: .Q.opt .z.x;
srcDir: first args`src;
dstDir: first args`dst;
tbls: $[`tables in key args; `$"," vs first args`tables;
  `capture_gaps`own_trades`securities`trades`orderbook_snapshots`orderbook_levels];
dst: hsym `$dstDir;
sliceRows: $[`slice in key args; "J" $ first args`slice; 1000000];
system "mkdir -p ", dstDir;

-1 "src=", srcDir, "  dst=", dstDir;

migrateOne: {[t]
  p: hsym `$ srcDir, "/", string t;
  if[() ~ key p; -1 "  ", (string t), ": absent, skipped"; :()];
  tab: get p;
  n: count tab;
  if[n = 0; -1 "  ", (string t), ": empty, skipped"; :()];
  dates: distinct `date$tab`time;
  -1 "  ", (string t), ": ", (string n), " rows over ", (string count dates), " date(s)";
  // Write in ROW SLICES. Copying a whole date at once peaks at
  // (full table + that date's rows); on the 2.5GB orderbook_levels that is
  // ~3.7GB on a 3.8GB box. Slicing keeps the peak at (full table + sliceRows).
  dayIdx: group `date$tab`time;
  {[t;tab;dayIdx;d]
    idx: dayIdx d;
    path: .Q.dd[dst; (`$string d; t; `)];
    total: count idx;
    off: 0;
    while[off < total;
      take: min(sliceRows; total - off);
      path upsert .Q.en[dst; tab idx[off + til take]];
      off: off + take;
    ];
    -1 "      ", (string d), " <- ", (string total), " rows";
  }[t;tab;dayIdx;] each dates;
  tab: 0#tab;
  .Q.gc[];
  };

migrateOne each tbls;
-1 "done";
exit 0;
