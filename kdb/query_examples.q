// Example queries for kdb+ order book server
// Connect to the server and query data

// Open connection to server
h:hopen `:localhost:5001

// Check if connection is open
if[null h; '`connection_failed]

// Basic queries
show "=== Table Counts ==="
show h"count orderbook_levels"
show h"count trades"
show h"count orderbook_snapshots"

// Show table structure
show "=== Table Structure ==="
show h"meta orderbook_levels"
show h"meta trades"
show h"meta orderbook_snapshots"

// Sample data
show "=== Sample Data (first 5 rows) ==="
show h"5#orderbook_levels"
show h"5#trades"
show h"5#orderbook_snapshots"

// Aggregations
show "=== Aggregations ==="
show h"select count by symbol from orderbook_levels"
show h"select sum size by symbol from trades"
show h"select last best_bid, last best_ask, last spread by symbol from orderbook_snapshots"

show "=== capture_gaps ==="
show h"count capture_gaps"
show h"meta capture_gaps"
show h"select from capture_gaps"
show h"select from capture_gaps where expected_seq=0"   / buffer overflow during kdb outage
show h"select from capture_gaps where expected_seq>0"   / FIX sequence gaps

show "=== seq_num live vs history ==="
show h"select seq_num, time, symbol from orderbook_snapshots where symbol=`BTC_USDT"
show h"select from orderbook_snapshots where symbol=`BTC_USDT, not null seq_num"  / live
show h"select from orderbook_snapshots where symbol=`BTC_USDT, null seq_num"       / history

// Recent data
show "=== Recent Data (last 10) ==="
show h"10#`time xdesc orderbook_levels"
show h"10#`time xdesc trades"

// Close connection
hclose h
show "Done!"

