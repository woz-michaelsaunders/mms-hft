// Query kdb+ data from the server
// Usage: q query_data.q

// Connect to kdb+ server
h:hopen `:localhost:5001;

// Query counts
show "═══════════════════════════════════════════════════════════";
show "📊 KDB+ Data Summary";
show "═══════════════════════════════════════════════════════════";
show "";

count_levels: h"count orderbook_levels";
count_trades: h"count trades";
count_snapshots: h"count orderbook_snapshots";
count_securities: h"count securities";

show "Orderbook Levels: ", string count_levels;
show "Trades: ", string count_trades;
show "Snapshots: ", string count_snapshots;
show "Securities: ", string count_securities;
show "";

// Show sample data if available
if[count_levels > 0;
    show "═══════════════════════════════════════════════════════════";
    show "📋 Sample Orderbook Levels (last 10):";
    show "═══════════════════════════════════════════════════════════";
    sample_levels: h"select time, symbol, side, price, size, action from (select from orderbook_levels where i >= count[orderbook_levels] - 10)";
    show sample_levels;
    show "";
    
    show "📊 Count by Symbol:";
    by_symbol: h"select count i by symbol from orderbook_levels";
    -1 string by_symbol;
    show "";
];

if[count_snapshots > 0;
    show "═══════════════════════════════════════════════════════════";
    show "📸 Sample Snapshots (last 5):";
    show "═══════════════════════════════════════════════════════════";
    sample_snapshots: h"select time, symbol, best_bid, best_ask, spread from (select from orderbook_snapshots where i >= count[orderbook_snapshots] - 5)";
    show sample_snapshots;
    show "";
];

if[count_trades > 0;
    show "═══════════════════════════════════════════════════════════";
    show "💰 Sample Trades (last 5):";
    show "═══════════════════════════════════════════════════════════";
    sample_trades: h"select time, symbol, price, size from (select from trades where i >= count[trades] - 5)";
    show sample_trades;
    show "";
];

hclose h;
show "✅ Query complete";

