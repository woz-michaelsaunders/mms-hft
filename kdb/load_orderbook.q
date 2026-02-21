// kdb+ script to load order book data from CSV files
// Usage: q load_orderbook.q

// Set data directory (relative to script location or absolute path)
data_dir: `$getenv[`PWD],"/kdb_data"

// Function to load CSV and convert to proper types
load_csv:{[filepath;coltypes]
    data:("SFF"; enlist ",") 0: filepath;
    // Convert first column (timestamp) to timestamp type
    data[;0]:`timestamp$data[;0];
    data
    }

// Load order book levels
if[()~key hsym `$string[data_dir],"/orderbook_levels.csv";
    orderbook_levels: ([] time:`timestamp$(); symbol:`symbol$(); side:`char$(); price:`float$(); size:`float$(); action:`char$());
    ];
if[not ()~key hsym `$string[data_dir],"/orderbook_levels.csv";
    info:("SSCFC"; enlist ",") 0: hsym `$string[data_dir],"/orderbook_levels.csv";
    // Skip header row
    info:1_ info;
    // Convert types
    info[;0]:`timestamp$info[;0];
    info[;1]:`$info[;1];
    info[;2]:`$info[;2];
    info[;3]:"F"$info[;3];
    info[;4]:"F"$info[;4];
    info[;5]:`$info[;5];
    // Create table
    orderbook_levels: `time`symbol`side`price`size`action xcol flip info;
    ];

// Load trades
if[()~key hsym `$string[data_dir],"/trades.csv";
    trades: ([] time:`timestamp$(); symbol:`symbol$(); price:`float$(); size:`float$());
    ];
if[not ()~key hsym `$string[data_dir],"/trades.csv";
    info:("SSFF"; enlist ",") 0: hsym `$string[data_dir],"/trades.csv";
    // Skip header row
    info:1_ info;
    // Convert types
    info[;0]:`timestamp$info[;0];
    info[;1]:`$info[;1];
    info[;2]:"F"$info[;2];
    info[;3]:"F"$info[;3];
    // Create table
    trades: `time`symbol`price`size xcol flip info;
    ];

// Load snapshots
if[()~key hsym `$string[data_dir],"/orderbook_snapshots.csv";
    orderbook_snapshots: ([] time:`timestamp$(); symbol:`symbol$(); best_bid:`float$(); best_ask:`float$(); best_bid_size:`float$(); best_ask_size:`float$(); spread:`float$());
    ];
if[not ()~key hsym `$string[data_dir],"/orderbook_snapshots.csv";
    info:("SSFFFFFF"; enlist ",") 0: hsym `$string[data_dir],"/orderbook_snapshots.csv";
    // Skip header row
    info:1_ info;
    // Convert types
    info[;0]:`timestamp$info[;0];
    info[;1]:`$info[;1];
    info[;2]:"F"$info[;2];
    info[;3]:"F"$info[;3];
    info[;4]:"F"$info[;4];
    info[;5]:"F"$info[;5];
    info[;6]:"F"$info[;6];
    // Create table
    orderbook_snapshots: `time`symbol`best_bid`best_ask`best_bid_size`best_ask_size`spread xcol flip info;
    ];

// Display summary
show "Order book levels: ", string count orderbook_levels;
show "Trades: ", string count trades;
show "Snapshots: ", string count orderbook_snapshots;

// Example queries
// Select latest snapshot for each symbol
// select by symbol from `time xdesc orderbook_snapshots

// Select all trades for a symbol
// select from trades where symbol=`BTC_USDT

// Select order book updates for a symbol
// select from orderbook_levels where symbol=`BTC_USDT

