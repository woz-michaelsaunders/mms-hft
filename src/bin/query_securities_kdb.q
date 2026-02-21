// Query securities from kdb+
// Usage: q query_securities_kdb.q

// Connect to kdb+ server
h:hopen `:localhost:5001

// Count total securities
count_securities: h"count securities";
show "✅ Total securities in kdb+: ", string count_securities;

// Show all securities
show "=== Securities List ===";
securities_data: h"select from securities";
show securities_data;

// Show by symbol
show "=== Securities by Symbol ===";
by_symbol: h"select by symbol from securities";
show by_symbol;

// Close connection
hclose h
show "Done!"

