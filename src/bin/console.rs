/// FIX Server Management Console
///
/// Connects directly to the kdb+ server via kxkdb IPC (the same library used by
/// the main server) instead of spawning a `q` subprocess.  This is faster,
/// more reliable, and removes the hardcoded `/home/ubuntu/q/l64/q` path
/// dependency.

use anyhow::{anyhow, Result};
use std::io::Write;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::Mutex;
use kxkdb::ipc::{QStream, ConnectionMethod};
use regex::Regex;

const KDB_HOST: &str = "localhost";
const KDB_PORT: u16 = 5001;

/// Send a synchronous query to kdb+ and return the result as a display string.
async fn qquery(stream: &Arc<Mutex<QStream>>, query: &str) -> Result<String> {
    let mut guard = stream.lock().await;
    let k = guard.send_sync_message(&query).await
        .map_err(|e| anyhow!("kdb+ query failed: {}", e))?;
    Ok(format!("{}", k))
}

/// Same as `qquery` but returns a scalar integer/long (e.g., for `count` queries).
async fn qcount(stream: &Arc<Mutex<QStream>>, query: &str) -> u64 {
    match qquery(stream, query).await {
        Ok(s) => s.trim().parse().unwrap_or(0),
        Err(_) => 0,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let kdb_host = std::env::var("KDB_HOST").unwrap_or_else(|_| KDB_HOST.to_string());
    let kdb_port: u16 = std::env::var("KDB_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(KDB_PORT);

    println!("Connecting to kdb+ at {}:{}...", kdb_host, kdb_port);
    let raw_stream = tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        QStream::connect(ConnectionMethod::TCP, &kdb_host, kdb_port, ""),
    )
    .await
    .map_err(|_| anyhow!("Connection timed out after 5 seconds"))?
    .map_err(|e| anyhow!("Failed to connect to kdb+ at {}:{}: {}\n💡 Make sure kdb+ is running: q kdb/start_kdb_server.q", kdb_host, kdb_port, e))?;

    let stream = Arc::new(Mutex::new(raw_stream));
    println!("✅ Connected to kdb+ at {}:{}", kdb_host, kdb_port);

    let stdin = tokio::io::stdin();
    let reader = BufReader::new(stdin);
    let mut lines = reader.lines();

    loop {
        print!("\x1B[2J\x1B[1;1H");
        println!("╔════════════════════════════════════════════════════════════╗");
        println!("║     FIX Server Management Console (kdb+ Direct IPC)       ║");
        println!("╚════════════════════════════════════════════════════════════╝");
        println!();
        println!("Connected to kdb+ at {}:{}", kdb_host, kdb_port);
        println!();
        println!("Menu Options:");
        println!("  1 -> Data Summary");
        println!("  2 -> Query Trades (last 20)");
        println!("  3 -> Query Order Book Levels (last 50)");
        println!("  4 -> Query Snapshots (last 10)");
        println!("  5 -> Query Securities");
        println!("  6 -> Custom Q Query");
        println!("  7 -> Query Individual Security");
        println!("  99 -> Exit");
        println!();
        print!("Select option: ");
        std::io::stdout().flush()?;

        let line = match lines.next_line().await {
            Ok(Some(l)) => l,
            Ok(None) => break,
            Err(e) => { eprintln!("Error reading input: {}", e); continue; }
        };

        match line.trim() {
            "1"  => { display_summary(&stream).await?;               pause(&mut lines).await; }
            "2"  => { query_table(&stream, "trades", "-20#trades", 20).await?;           pause(&mut lines).await; }
            "3"  => { query_table(&stream, "orderbook_levels", "-50#orderbook_levels", 50).await?; pause(&mut lines).await; }
            "4"  => { query_table(&stream, "orderbook_snapshots", "-10#orderbook_snapshots", 10).await?; pause(&mut lines).await; }
            "5"  => { query_table(&stream, "securities", "-20#securities", 20).await?;   pause(&mut lines).await; }
            "6"  => { custom_query(&stream, &mut lines).await?;       pause(&mut lines).await; }
            "7"  => { query_individual_security(&stream, &mut lines).await?; pause(&mut lines).await; }
            "99" => { println!("\nExiting console..."); break; }
            _    => { println!("\nInvalid option. Please select 1-7 or 99."); pause(&mut lines).await; }
        }
    }

    Ok(())
}

async fn pause(lines: &mut tokio::io::Lines<BufReader<tokio::io::Stdin>>) {
    println!("\nPress Enter to return to menu...");
    let _ = lines.next_line().await;
}

async fn display_summary(stream: &Arc<Mutex<QStream>>) -> Result<()> {
    println!();
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║                    KDB+ DATA SUMMARY                       ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!();
    println!("  Querying kdb+...");

    let levels    = qcount(stream, "count orderbook_levels").await;
    let trades    = qcount(stream, "count trades").await;
    let snapshots = qcount(stream, "count orderbook_snapshots").await;
    let secs      = qcount(stream, "count securities").await;
    let syms      = qcount(stream, "exec count distinct symbol from orderbook_snapshots").await;

    println!();
    println!("  Orderbook Levels : {}", levels);
    println!("  Trades           : {}", trades);
    println!("  Snapshots        : {}", snapshots);
    println!("  Securities       : {}", secs);
    println!("  Unique Symbols   : {}", syms);
    println!();
    Ok(())
}

async fn query_table(
    stream: &Arc<Mutex<QStream>>,
    table: &str,
    query: &str,
    limit: u64,
) -> Result<()> {
    println!();
    println!("Table: {}  (last {})", table, limit);
    println!();

    let count = qcount(stream, &format!("count {}", table)).await;
    if count == 0 {
        println!("  No data in {}", table);
        return Ok(());
    }
    println!("  Total rows: {}", count);
    println!();

    match qquery(stream, query).await {
        Ok(result) => {
            for line in result.lines() {
                if !line.trim().is_empty() {
                    println!("  {}", line);
                }
            }
        }
        Err(e) => println!("  ❌ Query failed: {}", e),
    }
    Ok(())
}

async fn query_individual_security(
    stream: &Arc<Mutex<QStream>>,
    lines: &mut tokio::io::Lines<BufReader<tokio::io::Stdin>>,
) -> Result<()> {
    println!();
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║              QUERY INDIVIDUAL SECURITY                     ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!();
    println!("Enter symbol (e.g., BTC_USDT) or wildcard (e.g., BTC*, *USD):");
    print!("Symbol: ");
    std::io::stdout().flush()?;

    let symbol = match lines.next_line().await {
        Ok(Some(l)) => l.trim().to_string(),
        _ => return Ok(()),
    };
    if symbol.is_empty() { return Ok(()); }

    let is_wildcard = symbol.contains('*') || symbol.contains('?');
    println!();

    // Fetch recent securities from kdb+
    let raw = match qquery(stream, "-1000#securities").await {
        Ok(r) => r,
        Err(e) => { println!("  ❌ Query failed: {}", e); return Ok(()); }
    };

    // Build regex from wildcard pattern
    let pattern_re = if is_wildcard {
        let escaped = regex::escape(&symbol)
            .replace("\\*", ".*")
            .replace("\\?", ".");
        Regex::new(&format!("^{}$", escaped)).ok()
    } else {
        None
    };

    let mut found = false;
    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() { continue; }

        let matches = if let Some(ref re) = pattern_re {
            // Wildcard: extract symbol token between first pair of backticks
            if let Some(bt) = trimmed.find('`') {
                let after = &trimmed[bt + 1..];
                let sym_field = after.split(['`', ' ']).next().unwrap_or("").trim();
                re.is_match(sym_field)
            } else { false }
        } else {
            // Exact match: `SYMBOL followed by whitespace or backtick
            let pat_space = format!("`{} ", symbol);
            let pat_bt    = format!("`{}`", symbol);
            trimmed.contains(&pat_space) || trimmed.contains(&pat_bt)
        };

        if matches {
            if !found {
                println!("  Security details:");
                println!();
                found = true;
            }
            println!("  {}", trimmed);
        }
    }

    if !found {
        println!("  ❌ No match for: {} (searched last 1000 securities)", symbol);
        println!("  💡 Use option 5 to list available symbols.");
    }
    Ok(())
}

async fn custom_query(
    stream: &Arc<Mutex<QStream>>,
    lines: &mut tokio::io::Lines<BufReader<tokio::io::Stdin>>,
) -> Result<()> {
    println!();
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║                  CUSTOM Q QUERY                            ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!();
    println!("Enter a q query (or press Enter to cancel):");
    print!("q> ");
    std::io::stdout().flush()?;

    let input = match lines.next_line().await {
        Ok(Some(l)) => l.trim().to_string(),
        _ => return Ok(()),
    };
    if input.is_empty() { return Ok(()); }

    println!();
    println!("Executing: {}", input);
    println!();

    match qquery(stream, &input).await {
        Ok(result) => {
            println!("Result:");
            for line in result.lines() {
                if !line.trim().is_empty() {
                    println!("  {}", line);
                }
            }
        }
        Err(e) => println!("❌ Query failed: {}", e),
    }
    Ok(())
}
