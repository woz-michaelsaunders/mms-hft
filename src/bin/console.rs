use anyhow::{anyhow, Result};
use std::io::Write;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use regex;

const KDB_HOST: &str = "localhost";
const KDB_PORT: u16 = 5001;
const Q_PATH: &str = "/home/ubuntu/q/l64/q";

async fn execute_q_query_with_timeout(query: &str, timeout_secs: u64) -> Result<String> {
    // Use stdin pipe with -q flag to make q non-interactive
    // Escape quotes and backslashes, but preserve backticks for symbol literals
    let escaped_query = query.replace('"', "\\\"").replace('\\', "\\\\");
    let q_code = format!(
        "h:hopen `:{}:{}; result:h\"{}\"; hclose h; show result",
        KDB_HOST, KDB_PORT, escaped_query
    );
    
    // Execute q with stdin pipe and -q flag (quiet/non-interactive)
    let mut child = Command::new(Q_PATH)
        .arg("-q")  // Quiet mode - no banner, non-interactive
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| anyhow!("Failed to spawn q process: {}", e))?;
    
    // Write query to stdin
    if let Some(mut stdin) = child.stdin.take() {
        use tokio::io::AsyncWriteExt;
        stdin.write_all(q_code.as_bytes()).await
            .map_err(|e| anyhow!("Failed to write to q stdin: {}", e))?;
        stdin.write_all(b"\n").await
            .map_err(|e| anyhow!("Failed to write newline: {}", e))?;
        stdin.shutdown().await
            .map_err(|e| anyhow!("Failed to close stdin: {}", e))?;
    }
    
    // Wait for output with custom timeout
    let output = tokio::time::timeout(
        tokio::time::Duration::from_secs(timeout_secs),
        child.wait_with_output()
    )
    .await
    .map_err(|_| anyhow!("Query timed out after {} seconds", timeout_secs))?
    .map_err(|e| anyhow!("Failed to wait for q process: {}", e))?;
    
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    
    // Combine outputs - q's show outputs to stderr
    let all_output = format!("{}\n{}", stdout, stderr);
    
    // Look for the result - it should be a number on its own line
    // show outputs like "10990" or "10990\n"
    for line in all_output.lines() {
        let trimmed = line.trim();
        // Pure number (the query result) - check if it's all digits
        if !trimmed.is_empty() && trimmed.chars().all(|c| c.is_ascii_digit()) {
            return Ok(trimmed.to_string());
        }
    }
    
    // If no pure number found, look for any non-empty line that's not metadata
    let filtered: Vec<String> = all_output.lines()
        .map(|l| l.trim().to_string())
        .filter(|l| {
            !l.is_empty() && 
            !l.contains("Copyright") && 
            !l.contains("KDB+") && 
            !l.contains("l64") &&
            !l.contains("EXPIRE") &&
            !l.contains("michael.saunders") &&
            !l.contains("wozingagroup") &&
            !l.contains("PERSONAL") &&
            !l.contains("#5031724") &&
            !(l.starts_with("'") && l.len() < 30)
            // Note: Removed length limit - securities table rows can be long due to attributes_json
        })
        .collect();
    
    if !filtered.is_empty() {
        // Return all filtered lines (for table results, we want all rows)
        Ok(filtered.join("\n"))
    } else {
        // Last resort: return raw output if it's short
        let raw_stdout = stdout.trim();
        let raw_stderr = stderr.trim();
        if !raw_stdout.is_empty() && raw_stdout.len() < 50 {
            Ok(raw_stdout.to_string())
        } else if !raw_stderr.is_empty() && raw_stderr.len() < 50 && !raw_stderr.contains("Copyright") {
            Ok(raw_stderr.to_string())
        } else {
            Ok("(no output)".to_string())
        }
    }
}

async fn execute_q_query(query: &str) -> Result<String> {
    execute_q_query_with_timeout(query, 10).await
}

#[tokio::main]
async fn main() -> Result<()> {
    // Test connection with timeout
    println!("Testing connection to kdb+ at {}:{}...", KDB_HOST, KDB_PORT);
    match tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        execute_q_query("count orderbook_levels")
    ).await {
        Ok(Ok(_)) => println!("✅ Connected to kdb+"),
        Ok(Err(e)) => {
            eprintln!("❌ Failed to connect to kdb+: {}", e);
            eprintln!("💡 Make sure kdb+ server is running: q -p {}", KDB_PORT);
            return Err(e);
        }
        Err(_) => {
            eprintln!("❌ Connection test timed out after 5 seconds");
            eprintln!("💡 Make sure kdb+ server is running: q -p {}", KDB_PORT);
            return Err(anyhow!("Connection timeout"));
        }
    }

    let stdin = tokio::io::stdin();
    let reader = BufReader::new(stdin);
    let mut lines = reader.lines();
    
    loop {
        // Clear screen and display menu
        print!("\x1B[2J\x1B[1;1H");
        println!("╔════════════════════════════════════════════════════════════╗");
        println!("║     FIX Server Management Console (kdb+ Direct)          ║");
        println!("╚════════════════════════════════════════════════════════════╝");
        println!();
        println!("Connected to kdb+ at {}:{}", KDB_HOST, KDB_PORT);
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
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(e) => {
                eprintln!("Error reading input: {}", e);
                continue;
            }
        };
        
        let input = line.trim();
        match input {
            "1" => {
                display_summary().await?;
                println!();
                println!("Press Enter to return to menu...");
                let _ = lines.next_line().await;
            }
            "2" => {
                query_trades().await?;
                println!();
                println!("Press Enter to return to menu...");
                let _ = lines.next_line().await;
            }
            "3" => {
                query_orderbook_levels().await?;
                println!();
                println!("Press Enter to return to menu...");
                let _ = lines.next_line().await;
            }
            "4" => {
                query_snapshots().await?;
                println!();
                println!("Press Enter to return to menu...");
                let _ = lines.next_line().await;
            }
            "5" => {
                query_securities().await?;
                println!();
                println!("Press Enter to return to menu...");
                let _ = lines.next_line().await;
            }
            "6" => {
                custom_query(&mut lines).await?;
                println!();
                println!("Press Enter to return to menu...");
                let _ = lines.next_line().await;
            }
            "7" => {
                query_individual_security(&mut lines).await?;
                println!();
                println!("Press Enter to return to menu...");
                let _ = lines.next_line().await;
            }
            "99" => {
                println!();
                println!("Exiting console...");
                break;
            }
            _ => {
                println!();
                println!("Invalid option. Please select 1-7 or 99.");
                println!("Press Enter to continue...");
                let _ = lines.next_line().await;
            }
        }
    }
    
    Ok(())
}

async fn display_summary() -> Result<()> {
    println!();
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║                    KDB+ DATA SUMMARY                       ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!();
    println!("  Querying kdb+...");

    let levels_count = execute_q_query("count orderbook_levels").await.unwrap_or_else(|e| {
        eprintln!("Error getting levels: {}", e);
        "Error".to_string()
    });
    let trades_count = execute_q_query("count trades").await.unwrap_or_else(|e| {
        eprintln!("Error getting trades: {}", e);
        "Error".to_string()
    });
    let snapshots_count = execute_q_query("count orderbook_snapshots").await.unwrap_or_else(|e| {
        eprintln!("Error getting snapshots: {}", e);
        "Error".to_string()
    });
    let securities_count = execute_q_query("count securities").await.unwrap_or_else(|e| {
        eprintln!("Error getting securities: {}", e);
        "Error".to_string()
    });
    // Now that we have indexes (g# on symbol columns), distinct count should be faster
    // Use snapshots table (smaller) for faster distinct count
    let symbols_count = execute_q_query_with_timeout("exec count distinct symbol from orderbook_snapshots", 5).await.unwrap_or_else(|_| {
        "N/A".to_string()
    });

    println!();
    println!("  Orderbook Levels: {}", levels_count.trim());
    println!("  Trades:           {}", trades_count.trim());
    println!("  Snapshots:        {}", snapshots_count.trim());
    println!("  Securities:       {}", securities_count.trim());
    println!("  Unique Symbols:   {}", symbols_count.trim());
    println!();

    Ok(())
}

async fn query_trades() -> Result<()> {
    println!();
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║                    TRADES (from kdb+)                     ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!();

    let count = execute_q_query("count trades").await.unwrap_or_else(|_| "0".to_string());
    let count_num: u64 = count.trim().parse().unwrap_or(0);
    
    if count_num == 0 {
        println!("  No trades in kdb+");
        return Ok(());
    }

    println!("  Total trades: {}", count.trim());
    println!();
    println!("  Recent trades (last 20):");
    println!();

    // Use kdb+ negative indexing to get last 20 rows - much faster than sorting
    // Get the last 20 rows directly (this is the fastest way)
    let query = "-20#trades";
    match execute_q_query_with_timeout(query, 15).await {
        Ok(result) => {
            // Print the result - q will show the table
            for line in result.lines() {
                let trimmed = line.trim();
                if !trimmed.is_empty() 
                    && !trimmed.contains("Copyright") 
                    && !trimmed.contains("KDB+")
                    && !trimmed.contains("EXPIRE")
                    && trimmed.len() > 10 {  // Filter out noise
                    println!("  {}", trimmed);
                }
            }
        }
        Err(e) => {
            println!("  ❌ Query failed: {}", e);
        }
    }

    Ok(())
}

async fn query_orderbook_levels() -> Result<()> {
    println!();
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║              ORDER BOOK LEVELS (from kdb+)                ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!();

    let count = execute_q_query("count orderbook_levels").await.unwrap_or_else(|_| "0".to_string());
    let count_num: u64 = count.trim().parse().unwrap_or(0);
    
    if count_num == 0 {
        println!("  No orderbook levels in kdb+");
        return Ok(());
    }

    println!("  Total levels: {}", count.trim());
    println!();
    println!("  Recent levels (last 50):");
    println!();

    // Use negative indexing to get last 50 rows - much faster than sorting
    let query = "-50#orderbook_levels";
    match execute_q_query_with_timeout(query, 15).await {
        Ok(result) => {
            for line in result.lines() {
                let trimmed = line.trim();
                if !trimmed.is_empty() 
                    && !trimmed.contains("Copyright") 
                    && !trimmed.contains("KDB+")
                    && !trimmed.contains("EXPIRE")
                    && trimmed.len() > 10 {  // Filter out noise but keep data lines
                    println!("  {}", trimmed);
                }
            }
        }
        Err(e) => {
            println!("  ❌ Query failed: {}", e);
        }
    }

    Ok(())
}

async fn query_snapshots() -> Result<()> {
    println!();
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║              ORDER BOOK SNAPSHOTS (from kdb+)              ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!();

    let count = execute_q_query("count orderbook_snapshots").await.unwrap_or_else(|_| "0".to_string());
    let count_num: u64 = count.trim().parse().unwrap_or(0);
    
    if count_num == 0 {
        println!("  No snapshots in kdb+");
        return Ok(());
    }

    println!("  Total snapshots: {}", count.trim());
    println!();
    println!("  Latest snapshots (last 10):");
    println!();

    // Use negative indexing to get last 10 rows - much faster than sorting
    let query = "-10#orderbook_snapshots";
    match execute_q_query_with_timeout(query, 15).await {
        Ok(result) => {
            for line in result.lines() {
                if !line.trim().is_empty() {
                    println!("  {}", line);
                }
            }
        }
        Err(e) => {
            println!("  ❌ Query failed: {}", e);
        }
    }

    Ok(())
}

async fn query_securities() -> Result<()> {
    println!();
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║                  SECURITIES (from kdb+)                    ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!();

    let count = execute_q_query("count securities").await.unwrap_or_else(|_| "0".to_string());
    let count_num: u64 = count.trim().parse().unwrap_or(0);
    
    if count_num == 0 {
        println!("  No securities in kdb+");
        return Ok(());
    }

    println!("  Total securities: {}", count.trim());
    println!();
    println!("  Recent securities (last 20):");
    println!();

    // Use negative indexing to get last 20 rows - much faster than selecting all
    let query = "-20#securities";
    match execute_q_query_with_timeout(query, 15).await {
        Ok(result) => {
            for line in result.lines() {
                if !line.trim().is_empty() {
                    println!("  {}", line);
                }
            }
        }
        Err(e) => {
            println!("  ❌ Query failed: {}", e);
        }
    }

    Ok(())
}

async fn query_individual_security(lines: &mut tokio::io::Lines<BufReader<tokio::io::Stdin>>) -> Result<()> {
    println!();
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║              QUERY INDIVIDUAL SECURITY                      ⑑");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!();
    println!("Enter symbol (e.g., BTC_USD, ETH_USDT) or wildcard pattern (e.g., BTC*, *USD):");
    println!("  Use * for any sequence of characters, ? for a single character");
    print!("Symbol: ");
    std::io::stdout().flush()?;
    
    let symbol = match lines.next_line().await {
        Ok(Some(line)) => line.trim().to_string(),
        _ => return Ok(()),
    };
    
    if symbol.is_empty() {
        return Ok(());
    }
    
    let is_wildcard = symbol.contains('*') || symbol.contains('?');
    println!();
    if is_wildcard {
        println!("  Searching for securities matching pattern: {}", symbol);
    } else {
        println!("  Querying security: {}", symbol);
    }
    println!();
    
    // Query the specific security
    // Use a simpler approach: get recent securities and filter client-side
    // This avoids backtick escaping issues in IPC query strings
    println!("  (Note: Searching in recent securities...)");
    let query = "-1000#securities";  // Get last 1000 securities for better coverage
    match execute_q_query_with_timeout(query, 10).await {
        Ok(result) => {
            // Filter results client-side to find matching symbol
            // Note: securities lines can be long due to attributes_json, so we don't filter by length
            let mut found = false;
            let all_lines: Vec<&str> = result.lines().collect();
            for line in &all_lines {
                let trimmed = line.trim();
                if !trimmed.is_empty() 
                    && !trimmed.contains("Copyright") 
                    && !trimmed.contains("KDB+")
                    && !trimmed.contains("EXPIRE")
                    && trimmed.len() > symbol.len() + 5 {  // At least longer than symbol
                    // Check if this line contains the symbol
                    // kdb+ table format: timestamp `SYMBOL      ` ` "attributes"
                    // Extract the symbol from the line (between backticks)
                    let mut matches = false;
                    
                    if is_wildcard {
                        // Wildcard search: extract symbol from line and match against pattern
                        // Find the symbol in the format: timestamp `SYMBOL ` `
                        // Look for pattern: `SYMBOL followed by spaces and then `
                        if let Some(backtick_start) = trimmed.find('`') {
                            let after_first_backtick = &trimmed[backtick_start + 1..];
                            // Find the next backtick (which ends the symbol field)
                            if let Some(second_backtick_pos) = after_first_backtick.find('`') {
                                // Extract symbol (between first ` and second `, trim spaces)
                                let symbol_field = &after_first_backtick[..second_backtick_pos].trim();
                                
                                // Convert wildcard pattern to regex
                                // Escape special regex chars, then convert * and ?
                                let pattern = symbol
                                    .replace("\\", "\\\\")
                                    .replace(".", "\\.")
                                    .replace("^", "\\^")
                                    .replace("$", "\\$")
                                    .replace("+", "\\+")
                                    .replace("(", "\\(")
                                    .replace(")", "\\)")
                                    .replace("[", "\\[")
                                    .replace("]", "\\]")
                                    .replace("{", "\\{")
                                    .replace("}", "\\}")
                                    .replace("|", "\\|")
                                    .replace("*", ".*")
                                    .replace("?", ".");
                                
                                if let Ok(re) = regex::Regex::new(&format!("^{}$", pattern)) {
                                    if re.is_match(symbol_field) {
                                        matches = true;
                                    }
                                }
                            }
                        }
                    } else {
                        // Exact match: `SYMBOL followed by space or backtick
                        let backtick_symbol = format!("`{}", symbol);
                        if trimmed.contains(&backtick_symbol) {
                            // Verify it's an exact match by checking what comes after the symbol
                            // Pattern should be: `SYMBOL followed by space(s) then ` or end
                            // This prevents VET_USD from matching VET_USDT
                            let pattern_after = format!("`{} ", symbol);  // `SYMBOL followed by space
                            let pattern_backtick = format!("`{}`", symbol);  // `SYMBOL` (direct)
                            if trimmed.contains(&pattern_after) || trimmed.contains(&pattern_backtick) {
                                matches = true;
                            }
                        }
                    }
                    
                    if matches {
                        // Found match - display it
                        if !found {
                            println!("  Security details:");
                            println!();
                            found = true;
                        }
                        println!("  {}", trimmed);
                    }
                }
            }
            if !found {
                if is_wildcard {
                    println!("  ❌ No securities found matching pattern: {} (searched in last 1000 securities)", symbol);
                } else {
                    println!("  ❌ No security found with symbol: {} (searched in last 1000 securities)", symbol);
                }
                println!("  💡 Try using option 5 to see all available symbols");
            } else {
                if is_wildcard {
                    println!();
                    println!("  (Found matching securities above)");
                }
            }
        }
        Err(e) => {
            println!("  ❌ Query failed: {}", e);
        }
    }
    
    Ok(())
}

async fn custom_query(lines: &mut tokio::io::Lines<BufReader<tokio::io::Stdin>>) -> Result<()> {
    println!();
    println!("╔════════════════════════════════════════════════════════════╗");
    println!("║                  CUSTOM Q QUERY                            ║");
    println!("╚════════════════════════════════════════════════════════════╝");
    println!();
    println!("Enter a q query (or press Enter to cancel):");
    print!("q> ");
    std::io::stdout().flush()?;
    
    let line = match lines.next_line().await {
        Ok(Some(line)) => line.trim().to_string(),
        _ => return Ok(()),
    };
    
    if line.is_empty() {
        return Ok(());
    }
    
    println!();
    println!("Executing: {}", line);
    println!();
    
    match execute_q_query(&line).await {
        Ok(result) => {
            println!("Result:");
            for result_line in result.lines() {
                if !result_line.trim().is_empty() {
                    println!("  {}", result_line);
                }
            }
        }
        Err(e) => {
            println!("❌ Query failed: {}", e);
        }
    }
    
    Ok(())
}
