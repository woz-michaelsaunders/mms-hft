/// query_securities — list securities stored in kdb+.
///
/// Replaces the old SQLite-based implementation.  Uses the same kxkdb IPC
/// library as the main server so there is a single, consistent data source.
///
/// Usage:
///   cargo run --bin query_securities
///
/// Environment variables (optional):
///   KDB_HOST  (default: localhost)
///   KDB_PORT  (default: 5001)

use anyhow::{anyhow, Result};
use kxkdb::ipc::{QStream, ConnectionMethod};

#[tokio::main]
async fn main() -> Result<()> {
    let kdb_host = std::env::var("KDB_HOST").unwrap_or_else(|_| "localhost".to_string());
    let kdb_port: u16 = std::env::var("KDB_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5001);

    println!("Connecting to kdb+ at {}:{}...", kdb_host, kdb_port);
    let mut stream = tokio::time::timeout(
        tokio::time::Duration::from_secs(5),
        QStream::connect(ConnectionMethod::TCP, &kdb_host, kdb_port, ""),
    )
    .await
    .map_err(|_| anyhow!("Connection timed out"))?
    .map_err(|e| anyhow!("Failed to connect to kdb+ at {}:{}: {}\n💡 Start kdb+: q kdb/start_kdb_server.q", kdb_host, kdb_port, e))?;

    // Count
    let q_count = "count securities";
    let count_k = stream.send_sync_message(&q_count).await
        .map_err(|e| anyhow!("Query error: {}", e))?;
    let count: u64 = format!("{}", count_k).trim().parse().unwrap_or(0);
    println!("✅ Total securities in kdb+: {}\n", count);

    if count == 0 {
        println!("No securities found. Run the main server to populate from the exchange.");
        return Ok(());
    }

    // Fetch all securities
    let q_select = "select from securities";
    let rows_k = stream.send_sync_message(&q_select).await
        .map_err(|e| anyhow!("Query error: {}", e))?;

    println!("Securities List:");
    println!("{}", "─".repeat(80));
    println!("{}", rows_k);
    println!("{}", "─".repeat(80));
    println!("\n{} securities total.", count);

    Ok(())
}
