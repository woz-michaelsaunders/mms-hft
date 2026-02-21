use rusqlite::Connection;

fn main() {
    match Connection::open("securities.db") {
        Ok(conn) => {
            // Count total
            match conn.query_row("SELECT COUNT(*) FROM securities", [], |row| row.get::<_, usize>(0)) {
                Ok(count) => {
                    println!("✅ Total securities in database: {}\n", count);

                    // Show all securities
                    match conn.prepare("SELECT id, symbol, security_id, exchange, attributes_json FROM securities ORDER BY id") {
                        Ok(mut stmt) => {
                            let securities = stmt.query_map([], |row| {
                                Ok((
                                    row.get::<_, i32>(0)?,
                                    row.get::<_, String>(1)?,
                                    row.get::<_, Option<String>>(2)?,
                                    row.get::<_, Option<String>>(3)?,
                                    row.get::<_, String>(4)?,
                                ))
                            });

                            if let Ok(securities) = securities {
                                println!("Securities List:");
                                println!("{}", "─".repeat(80));
                                for (idx, security) in securities.enumerate() {
                                    if let Ok((id, symbol, sec_id, exchange, attrs)) = security {
                                        println!("{}. {} (ID: {})", idx + 1, symbol, id);
                                        if let Some(id) = sec_id {
                                            println!("   SecurityID: {}", id);
                                        }
                                        if let Some(exch) = exchange {
                                            println!("   Exchange: {}", exch);
                                        }
                                        println!("   Attributes: {}", attrs);
                                        println!();
                                    }
                                }
                            }
                        }
                        Err(e) => eprintln!("Error preparing query: {}", e),
                    }
                }
                Err(e) => eprintln!("Error counting securities: {}", e),
            }
        }
        Err(e) => eprintln!("❌ Error opening database: {}", e),
    }
}
