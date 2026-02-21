use std::fs;
use std::path::Path;
use serde_json::Value;
use std::io::Write;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let processed_dir = "orders/processed";
    let output_file = "orders/processed/open_orders_summary.csv";
    
    // Create CSV header
    let mut csv_content = String::from("ClOrdID,OrderID,ExecID,Symbol,Side,OrderQty,Price,OrdStatus,ExecType,CumQty,LeavesQty,LastPx,LastQty,AvgPx,BestBid,BestAsk,Spread,TransactTime,OrderCreateTime\n");
    
    // Read all JSON files in processed directory
    let entries = fs::read_dir(processed_dir)?;
    let mut open_orders_count = 0;
    
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        
        if path.extension().and_then(|s| s.to_str()) == Some("json") {
            // Skip the summary file itself
            if path.file_name().and_then(|s| s.to_str()) == Some("open_orders_summary.csv") {
                continue;
            }
            
            // Read and parse JSON
            let content = fs::read_to_string(&path)?;
            let json: Value = serde_json::from_str(&content)?;
            
            // Extract execution report
            if let Some(exec_report) = json.get("execution_report") {
                // Check if order is still open
                // An order is open if:
                // 1. LeavesQty > 0 (has remaining quantity)
                // 2. OrdStatus is not "8" (Rejected) or "4" (Cancelled)
                // 3. ExecType is not "8" (Rejected)
                
                let leaves_qty = exec_report.get("leaves_qty")
                    .and_then(|v| v.as_str())
                    .and_then(|s| s.parse::<f64>().ok())
                    .unwrap_or(0.0);
                
                let ord_status = exec_report.get("ord_status")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                
                let exec_type = exec_report.get("exec_type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                
                // Check if order is open
                let is_open = leaves_qty > 0.0 
                    && ord_status != "8" // Not rejected
                    && ord_status != "4" // Not cancelled
                    && exec_type != "8"; // Not rejected
                
                if is_open {
                    open_orders_count += 1;
                    
                    // Extract fields
                    let cl_ord_id = exec_report.get("cl_ord_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    
                    let order_id = exec_report.get("order_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    
                    let exec_id = exec_report.get("exec_id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    
                    let symbol = exec_report.get("symbol")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    
                    let side = exec_report.get("side")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    
                    let order_qty = exec_report.get("order_qty")
                        .and_then(|v| v.as_str())
                        .unwrap_or("0");
                    
                    let price = exec_report.get("price")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    
                    let cum_qty = exec_report.get("cum_qty")
                        .and_then(|v| v.as_str())
                        .unwrap_or("0");
                    
                    let last_px = exec_report.get("last_px")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    
                    let last_qty = exec_report.get("last_qty")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    
                    let avg_px = exec_report.get("avg_px")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    
                    let transact_time = exec_report.get("transact_time")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    
                    let order_create_time = exec_report.get("order_create_time")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    
                    // Get current market data from order book if available
                    // For now, we'll leave these empty - can be enhanced later
                    let best_bid = "";
                    let best_ask = "";
                    let spread = "";
                    
                    // Write CSV row
                    csv_content.push_str(&format!(
                        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
                        cl_ord_id,
                        order_id,
                        exec_id,
                        symbol,
                        side,
                        order_qty,
                        price,
                        ord_status,
                        exec_type,
                        cum_qty,
                        leaves_qty,
                        last_px,
                        last_qty,
                        avg_px,
                        best_bid,
                        best_ask,
                        spread,
                        transact_time,
                        order_create_time
                    ));
                }
            }
        }
    }
    
    // Write CSV file
    let mut file = fs::File::create(output_file)?;
    file.write_all(csv_content.as_bytes())?;
    
    println!("Created open orders summary: {}", output_file);
    println!("Total open orders: {}", open_orders_count);
    
    Ok(())
}

