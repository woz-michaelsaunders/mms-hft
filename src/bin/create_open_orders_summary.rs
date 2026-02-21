use std::fs;
use serde_json::Value;
use std::io::Write;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let processed_dir = "orders/processed";
    let output_file = "orders/processed/open_orders_summary.csv";
    
    // Create CSV header
    let mut csv_content = String::from("ClOrdID,OrderID,ExecID,Symbol,Side,OrderQty,Price,OrdStatus,ExecType,CumQty,LeavesQty,LastPx,LastQty,AvgPx,TransactTime,OrderCreateTime,RejectionReason\n");
    
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
                    .and_then(|v| {
                        if v.is_null() {
                            Some(0.0)
                        } else if let Some(s) = v.as_str() {
                            s.parse::<f64>().ok()
                        } else if let Some(n) = v.as_f64() {
                            Some(n)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0.0);
                
                    let ord_status = exec_report.get("ord_status")
                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                        .or_else(|| {
                            exec_report.get("exec_status")
                                .and_then(|v| v.as_str().map(|s| s.to_string()))
                        })
                        .unwrap_or_else(|| String::new());
                    
                    let exec_type = exec_report.get("exec_type")
                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                        .unwrap_or_else(|| String::new());
                
                // Check if order is open
                // OrdStatus: 0=New, 1=PartiallyFilled, 2=Filled, 4=Cancelled, 8=Rejected
                // ExecType: 0=New, A=Accepted, 8=Rejected, 4=Cancelled, F=Trade
                let is_open = leaves_qty > 0.0 
                    && ord_status != "8" // Not rejected
                    && ord_status != "4" // Not cancelled
                    && ord_status != "2" // Not fully filled
                    && exec_type != "8"; // Not rejected
                
                if is_open {
                    open_orders_count += 1;
                    
                    // Extract fields with null handling
                    let cl_ord_id = exec_report.get("cl_ord_id")
                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                        .or_else(|| {
                            json.get("order")
                                .and_then(|o| o.get("cl_ord_id"))
                                .and_then(|v| v.as_str().map(|s| s.to_string()))
                        })
                        .unwrap_or_else(|| String::new());
                    
                    let order_id = exec_report.get("order_id")
                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                        .unwrap_or_else(|| String::new());
                    
                    let exec_id = exec_report.get("exec_id")
                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                        .unwrap_or_else(|| String::new());
                    
                    let symbol = exec_report.get("symbol")
                        .and_then(|v| v.as_str())
                        .or_else(|| json.get("order").and_then(|o| o.get("symbol")).and_then(|v| v.as_str()))
                        .unwrap_or("");
                    
                    let side = exec_report.get("side")
                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                        .or_else(|| {
                            json.get("order")
                                .and_then(|o| o.get("side"))
                                .and_then(|v| {
                                    if let Some(n) = v.as_u64() {
                                        Some(n.to_string())
                                    } else if let Some(s) = v.as_str() {
                                        Some(s.to_string())
                                    } else {
                                        None
                                    }
                                })
                        })
                        .unwrap_or_else(|| String::new());
                    
                    let order_qty = exec_report.get("order_qty")
                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                        .or_else(|| {
                            json.get("order")
                                .and_then(|o| o.get("qty"))
                                .and_then(|v| {
                                    if let Some(n) = v.as_f64() {
                                        Some(n.to_string())
                                    } else if let Some(s) = v.as_str() {
                                        Some(s.to_string())
                                    } else {
                                        None
                                    }
                                })
                        })
                        .unwrap_or_else(|| "0".to_string());
                    
                    let price = exec_report.get("price")
                        .and_then(|v| {
                            if v.is_null() {
                                None
                            } else if let Some(s) = v.as_str() {
                                Some(s.to_string())
                            } else if let Some(n) = v.as_f64() {
                                Some(n.to_string())
                            } else {
                                None
                            }
                        })
                        .or_else(|| json.get("order").and_then(|o| o.get("price")).and_then(|v| {
                            if let Some(n) = v.as_f64() {
                                Some(n.to_string())
                            } else {
                                None
                            }
                        }))
                        .unwrap_or_default();
                    
                    let cum_qty = exec_report.get("cum_qty")
                        .and_then(|v| {
                            if v.is_null() {
                                Some("0".to_string())
                            } else if let Some(s) = v.as_str() {
                                Some(s.to_string())
                            } else if let Some(n) = v.as_f64() {
                                Some(n.to_string())
                            } else {
                                None
                            }
                        })
                        .unwrap_or_else(|| "0".to_string());
                    
                    let last_px = exec_report.get("last_px")
                        .and_then(|v| {
                            if v.is_null() {
                                Some(String::new())
                            } else if let Some(s) = v.as_str() {
                                Some(s.to_string())
                            } else if let Some(n) = v.as_f64() {
                                Some(n.to_string())
                            } else {
                                None
                            }
                        })
                        .unwrap_or_default();
                    
                    let last_qty = exec_report.get("last_qty")
                        .and_then(|v| {
                            if v.is_null() {
                                Some(String::new())
                            } else if let Some(s) = v.as_str() {
                                Some(s.to_string())
                            } else if let Some(n) = v.as_f64() {
                                Some(n.to_string())
                            } else {
                                None
                            }
                        })
                        .unwrap_or_default();
                    
                    let avg_px = exec_report.get("avg_px")
                        .and_then(|v| {
                            if v.is_null() {
                                Some(String::new())
                            } else if let Some(s) = v.as_str() {
                                Some(s.to_string())
                            } else if let Some(n) = v.as_f64() {
                                Some(n.to_string())
                            } else {
                                None
                            }
                        })
                        .unwrap_or_default();
                    
                    let transact_time = exec_report.get("transact_time")
                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                        .unwrap_or_else(|| String::new());
                    
                    let order_create_time = exec_report.get("order_create_time")
                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                        .unwrap_or_else(|| String::new());
                    
                    let rejection_reason = exec_report.get("rejection_reason_text")
                        .and_then(|v| v.as_str().map(|s| s.to_string()))
                        .or_else(|| {
                            exec_report.get("rejection_reason_code")
                                .and_then(|v| v.as_str().map(|s| s.to_string()))
                        })
                        .unwrap_or_else(|| String::new());
                    
                    // Write CSV row (escape commas in fields)
                    let escape_csv = |s: &str| {
                        if s.contains(',') || s.contains('"') || s.contains('\n') {
                            format!("\"{}\"", s.replace("\"", "\"\""))
                        } else {
                            s.to_string()
                        }
                    };
                    
                    csv_content.push_str(&format!(
                        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
                        escape_csv(&cl_ord_id),
                        escape_csv(&order_id),
                        escape_csv(&exec_id),
                        escape_csv(&symbol),
                        escape_csv(&side),
                        escape_csv(&order_qty),
                        escape_csv(&price),
                        escape_csv(&ord_status),
                        escape_csv(&exec_type),
                        escape_csv(&cum_qty),
                        escape_csv(&leaves_qty.to_string()),
                        escape_csv(&last_px),
                        escape_csv(&last_qty),
                        escape_csv(&avg_px),
                        escape_csv(&transact_time),
                        escape_csv(&order_create_time),
                        escape_csv(&rejection_reason)
                    ));
                }
            }
        }
    }
    
    // Write CSV file
    let mut file = fs::File::create(output_file)?;
    file.write_all(csv_content.as_bytes())?;
    
    println!("✅ Created open orders summary: {}", output_file);
    println!("   Total open orders: {}", open_orders_count);
    
    Ok(())
}

