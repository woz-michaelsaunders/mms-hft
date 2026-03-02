// kdb+ Integration Module
// Handles writing order book data to kdb+ via direct IPC connection using kxkdb library
// Optimized for high-speed bulk inserts

use anyhow::{anyhow, Result};
use log::{info, error, warn};
use std::sync::Arc;
use tokio::sync::Mutex;
use chrono::Utc;
use kxkdb::ipc::*;
use kxkdb::qtype;
use std::collections::VecDeque;

/// kdb+ epoch offset in nanoseconds (nanoseconds from Unix epoch 1970-01-01 to kdb+ epoch 2000-01-01)
const KDB_TIMESTAMP_OFFSET_NS: i64 = 946_684_800_000_000_000_i64;

// Batch buffer structures
#[derive(Clone)]
struct OwnTrade {
    timestamp: i64,
    symbol: String,
    side: char,
    last_px: f64,
    last_qty: f64,
    avg_px: f64,
    cl_ord_id: String,
    order_id: String,
    exec_type: char,
}

#[derive(Clone)]
struct OrderBookLevel {
    timestamp: i64,
    symbol: String,
    side: char,
    price: f64,
    size: f64,
    action: char,
}

#[derive(Clone)]
struct Trade {
    timestamp: i64,
    symbol: String,
    price: f64,
    size: f64,
}

#[derive(Clone)]
struct Snapshot {
    timestamp: i64,
    symbol: String,
    best_bid: f64,
    best_ask: f64,
    best_bid_size: f64,
    best_ask_size: f64,
    spread: f64,
}

/// kdb+ IPC connection manager using kxkdb library with batched writes for maximum performance
pub struct KdbWriter {
    stream: Arc<Mutex<Option<QStream>>>,
    host: String,
    port: u16,
    connected: Arc<tokio::sync::Mutex<bool>>,
    
    // Batch buffers
    level_buffer: Arc<tokio::sync::Mutex<VecDeque<OrderBookLevel>>>,
    trade_buffer: Arc<tokio::sync::Mutex<VecDeque<Trade>>>,
    snapshot_buffer: Arc<tokio::sync::Mutex<VecDeque<Snapshot>>>,
    own_trade_buffer: Arc<tokio::sync::Mutex<VecDeque<OwnTrade>>>,
    
    // Batch size thresholds
    batch_size: usize,
}

impl KdbWriter {
    pub fn new(host: String, port: u16) -> Self {
        KdbWriter {
            stream: Arc::new(Mutex::new(None)),
            host,
            port,
            connected: Arc::new(tokio::sync::Mutex::new(false)),
            level_buffer: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
            trade_buffer: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
            snapshot_buffer: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
            own_trade_buffer: Arc::new(tokio::sync::Mutex::new(VecDeque::new())),
            batch_size: 100, // Flush when buffer reaches 100 items
        }
    }

    /// Connect to kdb+ server via IPC
    pub async fn initialize(&self) -> Result<()> {
        let addr = format!("{}:{}", self.host, self.port);
        info!("🔌 Connecting to kdb+ at {}...", addr);
        
        // Connect using kxkdb library
        match QStream::connect(ConnectionMethod::TCP, &self.host, self.port, "").await {
            Ok(stream) => {
                *self.stream.lock().await = Some(stream);
                *self.connected.lock().await = true;
                info!("✅ Connected to kdb+ at {}", addr);
                
                // Initialize tables in kdb+
                self.initialize_tables().await?;
                
                // Start background flush task
                self.start_flush_task();
                
                Ok(())
            }
            Err(e) => {
                error!("Failed to connect to kdb+ at {}: {}", addr, e);
                Err(anyhow!("Connection failed: {}", e))
            }
        }
    }

    /// Start background task to periodically flush buffers
    fn start_flush_task(&self) {
        let level_buf = Arc::clone(&self.level_buffer);
        let trade_buf = Arc::clone(&self.trade_buffer);
        let snapshot_buf = Arc::clone(&self.snapshot_buffer);
        let own_trade_buf = Arc::clone(&self.own_trade_buffer);
        let stream = Arc::clone(&self.stream);
        let connected = Arc::clone(&self.connected);
        let batch_size = self.batch_size;
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(100)); // Flush every 100ms
            loop {
                interval.tick().await;
                
                // Flush if buffers have data
                if *connected.lock().await {
                    // Flush levels
                    let mut levels = level_buf.lock().await;
                    if !levels.is_empty() {
                        let batch: Vec<_> = levels.drain(..).collect();
                        drop(levels);
                        if let Err(e) = Self::flush_levels_batch(&stream, &batch).await {
                            warn!("Failed to flush level batch: {}", e);
                        }
                    }
                    
                    // Flush trades
                    let mut trades = trade_buf.lock().await;
                    if !trades.is_empty() {
                        let batch: Vec<_> = trades.drain(..).collect();
                        drop(trades);
                        if let Err(e) = Self::flush_trades_batch(&stream, &batch).await {
                            warn!("Failed to flush trade batch: {}", e);
                        }
                    }
                    
                    // Flush snapshots
                    let mut snapshots = snapshot_buf.lock().await;
                    if !snapshots.is_empty() {
                        let batch: Vec<_> = snapshots.drain(..).collect();
                        drop(snapshots);
                        if let Err(e) = Self::flush_snapshots_batch(&stream, &batch).await {
                            warn!("Failed to flush snapshot batch: {}", e);
                        }
                    }
                    
                    // Flush own trades (execution fills)
                    let mut own_trades = own_trade_buf.lock().await;
                    if !own_trades.is_empty() {
                        let batch: Vec<_> = own_trades.drain(..).collect();
                        drop(own_trades);
                        if let Err(e) = Self::flush_own_trades_batch(&stream, &batch).await {
                            warn!("Failed to flush own_trades batch: {}", e);
                        }
                    }
                }
                let _ = batch_size; // suppress unused warning
            }
        });
    }

    /// Initialize kdb+ tables with optimized schema
    async fn initialize_tables(&self) -> Result<()> {
        info!("📊 Initializing kdb+ tables...");
        
        // Create tables - use simple assignment (tables should already exist from server script, but ensure they do)
        // Note: Tables are created in the kdb+ server script, but we verify/ensure they exist here
        let init_levels = "if[not `orderbook_levels in key `; orderbook_levels: ([] time:`timestamp$(); symbol:`symbol$(); side:`char$(); price:`float$(); size:`float$(); action:`char$()); ];";
        let init_trades = "if[not `trades in key `; trades: ([] time:`timestamp$(); symbol:`symbol$(); price:`float$(); size:`float$()); ];";
        let init_snapshots = "if[not `orderbook_snapshots in key `; orderbook_snapshots: ([] time:`timestamp$(); symbol:`symbol$(); best_bid:`float$(); best_ask:`float$(); best_bid_size:`float$(); best_ask_size:`float$(); spread:`float$()); ];";
        let init_securities = "if[not `securities in key `; securities: ([] time:`timestamp$(); symbol:`symbol$(); security_id:`symbol$(); exchange:`symbol$(); attributes_json:()); ];";
        let init_own_trades = "if[not `own_trades in key `; own_trades: ([] time:`timestamp$(); symbol:`symbol$(); side:`char$(); last_px:`float$(); last_qty:`float$(); avg_px:`float$(); cl_ord_id:`symbol$(); order_id:`symbol$(); exec_type:`char$()); ];";
        
        // Execute initialization and verify
        self.execute_q(init_levels).await?;
        self.execute_q(init_trades).await?;
        self.execute_q(init_snapshots).await?;
        self.execute_q(init_securities).await?;
        self.execute_q(init_own_trades).await?;
        
        // Verify tables exist
        match self.execute_q("count orderbook_levels").await {
            Ok(_) => info!("✅ Verified orderbook_levels table exists"),
            Err(e) => warn!("⚠️  Could not verify orderbook_levels: {}", e),
        }
        
        info!("✅ kdb+ tables initialized");
        Ok(())
    }

    /// Execute a q expression via IPC using kxkdb (async for speed)
    async fn execute_q_async(&self, q_code: &str) -> Result<()> {
        if !*self.connected.lock().await {
            return Err(anyhow!("Not connected to kdb+"));
        }
        
        let mut stream_guard = self.stream.lock().await;
        let stream = stream_guard.as_mut()
            .ok_or_else(|| anyhow!("Stream not available"))?;
        
        // Use async message for speed (no response wait)
        match stream.send_async_message(&q_code).await {
            Ok(_) => Ok(()),
            Err(e) => {
                warn!("Failed to execute q code async: {} - Error: {}", q_code, e);
                Err(anyhow!("kdb+ execution error: {}", e))
            }
        }
    }

    /// Execute a single insert statement (helper for batch inserts)
    async fn execute_single_insert(stream: &Arc<Mutex<Option<QStream>>>, q_code: &str) -> Result<()> {
        let mut stream_guard = stream.lock().await;
        let qstream = stream_guard.as_mut()
            .ok_or_else(|| anyhow!("Stream not available"))?;
        // Use sync message to get error feedback
        match qstream.send_sync_message(&q_code).await {
            Ok(_) => Ok(()),
            Err(e) => Err(anyhow!("Insert failed: {}", e))
        }
    }

    /// Execute a q expression via IPC using kxkdb (sync for initialization)
    async fn execute_q(&self, q_code: &str) -> Result<()> {
        if !*self.connected.lock().await {
            return Err(anyhow!("Not connected to kdb+"));
        }
        
        let mut stream_guard = self.stream.lock().await;
        let stream = stream_guard.as_mut()
            .ok_or_else(|| anyhow!("Stream not available"))?;
        
        // Use sync message for initialization (need confirmation)
        match stream.send_sync_message(&q_code).await {
            Ok(_) => Ok(()),
            Err(e) => {
                warn!("Failed to execute q code: {} - Error: {}", q_code, e);
                Err(anyhow!("kdb+ execution error: {}", e))
            }
        }
    }

    /// Flush a batch of order book levels using bulk insert (FASTEST method)
    async fn flush_levels_batch(stream: &Arc<Mutex<Option<QStream>>>, batch: &[OrderBookLevel]) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        
        use log::info;
        info!("🔄 Flushing {} order book levels to kdb+", batch.len());
        
        // Build bulk insert with proper kdb+ list syntax
        // Convert timestamps to kdb+ timestamp format: `timestamp$nanoseconds`
        let times: Vec<String> = batch.iter().map(|l| {
            // Convert nanoseconds to kdb+ timestamp
            format!("`timestamp${}", l.timestamp)
        }).collect();
        let symbols: Vec<String> = batch.iter().map(|l| format!("`{}", l.symbol)).collect();
        let sides: Vec<String> = batch.iter().map(|l| format!("`{}", l.side)).collect();
        let prices: Vec<String> = batch.iter().map(|l| format!("{}f", l.price)).collect();
        let sizes: Vec<String> = batch.iter().map(|l| format!("{}f", l.size)).collect();
        let actions: Vec<String> = batch.iter().map(|l| format!("`{}", l.action)).collect();
        
        // Insert rows one at a time using execute_q_async
        // This ensures each insert is executed properly
        let mut success_count = 0;
        for level in batch {
            // Use table append syntax (this works!)
            let q_code = format!(
                "orderbook_levels: orderbook_levels, enlist (`timestamp${}; `{}; `{}; {}f; {}f; `{})",
                level.timestamp, level.symbol, level.side, level.price, level.size, level.action
            );
            if let Ok(()) = Self::execute_single_insert(stream, q_code.as_str()).await {
                success_count += 1;
            } else {
                warn!("❌ Failed to insert level: {}", q_code);
            }
        }
        
        if success_count > 0 {
            info!("✅ Successfully flushed {} order book levels", success_count);
        }
        Ok(())
    }

    /// Flush a batch of trades using bulk insert
    async fn flush_trades_batch(stream: &Arc<Mutex<Option<QStream>>>, batch: &[Trade]) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        
        use log::info;
        info!("🔄 Flushing {} trades to kdb+", batch.len());
        
        let times: Vec<String> = batch.iter().map(|t| format!("`timestamp${}", t.timestamp)).collect();
        let symbols: Vec<String> = batch.iter().map(|t| format!("`{}", t.symbol)).collect();
        let prices: Vec<String> = batch.iter().map(|t| format!("{}f", t.price)).collect();
        let sizes: Vec<String> = batch.iter().map(|t| format!("{}f", t.size)).collect();
        
        // Insert rows one at a time
        let mut success_count = 0;
        for trade in batch {
            // Use direct table append
            let q_code = format!(
                "trades: trades, enlist (`timestamp${}; `{}; {}f; {}f)",
                trade.timestamp, trade.symbol, trade.price, trade.size
            );
            if let Ok(()) = Self::execute_single_insert(stream, q_code.as_str()).await {
                success_count += 1;
            } else {
                warn!("❌ Failed to insert trade: {}", q_code);
            }
        }
        
        if success_count > 0 {
            info!("✅ Successfully flushed {} trades", success_count);
        }
        Ok(())
    }

    /// Flush a batch of snapshots using bulk insert
    async fn flush_snapshots_batch(stream: &Arc<Mutex<Option<QStream>>>, batch: &[Snapshot]) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        
        use log::info;
        info!("🔄 Flushing {} snapshots to kdb+", batch.len());
        
        let times: Vec<String> = batch.iter().map(|s| format!("`timestamp${}", s.timestamp)).collect();
        let symbols: Vec<String> = batch.iter().map(|s| format!("`{}", s.symbol)).collect();
        let best_bids: Vec<String> = batch.iter().map(|s| format!("{}f", s.best_bid)).collect();
        let best_asks: Vec<String> = batch.iter().map(|s| format!("{}f", s.best_ask)).collect();
        let best_bid_sizes: Vec<String> = batch.iter().map(|s| format!("{}f", s.best_bid_size)).collect();
        let best_ask_sizes: Vec<String> = batch.iter().map(|s| format!("{}f", s.best_ask_size)).collect();
        let spreads: Vec<String> = batch.iter().map(|s| format!("{}f", s.spread)).collect();
        
        // Insert rows one at a time
        let mut success_count = 0;
        for snapshot in batch {
            // Use direct table append
            let q_code = format!(
                "orderbook_snapshots: orderbook_snapshots, enlist (`timestamp${}; `{}; {}f; {}f; {}f; {}f; {}f)",
                snapshot.timestamp, snapshot.symbol, snapshot.best_bid, snapshot.best_ask,
                snapshot.best_bid_size, snapshot.best_ask_size, snapshot.spread
            );
            if let Ok(()) = Self::execute_single_insert(stream, q_code.as_str()).await {
                success_count += 1;
            } else {
                warn!("❌ Failed to insert snapshot: {}", q_code);
            }
        }
        
        if success_count > 0 {
            info!("✅ Successfully flushed {} snapshots", success_count);
        }
        Ok(())
    }

    /// Write order book level update to kdb+ (buffered, batched)
    pub async fn write_order_book_level(
        &self,
        symbol: &str,
        side: char,
        price: f64,
        size: f64,
        action: char,
    ) -> Result<()> {
        use log::info;
        info!("📝 write_order_book_level called: {} {} {} {} {}", symbol, side, price, size, action);
        
        if !*self.connected.lock().await {
            warn!("⚠️  Not connected to kdb+, skipping write");
            return Ok(()); // Silently skip if not connected
        }
        
        let timestamp = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        
        let level = OrderBookLevel {
            timestamp,
            symbol: symbol.to_string(),
            side,
            price,
            size,
            action,
        };
        
        let mut buffer = self.level_buffer.lock().await;
        buffer.push_back(level);
        let buffer_len = buffer.len();
        drop(buffer);
        
        info!("📦 Buffered order book level. Buffer size: {}", buffer_len);
        
        // Flush immediately if batch size reached
        if buffer_len >= self.batch_size {
            let mut buffer = self.level_buffer.lock().await;
            let batch: Vec<_> = buffer.drain(..).collect();
            drop(buffer);
            
            if let Err(e) = Self::flush_levels_batch(&self.stream, &batch).await {
                warn!("Failed to flush level batch: {}", e);
            }
        }
        
        Ok(())
    }

    /// Write trade to kdb+ (buffered, batched)
    pub async fn write_trade(
        &self,
        symbol: &str,
        price: f64,
        size: f64,
        timestamp: i64,
    ) -> Result<()> {
        if !*self.connected.lock().await {
            return Ok(()); // Silently skip if not connected
        }
        
        let trade = Trade {
            timestamp,
            symbol: symbol.to_string(),
            price,
            size,
        };
        
        let mut buffer = self.trade_buffer.lock().await;
        buffer.push_back(trade);
        
        // Flush immediately if batch size reached
        if buffer.len() >= self.batch_size {
            let batch: Vec<_> = buffer.drain(..).collect();
            drop(buffer);
            
            if let Err(e) = Self::flush_trades_batch(&self.stream, &batch).await {
                warn!("Failed to flush trade batch: {}", e);
            }
        }
        
        Ok(())
    }

    /// Write best bid/ask snapshot to kdb+ (buffered, batched)
    pub async fn write_snapshot(
        &self,
        symbol: &str,
        best_bid: f64,
        best_ask: f64,
        best_bid_size: f64,
        best_ask_size: f64,
        spread: f64,
    ) -> Result<()> {
        if !*self.connected.lock().await {
            return Ok(()); // Silently skip if not connected
        }
        
        let timestamp = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        
        let snapshot = Snapshot {
            timestamp,
            symbol: symbol.to_string(),
            best_bid,
            best_ask,
            best_bid_size,
            best_ask_size,
            spread,
        };
        
        let mut buffer = self.snapshot_buffer.lock().await;
        buffer.push_back(snapshot);
        
        // Flush immediately if batch size reached
        if buffer.len() >= self.batch_size {
            let batch: Vec<_> = buffer.drain(..).collect();
            drop(buffer);
            
            if let Err(e) = Self::flush_snapshots_batch(&self.stream, &batch).await {
                warn!("Failed to flush snapshot batch: {}", e);
            }
        }
        
        Ok(())
    }

    /// Flush a batch of own execution fills to kdb+
    async fn flush_own_trades_batch(stream: &Arc<Mutex<Option<QStream>>>, batch: &[OwnTrade]) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }
        
        use log::info;
        info!("🔄 Flushing {} own trades to kdb+", batch.len());
        
        let mut success_count = 0;
        for t in batch {
            let q_code = format!(
                "own_trades: own_trades, enlist (`timestamp${}; `{}; `{}; {}f; {}f; {}f; `{}; `{}; `{})",
                t.timestamp, t.symbol, t.side, t.last_px, t.last_qty, t.avg_px,
                t.cl_ord_id, t.order_id, t.exec_type
            );
            if let Ok(()) = Self::execute_single_insert(stream, &q_code).await {
                success_count += 1;
            } else {
                warn!("❌ Failed to insert own_trade: cl_ord_id={}", t.cl_ord_id);
            }
        }
        
        if success_count > 0 {
            info!("✅ Successfully flushed {} own trades", success_count);
        }
        Ok(())
    }

    /// Write an own execution fill to kdb+ (buffered)
    /// Call this from the FIX execution report handler when ExecType is F (partial) or 2 (full fill).
    pub async fn write_own_trade(
        &self,
        symbol: &str,
        side: char,
        last_px: f64,
        last_qty: f64,
        avg_px: f64,
        cl_ord_id: &str,
        order_id: &str,
        exec_type: char,
    ) -> Result<()> {
        if !*self.connected.lock().await {
            return Ok(());
        }
        
        let timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        
        let own_trade = OwnTrade {
            timestamp,
            symbol: symbol.to_string(),
            side,
            last_px,
            last_qty,
            avg_px,
            cl_ord_id: cl_ord_id.to_string(),
            order_id: order_id.to_string(),
            exec_type,
        };
        
        let mut buffer = self.own_trade_buffer.lock().await;
        buffer.push_back(own_trade);
        
        if buffer.len() >= self.batch_size {
            let batch: Vec<_> = buffer.drain(..).collect();
            drop(buffer);
            if let Err(e) = Self::flush_own_trades_batch(&self.stream, &batch).await {
                warn!("Failed to flush own_trades batch: {}", e);
            }
        }
        
        Ok(())
    }

    /// Write securities to kdb+ (bulk insert, no batching needed as this is infrequent)
    pub async fn write_securities(&self, securities: &[(String, Option<String>, Option<String>, String)]) -> Result<()> {
        if !*self.connected.lock().await {
            warn!("⚠️  Not connected to kdb+, skipping securities write");
            return Ok(()); // Silently skip if not connected
        }
        
        if securities.is_empty() {
            return Ok(());
        }
        
        use log::info;
        info!("📝 Writing {} securities to kdb+", securities.len());
        
        let timestamp = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        
        // Insert securities one at a time using proper table append syntax
        let mut success_count = 0;
        for (symbol, security_id, exchange, attributes_json) in securities {
            // Escape JSON string for kdb+
            let escaped_attrs = attributes_json.replace('\\', "\\\\").replace('"', "\\\"");
            
            // Format optional fields
            let sec_id_str = security_id.as_ref()
                .map(|s| format!("`{}", s))
                .unwrap_or_else(|| "`".to_string());
            let exch_str = exchange.as_ref()
                .map(|s| format!("`{}", s))
                .unwrap_or_else(|| "`".to_string());
            
            // Use direct table append (same pattern as orderbook_levels)
            let q_code = format!(
                "securities: securities, enlist (`timestamp${}; `{}; {}; {}; \"{}\")",
                timestamp, symbol, sec_id_str, exch_str, escaped_attrs
            );
            
            if let Ok(()) = Self::execute_single_insert(&self.stream, q_code.as_str()).await {
                success_count += 1;
            } else {
                warn!("❌ Failed to insert security: {}", symbol);
            }
        }
        
        if success_count > 0 {
            info!("✅ Successfully wrote {} securities to kdb+", success_count);
        }
        
        Ok(())
    }

    /// Execute a kdb+ query and return the result serialised as a JSON string.
    /// Wraps `q_query` with `.j.j` automatically.
    pub async fn query_json(&self, q_query: &str) -> Result<String> {
        self.query_json_full(&format!(".j.j {}", q_query)).await
    }

    /// Execute a complete kdb+ expression and return the string result as-is.
    /// Use this when you need full control over the expression (e.g. to pre-convert types
    /// before calling `.j.j`), or when the expression already contains `.j.j`.
    pub async fn query_json_full(&self, full_expr: &str) -> Result<String> {
        if !*self.connected.lock().await {
            return Err(anyhow!("Not connected to kdb+"));
        }

        let mut stream_guard = self.stream.lock().await;
        let stream = stream_guard.as_mut()
            .ok_or_else(|| anyhow!("Stream not available"))?;

        match stream.send_sync_message(&full_expr).await {
            Ok(k) => {
                match k.as_string() {
                    Ok(s) => Ok(s.to_string()),
                    Err(_) => {
                        // K object is not a string — likely a kdb+ error object ('type, etc.)
                        // Return it via Display so the caller can detect and report it.
                        Ok(format!("{}", k))
                    }
                }
            }
            Err(e) => Err(anyhow!("kdb+ query error: {}", e)),
        }
    }

    /// Execute a kdb+ SELECT query and return the result as a JSON array of row objects.
    /// This bypasses `.j.j` entirely and converts the kdb+ binary TABLE response on the
    /// Rust side, avoiding serialization issues with timestamp$/symbol$ types in old kdb+.
    /// Returns a JSON string: `[{"col": val, ...}, ...]` or `[]` for empty tables.
    pub async fn query_table_to_json(&self, q_query: &str) -> Result<String> {
        if !*self.connected.lock().await {
            return Err(anyhow!("Not connected to kdb+"));
        }

        let mut stream_guard = self.stream.lock().await;
        let stream = stream_guard.as_mut()
            .ok_or_else(|| anyhow!("Stream not available"))?;

        let k = stream.send_sync_message(&q_query).await
            .map_err(|e| anyhow!("kdb+ query error: {}", e))?;

        // Detect kdb+ error response
        if k.get_type() == qtype::ERROR {
            let err_msg = k.get_error_string().unwrap_or("unknown error");
            return Err(anyhow!("kdb+ error: '{}", err_msg));
        }

        // Expect a TABLE response (type 98)
        if k.get_type() != qtype::TABLE {
            // Might be a long (count), atom, etc. — fall back to Display for debug
            return Err(anyhow!("kdb+ returned non-table type {}: {}", k.get_type(), k));
        }

        Self::k_table_to_json(&k)
    }

    /// Convert a kxkdb TABLE K object to a JSON array string.
    /// Handles columns of type: timestamp (12), float (9), symbol (11), long (7).
    fn k_table_to_json(k: &K) -> Result<String> {
        // A TABLE in kxkdb is a flip of a dictionary.
        // get_dictionary() returns the underlying [keys: symbol_list, values: compound_list]
        let dict_k = k.get_dictionary()
            .map_err(|e| anyhow!("Failed to get table dictionary: {}", e))?;

        let kv = dict_k.as_vec::<K>()
            .map_err(|e| anyhow!("Failed to get dict K-vec: {}", e))?;

        let col_names = kv[0].as_vec::<S>()
            .map_err(|e| anyhow!("Failed to get column names: {}", e))?;

        let col_data = kv[1].as_vec::<K>()
            .map_err(|e| anyhow!("Failed to get column data: {}", e))?;

        if col_names.is_empty() || col_data.is_empty() {
            return Ok("[]".to_string());
        }

        // Determine the number of rows from the first column
        let nrows = k.len();
        if nrows == 0 {
            return Ok("[]".to_string());
        }

        let ncols = col_names.len().min(col_data.len());

        // Build rows
        let mut rows: Vec<String> = Vec::with_capacity(nrows);
        for row_idx in 0..nrows {
            let mut fields: Vec<String> = Vec::with_capacity(ncols);
            for col_idx in 0..ncols {
                let col_name = &col_names[col_idx];
                let col = &col_data[col_idx];
                let json_val = Self::k_col_value_to_json(col, row_idx);
                fields.push(format!("\"{}\":{}", col_name, json_val));
            }
            rows.push(format!("{{{}}}", fields.join(",")));
        }

        Ok(format!("[{}]", rows.join(",")))
    }

    /// Extract a single value from a typed kdb+ column K object and return it as a JSON token.
    fn k_col_value_to_json(col: &K, idx: usize) -> String {
        match col.get_type() {
            qtype::TIMESTAMP_LIST => {
                // i64 nanoseconds since kdb+ epoch (2000-01-01). Convert to Unix milliseconds.
                if let Ok(v) = col.as_vec::<J>() {
                    if let Some(&ns) = v.get(idx) {
                        let unix_ms = (ns + KDB_TIMESTAMP_OFFSET_NS) / 1_000_000;
                        return format!("{}", unix_ms);
                    }
                }
                "null".to_string()
            }
            qtype::FLOAT_LIST => {
                if let Ok(v) = col.as_vec::<F>() {
                    if let Some(&f) = v.get(idx) {
                        if f.is_finite() {
                            return format!("{}", f);
                        }
                    }
                }
                "null".to_string()
            }
            qtype::SYMBOL_LIST => {
                if let Ok(v) = col.as_vec::<S>() {
                    if let Some(s) = v.get(idx) {
                        let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
                        return format!("\"{}\"", escaped);
                    }
                }
                "\"\"".to_string()
            }
            qtype::LONG_LIST => {
                if let Ok(v) = col.as_vec::<J>() {
                    if let Some(&j) = v.get(idx) {
                        return format!("{}", j);
                    }
                }
                "null".to_string()
            }
            qtype::INT_LIST => {
                if let Ok(v) = col.as_vec::<I>() {
                    if let Some(&i) = v.get(idx) {
                        return format!("{}", i);
                    }
                }
                "null".to_string()
            }
            _ => {
                // Fallback: use Display on the whole column and extract the idx-th element
                "null".to_string()
            }
        }
    }

    /// Disconnect from kdb+ and flush any remaining buffers
    pub async fn disconnect(&self) {
        *self.connected.lock().await = false;
        
        // Flush remaining buffers
        let levels: Vec<_> = self.level_buffer.lock().await.drain(..).collect();
        let trades: Vec<_> = self.trade_buffer.lock().await.drain(..).collect();
        let snapshots: Vec<_> = self.snapshot_buffer.lock().await.drain(..).collect();
        let own_trades: Vec<_> = self.own_trade_buffer.lock().await.drain(..).collect();
        
        if !levels.is_empty() {
            let _ = Self::flush_levels_batch(&self.stream, &levels).await;
        }
        if !trades.is_empty() {
            let _ = Self::flush_trades_batch(&self.stream, &trades).await;
        }
        if !snapshots.is_empty() {
            let _ = Self::flush_snapshots_batch(&self.stream, &snapshots).await;
        }
        if !own_trades.is_empty() {
            let _ = Self::flush_own_trades_batch(&self.stream, &own_trades).await;
        }
        
        if let Some(mut stream) = self.stream.lock().await.take() {
            if let Err(e) = stream.shutdown().await {
                warn!("Error shutting down kdb+ connection: {}", e);
            }
        }
        info!("🔌 Disconnected from kdb+");
    }
}
