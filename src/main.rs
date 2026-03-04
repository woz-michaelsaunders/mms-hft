mod fix_protocol;
mod order_book;
mod order_book_buffer;
mod kdb_integration;
pub mod algo;

use anyhow::{anyhow, Result};
use fix_protocol::{FIXMessage, tags};
use log::{info, error, warn};
use std::sync::Arc;
use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc, broadcast};
use tokio::time::{sleep, Duration, interval, Instant};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use zmq;
use order_book::{OrderBookSnapshot, Side, UpdateAction};
use order_book_buffer::DoubleBufferedOrderBook;
use arc_swap::ArcSwap;
use kdb_integration::KdbWriter;
use axum::{
    extract::Path as AxumPath,
    http::{header::{CACHE_CONTROL, HeaderValue}, StatusCode},
    response::{Html, Json},
    routing::{get, post},
    Router,
};
use tower_http::cors::CorsLayer;

const MAX_TICK_BUFFER: usize = 10_000;


// Configuration structure
#[derive(Debug, Clone, serde::Deserialize)]
struct Config {
    order_management: OrderManagementConfig,
    market_data_btc: MarketDataConfig,
    market_data_others: MarketDataOthersConfig,
    api: ApiConfig,
    connection: ConnectionConfig,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct OrderManagementConfig {
    gateway_url: String,
    sender_comp_id: String,
    target_comp_id: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct MarketDataConfig {
    gateway_url: String,
    sender_comp_id: String,
    target_comp_id: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct MarketDataOthersConfig {
    base_url: String,
    sender_comp_id: String,  // Same CompID for all three ports
    target_comp_id: String,
    port_34405: u16,
    port_34406: u16,
    port_34407: u16,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct ApiConfig {
    api_key: String,
    api_secret: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
struct ConnectionConfig {
    heartbeat_interval: u64,
    max_reconnect_attempts: u32,
    reconnect_backoff_ms: u64,
}

// Load configuration from file
fn load_config() -> Result<Config> {
    let config_path = "config.toml";
    let config_content = std::fs::read_to_string(config_path)
        .map_err(|e| anyhow!("Failed to read config file {}: {}", config_path, e))?;
    
    let config: Config = toml::from_str(&config_content)
        .map_err(|e| anyhow!("Failed to parse config file: {}", e))?;
    
    Ok(config)
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct OrderRequest {
    // Required fields
    pub cl_ord_id: String,      // Tag 11: ClOrdID
    pub symbol: String,          // Tag 55: Symbol
    pub side: char,              // Tag 54: Side (1=BUY, 2=SELL)
    pub qty: f64,                // Tag 38: OrderQty
    pub price: Option<f64>,     // Tag 44: Price (required for LIMIT orders)
    
    // Optional fields
    pub ord_type: Option<String>, // Tag 40: OrdType (1=MARKET, 2=LIMIT, D=PREVIOUSLY_QUOTED)
    pub time_in_force: Option<String>, // Tag 59: TimeInForce (1=GTC, 3=IOC, 4=FOK, 0=Day)
    pub currency: Option<String>, // Tag 15: Currency (for OTC orders)
    pub exec_inst: Option<String>, // Tag 18: ExecInst (6=POST_ONLY, 8=SMART_POST_ONLY)
    pub quote_id: Option<String>, // Tag 117: QuoteID (for OTC orders, must be "RFS" for OTC RFS)
    pub cash_order_qty: Option<f64>, // Tag 152: CashOrderQty (for MARKET BUY orders)
    pub cash_margin: Option<String>, // Tag 544: CashMargin (1=SPOT no margin, 2=SPOT with margin)
    pub trigger_price_type: Option<String>, // Tag 1107: TriggerPriceType (2=LAST_PRICE, I=INDEX_PRICE, M=MARK_PRICE)
    pub self_match_prevention_scope: Option<String>, // Tag 12362: SelfMatchPreventionScope (M, S)
    pub self_match_prevention_instruction: Option<String>, // Tag 2964: SelfMatchPreventionInstruction (1, 2, 3)
    pub self_match_prevention_id: Option<String>, // Tag 2362: SelfMatchPreventionID (0-32767)
    pub broker_id: Option<String>, // Tag 7933: BrokerId
    pub receive_window: Option<u32>, // Tag 20100: ReceiveWindow (100-5000 milliseconds)
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct CancelOrderRequest {
    // Required fields
    pub cl_ord_id: String,          // Tag 11: ClOrdID - Unique ID for this cancel request
    pub orig_cl_ord_id: String,      // Tag 41: OrigClOrdID - ClOrdID of the original order to cancel
    pub symbol: String,              // Tag 55: Symbol
    pub side: String,                // Tag 54: Side ("1"=BUY, "2"=SELL) - stored as String for JSON compatibility
}

/// Open order information extracted from execution reports
#[derive(Debug, Clone, serde::Serialize)]
pub struct OpenOrder {
    pub cl_ord_id: String,
    pub order_id: String,
    pub exec_id: String,
    pub symbol: String,
    pub side: String,
    pub order_qty: String,
    pub price: String,
    pub ord_status: String,
    pub exec_type: String,
    pub cum_qty: String,
    pub leaves_qty: String,
    pub last_px: String,
    pub last_qty: String,
    pub avg_px: String,
    pub transact_time: String,
    pub order_create_time: String,
    pub rejection_reason: String,
}

/// Thread-safe, lock-free store for open orders using ArcSwap for zero-lock snapshots
pub struct OpenOrdersStore {
    // ArcSwap allows lock-free atomic swaps of the entire HashMap
    // Readers can snapshot without blocking writers
    store: ArcSwap<HashMap<String, OpenOrder>>,
}

impl OpenOrdersStore {
    pub fn new() -> Self {
        Self {
            store: ArcSwap::from_pointee(HashMap::new()),
        }
    }

    /// Update or insert an order (thread-safe, uses ArcSwap for lock-free updates)
    pub fn update_order(&self, cl_ord_id: String, order: OpenOrder) {
        // Load current snapshot
        let current = self.store.load();
        
        // Create new HashMap with updated order
        let mut new_map = (**current).clone();
        new_map.insert(cl_ord_id, order);
        
        // Atomically swap the entire HashMap (lock-free)
        self.store.store(Arc::new(new_map));
    }

    /// Remove an order (thread-safe)
    pub fn remove_order(&self, cl_ord_id: &str) {
        let current = self.store.load();
        let mut new_map = (**current).clone();
        new_map.remove(cl_ord_id);
        self.store.store(Arc::new(new_map));
    }

    /// Get a lock-free snapshot of all open orders
    /// This is zero-copy and doesn't block writers
    pub fn snapshot(&self) -> Arc<HashMap<String, OpenOrder>> {
        self.store.load_full()
    }

    /// Get open orders only (filters out closed/rejected/cancelled orders)
    pub fn get_open_orders(&self) -> Vec<OpenOrder> {
        let snapshot = self.snapshot();
        snapshot
            .values()
            .filter(|order| {
                let leaves_qty: f64 = order.leaves_qty.parse().unwrap_or(0.0);
                let ord_status = &order.ord_status;
                let exec_type = &order.exec_type;
                
                // Order is open if:
                // - Has remaining quantity
                // - Not rejected (ord_status != "8")
                // - Not cancelled (ord_status != "4")
                // - Not fully filled (ord_status != "2")
                // - Not rejected (exec_type != "8")
                leaves_qty > 0.0
                    && ord_status != "8"
                    && ord_status != "4"
                    && ord_status != "2"
                    && exec_type != "8"
            })
            .cloned()
            .collect()
    }
}

/// Aggregated position for a single symbol, updated on every execution fill.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Position {
    pub symbol: String,
    /// Net quantity: positive = long, negative = short
    pub net_qty: f64,
    /// Volume-weighted average entry price of the current open position
    pub avg_entry_px: f64,
    /// Cumulative realized P&L (closed legs only)
    pub realized_pnl: f64,
    /// Unrealized P&L vs latest mid price (updated on read via order book)
    pub unrealized_pnl: f64,
    /// Most recent mid price used for unrealized P&L calculation
    pub last_mid_px: f64,
    /// Total number of fill events counted
    pub trade_count: u32,
}

impl Position {
    fn new(symbol: String) -> Self {
        Position {
            symbol,
            net_qty: 0.0,
            avg_entry_px: 0.0,
            realized_pnl: 0.0,
            unrealized_pnl: 0.0,
            last_mid_px: 0.0,
            trade_count: 0,
        }
    }

    /// Apply a fill: side "1"=buy, "2"=sell.  Returns updated self.
    fn apply_fill(&mut self, side: &str, last_qty: f64, last_px: f64) {
        if last_qty <= 0.0 || last_px <= 0.0 {
            return;
        }
        let signed_qty = if side == "1" { last_qty } else { -last_qty };
        let prev_net = self.net_qty;
        let new_net = prev_net + signed_qty;

        if prev_net == 0.0 {
            // Opening a new position
            self.avg_entry_px = last_px;
        } else if prev_net > 0.0 && signed_qty > 0.0 {
            // Adding to long
            let total_cost = self.avg_entry_px * prev_net + last_px * last_qty;
            self.avg_entry_px = total_cost / new_net;
        } else if prev_net < 0.0 && signed_qty < 0.0 {
            // Adding to short
            let total_cost = self.avg_entry_px * prev_net.abs() + last_px * last_qty;
            self.avg_entry_px = total_cost / new_net.abs();
        } else {
            // Closing / reversing
            let closing_qty = last_qty.min(prev_net.abs());
            if prev_net > 0.0 {
                // Long being reduced by a sell
                self.realized_pnl += closing_qty * (last_px - self.avg_entry_px);
            } else {
                // Short being reduced by a buy
                self.realized_pnl += closing_qty * (self.avg_entry_px - last_px);
            }
            if new_net.abs() < 1e-10 {
                self.avg_entry_px = 0.0;
            } else if new_net.signum() != prev_net.signum() {
                // Position reversed
                self.avg_entry_px = last_px;
            }
        }

        self.net_qty = new_net;
        self.trade_count += 1;
    }

    /// Refresh unrealized P&L using a live mid price.
    pub fn refresh_unrealized(&mut self, mid_px: f64) {
        if mid_px > 0.0 && self.net_qty.abs() > 1e-10 && self.avg_entry_px > 0.0 {
            self.unrealized_pnl = self.net_qty * (mid_px - self.avg_entry_px);
            self.last_mid_px = mid_px;
        } else {
            self.unrealized_pnl = 0.0;
        }
    }
}

/// Thread-safe, lock-free store for live positions using ArcSwap.
pub struct PositionsStore {
    store: ArcSwap<HashMap<String, Position>>,
}

impl PositionsStore {
    pub fn new() -> Self {
        Self {
            store: ArcSwap::from_pointee(HashMap::new()),
        }
    }

    /// Apply a fill event for a symbol (side "1"=buy / "2"=sell).
    pub fn apply_fill(&self, symbol: &str, side: &str, last_qty: f64, last_px: f64) {
        let current = self.store.load();
        let mut new_map = (**current).clone();
        let pos = new_map
            .entry(symbol.to_string())
            .or_insert_with(|| Position::new(symbol.to_string()));
        pos.apply_fill(side, last_qty, last_px);
        self.store.store(Arc::new(new_map));
    }

    /// Return a snapshot of all positions as a Vec.
    pub fn snapshot(&self) -> Vec<Position> {
        self.store.load().values().cloned().collect()
    }

    /// Replay fills from data/trades.csv to rebuild positions after a restart.
    /// CSV columns: timestamp_ns,cl_ord_id,order_id,symbol,side,exec_type,last_qty,last_px,avg_px
    pub fn load_from_trades_csv(&self) {
        use std::io::BufRead;
        let file = match std::fs::File::open("data/trades.csv") {
            Ok(f) => f,
            Err(_) => return, // no file yet — first run
        };
        let mut replayed = 0u32;
        let mut first = true;
        for line in std::io::BufReader::new(file).lines().flatten() {
            if first { first = false; continue; } // skip header
            let cols: Vec<&str> = line.splitn(9, ',').collect();
            if cols.len() < 9 { continue; }
            // cols: ts, cl_ord_id, order_id, symbol, side, exec_type, last_qty, last_px, avg_px
            let symbol   = cols[3];
            let side     = cols[4]; // "1" or "2"
            let last_qty: f64 = cols[6].parse().unwrap_or(0.0);
            let last_px:  f64 = cols[7].parse().unwrap_or(0.0);
            if last_qty > 0.0 && last_px > 0.0 {
                self.apply_fill(symbol, side, last_qty, last_px);
                replayed += 1;
            }
        }
        if replayed > 0 {
            info!("📂 Restored positions from data/trades.csv ({} fills replayed)", replayed);
        }
    }
}

/// Order processing timing metrics
#[derive(Debug, Clone, serde::Serialize)]
pub struct OrderTiming {
    pub file_read_us: Option<u64>,
    pub json_parse_us: Option<u64>,
    pub channel_send_us: Option<u64>,
    pub fix_encode_us: Option<u64>,
    pub fix_send_us: Option<u64>,
    pub execution_report_us: Option<u64>,
    pub total_us: Option<u64>,
    pub file_read_timestamp: Option<i64>,
    pub execution_report_timestamp: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct SecurityInfo {
    pub symbol: String,
    pub security_id: Option<String>,
    pub exchange: Option<String>,
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct TickData {
    pub timestamp: i64,  // nanoseconds since epoch
    pub symbol: String,
    pub bid: f64,
    pub ask: f64,
    pub bid_size: f64,
    pub ask_size: f64,
    pub last_price: f64,
    pub last_size: f64,
}

pub struct FIXSession {
    stream: Arc<Mutex<Option<TcpStream>>>,
    sender_comp_id: String,
    target_comp_id: String,
    seq_num: Arc<Mutex<u32>>,
    is_connected: Arc<Mutex<bool>>,
    connection_state: Arc<Mutex<ConnectionState>>,
    api_key: String,
    api_secret: String,
    // Track order submission times for latency measurement (keyed by ClOrdID)
    order_times: Arc<Mutex<HashMap<String, Instant>>>,
    // Track order processing timings (keyed by ClOrdID)
    order_timings: Arc<Mutex<HashMap<String, OrderTiming>>>,
    // Track original order data for writing processed JSON (keyed by ClOrdID)
    order_data: Arc<Mutex<HashMap<String, OrderRequest>>>,
    // Track securities received from market data server
    securities: Arc<Mutex<Vec<SecurityInfo>>>,
    // Track expected security count for auto-save trigger
    expected_security_count: Arc<Mutex<Option<usize>>>,
    // Track when we started receiving securities
    securities_last_save_time: Arc<Mutex<Instant>>,
    // Track if we've already saved to file
    securities_file_saved: Arc<Mutex<bool>>,
    // Buffer for partial FIX messages (TCP fragmentation)
    message_buffer: Arc<Mutex<Vec<u8>>>,
    // Store tick data for parquet export
    tick_data: Arc<Mutex<Vec<TickData>>>,
    // High-performance in-memory order books with double buffering (keyed by symbol)
    order_books: Arc<Mutex<HashMap<String, DoubleBufferedOrderBook>>>,
    // Lock-free open orders store using ArcSwap for zero-lock snapshots
    open_orders: Arc<OpenOrdersStore>,
    // Lock-free positions store — aggregated fills per symbol
    positions: Arc<PositionsStore>,
    // Event-driven update channels (optional, set after creation)
    order_update_tx: Arc<Mutex<Option<broadcast::Sender<()>>>>,
    order_book_update_tx: Arc<Mutex<Option<broadcast::Sender<String>>>>,
    // kdb+ writer for order book data (shared across sessions)
    kdb_writer: Arc<KdbWriter>,
}

#[derive(Debug, Clone, Copy)]
enum ConnectionState {
    Disconnected,
    Connecting,
    Connected,
    Logging,
    Ready,
    Disconnecting,
}

impl FIXSession {
    pub fn new(sender: &str, target: &str) -> Self {
        Self::new_with_credentials(sender, target, "", "")
    }
    
    pub fn new_with_credentials(sender: &str, target: &str, api_key: &str, api_secret: &str) -> Self {
        let kdb_host = std::env::var("KDB_HOST").unwrap_or_else(|_| "localhost".to_string());
        let kdb_port: u16 = std::env::var("KDB_PORT")
            .unwrap_or_else(|_| "5001".to_string())
            .parse()
            .unwrap_or(5001);
        Self::new_with_credentials_and_kdb(sender, target, api_key, api_secret, Arc::new(KdbWriter::new(kdb_host, kdb_port)))
    }
    
    pub fn new_with_credentials_and_kdb(sender: &str, target: &str, api_key: &str, api_secret: &str, kdb_writer: Arc<KdbWriter>) -> Self {
        FIXSession {
            stream: Arc::new(Mutex::new(None)),
            sender_comp_id: sender.to_string(),
            target_comp_id: target.to_string(),
            seq_num: Arc::new(Mutex::new(0)),
            is_connected: Arc::new(Mutex::new(false)),
            connection_state: Arc::new(Mutex::new(ConnectionState::Disconnected)),
            api_key: api_key.to_string(),
            api_secret: api_secret.to_string(),
            order_times: Arc::new(Mutex::new(HashMap::new())),
            order_timings: Arc::new(Mutex::new(HashMap::new())),
            order_data: Arc::new(Mutex::new(HashMap::new())),
            securities: Arc::new(Mutex::new(Vec::new())),
            expected_security_count: Arc::new(Mutex::new(None)),
            securities_last_save_time: Arc::new(Mutex::new(Instant::now())),
            securities_file_saved: Arc::new(Mutex::new(false)),
            message_buffer: Arc::new(Mutex::new(Vec::new())),
            tick_data: Arc::new(Mutex::new(Vec::new())),
            order_books: Arc::new(Mutex::new(HashMap::new())),
            open_orders: Arc::new(OpenOrdersStore::new()),
            positions: {
                let ps = Arc::new(PositionsStore::new());
                ps.load_from_trades_csv();
                ps
            },
            order_update_tx: Arc::new(Mutex::new(None)),
            order_book_update_tx: Arc::new(Mutex::new(None)),
            kdb_writer,
        }
    }
    
    /// Set event-driven update channels
    pub async fn set_update_channels(
        &self,
        order_tx: broadcast::Sender<()>,
        order_book_tx: broadcast::Sender<String>,
    ) {
        *self.order_update_tx.lock().await = Some(order_tx);
        *self.order_book_update_tx.lock().await = Some(order_book_tx);
        log::info!("✅ Update channels set for session");
    }

    /// Connect with automatic reconnection
    pub async fn connect_with_retry(&self, gateway_url: &str, max_attempts: u32, initial_backoff_ms: u64) -> Result<()> {
        let mut attempt = 0;
        let mut backoff = initial_backoff_ms;

        loop {
            attempt += 1;
            info!("Connection attempt {} to {}", attempt, gateway_url);

            // Try connection with 30-second timeout (FIX gateways can be slow)
            match tokio::time::timeout(Duration::from_secs(30), TcpStream::connect(gateway_url)).await {
                Ok(Ok(stream)) => {
                    // Set TCP_NODELAY for faster FIX messaging
                    let _ = stream.set_nodelay(true);

                    *self.stream.lock().await = Some(stream);
                    *self.connection_state.lock().await = ConnectionState::Connected;
                    info!("✅ Connected successfully to FIX gateway at attempt {}", attempt);
                    return Ok(());
                }
                Ok(Err(e)) => {
                    error!("Connection attempt {} failed: {}", attempt, e);

                    if attempt >= max_attempts {
                        return Err(anyhow!(
                            "Max reconnection attempts ({}) exceeded",
                            max_attempts
                        ));
                    }

                    warn!("Retrying in {}ms...", backoff);
                    sleep(Duration::from_millis(backoff)).await;

                    // Exponential backoff with max 30 seconds
                    backoff = std::cmp::min(backoff * 2, 30_000);
                }
                Err(_) => {
                    error!("Connection attempt {} timeout (30s)", attempt);

                    if attempt >= max_attempts {
                        return Err(anyhow!(
                            "Max reconnection attempts ({}) exceeded",
                            max_attempts
                        ));
                    }

                    warn!("Retrying in {}ms...", backoff);
                    sleep(Duration::from_millis(backoff)).await;

                    // Exponential backoff with max 30 seconds
                    backoff = std::cmp::min(backoff * 2, 30_000);
                }
            }
        }
    }

    pub async fn logon(&self, heartbeat_interval: u64) -> Result<()> {
        self.logon_with_auth(heartbeat_interval, false).await
    }

    pub async fn logon_market_data(&self, heartbeat_interval: u64) -> Result<()> {
        self.logon_with_auth(heartbeat_interval, true).await
    }

    async fn logon_with_auth(&self, heartbeat_interval: u64, is_market_data: bool) -> Result<()> {
        *self.connection_state.lock().await = ConnectionState::Logging;

        let seq_num = self.next_seq_num().await;
        let mut msg = FIXMessage::new('A', &self.sender_comp_id, &self.target_comp_id, seq_num);

        // Generate RawData - must be nonce in milliseconds per documentation
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let raw_data = now_ms.to_string();
        let raw_data_len = raw_data.len().to_string();

        // Different signature patterns based on flow:
        // Order Management: "public/auth" + MsgSeqNum (34) + api_key + "system_labelONEEX" + RawData (96)
        // Market Data: "public/auth" + MsgSeqNum (34) + api_key + RawData (96)
        let signature_input = if is_market_data {
            format!(
                "public/auth{}{}{}",
                seq_num,         // MsgSeqNum (field 34)
                self.api_key,         // api_key
                raw_data         // RawData (field 96)
            )
        } else {
            format!(
                "public/auth{}{}system_labelONEEX{}",
                seq_num,         // MsgSeqNum (field 34)
                self.api_key,         // api_key
                raw_data         // RawData (field 96)
            )
        };

        let mut mac = Hmac::<Sha256>::new_from_slice(self.api_secret.as_bytes())?;
        mac.update(signature_input.as_bytes());
        let signature_bytes = mac.finalize();
        let signature_hex = format!("{:x}", signature_bytes.into_bytes());

        // Set authentication fields per documentation
        msg.set_field(95, &raw_data_len);                    // RawDataLength (95) - REQUIRED
        msg.set_field(96, &raw_data);                        // RawData (96) - REQUIRED
        msg.set_field(98, "0");                              // EncryptMethod (98) - REQUIRED, must be 0
        msg.set_field(108, &heartbeat_interval.to_string()); // HeartBtInt (108) - REQUIRED
        msg.set_field(553, &self.api_key);                         // Username (553) - REQUIRED
        msg.set_field(554, &signature_hex);                  // Digital Signature (554) - REQUIRED
        msg.set_field(141, "Y");                             // ResetSeqNumFlag (141) - Reset sequence numbers

        let flow_type = if is_market_data { "Market Data" } else { "Order Management" };
        info!("Sending Logon (35=A) message ({}) with HMAC-SHA256 Digital Signature...", flow_type);
        info!("  Sender CompID (49): {}", self.sender_comp_id);
        info!("  Target CompID (56): {}", self.target_comp_id);
        info!("  MsgSeqNum (34): {}", seq_num);
        info!("  RawData (96): {}", raw_data);
        info!("  RawDataLength (95): {}", raw_data_len);
        info!("  Signature input: {}", signature_input);
        info!("  Digital Signature (554): {}", signature_hex);
        self.send_message(&msg).await?;

        // Don't set Ready yet - wait for ACK first
        // Connection state will be set to Ready when ACK is received
        *self.is_connected.lock().await = true;
        info!("Logon message transmitted. Waiting for ACK...");

        Ok(())
    }

    pub async fn logout(&self) -> Result<()> {
        *self.connection_state.lock().await = ConnectionState::Disconnecting;

        let seq_num = self.next_seq_num().await;
        let msg = FIXMessage::new('5', &self.sender_comp_id, &self.target_comp_id, seq_num);

        self.send_message(&msg).await?;

        *self.is_connected.lock().await = false;
        *self.connection_state.lock().await = ConnectionState::Disconnected;

        info!("Logout sent");

        Ok(())
    }

    pub async fn heartbeat(&self) -> Result<()> {
        let seq_num = self.next_seq_num().await;
        let msg = FIXMessage::new('0', &self.sender_comp_id, &self.target_comp_id, seq_num);
        self.send_message(&msg).await?;
        Ok(())
    }

    pub async fn order_status_request(
        &self,
        cl_ord_id: &str,
        symbol: &str,
    ) -> Result<()> {
        if !self.is_ready().await {
            return Err(anyhow!("Session not ready"));
        }

        let seq_num = self.next_seq_num().await;
        let mut msg = FIXMessage::new('H', &self.sender_comp_id, &self.target_comp_id, seq_num);

        // Required fields for Order Status Request
        msg.set_field(tags::CL_ORD_ID, cl_ord_id);
        msg.set_field(tags::SYMBOL, symbol);
        msg.set_field(tags::TRANSACT_TIME, &FIXMessage::timestamp());

        info!("Sending Order Status Request:");
        info!("  ClOrdID (11): {}", cl_ord_id);
        info!("  Symbol (55): {}", symbol);
        info!("  TransactTime (60): {}", FIXMessage::timestamp());

        self.send_message(&msg).await?;
        info!("Order Status Request sent successfully");

        Ok(())
    }

    /// Cancel an order using OrderCancelRequest (35=F)
    pub async fn cancel_order(&self, cancel_request: &CancelOrderRequest) -> Result<()> {
        if !self.is_ready().await {
            return Err(anyhow!("Session not ready"));
        }

        let seq_num = self.next_seq_num().await;
        let mut msg = FIXMessage::new('F', &self.sender_comp_id, &self.target_comp_id, seq_num);

        // Required fields for Order Cancel Request
        msg.set_field(tags::CL_ORD_ID, &cancel_request.cl_ord_id);
        msg.set_field(tags::ORIG_CL_ORD_ID, &cancel_request.orig_cl_ord_id);
        msg.set_field(tags::SYMBOL, &cancel_request.symbol);
        msg.set_field(tags::SIDE, &cancel_request.side);
        msg.set_field(tags::TRANSACT_TIME, &FIXMessage::timestamp());

        info!("Sending Order Cancel Request:");
        info!("  ClOrdID (11): {} (Cancel Request ID)", cancel_request.cl_ord_id);
        info!("  OrigClOrdID (41): {} (Original Order ID)", cancel_request.orig_cl_ord_id);
        info!("  Symbol (55): {}", cancel_request.symbol);
        info!("  Side (54): {}", cancel_request.side);
        info!("  TransactTime (60): {}", FIXMessage::timestamp());

        // Log FIX command
        let encoded = msg.encode()?;
        Self::log_fix_command(&msg, &encoded);

        // Send message
        let mut stream_guard = self.stream.lock().await;
        let stream = stream_guard.as_mut().ok_or_else(|| anyhow!("Not connected"))?;
        stream.write_all(&encoded).await?;
        stream.flush().await?;
        drop(stream_guard);

        info!("Order Cancel Request sent successfully");

        Ok(())
    }

    /// Create order from OrderRequest struct (supports all FIX order fields)
    pub async fn new_order_single_from_request(&self, order: &OrderRequest) -> Result<()> {
        if !self.is_ready().await {
            return Err(anyhow!("Session not ready"));
        }

        let seq_num = self.next_seq_num().await;
        let mut msg = FIXMessage::new('D', &self.sender_comp_id, &self.target_comp_id, seq_num);

        // Required fields
        msg.set_field(tags::CL_ORD_ID, &order.cl_ord_id);
        msg.set_field(tags::SYMBOL, &order.symbol);
        msg.set_field(tags::SIDE, &order.side.to_string());
        msg.set_field(tags::ORDER_QTY, &order.qty.to_string());
        msg.set_field(tags::TRANSACT_TIME, &FIXMessage::timestamp());

        // OrdType (40): Default to LIMIT (2) if not specified, but support MARKET (1) and PREVIOUSLY_QUOTED (D)
        let ord_type = order.ord_type.as_deref().unwrap_or("2");
        msg.set_field(tags::ORD_TYPE, ord_type);

        // Price (44): Required for LIMIT orders, optional for MARKET
        if let Some(price) = order.price {
            msg.set_field(tags::PRICE, &price.to_string());
        } else if ord_type == "2" {
            return Err(anyhow!("Price is required for LIMIT orders (OrdType=2)"));
        }

        // TimeInForce (59): Default to GTC (1) if not specified
        let time_in_force = order.time_in_force.as_deref().unwrap_or("1");
        msg.set_field(tags::TIME_IN_FORCE, time_in_force);

        // Optional fields
        if let Some(ref currency) = order.currency {
            msg.set_field(15, currency); // Tag 15: Currency
        }
        if let Some(ref exec_inst) = order.exec_inst {
            msg.set_field(18, exec_inst); // Tag 18: ExecInst
        }
        if let Some(ref quote_id) = order.quote_id {
            msg.set_field(117, quote_id); // Tag 117: QuoteID
        }
        if let Some(cash_order_qty) = order.cash_order_qty {
            msg.set_field(152, &cash_order_qty.to_string()); // Tag 152: CashOrderQty
        }
        if let Some(ref cash_margin) = order.cash_margin {
            msg.set_field(544, cash_margin); // Tag 544: CashMargin
        }
        if let Some(ref trigger_price_type) = order.trigger_price_type {
            msg.set_field(1107, trigger_price_type); // Tag 1107: TriggerPriceType
        }
        if let Some(ref smp_scope) = order.self_match_prevention_scope {
            msg.set_field(12362, smp_scope); // Tag 12362: SelfMatchPreventionScope
        }
        if let Some(ref smp_instruction) = order.self_match_prevention_instruction {
            msg.set_field(2964, smp_instruction); // Tag 2964: SelfMatchPreventionInstruction
        }
        if let Some(ref smp_id) = order.self_match_prevention_id {
            msg.set_field(2362, smp_id); // Tag 2362: SelfMatchPreventionID
        }
        if let Some(ref broker_id) = order.broker_id {
            msg.set_field(7933, broker_id); // Tag 7933: BrokerId
        }
        if let Some(receive_window) = order.receive_window {
            // Validate range: 100-5000 milliseconds
            let valid_window = receive_window.max(100).min(5000);
            msg.set_field(20100, &valid_window.to_string()); // Tag 20100: ReceiveWindow
        }

        // Log the order for debugging
        let transact_time = FIXMessage::timestamp();
        info!("Order message being sent:");
        info!("  ClOrdID (11): {}", order.cl_ord_id);
        info!("  Symbol (55): {}", order.symbol);
        info!("  Side (54): {}", order.side);
        info!("  OrderQty (38): {}", order.qty);
        if let Some(price) = order.price {
            info!("  Price (44): {}", price);
        }
        info!("  OrdType (40): {}", ord_type);
        info!("  TimeInForce (59): {}", time_in_force);
        info!("  TransactTime (60): {}", transact_time);

        // Measure FIX encoding time
        let fix_encode_start = Instant::now();
        let encoded = msg.encode()?;
        let fix_encode_us = fix_encode_start.elapsed().as_micros() as u64;
        
        // Log FIX command to separate file
        Self::log_fix_command(&msg, &encoded);
        
        // Measure FIX send time
        let fix_send_start = Instant::now();
        let mut stream_guard = self.stream.lock().await;
        let stream = stream_guard.as_mut().ok_or_else(|| anyhow!("Not connected"))?;
        stream.write_all(&encoded).await?;
        stream.flush().await?;
        let fix_send_us = fix_send_start.elapsed().as_micros() as u64;
        drop(stream_guard);
        
        // Record submission time for latency measurement
        let submission_time = Instant::now();
        self.order_times.lock().await.insert(order.cl_ord_id.clone(), submission_time);
        
        // Update timing metrics
        {
            let mut timings = self.order_timings.lock().await;
            if let Some(timing) = timings.get_mut(&order.cl_ord_id) {
                timing.fix_encode_us = Some(fix_encode_us);
                timing.fix_send_us = Some(fix_send_us);
            }
        }
        
        info!("Order {} sent (Encode: {}µs, Send: {}µs)", order.cl_ord_id, fix_encode_us, fix_send_us);

        Ok(())
    }

    /// Legacy new_order_single function (kept for backward compatibility)
    pub async fn new_order_single(
        &self,
        cl_ord_id: &str,
        symbol: &str,
        side: char,
        qty: f64,
        price: f64,
    ) -> Result<()> {
        let order = OrderRequest {
            cl_ord_id: cl_ord_id.to_string(),
            symbol: symbol.to_string(),
            side,
            qty,
            price: Some(price),
            ord_type: Some("2".to_string()), // LIMIT
            time_in_force: Some("1".to_string()), // GTC
            currency: None,
            exec_inst: None,
            quote_id: None,
            cash_order_qty: None,
            cash_margin: None,
            trigger_price_type: None,
            self_match_prevention_scope: None,
            self_match_prevention_instruction: None,
            self_match_prevention_id: None,
            broker_id: None,
            receive_window: None,
        };
        self.new_order_single_from_request(&order).await
    }

    pub async fn security_list_request(&self, req_id: &str) -> Result<()> {
        if !self.is_ready().await {
            return Err(anyhow!("Session not ready"));
        }

        let seq_num = self.next_seq_num().await;
        let mut msg = FIXMessage::new('x', &self.sender_comp_id, &self.target_comp_id, seq_num);

        // Required fields for SecurityListRequest per Crypto.com documentation
        // NOTE: Field order matters! Gateway expects: 320, 559, 263, 60 (NOT sorted)
        msg.set_field(tags::SECURITY_REQ_ID, req_id);                    // Tag 320 - REQUIRED
        msg.set_field(tags::SECURITY_LIST_REQUEST_TYPE, "0");            // Tag 559 - REQUIRED (0 = all securities per Crypto.com)
        msg.set_field(tags::SUBSCRIPTION_REQUEST_TYPE, "0");             // Tag 263 - REQUIRED (0=snapshot only, no streaming subscription)
        msg.set_field(tags::TRANSACT_TIME, &FIXMessage::timestamp());    // Tag 60

        info!("📋 Requesting all available securities...");
        info!("Sending SecurityListRequest (35=x):");
        info!("  SecurityReqID (320): {}", req_id);
        info!("  SecurityListRequestType (559): 0 (All Securities)");
        info!("  SubscriptionRequestType (263): 0 (Snapshot Only - No Streaming)");

        self.send_message(&msg).await?;
        info!("SecurityListRequest sent successfully");

        Ok(())
    }

    pub async fn market_data_request(&self, symbol: &str, market_depth: u32) -> Result<()> {
        if !self.is_ready().await {
            return Err(anyhow!("Session not ready"));
        }

        let seq_num = self.next_seq_num().await;
        let mut msg = FIXMessage::new('V', &self.sender_comp_id, &self.target_comp_id, seq_num);

        // MarketDataRequest fields - Crypto.com format
        // Per Crypto.com docs: MDReqID (262) must be in format: book.{Symbol}.{MarketDepth}
        // IMPORTANT: Field order and structure must match Crypto.com expectations exactly
        let md_req_id = format!("book.{}.{}", symbol, market_depth);
        
        // Set fields in the exact order Crypto.com expects (based on FIX 4.4 standard order)
        msg.set_field(262, &md_req_id);                       // Tag 262 - MarketDataReqID (format: book.SYMBOL.DEPTH)
        msg.set_field(263, "1");                              // Tag 263 - SubscriptionRequestType (1 = snapshot + subscribe)
        msg.set_field(264, &market_depth.to_string());        // Tag 264 - MarketDepth
        msg.set_field(265, "1");                              // Tag 265 - MDUpdateType (1 = Incremental Refresh - REQUIRED)
        msg.set_field(266, "Y");                              // Tag 266 - AggregatedBook (Y when 262 = book.{Symbol}.{MarketDepth} - REQUIRED)
        msg.set_field(547, "N");                              // Tag 547 - MDImplicitDelete (Must be N - REQUIRED)
        msg.set_field(267, "2");                              // Tag 267 - NoMDEntryTypes (2 entries: Bid and Ask)
        // Repeating group: MDEntryType
        // Group 1: Bid
        msg.set_field(269, "0");                              // Tag 269 - MDEntryType (0 = Bid)
        // Group 2: Ask  
        msg.set_field(269, "1");                              // Tag 269 - MDEntryType (1 = Ask)
        // Related symbols group
        msg.set_field(146, "1");                              // Tag 146 - NoRelatedSym (1 symbol)
        msg.set_field(55, symbol);                            // Tag 55 - Symbol
        msg.set_field(60, &FIXMessage::timestamp());          // Tag 60 - TransactTime

        info!("📊 Sending MarketDataRequest for {} (Real-time subscription)", symbol);
        info!("    MarketDataReqID (262): {} (format: book.SYMBOL.DEPTH)", md_req_id);
        info!("    MarketDepth (264): {}", market_depth);
        info!("    SubscriptionType (263): 1 (Subscribe)");
        info!("    MDUpdateType (265): 1 (Incremental Refresh - REQUIRED)");
        info!("    AggregatedBook (266): Y (REQUIRED for book.* format)");
        info!("    MDImplicitDelete (547): N (REQUIRED)");

        // Debug: Print the message before sending
        if let Ok(encoded) = msg.encode() {
            let msg_readable = String::from_utf8_lossy(&encoded);
            info!("DEBUG: MarketDataRequest message format: {}", msg_readable.replace("\x01", "|"));
        }

        self.send_message(&msg).await?;
        info!("✅ MarketDataRequest for {} sent successfully", symbol);

        Ok(())
    }

    pub async fn trade_data_request(&self, symbol: &str) -> Result<()> {
        if !self.is_ready().await {
            return Err(anyhow!("Session not ready"));
        }

        let seq_num = self.next_seq_num().await;
        let mut msg = FIXMessage::new('V', &self.sender_comp_id, &self.target_comp_id, seq_num);

        // MarketDataRequest fields for trade data - Crypto.com format
        // Per Crypto.com docs: MDReqID (262) must be in format: trade.{Symbol}
        let md_req_id = format!("trade.{}", symbol);
        
        // Set fields in the exact order Crypto.com expects
        msg.set_field(262, &md_req_id);                       // Tag 262 - MarketDataReqID (format: trade.SYMBOL)
        msg.set_field(263, "1");                              // Tag 263 - SubscriptionRequestType (1 = snapshot + subscribe)
        msg.set_field(264, "0");                              // Tag 264 - MarketDepth (Must be 0 when 262 = trade.{Symbol})
        msg.set_field(265, "1");                              // Tag 265 - MDUpdateType (1 = Incremental Refresh - REQUIRED)
        msg.set_field(266, "N");                              // Tag 266 - AggregatedBook (N when 262 = trade.{Symbol} - REQUIRED)
        msg.set_field(547, "N");                              // Tag 547 - MDImplicitDelete (Must be N - REQUIRED)
        msg.set_field(267, "1");                              // Tag 267 - NoMDEntryTypes (1 entry: Trade)
        // Repeating group: MDEntryType
        msg.set_field(269, "2");                              // Tag 269 - MDEntryType (2 = Trade)
        // Related symbols group
        msg.set_field(146, "1");                              // Tag 146 - NoRelatedSym (1 symbol)
        msg.set_field(55, symbol);                            // Tag 55 - Symbol
        msg.set_field(60, &FIXMessage::timestamp());          // Tag 60 - TransactTime

        info!("📊 Sending TradeDataRequest for {} (Real-time subscription)", symbol);
        info!("    MarketDataReqID (262): {} (format: trade.SYMBOL)", md_req_id);
        info!("    MarketDepth (264): 0 (REQUIRED for trade.* format)");
        info!("    SubscriptionType (263): 1 (Subscribe)");
        info!("    MDUpdateType (265): 1 (Incremental Refresh - REQUIRED)");
        info!("    AggregatedBook (266): N (REQUIRED for trade.* format)");
        info!("    MDImplicitDelete (547): N (REQUIRED)");
        info!("    MDEntryType (269): 2 (Trade)");

        // Debug: Print the message before sending
        if let Ok(encoded) = msg.encode() {
            let msg_readable = String::from_utf8_lossy(&encoded);
            info!("DEBUG: TradeDataRequest message format: {}", msg_readable.replace("\x01", "|"));
        }

        self.send_message(&msg).await?;
        info!("✅ TradeDataRequest for {} sent successfully", symbol);

        Ok(())
    }

    /// Get snapshot of order book for a symbol (lock-free read)
    pub async fn get_order_book_snapshot(&self, symbol: &str) -> Option<Arc<OrderBookSnapshot>> {
        let books = self.order_books.lock().await;
        books.get(symbol).map(|book| book.get_snapshot())
    }

    /// Get best bid/ask for a symbol (from snapshot, lock-free)
    pub async fn get_best_quote(&self, symbol: &str) -> Option<(f64, f64, f64)> {
        let books = self.order_books.lock().await;
        if let Some(book) = books.get(symbol) {
            let snap = book.get_snapshot();
            let bid = snap.best_bid;
            let ask = snap.best_ask;
            if bid > 0.0 && ask > 0.0 {
                Some((bid, ask, snap.spread()))
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Get order book depth (top N levels) from snapshot (lock-free)
    pub async fn get_order_book_depth(&self, symbol: &str, side: Side, depth: usize) -> Vec<(f64, f64)> {
        let books = self.order_books.lock().await;
        if let Some(book) = books.get(symbol) {
            book.get_snapshot().get_depth(side, depth)
        } else {
            Vec::new()
        }
    }

    /// Place small marketable orders to generate test trades
    /// Places buy orders at ask price and sell orders at bid price to ensure execution
    pub async fn place_test_orders_for_trades(&self, order_session: Arc<FIXSession>, symbol: &str, count: usize) -> Result<()> {
        info!("");
        info!("💹 Placing {} small test orders to generate trades...", count);
        
        // Get current market prices from order book
        let books = self.order_books.lock().await;
        let best_bid = books.get(symbol)
            .map(|b| b.get_active().best_bid())
            .unwrap_or(0.0);
        let best_ask = books.get(symbol)
            .map(|b| b.get_active().best_ask())
            .unwrap_or(0.0);
        drop(books);

        if best_bid <= 0.0 || best_ask <= 0.0 {
            warn!("⚠️  Cannot place test orders - order book not ready (Bid={}, Ask={})", best_bid, best_ask);
            return Err(anyhow!("Order book not ready"));
        }

        info!("  Current market: Bid={:.2}, Ask={:.2}, Spread={:.2}", best_bid, best_ask, best_ask - best_bid);
        
        // Place alternating buy/sell orders at market prices
        for i in 0..count {
            let order_num = i + 1;
            let side = if i % 2 == 0 { '1' } else { '2' }; // Alternate: Buy, Sell, Buy, Sell...
            let (price, side_name) = if side == '1' {
                // Buy order - place at ask price (will execute immediately)
                (best_ask, "Buy")
            } else {
                // Sell order - place at bid price (will execute immediately)
                (best_bid, "Sell")
            };
            
            // Very small quantity to minimize impact
            let qty = 0.0001; // 0.0001 BTC
            
            let cl_ord_id = format!("TEST_TRADE_{:03}", order_num);
            
            info!("  [{}/{}] Placing {} order: {} @ {:.2} (qty: {})", 
                  order_num, count, side_name, cl_ord_id, price, qty);
            
            if let Err(e) = order_session.new_order_single(&cl_ord_id, symbol, side, qty, price).await {
                warn!("    Failed to place order {}: {}", cl_ord_id, e);
            } else {
                info!("    ✅ Order {} placed successfully", cl_ord_id);
            }
            
            // Small delay between orders
            sleep(Duration::from_millis(500)).await;
        }
        
        info!("✅ Finished placing {} test orders", count);
        info!("   Trades should appear in order book if orders execute");
        
        Ok(())
    }

    /// Dump order book to file (creates snapshot, writes to file)

    pub async fn receive_message(&self) -> Result<FIXMessage> {
        // First, try to extract from existing buffer if it has enough data
        let buffer_len = {
            let msg_buffer = self.message_buffer.lock().await;
            msg_buffer.len()
        };

        if buffer_len >= 15 {
            // Buffer has potential message data, try to extract
            match self.extract_complete_message().await {
                Ok(msg) => return Ok(msg),
                Err(e) => {
                    // If buffer is incomplete, we need to read more data
                    let err_str = e.to_string();
                    if !err_str.contains("waiting for more data") && !err_str.contains("waiting for message start") {
                        // Real error, not just incomplete message
                        return Err(e);
                    }
                }
            }
        }

        // Read more data from network
        let mut stream_guard = self.stream.lock().await;
        let stream = stream_guard.as_mut().ok_or_else(|| anyhow!("Not connected"))?;

        let mut read_buffer = vec![0; 4096];
        let n = stream.read(&mut read_buffer).await?;

        if n == 0 {
            return Err(anyhow!("Connection closed by gateway"));
        }

        read_buffer.truncate(n);

        // Debug: log raw received data
        let hex_str = read_buffer.iter()
            .take(100)
            .map(|b| format!("{:02x}", b))
            .collect::<Vec<_>>()
            .join("");
        info!("DEBUG RX: Received {} bytes (hex first 100 chars): {}", n, &hex_str[..std::cmp::min(100, hex_str.len())]);

        // Add to message buffer
        let mut msg_buffer = self.message_buffer.lock().await;
        msg_buffer.extend_from_slice(&read_buffer);
        drop(msg_buffer);

        // Try to parse a complete message from the buffer
        self.extract_complete_message().await
    }

    /// Extract a complete FIX message from the buffer
    /// Handles fragmented messages by waiting for the full message to arrive
    async fn extract_complete_message(&self) -> Result<FIXMessage> {
        let mut msg_buffer = self.message_buffer.lock().await;

        // Need at least "8=FIX.4.4" and "9=XXX" to parse
        if msg_buffer.len() < 15 {
            drop(msg_buffer);
            return Err(anyhow!("Incomplete FIX message in buffer"));
        }

        // Find the body length field (tag 9)
        // Format: 8=FIX.4.4|9=XXX|...
        let buffer_str = String::from_utf8_lossy(&msg_buffer);

        // Buffer must start with "8=" for a valid FIX message
        if !buffer_str.starts_with("8=") {
            drop(msg_buffer);
            return Err(anyhow!("Incomplete FIX message - waiting for message start"));
        }

        // Look for "9=" to find body length
        if let Some(pos) = buffer_str.find("9=") {
            let after_tag9 = &buffer_str[pos + 2..];
            if let Some(end_pos) = after_tag9.find('\x01') {
                if let Ok(body_len) = after_tag9[..end_pos].parse::<usize>() {
                    // Find the start of the body (after tag 9 and its SOH)
                    if let Some(body_start_pos) = buffer_str[pos..].find("\x01") {
                        let body_actual_start = pos + body_start_pos + 1;

                        // Calculate where the checksum should be based on body length
                        // Message structure: 8=FIX.4.4\x01 9=<len>\x01 <body> 10=<checksum>\x01
                        // Expected position: body_actual_start + body_len + (at least "10=XXX\x01")
                        let checksum_search_start = body_actual_start + body_len;

                        // Now look for the LAST "10=" in the buffer (which should be the checksum)
                        // We search backward from the end to find the true checksum field
                        if let Some(last_10_pos) = buffer_str.rfind("10=") {
                            // Verify this is likely the checksum (comes after body length position)
                            if last_10_pos >= checksum_search_start {
                                // Find the SOH after the checksum value
                                if let Some(checksum_end) = buffer_str[last_10_pos..].find('\x01') {
                                    let message_end = last_10_pos + checksum_end + 1;

                                    // Verify this message makes sense (checksum comes after body start)
                                    // and the remaining data after checksum is minimal (shouldn't have another message yet)
                                    if message_end > body_actual_start && msg_buffer.len() >= message_end {
                                        // Extract complete message
                                        let complete_msg = msg_buffer.drain(..message_end).collect::<Vec<_>>();
                                        drop(msg_buffer);

                                        info!("✅ Extracted complete FIX message ({} bytes) from buffer", complete_msg.len());
                                        return FIXMessage::decode(&complete_msg);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        drop(msg_buffer);
        Err(anyhow!("Incomplete FIX message - waiting for more data"))
    }

    pub async fn handle_message(&self, msg: &FIXMessage) -> Result<()> {
        match msg.msg_type {
            'A' => info!("Logon response received"),
            '0' => log::debug!("Heartbeat received"),
            '8' => {
                // Execution Report - measure latency from order submission
                if let Some(cl_ord_id_str) = msg.get_field(tags::CL_ORD_ID) {
                    let execution_report_time = Instant::now();
                    let mut order_times = self.order_times.lock().await;
                    let cl_ord_id_owned = cl_ord_id_str.to_string();

                    let execution_report_us = if let Some(submission_time) = order_times.remove(&cl_ord_id_owned) {
                        let latency_us = execution_report_time.duration_since(submission_time).as_micros() as u64;
                        let latency_ms = latency_us as f64 / 1000.0;
                        info!("📊 Order-to-ACK Latency for {}: {} µs ({:.2} ms)",
                              cl_ord_id_owned, latency_us, latency_ms);
                        Some(latency_us)
                    } else {
                        None
                    };
                    drop(order_times);
                    
                    // Complete timing and write processed JSON
                    let mut timings = self.order_timings.lock().await;
                    let mut order_data_map = self.order_data.lock().await;
                    
                    if let (Some(mut timing), Some(order)) = (
                        timings.remove(&cl_ord_id_owned),
                        order_data_map.remove(&cl_ord_id_owned)
                    ) {
                        timing.execution_report_us = execution_report_us;
                        timing.execution_report_timestamp = Some(chrono::Utc::now().timestamp_millis());
                        
                        // Calculate total time (from file read to execution report)
                        if let Some(file_read_ts) = timing.file_read_timestamp {
                            if let Some(exec_ts) = timing.execution_report_timestamp {
                                timing.total_us = Some((exec_ts - file_read_ts) as u64 * 1000); // Convert ms to µs
                            }
                        }
                        
                        // Create processed JSON with execution report at the top
                        let processed_data = serde_json::json!({
                            "execution_report": {
                                "order_id": msg.get_field(tags::ORDER_ID),
                                "exec_id": msg.get_field(tags::EXEC_ID),
                                "cl_ord_id": msg.get_field(tags::CL_ORD_ID),
                                "orig_cl_ord_id": msg.get_field(41),
                                "symbol": msg.get_field(tags::SYMBOL),
                                "side": msg.get_field(tags::SIDE),
                                "order_qty": msg.get_field(tags::ORDER_QTY),
                                "price": msg.get_field(tags::PRICE),
                                "exec_type": msg.get_field(150), // ExecType
                                "ord_status": msg.get_field(tags::EXEC_STATUS), // OrdStatus (39)
                                "cum_qty": msg.get_field(14),
                                "leaves_qty": msg.get_field(tags::LEAVES_QTY),
                                "last_px": msg.get_field(tags::LAST_PX),
                                "last_qty": msg.get_field(tags::LAST_QTY),
                                "avg_px": msg.get_field(6),
                                "commission": msg.get_field(12),
                                "commission_currency": msg.get_field(2643),
                                "comm_type": msg.get_field(13),
                                "exec_inst": msg.get_field(18),
                                "exec_ref_id": msg.get_field(19),
                                "working_indicator": msg.get_field(636),
                                "transact_time": msg.get_field(tags::TRANSACT_TIME),
                                "order_create_time": msg.get_field(6616),
                                "last_liquidity_ind": msg.get_field(851),
                                "cash_margin": msg.get_field(544),
                                "trd_match_id": msg.get_field(880),
                                "trade_transact_time": msg.get_field(10060),
                                "match_count": msg.get_field(20101),
                                "match_index": msg.get_field(20102),
                                "rejection_reason_code": msg.get_field(103), // OrdRejReason
                                "rejection_reason_text": msg.get_field(tags::TEXT)
                            },
                            "order": {
                                "cl_ord_id": order.cl_ord_id,
                                "symbol": order.symbol,
                                "side": order.side,
                                "qty": order.qty,
                                "price": order.price
                            },
                            "timing": {
                                "file_read_us": timing.file_read_us,
                                "json_parse_us": timing.json_parse_us,
                                "channel_send_us": timing.channel_send_us,
                                "fix_encode_us": timing.fix_encode_us,
                                "fix_send_us": timing.fix_send_us,
                                "execution_report_us": timing.execution_report_us,
                                "total_us": timing.total_us,
                                "file_read_timestamp": timing.file_read_timestamp,
                                "execution_report_timestamp": timing.execution_report_timestamp
                            }
                        });
                        
                        // Write processed JSON file
                        let processed_file = format!("orders/processed/{}.json", cl_ord_id_owned);
                        if let Ok(json_str) = serde_json::to_string_pretty(&processed_data) {
                            if let Err(e) = tokio::fs::write(&processed_file, json_str).await {
                                warn!("Failed to write processed order file {}: {}", processed_file, e);
                            } else {
                                info!("💾 Processed order saved: {} (Total: {}µs)", processed_file, 
                                      timing.total_us.unwrap_or(0));
                            }
                        }
                    }
                    drop(timings);
                    drop(order_data_map);

                    // Update open orders store with execution report data
                    let open_order = OpenOrder {
                        cl_ord_id: msg.get_field(tags::CL_ORD_ID).unwrap_or_default().to_string(),
                        order_id: msg.get_field(tags::ORDER_ID).unwrap_or_default().to_string(),
                        exec_id: msg.get_field(tags::EXEC_ID).unwrap_or_default().to_string(),
                        symbol: msg.get_field(tags::SYMBOL).unwrap_or_default().to_string(),
                        side: msg.get_field(tags::SIDE).unwrap_or_default().to_string(),
                        order_qty: msg.get_field(tags::ORDER_QTY).unwrap_or_default().to_string(),
                        price: msg.get_field(tags::PRICE).unwrap_or_default().to_string(),
                        ord_status: msg.get_field(tags::EXEC_STATUS).unwrap_or_default().to_string(),
                        exec_type: msg.get_field(150).unwrap_or_default().to_string(), // ExecType
                        cum_qty: msg.get_field(14).unwrap_or_default().to_string(),
                        leaves_qty: msg.get_field(tags::LEAVES_QTY).unwrap_or_default().to_string(),
                        last_px: msg.get_field(tags::LAST_PX).unwrap_or_default().to_string(),
                        last_qty: msg.get_field(tags::LAST_QTY).unwrap_or_default().to_string(),
                        avg_px: msg.get_field(6).unwrap_or_default().to_string(),
                        transact_time: msg.get_field(tags::TRANSACT_TIME).unwrap_or_default().to_string(),
                        order_create_time: msg.get_field(6616).unwrap_or_default().to_string(),
                        rejection_reason: msg.get_field(tags::TEXT)
                            .or_else(|| msg.get_field(103)) // OrdRejReason
                            .unwrap_or_default()
                            .to_string(),
                    };
                    
                    // Update the store (lock-free, uses ArcSwap)
                    self.open_orders.update_order(cl_ord_id_owned.clone(), open_order);
                    
                    // If order is closed (rejected, cancelled, or fully filled), remove it
                    let leaves_qty: f64 = msg.get_field(tags::LEAVES_QTY)
                        .and_then(|s| s.parse().ok())
                        .unwrap_or(0.0);
                    let ord_status = msg.get_field(tags::EXEC_STATUS).unwrap_or_default();
                    let exec_type = msg.get_field(150).unwrap_or_default();
                    
                    if leaves_qty == 0.0 || ord_status == "8" || ord_status == "4" || ord_status == "2" || exec_type == "8" {
                        self.open_orders.remove_order(&cl_ord_id_owned);
                    }

                    // On every fill (partial or full), persist to kdb+ own_trades and update positions.
                    // ExecType "F" = partial fill, "2" = full fill, "1" = partial fill (some venues).
                    if exec_type == "F" || exec_type == "2" || exec_type == "1" {
                        let fill_symbol = msg.get_field(tags::SYMBOL).unwrap_or_default();
                        let fill_side   = msg.get_field(tags::SIDE).unwrap_or_default();
                        let last_px: f64 = msg.get_field(tags::LAST_PX)
                            .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        let last_qty: f64 = msg.get_field(tags::LAST_QTY)
                            .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        let avg_px: f64 = msg.get_field(6)
                            .and_then(|s| s.parse().ok()).unwrap_or(0.0);
                        let fill_order_id = msg.get_field(tags::ORDER_ID).unwrap_or_default();
                        let exec_type_char = exec_type.chars().next().unwrap_or('?');
                        let side_char = fill_side.chars().next().unwrap_or('?');

                        // Write fill to kdb+ own_trades table
                        let _ = self.kdb_writer.write_own_trade(
                            fill_symbol,
                            side_char,
                            last_px,
                            last_qty,
                            avg_px,
                            &cl_ord_id_owned,
                            fill_order_id,
                            exec_type_char,
                        ).await;

                        // Durably persist this fill to data/trades.csv immediately.
                        // This file survives KDB+ and server restarts.
                        append_trade_to_csv(
                            &cl_ord_id_owned,
                            fill_order_id,
                            fill_symbol,
                            side_char,
                            exec_type_char,
                            last_qty,
                            last_px,
                            avg_px,
                        );

                        // Update in-memory positions store
                        if last_qty > 0.0 && last_px > 0.0 {
                            self.positions.apply_fill(fill_symbol, fill_side, last_qty, last_px);
                            info!("📈 Position updated: symbol={} side={} qty={} px={}", fill_symbol, fill_side, last_qty, last_px);
                        }
                    }

                    // Signal CSV update (event-driven, immediate)
                    if let Some(tx) = self.order_update_tx.lock().await.as_ref() {
                        let _ = tx.send(());
                    }
                    
                    info!("");
                    info!("═══════════════════════════════════════════════════════════");
                    info!("✅ EXECUTION REPORT RECEIVED");
                    info!("═══════════════════════════════════════════════════════════");
                    info!("Client Order ID: {}", cl_ord_id_owned);

                    if let Some(order_id) = msg.get_field(tags::ORDER_ID) {
                        info!("Order ID: {}", order_id);
                    }
                    if let Some(symbol) = msg.get_field(tags::SYMBOL) {
                        info!("Symbol: {}", symbol);
                    }
                    if let Some(side) = msg.get_field(tags::SIDE) {
                        let side_name = match side {
                            "1" => "BUY",
                            "2" => "SELL",
                            _ => side,
                        };
                        info!("Side: {}", side_name);
                    }
                    if let Some(qty) = msg.get_field(tags::ORDER_QTY) {
                        info!("Quantity: {}", qty);
                    }
                    if let Some(price) = msg.get_field(tags::PRICE) {
                        info!("Price: {}", price);
                    }
                    if let Some(exec_status) = msg.get_field(tags::EXEC_STATUS) {
                        let status_name = match exec_status {
                            "0" => "New",
                            "1" => "Partially Filled",
                            "2" => "Filled",
                            "4" => "Canceled",
                            "8" => "Rejected",
                            _ => exec_status,
                        };
                        info!("Status: {}", status_name);
                        
                        // If rejected, show rejection reason
                        if exec_status == "8" {
                            if let Some(reject_text) = msg.get_field(tags::TEXT) {
                                warn!("❌ Rejection Reason (58/TEXT): {}", reject_text);
                            }
                            if let Some(reject_code) = msg.get_field(103) {
                                let code_name = match reject_code {
                                    "0" => "Broker/Exchange Option",
                                    "1" => "Unknown Symbol",
                                    "2" => "Exchange Closed",
                                    "3" => "Order Exceeds Limit",
                                    "4" => "Too Late To Enter",
                                    "5" => "Unknown Order",
                                    "6" => "Duplicate Order",
                                    "7" => "Duplicate Of A Verbally Communicated Order",
                                    "8" => "Stale Order",
                                    "9" => "Trade Along Required",
                                    "10" => "Invalid Investor ID",
                                    "11" => "Unsupported Order Characteristic",
                                    "12" => "Surveillance Option",
                                    "13" => "Incorrect Quantity",
                                    "14" => "Incorrect Allocated Quantity",
                                    "15" => "Unknown Account(s)",
                                    "99" => "Other",
                                    _ => "Unknown Code",
                                };
                                warn!("❌ Rejection Code (103/OrdRejReason): {} ({})", reject_code, code_name);
                            }
                        }
                    }
                    if let Some(last_qty) = msg.get_field(tags::LAST_QTY) {
                        info!("Executed Quantity: {}", last_qty);
                    }
                    if let Some(leaves_qty) = msg.get_field(tags::LEAVES_QTY) {
                        info!("Remaining Quantity: {}", leaves_qty);
                    }
                    info!("═══════════════════════════════════════════════════════════");
                    info!("");
                }
            }
            'j' => {
                // Business Message Reject (35=j)
                info!("");
                info!("═══════════════════════════════════════════════════════════");
                warn!("❌ BUSINESS MESSAGE REJECT (35=j)");
                info!("═══════════════════════════════════════════════════════════");

                // Log key rejection details for MarketDataRequest debugging
                if let Some(ref_msg_type) = msg.get_field(372) {
                    info!("Rejected Message Type (372): {} ({})", ref_msg_type, 
                          if ref_msg_type == "V" { "MarketDataRequest" } else { "Other" });
                }
                if let Some(ref_seq_num) = msg.get_field(45) {
                    info!("Rejected Message SeqNum (45): {}", ref_seq_num);
                }
                if let Some(business_ref_id) = msg.get_field(379) {
                    info!("BusinessRejectRefID (379): {} (may indicate problematic field/tag)", business_ref_id);
                }
                if let Some(reject_reason) = msg.get_field(380) {
                    let reason_text = match reject_reason {
                        "0" => "Other",
                        "1" => "Unknown ID",
                        "2" => "Unknown Security",
                        "3" => "Unsupported Message Type",
                        "4" => "Application not available",
                        "5" => "Conditionally Required Field Missing",
                        "6" => "Not authorized",
                        "7" => "DeliverTo firm not available",
                        _ => "Unknown",
                    };
                    info!("BusinessRejectReason (380): {} ({})", reject_reason, reason_text);
                }

                if let Some(cl_ord_id_str) = msg.get_field(tags::CL_ORD_ID) {
                    warn!("Client Order ID: {}", cl_ord_id_str);
                    let mut order_times = self.order_times.lock().await;
                    if let Some(submission_time) = order_times.remove(cl_ord_id_str) {
                        let ack_time = Instant::now();
                        let latency_us = ack_time.duration_since(submission_time).as_micros();
                        let latency_ms = latency_us as f64 / 1000.0;
                        info!("Time to Rejection: {} µs ({:.2} ms)", latency_us, latency_ms);
                    }
                }

                // Tag 58 is TEXT field for rejection reason
                if let Some(text) = msg.get_field(tags::TEXT) {
                    warn!("Rejection Reason (58): {}", text);
                }

                // Print all fields for debugging
                info!("");
                info!("Full Message Fields:");
                for (tag, value) in &msg.fields {
                    if value.len() > 100 {
                        info!("  Tag {}: {}...", tag, &value[..100]);
                    } else {
                        info!("  Tag {}: {}", tag, value);
                    }
                }

                info!("═══════════════════════════════════════════════════════════");
                info!("");
            }
            'D' => info!("Logout received"),
            '3' => {
                if let Some(text) = msg.get_field(tags::TEXT) {
                    warn!("Message rejected: {}", text);
                }
            }
            'H' => {
                info!("Order Status Request Response (35=H) received");
            }
            'y' => {
                // Security List Response (35=y)
                info!("");
                info!("═══════════════════════════════════════════════════════════");
                info!("📋 SECURITY LIST RESPONSE RECEIVED");
                info!("═══════════════════════════════════════════════════════════");

                let mut last_frag = false;
                let mut expected_count: Option<usize> = None;

                if let Some(req_id) = msg.get_field(320) {
                    info!("SecurityReqID (320): {}", req_id);
                }
                if let Some(resp_id) = msg.get_field(322) {
                    info!("SecurityResponseID (322): {}", resp_id);
                }
                if let Some(result) = msg.get_field(560) {
                    let result_text = match result {
                        "0" => "Valid",
                        "2" => "Not found",
                        "3" => "Not authorised",
                        "4" => "Unavailable",
                        _ => result,
                    };
                    info!("SecurityRequestResult (560): {} ({})", result, result_text);
                }
                if let Some(frag) = msg.get_field(893) {
                    last_frag = frag == "Y";
                    info!("LastFragment (893): {} ({})", frag, if last_frag { "Last message" } else { "More messages coming" });
                }
                if let Some(num_symbols) = msg.get_field(146) {
                    if let Ok(count) = num_symbols.parse::<usize>() {
                        expected_count = Some(count);
                        info!("NoRelatedSym (146): {} securities total in list", count);
                        *self.expected_security_count.lock().await = Some(count);
                    }
                }

                // Extract repeating group instances (tag 146 = NoRelatedSym, tag 55 = Symbol)
                let repeating_groups = msg.get_repeating_group(146, 55);
                let has_repeating_groups = !repeating_groups.is_empty();
                let repeating_groups_count = repeating_groups.len();

                if has_repeating_groups {
                    info!("📊 Extracting {} securities from repeating groups", repeating_groups_count);

                    let mut securities = self.securities.lock().await;
                    for group in repeating_groups {
                        if let Some(symbol_val) = group.get(&55).map(|s| s.as_str()) {
                            let mut security = SecurityInfo {
                                symbol: symbol_val.to_string(),
                                security_id: group.get(&48).map(|s| s.as_str()).map(|s| s.to_string()),
                                exchange: group.get(&207).map(|s| s.as_str()).map(|s| s.to_string()),
                                attributes: HashMap::new(),
                            };

                            // Collect instrument attributes
                            for (tag, value) in &group {
                                if *tag >= 871 && *tag <= 872 {
                                    security.attributes.insert(tag.to_string(), value.clone());
                                }
                            }

                            securities.push(security.clone());
                            info!("  ✅ {}", symbol_val);
                        }
                    }
                    info!("📊 Total securities loaded: {}", securities.len());
                } else {
                    // Fallback to old logic for non-repeating group format
                    if let Some(symbol) = msg.get_field(55) {
                        let mut security = SecurityInfo {
                            symbol: symbol.to_string(),
                            security_id: msg.get_field(48).map(|s| s.to_string()),
                            exchange: msg.get_field(207).map(|s| s.to_string()),
                            attributes: HashMap::new(),
                        };

                        // Collect instrument attributes
                        for (tag, value) in &msg.fields {
                            if *tag >= 871 && *tag <= 872 {
                                security.attributes.insert(tag.to_string(), value.clone());
                            }
                        }

                        let mut securities = self.securities.lock().await;
                        securities.push(security.clone());
                        info!("✅ Saved security: {} (Total: {})", symbol, securities.len());
                    } else {
                        info!("ℹ️ No securities found in SecurityListResponse");
                    }
                }

                // Always save on each SecurityListResponse with repeating groups (to accumulate all securities)
                if has_repeating_groups {
                    let securities = self.securities.lock().await;
                    let current_count = securities.len();
                    drop(securities);

                    info!("💾 Saving {} securities to database (background thread)...", current_count);
                    if let Err(e) = self.save_securities_to_db().await {
                        error!("Failed to save securities to database: {}", e);
                    }

                    // Also save file on first response only
                    let should_save_file = {
                        let saved = self.securities_file_saved.lock().await;
                        !*saved
                    };

                    if should_save_file {
                        *self.securities_file_saved.lock().await = true;
                        if let Err(e) = self.save_securities_to_file().await {
                            error!("Failed to save securities to file: {}", e);
                        }
                    }
                }

                info!("");
                info!("Sample fields from response:");
                for (tag, value) in &msg.fields {
                    if *tag >= 55 && *tag <= 900 && value.len() <= 50 {
                        info!("  Tag {}: {}", tag, value);
                    }
                }
                info!("═══════════════════════════════════════════════════════════");
                info!("");
            }
            'd' => {
                // Security Definition (35=d)
                info!("");
                info!("📄 SECURITY DEFINITION RECEIVED");

                if let Some(symbol) = msg.get_field(55) {
                    info!("Symbol (55): {}", symbol);
                }
                if let Some(security_id) = msg.get_field(48) {
                    info!("SecurityID (48): {}", security_id);
                }
                if let Some(exchange) = msg.get_field(207) {
                    info!("SecurityExchange (207): {}", exchange);
                }

                // Log key attribute tags if present
                if let Some(num_attribs) = msg.get_field(870) {
                    info!("NoInstrAttrib (870): {} attributes", num_attribs);
                }

                info!("");
            }
            'W' => {
                // Market Data Snapshot Full Refresh (35=W)
                info!("");
                info!("📈 MARKET DATA SNAPSHOT RECEIVED (35=W)");

                if let Some(md_req_id) = msg.get_field(tags::MARKET_DATA_REQ_ID) {
                    info!("MarketDataReqID (262): {}", md_req_id);
                }
                if let Some(symbol) = msg.get_field(tags::SYMBOL) {
                    info!("Symbol (55): {}", symbol);
                }

                // Extract repeating group for market data entries
                // For Market Data messages, the group can start with either 279 (MDUpdateAction) or 269 (MDEntryType)
                // Try 279 first (for incremental refresh), then fall back to 269 (for snapshot)
                let expected_count = msg.get_field(268).and_then(|s| s.parse::<usize>().ok()).unwrap_or(0);
                let repeating_groups = if msg.fields_ordered.iter().any(|(tag, _)| *tag == 279 && *tag > 268) {
                    // Use 279 as group start tag (MDUpdateAction comes first in incremental refresh)
                    msg.get_repeating_group(268, 279)
                } else {
                    // Use 269 as group start tag (standard for snapshot)
                    msg.get_repeating_group(268, 269)
                };
                info!("NoMDEntries (268): {} entries (extracted: {})", expected_count, repeating_groups.len());
                
                // Debug: Show all fields after 268 to understand structure
                info!("Debug: Fields after 268 in message (first 20):");
                let mut found_268 = false;
                let mut count_after_268 = 0;
                for (tag, value) in &msg.fields_ordered {
                    if *tag == 268 {
                        found_268 = true;
                        continue;
                    }
                    if found_268 {
                        count_after_268 += 1;
                        if count_after_268 <= 20 { // Show first 20 fields
                            info!("  Tag {}: {}", tag, value);
                        } else {
                            break;
                        }
                    }
                }

                if !repeating_groups.is_empty() {
                    // Get symbol for consistency
                    let symbol = msg.get_field(tags::SYMBOL).map(|s| s.to_string()).unwrap_or_else(|| "UNKNOWN".to_string());
                    let timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

                    // Get or create double-buffered order book for this symbol
                    let mut books = self.order_books.lock().await;
                    let book = books.entry(symbol.clone())
                        .or_insert_with(|| DoubleBufferedOrderBook::new(symbol.clone(), 100_000)); // $1k dense range (reduced for memory)
                    
                    // For snapshot, clear the active book first (full refresh)
                    book.get_active_mut().clear();

                    // Combine Bid and Ask from same snapshot into single tick
                    let mut combined_tick = TickData {
                        timestamp,
                        symbol: symbol.clone(),
                        bid: 0.0,
                        ask: 0.0,
                        bid_size: 0.0,
                        ask_size: 0.0,
                        last_price: 0.0,
                        last_size: 0.0,
                    };

                    for (idx, entry) in repeating_groups.iter().enumerate() {
                        if let Some(entry_type) = entry.get(&269) {
                            // 0 = Bid, 1 = Ask, 2 = Trade, 4 = Opening Price, etc.
                            let entry_type_name = match entry_type.as_str() {
                                "0" => "Bid",
                                "1" => "Ask",
                                "2" => "Trade",
                                "4" => "Opening",
                                _ => "Other",
                            };
                            info!("  [{}] {} (type: {})", idx, entry_type_name, entry_type);

                            // Extract price (tag 270) and size (tag 271)
                            if let Some(price_str) = entry.get(&270) {
                                if let Ok(price) = price_str.parse::<f64>() {
                                    let size = entry.get(&271).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                                    info!("      Price: {}, Size: {}", price, size);

                                    // Update active order book (snapshot = New action)
                                    let active_book = book.get_active_mut();
                                    match entry_type.as_str() {
                                        "0" => {
                                            // Bid update
                                            active_book.update_level(Side::Bid, price, size, UpdateAction::New);
                                            combined_tick.bid = price;
                                            combined_tick.bid_size = size;
                                            // Write to kdb+
                                            let _ = self.kdb_writer.write_order_book_level(&symbol, 'B', price, size, 'N').await;
                                        }
                                        "1" => {
                                            // Ask update
                                            active_book.update_level(Side::Ask, price, size, UpdateAction::New);
                                            combined_tick.ask = price;
                                            combined_tick.ask_size = size;
                                            // Write to kdb+
                                            let _ = self.kdb_writer.write_order_book_level(&symbol, 'A', price, size, 'N').await;
                                        }
                                        "2" => {
                                            // Trade/Last price - add to order book trade history
                                            active_book.add_trade(price, size, timestamp);
                                            combined_tick.last_price = price;
                                            combined_tick.last_size = size;
                                            // Write to kdb+
                                            let _ = self.kdb_writer.write_trade(&symbol, price, size, timestamp).await;
                                        }
                                        _ => {
                                            // Unknown entry type - skip
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Log best bid/ask from active order book
                    let active_book = book.get_active();
                    let best_bid = active_book.best_bid();
                    let best_ask = active_book.best_ask();
                    let spread = active_book.spread();
                    info!("  📊 Order Book: Bid={:.2}, Ask={:.2}, Spread={:.2}", best_bid, best_ask, spread);

                    // Get snapshot values before swapping
                    let best_bid_size = active_book.best_bid_size();
                    let best_ask_size = active_book.best_ask_size();
                    
                    // CRITICAL: Swap snapshot after snapshot processing to make updates visible
                    book.swap_snapshot();

                    // Write snapshot to kdb+ and persist to CSV for backtest fallback
                    let _ = self.kdb_writer.write_snapshot(&symbol, best_bid, best_ask, 
                        best_bid_size, best_ask_size, spread).await;
                    append_snapshot_to_csv(&symbol, best_bid, best_ask);

                    // Only add tick if we have at least bid or ask data
                    if combined_tick.bid > 0.0 || combined_tick.ask > 0.0 || combined_tick.last_price > 0.0 {
                        info!("  💾 Saving tick: Bid={}, Ask={}, BidSize={}, AskSize={}", 
                              combined_tick.bid, combined_tick.ask, combined_tick.bid_size, combined_tick.ask_size);
                        self.add_tick(combined_tick).await;
                    }
                }

                info!("═══════════════════════════════════════════════════════════");
                info!("");
            }
            'X' => {
                // Market Data Incremental Refresh (35=X) - Real-time updates
                info!("");
                info!("📈 MARKET DATA INCREMENTAL REFRESH RECEIVED (35=X)");

                let md_req_id = msg.get_field(tags::MARKET_DATA_REQ_ID);
                if let Some(ref md_req_id) = md_req_id {
                    info!("MarketDataReqID (262): {}", md_req_id);
                    if md_req_id.starts_with("trade.") {
                        info!("💹 TRADE DATA MESSAGE RECEIVED for {}", md_req_id);
                    }
                }
                if let Some(symbol) = msg.get_field(tags::SYMBOL) {
                    info!("Symbol (55): {}", symbol);
                }

                // Extract repeating group for market data entries
                // For incremental refresh, group starts with 279 (MDUpdateAction)
                let expected_count = msg.get_field(268).and_then(|s| s.parse::<usize>().ok()).unwrap_or(0);
                let repeating_groups = msg.get_repeating_group(268, 279);
                info!("NoMDEntries (268): {} entries (extracted: {})", expected_count, repeating_groups.len());
                if !repeating_groups.is_empty() {
                    // Get symbol - try from message, or extract from MDReqID if it's trade.BTC_USDT format
                    let symbol = if let Some(s) = msg.get_field(tags::SYMBOL) {
                        s.to_string()
                    } else if let Some(ref req_id) = md_req_id {
                        // Extract symbol from trade.BTC_USDT or book.BTC_USDT.50 format
                        if req_id.starts_with("trade.") {
                            req_id.strip_prefix("trade.").unwrap_or("UNKNOWN").to_string()
                        } else if req_id.starts_with("book.") {
                            req_id.strip_prefix("book.")
                                .and_then(|s| s.split('.').next())
                                .unwrap_or("UNKNOWN")
                                .to_string()
                        } else {
                            "UNKNOWN".to_string()
                        }
                    } else {
                        "UNKNOWN".to_string()
                    };
                    let timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

                    // Get or create double-buffered order book for this symbol
                    let mut books = self.order_books.lock().await;
                    let book = books.entry(symbol.clone())
                        .or_insert_with(|| DoubleBufferedOrderBook::new(symbol.clone(), 100_000)); // $1k dense range (reduced for memory)

                    for (idx, entry) in repeating_groups.iter().enumerate() {
                        if let Some(entry_type) = entry.get(&269) {
                            // Check for special case: 'J' = Empty Book (clear entire order book)
                            if entry_type == "J" {
                                warn!("⚠️  EMPTY BOOK (269=J) - Clearing entire order book for {}", symbol);
                                book.get_active_mut().clear();
                                continue;
                            }
                            
                            // 0 = Bid, 1 = Ask, 2 = Trade, 4 = Opening Price, etc.
                            let entry_type_name = match entry_type.as_str() {
                                "0" => "Bid",
                                "1" => "Ask",
                                "2" => "Trade",
                                "4" => "Opening",
                                _ => "Other",
                            };
                            
                            // Check MDUpdateAction (tag 279) - 0=New, 1=Change, 2=Delete
                            let update_action_str = entry.get(&279).map(|s| s.as_str()).unwrap_or("0");
                            let update_action = UpdateAction::from(update_action_str);
                            let action_name = match update_action {
                                UpdateAction::New => "New",
                                UpdateAction::Change => "Change",
                                UpdateAction::Delete => "Delete",
                            };
                            
                            info!("  [{}] {} {} (type: {})", idx, action_name, entry_type_name, entry_type);

                            // Extract price (tag 270) and size (tag 271)
                            if let Some(price_str) = entry.get(&270) {
                                if let Ok(price) = price_str.parse::<f64>() {
                                    let size = entry.get(&271).and_then(|s| s.parse::<f64>().ok()).unwrap_or(0.0);
                                    info!("      Price: {}, Size: {}", price, size);

                                    // Update active order book based on entry type and action
                                    let active_book = book.get_active_mut();
                                    let action_char = match update_action {
                                        UpdateAction::New => 'N',
                                        UpdateAction::Change => 'C',
                                        UpdateAction::Delete => 'D',
                                    };
                                    match entry_type.as_str() {
                                        "0" => {
                                            // Bid update
                                            info!("📝 Processing Bid update: {} {} {} {}", symbol, price, size, action_char);
                                            active_book.update_level(Side::Bid, price, size, update_action);
                                            // Write to kdb+
                                            info!("📝 Calling write_order_book_level for Bid");
                                            let _ = self.kdb_writer.write_order_book_level(&symbol, 'B', price, size, action_char).await;
                                        }
                                        "1" => {
                                            // Ask update
                                            info!("📝 Processing Ask update: {} {} {} {}", symbol, price, size, action_char);
                                            active_book.update_level(Side::Ask, price, size, update_action);
                                            // Write to kdb+
                                            info!("📝 Calling write_order_book_level for Ask");
                                            let _ = self.kdb_writer.write_order_book_level(&symbol, 'A', price, size, action_char).await;
                                        }
                                        "2" => {
                                            // Trade/Last price - add to order book trade history
                                            info!("💹 Adding trade to order book: Price={}, Size={}, Symbol={}", price, size, symbol);
                                            active_book.add_trade(price, size, timestamp);
                                            info!("💹 Trade added! Total trades in book: {}", active_book.trade_count());
                                            // Write to kdb+
                                            info!("📝 Calling write_trade");
                                            let _ = self.kdb_writer.write_trade(&symbol, price, size, timestamp).await;
                                        }
                                        _ => {
                                            // Unknown entry type - skip
                                            continue;
                                        }
                                    }

                                    // Create tick data for CSV export
                                    let tick = match entry_type.as_str() {
                                        "0" => {
                                            // Bid update
                                            TickData {
                                                timestamp,
                                                symbol: symbol.clone(),
                                                bid: price,
                                                ask: 0.0,
                                                bid_size: size,
                                                ask_size: 0.0,
                                                last_price: 0.0,
                                                last_size: 0.0,
                                            }
                                        }
                                        "1" => {
                                            // Ask update
                                            TickData {
                                                timestamp,
                                                symbol: symbol.clone(),
                                                bid: 0.0,
                                                ask: price,
                                                bid_size: 0.0,
                                                ask_size: size,
                                                last_price: 0.0,
                                                last_size: 0.0,
                                            }
                                        }
                                        "2" => {
                                            // Trade/Last price
                                            TickData {
                                                timestamp,
                                                symbol: symbol.clone(),
                                                bid: 0.0,
                                                ask: 0.0,
                                                bid_size: 0.0,
                                                ask_size: 0.0,
                                                last_price: price,
                                                last_size: size,
                                            }
                                        }
                                        _ => {
                                            // Unknown entry type - skip
                                            continue;
                                        }
                                    };

                                    // Add tick to buffer
                                    self.add_tick(tick).await;
                                }
                            }
                        }
                    }

                    // Get snapshot values before swapping
                    let active_book_before_swap = book.get_active();
                    let best_bid = active_book_before_swap.best_bid();
                    let best_ask = active_book_before_swap.best_ask();
                    let spread = active_book_before_swap.spread();
                    let best_bid_size = active_book_before_swap.best_bid_size();
                    let best_ask_size = active_book_before_swap.best_ask_size();
                    
                    // CRITICAL: Swap snapshot after incremental update to make updates visible
                    book.swap_snapshot();

                    info!("  📊 Order Book: Bid={:.2}, Ask={:.2}, Spread={:.2}", best_bid, best_ask, spread);
                    
                    // Write snapshot to kdb+ and persist to CSV for backtest fallback
                    let _ = self.kdb_writer.write_snapshot(&symbol, best_bid, best_ask, 
                        best_bid_size, best_ask_size, spread).await;
                    append_snapshot_to_csv(&symbol, best_bid, best_ask);
                    
                    // Signal ZeroMQ update ONCE per incremental refresh (after all entries processed)
                    if let Some(tx) = self.order_book_update_tx.lock().await.as_ref() {
                        let _ = tx.send(symbol.clone());
                    }
                } else {
                    info!("⚠️  No market data entries found in message");
                }

                info!("═══════════════════════════════════════════════════════════");
                info!("");
            }
            'j' | 'J' => {
                // Business message or other response
                info!("Message type {} received - likely order status response", msg.msg_type);
                // Print all fields for debugging
                info!("Message Fields:");
                for (tag, value) in &msg.fields {
                    if value.len() > 100 {
                        info!("  Tag {}: {}...", tag, &value[..100]);
                    } else {
                        info!("  Tag {}: {}", tag, value);
                    }
                }
            }
            _ => log::debug!("Unknown message type: {} - fields: {:?}", msg.msg_type, msg.fields),
        }

        Ok(())
    }

    // Save securities to file in a background thread (non-blocking)
    async fn save_securities_to_file(&self) -> Result<()> {
        let securities = self.securities.lock().await;

        // Clone the securities data to move into the background thread
        let securities_data = securities.clone();
        let security_count = securities_data.len();

        // Spawn a standard thread to write to file without blocking the async runtime
        std::thread::spawn(move || {
            match (|| -> Result<()> {
                let filename = "securities.txt";
                let mut file = File::create(filename)?;

                writeln!(file, "╔════════════════════════════════════════════════════════════════════════════════╗")?;
                writeln!(file, "║                    CRYPTO.COM MARKET DATA SECURITIES LIST                     ║")?;
                writeln!(file, "║                           Generated by FIX Server                             ║")?;
                writeln!(file, "╚════════════════════════════════════════════════════════════════════════════════╝")?;
                writeln!(file)?;
                writeln!(file, "Total Securities: {}\n", security_count)?;
                writeln!(file, "─────────────────────────────────────────────────────────────────────────────────\n")?;

                for (idx, security) in securities_data.iter().enumerate() {
                    writeln!(file, "{}. Symbol: {}", idx + 1, security.symbol)?;

                    if let Some(id) = &security.security_id {
                        writeln!(file, "   SecurityID: {}", id)?;
                    }

                    if let Some(exch) = &security.exchange {
                        writeln!(file, "   Exchange: {}", exch)?;
                    }

                    if !security.attributes.is_empty() {
                        writeln!(file, "   Attributes:")?;
                        for (tag, value) in &security.attributes {
                            writeln!(file, "     Tag {}: {}", tag, value)?;
                        }
                    }

                    writeln!(file)?;
                }

                writeln!(file, "─────────────────────────────────────────────────────────────────────────────────")?;
                writeln!(file, "End of Security List")?;

                Ok(())
            })() {
                Ok(_) => {
                    info!("✅ Saved {} securities to securities.txt", security_count);
                }
                Err(e) => {
                    error!("❌ Failed to write securities file: {}", e);
                }
            }
        });

        Ok(())
    }

    // Save securities to kdb+ database (replaced SQLite)
    async fn save_securities_to_db(&self) -> Result<()> {
        let securities = self.securities.lock().await;
        let securities_data = securities.clone();
        let security_count = securities_data.len();

        // Prepare data for kdb+ insert
        let securities_for_kdb: Vec<(String, Option<String>, Option<String>, String)> = securities_data
            .iter()
            .map(|s| {
                let attributes_json = serde_json::to_string(&s.attributes)
                    .unwrap_or_else(|_| "{}".to_string());
                (s.symbol.clone(), s.security_id.clone(), s.exchange.clone(), attributes_json)
            })
            .collect();

        // Write to kdb+
        if let Err(e) = self.kdb_writer.write_securities(&securities_for_kdb).await {
            error!("❌ Failed to write securities to kdb+: {}", e);
            return Err(e);
        }

        info!("✅ Saved {} securities to kdb+", security_count);
        Ok(())
    }

    /// Save tick data to CSV file (compatible with Parquet tools).
    ///
    /// Uses append mode so multiple calls accumulate data across sessions.
    /// The CSV header is only written when the file is first created.
    pub async fn save_ticks_to_parquet(&self, filename: &str) -> Result<()> {
        let mut ticks = self.tick_data.lock().await;
        let tick_count = ticks.len();

        if tick_count == 0 {
            return Ok(());
        }

        // Drain the buffer so next call only writes new ticks.
        let ticks_clone: Vec<TickData> = ticks.drain(..).collect();
        drop(ticks);

        let filename_owned = filename.to_string();

        std::thread::spawn(move || {
            match (|| -> Result<()> {
                // Ensure parent directory exists.
                if let Some(parent) = Path::new(&filename_owned).parent() {
                    std::fs::create_dir_all(parent)?;
                }

                let file_is_new = !Path::new(&filename_owned).exists();
                let mut file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&filename_owned)?;

                if file_is_new {
                    writeln!(file, "timestamp,symbol,bid,ask,bid_size,ask_size,last_price,last_size")?;
                }

                for tick in &ticks_clone {
                    writeln!(file, "{},{},{},{},{},{},{},{}",
                        tick.timestamp,
                        tick.symbol,
                        tick.bid,
                        tick.ask,
                        tick.bid_size,
                        tick.ask_size,
                        tick.last_price,
                        tick.last_size
                    )?;
                }

                Ok(())
            })() {
                Ok(_) => {
                    info!("✅ Appended {} ticks to {}", tick_count, filename_owned);
                }
                Err(e) => {
                    error!("❌ Failed to write tick data file: {}", e);
                }
            }
        });

        Ok(())
    }

    /// Add a tick to the tick buffer
    pub async fn add_tick(&self, tick: TickData) {
        let mut ticks = self.tick_data.lock().await;
        ticks.push(tick);
        let len = ticks.len();
        if len > MAX_TICK_BUFFER {
            ticks.drain(..len - MAX_TICK_BUFFER);
        }
    }

    // Private helper methods
    async fn send_message(&self, msg: &FIXMessage) -> Result<()> {
        // CRITICAL: Only allow logon (A) and logout (5) messages before session is ready
        // All other messages (including heartbeat, SecurityListRequest, MarketDataRequest, orders, etc.) 
        // must wait until logon ACK is received
        if msg.msg_type != 'A' && msg.msg_type != '5' {
            if !self.is_ready().await {
                return Err(anyhow!("Cannot send message type {} - session not ready (logon ACK not received)", msg.msg_type));
            }
        }
        
        let encoded = msg.encode()?;
        info!("Sending FIX message (type: {}, size: {} bytes)", msg.msg_type, encoded.len());
        
        // Log FIX command to separate file
        Self::log_fix_command(msg, &encoded);

        // Debug: Log the message content in readable format
        if msg.msg_type == 'A' {
            // Logon message - log all fields for debugging
            info!("DEBUG: Logon message fields:");
            for (&tag, value) in &msg.fields {
                if tag == 96 {
                    info!("  Tag {}: {} (RawData - length: {})", tag, value, value.len());
                } else if tag == 554 {
                    info!("  Tag {}: {} (Password/Signature - first 16 chars: {})", tag, value, &value[..std::cmp::min(16, value.len())]);
                } else if tag != 8 && tag != 9 && tag != 93 { // Skip checksum fields for brevity
                    info!("  Tag {}: {}", tag, value);
                }
            }

            // Log raw message hex
            let hex_str = encoded.iter()
                .map(|b| format!("{:02x}", b))
                .collect::<Vec<_>>()
                .join("");
            info!("DEBUG: Raw message (hex, first 200 chars): {}", &hex_str[..std::cmp::min(200, hex_str.len())]);

            // Log readable format with SOH markers
            let readable = String::from_utf8_lossy(&encoded)
                .replace('\x01', "|");
            info!("DEBUG: Readable format (first 300 chars): {}", &readable[..std::cmp::min(300, readable.len())]);
        } else if msg.msg_type == 'D' {
            // NewOrderSingle message - log all fields for debugging
            info!("DEBUG: NewOrderSingle message fields:");
            for (&tag, value) in &msg.fields {
                if tag != 8 && tag != 9 && tag != 10 { // Skip header and checksum
                    info!("  Tag {}: {}", tag, value);
                }
            }

            // Log readable format with SOH markers
            let readable = String::from_utf8_lossy(&encoded)
                .replace('\x01', "|");
            info!("DEBUG: Message readable format: {}", readable);
        }

        let mut stream_guard = self.stream.lock().await;
        let stream = stream_guard.as_mut().ok_or_else(|| anyhow!("Not connected"))?;
        stream.write_all(&encoded).await?;
        stream.flush().await?;
        info!("Message sent successfully");
        Ok(())
    }
    
    /// Log FIX command to separate log file
    /// Routes Order Management messages to OM_fix_command.log
    /// Routes Market Data messages to fix_commands.log
    fn log_fix_command(msg: &FIXMessage, encoded: &[u8]) {
        // Convert FIX message to readable format (replace SOH with |)
        let readable = String::from_utf8_lossy(encoded)
            .replace('\x01', "|");
        
        // Determine if this is Order Management or Market Data based on sender/target comp IDs
        // Order Management: sender contains "WOZINGA.UD" or target is "UAT1.CDC.UD"
        // Market Data: sender contains "WOZINGA_BTC.MD" or target is "UAT1.CDC.MD"
        let sender_id = msg.get_field(tags::SENDER_COMP_ID).unwrap_or("");
        let target_id = msg.get_field(tags::TARGET_COMP_ID).unwrap_or("");
        let is_om = sender_id.contains("WOZINGA.UD") 
            || target_id == "UAT1.CDC.UD";
        
        // Debug output
        eprintln!("DEBUG: log_fix_command - sender_id={}, target_id={}, is_om={}", sender_id, target_id, is_om);
        
        // Get message type name
        let msg_type_name = match msg.msg_type {
            'A' => "Logon",
            '5' => "Logout",
            '0' => "Heartbeat",
            '1' => "TestRequest",
            'D' => "NewOrderSingle",
            'F' => "OrderCancelRequest",
            'H' => "OrderStatusRequest",
            'V' => "MarketDataRequest",
            _ => "Unknown",
        };
        
        // Format timestamp
        let timestamp = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S%.3f UTC");
        
        // Format log entry
        let log_entry = format!(
            "[{}] FIX {} (35={}) - {} bytes\n{}\n\n",
            timestamp,
            msg_type_name,
            msg.msg_type,
            encoded.len(),
            readable
        );
        
        // Determine log file based on gateway type
        let log_file = if is_om {
            "OM_fix_command.log"
        } else {
            "fix_commands.log"
        };
        
        // Write to file synchronously (simple approach)
        // Use blocking thread to avoid blocking async runtime
        let log_entry_clone = log_entry;
        let log_file_clone = log_file.to_string();
        std::thread::spawn(move || {
            if let Ok(mut file) = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log_file_clone)
            {
                let _ = writeln!(file, "{}", log_entry_clone);
                let _ = file.flush();
            }
        });
    }

    async fn next_seq_num(&self) -> u32 {
        let mut seq = self.seq_num.lock().await;
        *seq += 1;
        *seq
    }

    async fn is_ready(&self) -> bool {
        *self.is_connected.lock().await
            && matches!(
                *self.connection_state.lock().await,
                ConnectionState::Ready
            )
    }
}

/// Message processing loop with persistent connection support
async fn message_loop(
    session: Arc<FIXSession>,
    gateway_url: String,
    mut order_rx: mpsc::Receiver<OrderRequest>,
    heartbeat_interval: u64,
    max_reconnect_attempts: u32,
    reconnect_backoff_ms: u64,
) -> Result<()> {
    loop {
        // Ensure connection
        if let Err(e) = session.connect_with_retry(&gateway_url, max_reconnect_attempts, reconnect_backoff_ms).await {
            error!("Failed to establish connection: {}", e);
            sleep(Duration::from_secs(10)).await;
            continue;
        }

        // Logon
        if let Err(e) = session.logon(heartbeat_interval).await {
            error!("Logon failed: {}", e);
            let _ = session.logout().await;
            sleep(Duration::from_secs(5)).await;
            continue;
        }

        // CRITICAL: Wait for Logon ACK (35=A) from gateway
        // Per documentation: "We recommend adding 50 ms wait interval after receiving 35=A on both side"
        info!("Logon sent. Waiting for Logon ACK (35=A) from gateway...");

        let mut logon_acked = false;
        let logon_timeout = Instant::now();

        while logon_timeout.elapsed() < Duration::from_secs(5) {
            match tokio::time::timeout(Duration::from_millis(500), session.receive_message()).await {
                Ok(Ok(msg)) => {
                    info!("📨 Received message type: {}", msg.msg_type);

                    if msg.msg_type == 'A' {
                        // This is the Logon ACK response
                        logon_acked = true;
                        info!("✅ Logon ACK (35=A) received from gateway!");
                        
                        // Set connection state to Ready only after receiving ACK
                        *session.connection_state.lock().await = ConnectionState::Ready;
                        session.handle_message(&msg).await?;

                        // Per documentation: wait 50ms after receiving Logon ACK before sending first request
                        info!("Waiting 50ms as per documentation before proceeding...");
                        sleep(Duration::from_millis(50)).await;
                        break;
                    } else {
                        // Other message type - process and keep waiting for logon ACK
                        session.handle_message(&msg).await?;
                    }
                }
                Ok(Err(e)) => {
                    warn!("⚠️  Connection error while waiting for Logon ACK: {}", e);
                    break;
                }
                Err(_) => {
                    // Timeout, continue waiting
                    continue;
                }
            }
        }

        if !logon_acked {
            warn!("❌ Logon ACK not received from gateway - connection rejected");
            sleep(Duration::from_secs(5)).await;
            continue;
        }

        info!("🔗 Connection established - waiting for messages");
        info!("Waiting for gateway responses or connection close...");

        // Message processing - keep connection alive and monitor for messages
        // Run continuously - no timeout limit
        let mut connection_active = true;
        while connection_active {
            tokio::select! {
                // Handle incoming orders from the request channel (unused in current test)
                Some(order) = order_rx.recv() => {
                    if let Err(e) = session.new_order_single_from_request(&order).await {
                        warn!("Failed to send order {}: {}", order.cl_ord_id, e);
                    }
                }
                // Handle incoming messages from gateway
                result = session.receive_message() => {
                    match result {
                        Ok(msg) => {
                            info!("📨 Received message type: {} - processing", msg.msg_type);
                            if let Err(e) = session.handle_message(&msg).await {
                                error!("Message handling error: {}", e);
                            }
                        }
                        Err(e) => {
                            warn!("⚠️  Connection closed: {}", e);
                            connection_active = false;
                        }
                    }
                }
            }
        }

        // Wait before reconnecting
        info!("Reconnecting in 5 seconds...");
        sleep(Duration::from_secs(5)).await;
    }
}

/// Determine which market data port to use based on symbol's first character
fn get_market_data_port_for_symbol(symbol: &str, config: &MarketDataOthersConfig) -> u16 {
    if symbol.is_empty() {
        return config.port_34405; // Default
    }
    
    let first_char = symbol.chars().next().unwrap().to_ascii_uppercase();
    
    match first_char {
        '0'..='9' | 'A'..='D' => config.port_34405,
        'E'..='M' => config.port_34406,
        'N'..='Z' => config.port_34407,
        _ => config.port_34405, // Default fallback
    }
}

/// Get the full URL for a symbol's market data connection
fn get_market_data_url_for_symbol(symbol: &str, config: &MarketDataOthersConfig) -> String {
    let port = get_market_data_port_for_symbol(symbol, config);
    format!("{}:{}", config.base_url, port)
}

/// Market Data connection loop for securities requests
/// symbol_filter: Optional function to filter which symbols this server should subscribe to
async fn market_data_loop(
    session: Arc<FIXSession>,
    gateway_url: String,
    heartbeat_interval: u64,
    max_reconnect_attempts: u32,
    reconnect_backoff_ms: u64,
    symbol_filter: Option<Box<dyn Fn(&str) -> bool + Send + Sync>>,
) -> Result<()> {
    loop {
        // Ensure connection to market data server
        if let Err(e) = session.connect_with_retry(&gateway_url, max_reconnect_attempts, reconnect_backoff_ms).await {
            error!("Failed to establish market data connection to {}: {}", gateway_url, e);
            sleep(Duration::from_secs(10)).await;
            continue;
        }

        info!("📊 Connected to Market Data Server: {}", gateway_url);
        info!("  Using Sender CompID: {} -> Target CompID: {}", session.sender_comp_id, session.target_comp_id);

        // Logon to market data server with Market Data auth pattern
        if let Err(e) = session.logon_market_data(heartbeat_interval).await {
            error!("Market Data Logon failed for {}: {}", gateway_url, e);
            let _ = session.logout().await;
            sleep(Duration::from_secs(5)).await;
            continue;
        }

        info!("Logon sent to Market Data Server ({}). Waiting for Logon ACK...", gateway_url);

        let mut logon_acked = false;
        let logon_timeout = Instant::now();

        while logon_timeout.elapsed() < Duration::from_secs(5) {
            match tokio::time::timeout(Duration::from_millis(500), session.receive_message()).await {
                Ok(Ok(msg)) => {
                    info!("📨 Market Data received message type: {}", msg.msg_type);

                    if msg.msg_type == 'A' {
                        logon_acked = true;
                        info!("✅ Market Data Logon ACK (35=A) received!");
                        
                        // Set connection state to Ready only after receiving ACK
                        *session.connection_state.lock().await = ConnectionState::Ready;
                        session.handle_message(&msg).await?;
                        info!("Waiting 50ms as per documentation...");
                        sleep(Duration::from_millis(50)).await;
                        break;
                    } else {
                        session.handle_message(&msg).await?;
                    }
                }
                Ok(Err(e)) => {
                    warn!("Connection error while waiting for Market Data Logon ACK ({}): {}", gateway_url, e);
                    break;
                }
                Err(_) => {
                    continue;
                }
            }
        }

        if !logon_acked {
            warn!("❌ Market Data Logon ACK not received from {}", gateway_url);
            sleep(Duration::from_secs(5)).await;
            continue;
        }

        info!("🔗 Market Data connection established ({}): ready for requests", gateway_url);

        // Send SecurityListRequest immediately after logon succeeds
        info!("📋 Sending SecurityListRequest to Market Data Server...");
        if let Err(e) = session.security_list_request("SEC_LIST_MD_001").await {
            error!("Failed to send SecurityListRequest: {}", e);
        } else {
            info!("✅ SecurityListRequest sent successfully!");
        }

        // Wait for SecurityListResponse to complete and ensure session is ready
        info!("Waiting 3 seconds for SecurityListResponse to be fully processed...");
        sleep(Duration::from_secs(3)).await;
        
        // Verify session is still ready before sending MarketDataRequest
        if !session.is_ready().await {
            warn!("⚠️  Session not ready, skipping MarketDataRequest");
            continue;
        }

        // List of symbols to subscribe to for market data
        // Per Crypto.com docs: MDReqID format must be book.{Symbol}.{MarketDepth} or trade.{Symbol}
        let all_symbols = vec![
            "BTC_USDT",  // Bitcoin / Tether
            "BTC_USD",   // Bitcoin / US Dollar
            "BTC_EUR",   // Bitcoin / Euro
            "SOL_BTC",   // Solana / Bitcoin
            "XRP_BTC",   // Ripple / Bitcoin
            "DOGE_BTC",  // Dogecoin / Bitcoin
            "ADA_BTC",   // Cardano / Bitcoin
            "LINK_BTC",  // Chainlink / Bitcoin
            "AVAX_BTC",  // Avalanche / Bitcoin
            "LTC_BTC",   // Litecoin / Bitcoin
            "ATOM_BTC",  // Cosmos / Bitcoin
            "ALGO_BTC",  // Algorand / Bitcoin
        ];
        
        // Filter symbols based on the symbol_filter function
        let symbols_to_subscribe: Vec<&str> = if let Some(filter) = &symbol_filter {
            all_symbols.iter().filter(|s| filter(s)).copied().collect()
        } else {
            all_symbols.iter().copied().collect()
        };
        
        info!("");
        info!("📊 Subscribing to {} markets for order book and trade data...", symbols_to_subscribe.len());
        if symbols_to_subscribe.is_empty() {
            info!("   (No symbols match this server's routing criteria)");
        }
        
        // Subscribe to each symbol
        for symbol in &symbols_to_subscribe {
            // Send MarketDataRequest (Order Book)
            info!("");
            info!("📈 Sending MarketDataRequest for {} (Order Book)...", symbol);
            if !session.is_ready().await {
                warn!("⚠️  Session not ready, skipping {}", symbol);
                continue;
            }
            
            if let Err(e) = session.market_data_request(symbol, 50).await {
                error!("Failed to send MarketDataRequest for {}: {}", symbol, e);
            } else {
                info!("✅ MarketDataRequest for {} sent successfully!", symbol);
            }
            
            // Small delay between requests to avoid overwhelming the server
            sleep(Duration::from_millis(200)).await;
            
            // Send TradeDataRequest (Trades)
            info!("💹 Sending TradeDataRequest for {} (Trades)...", symbol);
            if let Err(e) = session.trade_data_request(symbol).await {
                error!("Failed to send TradeDataRequest for {}: {}", symbol, e);
            } else {
                info!("✅ TradeDataRequest for {} sent successfully!", symbol);
            }
            
            // Small delay between symbols
            sleep(Duration::from_millis(200)).await;
        }
        
        info!("");
        info!("✅ Subscribed to {} markets successfully!", symbols_to_subscribe.len());
        info!("   Waiting for market data updates...");
        info!("   Note: Trades may come in incremental refresh messages (35=X)");
        
        // Wait a moment to see if we get any rejections
        sleep(Duration::from_secs(2)).await;

        // Message processing loop for market data
        // Run continuously - no timeout limit
        let mut connection_active = true;
        let mut last_order_book_dump = Instant::now();
        
        while connection_active {
            match tokio::time::timeout(Duration::from_secs(5), session.receive_message()).await {
                Ok(Ok(msg)) => {
                    info!("📨 Market Data message type: {} - processing", msg.msg_type);
                    if let Err(e) = session.handle_message(&msg).await {
                        error!("Market Data message handling error: {}", e);
                    }
                }
                Ok(Err(e)) => {
                    let err_str = e.to_string();
                    // Check if it's just an incomplete message or a real error
                    if err_str.contains("Incomplete FIX message") || err_str.contains("waiting for more data") {
                        // This is expected - fragment received, continue reading
                        info!("📦 Waiting for complete FIX message: {}", err_str);
                        continue;
                    } else {
                        warn!("⚠️  Market Data connection closed: {}", e);
                        connection_active = false;
                    }
                }
                Err(_) => {
                    // Timeout - continue waiting
                    continue;
                }
            }
        }

        // Connection closed - will reconnect
        info!("Market Data session ended. Reconnecting in 5 seconds...");
        sleep(Duration::from_secs(5)).await;
    }
}

/// Heartbeat sender with connection check
async fn heartbeat_loop(session: Arc<FIXSession>, heartbeat_interval: u64) -> Result<()> {
    let mut interval = interval(Duration::from_secs(heartbeat_interval));

    loop {
        interval.tick().await;

        // Only send heartbeat if session is ready (logon ACK received)
        if session.is_ready().await {
            match session.heartbeat().await {
                Ok(()) => {}
                Err(e) => {
                    warn!("Heartbeat error: {}", e);
                }
            }
        } else {
            // Session not ready yet - skip this heartbeat
            // This prevents sending heartbeats before logon ACK is received
        }
    }
}

/// Generate CSV summary from OpenOrdersStore snapshot (lock-free)
/// Append a single fill to data/trades.csv.
/// Called on every execution report with ExecType F/2/1 (partial or full fill).
/// Uses append mode so the file accumulates fills across server restarts.
fn append_trade_to_csv(
    cl_ord_id: &str,
    order_id: &str,
    symbol: &str,
    side: char,
    exec_type: char,
    last_qty: f64,
    last_px: f64,
    avg_px: f64,
) {
    let _ = (|| -> std::io::Result<()> {
        std::fs::create_dir_all("data")?;
        let path = Path::new("data/trades.csv");
        let file_is_new = !path.exists();
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        if file_is_new {
            writeln!(file, "timestamp_ns,cl_ord_id,order_id,symbol,side,exec_type,last_qty,last_px,avg_px")?;
        }
        let ts = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        writeln!(file, "{},{},{},{},{},{},{},{},{}",
            ts, cl_ord_id, order_id, symbol, side, exec_type,
            last_qty, last_px, avg_px
        )
    })();
}

/// Append a best-bid/ask snapshot to data/snapshots.csv for backtest persistence.
/// Called on every 35=W and 35=X message where both best_bid and best_ask are available.
fn append_snapshot_to_csv(symbol: &str, best_bid: f64, best_ask: f64) {
    if best_bid <= 0.0 || best_ask <= 0.0 || best_ask <= best_bid {
        return;
    }
    let _ = (|| -> std::io::Result<()> {
        std::fs::create_dir_all("data")?;
        let path = Path::new("data/snapshots.csv");
        let file_is_new = !path.exists();
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        if file_is_new {
            writeln!(file, "timestamp_ms,symbol,best_bid,best_ask")?;
        }
        let ts_ms = chrono::Utc::now().timestamp_millis();
        writeln!(file, "{},{},{},{}", ts_ms, symbol, best_bid, best_ask)
    })();
}

fn generate_open_orders_csv(open_orders: &[OpenOrder]) -> Result<String> {
    let mut csv_content = String::from("ClOrdID,OrderID,ExecID,Symbol,Side,OrderQty,Price,OrdStatus,ExecType,CumQty,LeavesQty,LastPx,LastQty,AvgPx,TransactTime,OrderCreateTime,RejectionReason\n");
    
    // Helper to escape CSV fields
    let escape_csv = |s: &str| {
        if s.contains(',') || s.contains('"') || s.contains('\n') {
            format!("\"{}\"", s.replace("\"", "\"\""))
        } else {
            s.to_string()
        }
    };
    
    for order in open_orders {
        csv_content.push_str(&format!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
            escape_csv(&order.cl_ord_id),
            escape_csv(&order.order_id),
            escape_csv(&order.exec_id),
            escape_csv(&order.symbol),
            escape_csv(&order.side),
            escape_csv(&order.order_qty),
            escape_csv(&order.price),
            escape_csv(&order.ord_status),
            escape_csv(&order.exec_type),
            escape_csv(&order.cum_qty),
            escape_csv(&order.leaves_qty),
            escape_csv(&order.last_px),
            escape_csv(&order.last_qty),
            escape_csv(&order.avg_px),
            escape_csv(&order.transact_time),
            escape_csv(&order.order_create_time),
            escape_csv(&order.rejection_reason)
        ));
    }
    
    Ok(csv_content)
}

/// Event-driven CSV generator - updates immediately when execution reports arrive
/// Uses ArcSwap for zero-lock snapshots - doesn't block order updates
async fn open_orders_csv_generator_event_driven(
    open_orders_store: Arc<OpenOrdersStore>,
    mut update_rx: broadcast::Receiver<()>,
) -> Result<()> {
    let output_file = "orders/processed/open_orders_summary.csv";
    
    // Ensure processed directory exists
    std::fs::create_dir_all("orders/processed")?;
    
    info!("📊 Starting event-driven open orders CSV generator (updates immediately on execution reports)");
    
    // Initial write
    let open_orders = open_orders_store.get_open_orders();
    if let Ok(csv_content) = generate_open_orders_csv(&open_orders) {
        let file_path = output_file.to_string();
        if let Err(e) = tokio::task::spawn_blocking(move || {
            std::fs::write(&file_path, csv_content)
        }).await {
            warn!("Failed to write initial open orders summary: {}", e);
        }
    }
    
    loop {
        // Wait for update signal (event-driven)
        if update_rx.recv().await.is_ok() {
            // Get lock-free snapshot (zero-copy, doesn't block writers)
            let open_orders = open_orders_store.get_open_orders();
            
            // Generate CSV from snapshot
            match generate_open_orders_csv(&open_orders) {
                Ok(csv_content) => {
                    // Write to file (blocking I/O in spawn_blocking to avoid blocking async runtime)
                    let file_path = output_file.to_string();
                    if let Err(e) = tokio::task::spawn_blocking(move || {
                        std::fs::write(&file_path, csv_content)
                    }).await {
                        warn!("Failed to write open orders summary: {}", e);
                    } else {
                        log::debug!("✅ Updated open orders summary CSV ({} open orders)", open_orders.len());
                    }
                }
                Err(e) => {
                    warn!("Failed to generate open orders CSV: {}", e);
                }
            }
        }
    }
}


/// Watch orders directory and process order files
/// Supports both JSON and binary (MessagePack) formats for microsecond-level performance
/// Binary format is ~2-3x faster to parse than JSON
/// Order file format (JSON):
/// {
///   "cl_ord_id": "ORDER_001",
///   "symbol": "BTC_USDT",
///   "side": "1",  // "1" for Buy, "2" for Sell
///   "qty": 0.001,
///   "price": 45000.0
/// }
async fn watch_orders_directory(
    order_tx: mpsc::Sender<OrderRequest>,
    orders_dir: &str,
    session: Arc<FIXSession>,
) -> Result<()> {
    // Create orders directory if it doesn't exist
    std::fs::create_dir_all(orders_dir)?;
    
    // Create processed subdirectory
    let processed_dir = format!("{}/processed", orders_dir);
    std::fs::create_dir_all(&processed_dir)?;
    
    info!("📁 Watching orders directory: {}", orders_dir);
    info!("   Supported formats: JSON (.json) - recommended for human readability");
    info!("   Processed orders will be moved to: {}", processed_dir);
    
    let mut check_interval = interval(Duration::from_millis(100)); // Check every 100ms for low latency
    
    loop {
        check_interval.tick().await;
        
        // Read directory entries (async)
        match tokio::fs::read_dir(orders_dir).await {
            Ok(mut entries) => {
                loop {
                    match entries.next_entry().await {
                        Ok(Some(entry)) => {
                            let path = entry.path();
                            
                            // Process JSON files
                            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                                let file_name = path.file_name().unwrap().to_string_lossy().to_string();
                                let file_read_start = Instant::now();
                                
                                // Read and parse JSON file (async I/O for better performance)
                                match tokio::fs::read_to_string(&path).await {
                                    Ok(content) => {
                                        let file_read_us = file_read_start.elapsed().as_micros() as u64;
                                        let json_parse_start = Instant::now();
                                        
                                        // Try to parse as CancelOrderRequest first (check for orig_cl_ord_id field)
                                        let json_value: serde_json::Value = match serde_json::from_str(&content) {
                                            Ok(v) => v,
                                            Err(e) => {
                                                error!("Failed to parse JSON in {}: {}", file_name, e);
                                                let error_name = format!("{}.error", file_name);
                                                let error_path = Path::new(&processed_dir).join(&error_name);
                                                if let Err(e) = tokio::fs::rename(&path, &error_path).await {
                                                    warn!("Failed to move error file: {}", e);
                                                }
                                                continue;
                                            }
                                        };
                                        
                                        // Check if this is a cancel order (has orig_cl_ord_id field)
                                        if json_value.get("orig_cl_ord_id").is_some() {
                                            // This is a cancel order
                                            match serde_json::from_value::<CancelOrderRequest>(json_value) {
                                                Ok(cancel_request) => {
                                                    let json_parse_us = json_parse_start.elapsed().as_micros() as u64;
                                                    
                                                    info!("🚫 Processing cancel order: {} (Original Order: {}, Symbol: {}, Side: {})", 
                                                          cancel_request.cl_ord_id, cancel_request.orig_cl_ord_id, 
                                                          cancel_request.symbol, cancel_request.side);
                                                    
                                                    // Send cancel order directly
                                                    match session.cancel_order(&cancel_request).await {
                                                        Ok(_) => {
                                                            info!("✅ Cancel order {} sent successfully", cancel_request.cl_ord_id);
                                                            
                                                            // Move to processed directory
                                                            let processed_path = Path::new(&processed_dir).join(&file_name);
                                                            if let Err(e) = tokio::fs::rename(&path, &processed_path).await {
                                                                warn!("Failed to move processed cancel order file {}: {}", file_name, e);
                                                            }
                                                        }
                                                        Err(e) => {
                                                            error!("Failed to send cancel order {}: {}", cancel_request.cl_ord_id, e);
                                                            // Move to processed with error suffix
                                                            let error_name = format!("{}.error", file_name);
                                                            let error_path = Path::new(&processed_dir).join(&error_name);
                                                            if let Err(e) = tokio::fs::rename(&path, &error_path).await {
                                                                warn!("Failed to move error file: {}", e);
                                                            }
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    error!("Failed to parse cancel order JSON in {}: {}", file_name, e);
                                                    // Move to processed with error suffix
                                                    let error_name = format!("{}.error", file_name);
                                                    let error_path = Path::new(&processed_dir).join(&error_name);
                                                    if let Err(e) = tokio::fs::rename(&path, &error_path).await {
                                                        warn!("Failed to move error file: {}", e);
                                                    }
                                                }
                                            }
                                        } else {
                                            // This is a regular order
                                            match serde_json::from_value::<OrderRequest>(json_value) {
                                                Ok(order) => {
                                                    let json_parse_us = json_parse_start.elapsed().as_micros() as u64;
                                                    let channel_send_start = Instant::now();
                                                    
                                                    let price_str = order.price.map(|p| p.to_string()).unwrap_or_else(|| "N/A (MARKET)".to_string());
                                                    info!("📄 Processing order: {} (Symbol: {}, Side: {}, Qty: {}, Price: {})", 
                                                          order.cl_ord_id, order.symbol, order.side, order.qty, price_str);
                                                    
                                                    // Initialize timing structure
                                                    let mut timing = OrderTiming {
                                                        file_read_us: Some(file_read_us),
                                                        json_parse_us: Some(json_parse_us),
                                                        channel_send_us: None,
                                                        fix_encode_us: None,
                                                        fix_send_us: None,
                                                        execution_report_us: None,
                                                        total_us: None,
                                                        file_read_timestamp: Some(chrono::Utc::now().timestamp_millis()),
                                                        execution_report_timestamp: None,
                                                    };
                                                    
                                                    // Send order to channel
                                                    match order_tx.send(order.clone()).await {
                                                        Ok(_) => {
                                                            let channel_send_us = channel_send_start.elapsed().as_micros() as u64;
                                                            timing.channel_send_us = Some(channel_send_us);
                                                            
                                                            // Store timing and order data in session (will be updated when execution report arrives)
                                                            {
                                                                let mut timings = session.order_timings.lock().await;
                                                                let mut order_data = session.order_data.lock().await;
                                                                timings.insert(order.cl_ord_id.clone(), timing);
                                                                order_data.insert(order.cl_ord_id.clone(), order);
                                                            }
                                                            
                                                            // Move to processed directory (async for better performance)
                                                            let processed_path = Path::new(&processed_dir).join(&file_name);
                                                            if let Err(e) = tokio::fs::rename(&path, &processed_path).await {
                                                                warn!("Failed to move processed file {}: {}", file_name, e);
                                                            } else {
                                                                info!("✅ Order {} queued (File read: {}µs, Parse: {}µs, Channel: {}µs)", 
                                                                      file_name, file_read_us, json_parse_us, channel_send_us);
                                                            }
                                                        }
                                                        Err(e) => {
                                                            error!("Failed to send order to queue: {}", e);
                                                            // Move to processed with error suffix
                                                            let error_name = format!("{}.error", file_name);
                                                            let error_path = Path::new(&processed_dir).join(&error_name);
                                                            if let Err(e) = tokio::fs::rename(&path, &error_path).await {
                                                                warn!("Failed to move error file: {}", e);
                                                            }
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    error!("Failed to parse order JSON in {}: {}", file_name, e);
                                                    // Move to processed with error suffix
                                                    let error_name = format!("{}.error", file_name);
                                                    let error_path = Path::new(&processed_dir).join(&error_name);
                                                    if let Err(e) = tokio::fs::rename(&path, &error_path).await {
                                                        warn!("Failed to move error file: {}", e);
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        error!("Failed to read order file {}: {}", file_name, e);
                                    }
                                }
                            }
                        }
                        Ok(None) => break, // No more entries
                        Err(e) => {
                            warn!("Error reading directory entry: {}", e);
                            break;
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Failed to read orders directory: {}", e);
            }
        }
    }
}

/// Event-driven ZeroMQ Publisher - publishes immediately on order book updates
/// Uses the read copy (snapshot) of the order book for lock-free reads
/// Topics: symbol (e.g., "BTC_USDT")
/// Message format: [topic, message_type, json_data]
async fn zmq_order_book_publisher_event_driven(
    session: Arc<FIXSession>,
    bind_addr: &str,
    mut update_rx: broadcast::Receiver<String>,
) -> Result<()> {
    // Create ZeroMQ context and publisher socket in a blocking thread
    let bind_addr_for_log = bind_addr.to_string();
    let bind_addr_clone = bind_addr.to_string();
    let (socket, ctx) = tokio::task::spawn_blocking(move || -> Result<(zmq::Socket, zmq::Context)> {
        let ctx = zmq::Context::new();
        let socket = ctx.socket(zmq::PUB)?;
        socket.bind(&bind_addr_clone)?;
        Ok((socket, ctx))
    }).await??;
    
    // Wrap socket in Arc<Mutex<>> to share across async tasks
    let socket = Arc::new(Mutex::new(socket));
    let _ctx = Arc::new(ctx); // Keep context alive
    
    info!("📡 ZeroMQ Publisher started (event-driven, publishes immediately on order book updates)");
    info!("   Bind address: {}", bind_addr_for_log);
    info!("   Topics: <symbol> (e.g., BTC_USDT)");
    info!("   Message types: bids, asks, trades");
    info!("   Example: Subscribe to 'BTC_USDT' to receive all BTC_USDT updates");
    
    loop {
        // Wait for update signal (event-driven)
        if let Ok(symbol) = update_rx.recv().await {
            log::info!("📡 ZeroMQ received update signal for: {}", symbol);
            // Get order book snapshot (lock-free read)
            let snapshot = {
                let books = session.order_books.lock().await;
                log::info!("📚 Checking order books, total symbols: {}", books.len());
                if let Some(book) = books.get(&symbol) {
                    log::info!("✅ Found order book for {}", symbol);
                    Some(book.get_snapshot())
                } else {
                    log::warn!("⚠️  No order book found for symbol: {}", symbol);
                    None
                }
            };
            
            if let Some(order_book) = snapshot {
                let best_bid = order_book.best_bid;
                let best_ask = order_book.best_ask;
                
                log::info!("📊 Order book snapshot: best_bid={}, best_ask={}", best_bid, best_ask);
                
                // Only publish if order book has valid data
                if best_bid > 0.0 && best_ask > 0.0 {
                    log::info!("✅ Publishing ZeroMQ messages for {}", symbol);
                    // Get bid and ask levels (top 50)
                    let bid_levels = order_book.get_depth(Side::Bid, 50);
                    let ask_levels = order_book.get_depth(Side::Ask, 50);
                    
                    // Get recent trades
                    let recent_trades = order_book.get_recent_trades(100);
                    
                    // Generate a single timestamp for all messages to ensure consistency
                    let snapshot_timestamp = chrono::Utc::now().timestamp_millis();
                    
                    // Prepare JSON data (all with the same timestamp)
                    let bids_data = serde_json::json!({
                        "type": "bids",
                        "symbol": symbol,
                        "timestamp": snapshot_timestamp,
                        "best_bid": best_bid,
                        "best_ask": best_ask,
                        "spread": best_ask - best_bid,
                        "levels": bid_levels.iter().map(|(p, s)| {
                            serde_json::json!({"price": p, "size": s})
                        }).collect::<Vec<_>>()
                    });
                    
                    let asks_data = serde_json::json!({
                        "type": "asks",
                        "symbol": symbol,
                        "timestamp": snapshot_timestamp,
                        "best_bid": best_bid,
                        "best_ask": best_ask,
                        "spread": best_ask - best_bid,
                        "levels": ask_levels.iter().map(|(p, s)| {
                            serde_json::json!({"price": p, "size": s})
                        }).collect::<Vec<_>>()
                    });
                    
                    let trades_data = serde_json::json!({
                        "type": "trades",
                        "symbol": symbol,
                        "timestamp": snapshot_timestamp,
                        "trades": recent_trades.iter().map(|(p, s, ts)| {
                            serde_json::json!({"price": p, "size": s, "timestamp": ts})
                        }).collect::<Vec<_>>(),
                        "trade_count": order_book.trade_count()
                    });
                    
                    // Serialize JSON
                    let bids_json = match serde_json::to_string(&bids_data) {
                        Ok(s) => s,
                        Err(e) => {
                            warn!("Failed to serialize bids JSON: {}", e);
                            continue;
                        }
                    };
                    
                    let asks_json = match serde_json::to_string(&asks_data) {
                        Ok(s) => s,
                        Err(e) => {
                            warn!("Failed to serialize asks JSON: {}", e);
                            continue;
                        }
                    };
                    
                    let trades_json = match serde_json::to_string(&trades_data) {
                        Ok(s) => s,
                        Err(e) => {
                            warn!("Failed to serialize trades JSON: {}", e);
                            continue;
                        }
                    };
                    
                    // Publish messages with symbol as topic (in blocking thread)
                    let socket_clone = Arc::clone(&socket);
                    let symbol_bytes = symbol.as_bytes().to_vec();
                    let bids_type = b"bids".to_vec();
                    let asks_type = b"asks".to_vec();
                    let trades_type = b"trades".to_vec();
                    let bids_bytes = bids_json.into_bytes();
                    let asks_bytes = asks_json.into_bytes();
                    let trades_bytes = trades_json.into_bytes();
                    
                    let _ = tokio::task::spawn_blocking(move || {
                        let socket_guard = socket_clone.blocking_lock();
                        // Send with topic prefix: [symbol, message_type, data]
                        if let Err(e) = socket_guard.send_multipart(&[&symbol_bytes, &bids_type, &bids_bytes], 0) {
                            warn!("ZeroMQ publish bids error: {}", e);
                        } else {
                            log::info!("✅ Sent bids message");
                        }
                        if let Err(e) = socket_guard.send_multipart(&[&symbol_bytes, &asks_type, &asks_bytes], 0) {
                            warn!("ZeroMQ publish asks error: {}", e);
                        } else {
                            log::info!("✅ Sent asks message");
                        }
                        if let Err(e) = socket_guard.send_multipart(&[&symbol_bytes, &trades_type, &trades_bytes], 0) {
                            warn!("ZeroMQ publish trades error: {}", e);
                        } else {
                            log::info!("✅ Sent trades message");
                        }
                    }).await;
                } else {
                    log::warn!("⚠️  Skipping publish - invalid order book data: best_bid={}, best_ask={}", best_bid, best_ask);
                }
            }
        }
    }
}

/// ZeroMQ Publisher for order book data (bids, asks, trades) - OLD VERSION (kept for reference)
/// Uses the read copy (snapshot) of the order book for lock-free reads
/// Topics: symbol (e.g., "BTC_USDT")
/// Message format: [topic, message_type, json_data]
#[allow(dead_code)]
async fn zmq_order_book_publisher(session: Arc<FIXSession>, bind_addr: &str) -> Result<()> {
    // Create ZeroMQ context and publisher socket in a blocking thread
    let bind_addr_for_log = bind_addr.to_string();
    let bind_addr_clone = bind_addr.to_string();
    let (socket, ctx) = tokio::task::spawn_blocking(move || -> Result<(zmq::Socket, zmq::Context)> {
        let ctx = zmq::Context::new();
        let socket = ctx.socket(zmq::PUB)?;
        socket.bind(&bind_addr_clone)?;
        Ok((socket, ctx))
    }).await??;
    
    // Wrap socket in Arc<Mutex<>> to share across async tasks
    let socket = Arc::new(Mutex::new(socket));
    let _ctx = Arc::new(ctx); // Keep context alive
    
    info!("📡 ZeroMQ Publisher bound to: {}", bind_addr_for_log);
    info!("   Topics: <symbol> (e.g., BTC_USDT)");
    info!("   Message types: bids, asks, trades");
    info!("   Example: Subscribe to 'BTC_USDT' to receive all BTC_USDT updates");
    
    let mut update_interval = interval(Duration::from_millis(500)); // Publish every 0.5 seconds
    
    loop {
        update_interval.tick().await;
        
        // Get order book snapshot (lock-free read)
        let snapshot = {
            let books = session.order_books.lock().await;
            if let Some(book) = books.get("BTC_USDT") {
                Some(book.get_snapshot())
            } else {
                None
            }
        };
        
        if let Some(order_book) = snapshot {
            let symbol = "BTC_USDT";
            let best_bid = order_book.best_bid;
            let best_ask = order_book.best_ask;
            
            // Only publish if order book has valid data
            if best_bid > 0.0 && best_ask > 0.0 {
                // Get bid and ask levels (top 50)
                let bid_levels = order_book.get_depth(Side::Bid, 50);
                let ask_levels = order_book.get_depth(Side::Ask, 50);
                
                // Get recent trades
                let recent_trades = order_book.get_recent_trades(100);
                
                // Prepare JSON data
                let bids_data = serde_json::json!({
                    "type": "bids",
                    "symbol": symbol,
                    "timestamp": chrono::Utc::now().timestamp_millis(),
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "spread": best_ask - best_bid,
                    "levels": bid_levels.iter().map(|(p, s)| {
                        serde_json::json!({"price": p, "size": s})
                    }).collect::<Vec<_>>()
                });
                
                let asks_data = serde_json::json!({
                    "type": "asks",
                    "symbol": symbol,
                    "timestamp": chrono::Utc::now().timestamp_millis(),
                    "best_bid": best_bid,
                    "best_ask": best_ask,
                    "spread": best_ask - best_bid,
                    "levels": ask_levels.iter().map(|(p, s)| {
                        serde_json::json!({"price": p, "size": s})
                    }).collect::<Vec<_>>()
                });
                
                let trades_data = serde_json::json!({
                    "type": "trades",
                    "symbol": symbol,
                    "timestamp": chrono::Utc::now().timestamp_millis(),
                    "trades": recent_trades.iter().map(|(p, s, ts)| {
                        serde_json::json!({"price": p, "size": s, "timestamp": ts})
                    }).collect::<Vec<_>>(),
                    "trade_count": order_book.trade_count()
                });
                
                // Serialize JSON
                let bids_json = match serde_json::to_string(&bids_data) {
                    Ok(s) => s,
                    Err(e) => {
                        warn!("Failed to serialize bids JSON: {}", e);
                        continue;
                    }
                };
                
                let asks_json = match serde_json::to_string(&asks_data) {
                    Ok(s) => s,
                    Err(e) => {
                        warn!("Failed to serialize asks JSON: {}", e);
                        continue;
                    }
                };
                
                let trades_json = match serde_json::to_string(&trades_data) {
                    Ok(s) => s,
                    Err(e) => {
                        warn!("Failed to serialize trades JSON: {}", e);
                        continue;
                    }
                };
                
                // Publish messages with symbol as topic (in blocking thread)
                let socket_clone = Arc::clone(&socket);
                let symbol_bytes = symbol.as_bytes().to_vec();
                let bids_type = b"bids".to_vec();
                let asks_type = b"asks".to_vec();
                let trades_type = b"trades".to_vec();
                let bids_bytes = bids_json.into_bytes();
                let asks_bytes = asks_json.into_bytes();
                let trades_bytes = trades_json.into_bytes();
                
                tokio::task::spawn_blocking(move || {
                    let socket_guard = socket_clone.blocking_lock();
                    // Send with topic prefix: [symbol, message_type, data]
                    if let Err(e) = socket_guard.send_multipart(&[&symbol_bytes, &bids_type, &bids_bytes], 0) {
                        warn!("ZeroMQ publish bids error: {}", e);
                    }
                    if let Err(e) = socket_guard.send_multipart(&[&symbol_bytes, &asks_type, &asks_bytes], 0) {
                        warn!("ZeroMQ publish asks error: {}", e);
                    }
                    if let Err(e) = socket_guard.send_multipart(&[&symbol_bytes, &trades_type, &trades_bytes], 0) {
                        warn!("ZeroMQ publish trades error: {}", e);
                    }
                }).await;
            }
        }
    }
}

/// Web server for displaying order book on a webpage and API endpoints for console
/// Serves an HTML page with real-time order book updates
async fn order_book_web_server(
    order_session: Arc<FIXSession>,  // Order management session (has open_orders)
    market_data_session: Arc<FIXSession>,  // Market data session (has order books for web display)
    market_data_sessions: Vec<(Arc<FIXSession>, String, u16)>,  // All market data sessions for status API
    kdb_writer: Arc<KdbWriter>,  // kdb+ connection for historical queries
    algo: Arc<algo::TradingAlgo>,  // Trading algorithm
    bind_addr: &str,
) -> Result<()> {
    use serde::Deserialize;
    
    #[derive(Deserialize)]
    struct SymbolQuery {
        symbol: Option<String>,
    }
    
    // HTML page with embedded JavaScript for real-time updates
    let html_page = r#"
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Wozinga Trading Platform</title>
    <link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&family=Rajdhani:wght@300;400;600;700&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"></script>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        html {
            height: 100%;
            overflow: hidden;
        }
        body {
            font-family: 'Rajdhani', 'Segoe UI', sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
            background-attachment: fixed;
            color: #e5e7eb;
            height: 100vh;
            overflow: hidden;
            position: relative;
        }
        body::before {
            content: '';
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: 
                radial-gradient(circle at 20% 50%, rgba(255, 107, 53, 0.1) 0%, transparent 50%),
                radial-gradient(circle at 80% 80%, rgba(255, 107, 53, 0.08) 0%, transparent 50%);
            pointer-events: none;
            z-index: 0;
        }
        .container {
            width: 100%;
            height: 100vh;
            display: flex;
            flex-direction: column;
            background: rgba(255, 255, 255, 0.03);
            backdrop-filter: blur(10px);
            overflow: hidden;
            position: relative;
            z-index: 1;
        }
        .header {
            background: linear-gradient(135deg, rgba(255, 107, 53, 0.9) 0%, rgba(255, 140, 0, 0.9) 100%);
            background-size: 200% 200%;
            animation: gradientShift 8s ease infinite;
            color: white;
            padding: 4px 16px;
            flex-shrink: 0;
            position: relative;
            overflow: hidden;
        }
        @keyframes gradientShift {
            0%, 100% { background-position: 0% 50%; }
            50% { background-position: 100% 50%; }
        }
        .header::before {
            content: '';
            position: absolute;
            top: -50%;
            left: -50%;
            width: 200%;
            height: 200%;
            background: linear-gradient(45deg, transparent, rgba(255, 255, 255, 0.1), transparent);
            animation: shine 3s infinite;
        }
        @keyframes shine {
            0% { transform: translateX(-100%) translateY(-100%) rotate(45deg); }
            100% { transform: translateX(100%) translateY(100%) rotate(45deg); }
        }
        .header-row {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 16px;
            position: relative;
            z-index: 1;
        }
        .header h1 {
            font-family: 'Orbitron', monospace;
            font-size: 0.95em;
            font-weight: 900;
            text-shadow: 0 0 10px rgba(255, 255, 255, 0.4);
            letter-spacing: 2px;
            white-space: nowrap;
        }
        .header-controls {
            display: flex;
            align-items: center;
            gap: 8px;
            flex-shrink: 0;
        }
        .menu-item {
            background: rgba(255, 255, 255, 0.15);
            backdrop-filter: blur(10px);
            padding: 3px 10px;
            border-radius: 6px;
            font-size: 11px;
            font-weight: 600;
            cursor: default;
            border: 1px solid rgba(255, 255, 255, 0.2);
            white-space: nowrap;
        }
        .menu-item-value {
            font-family: 'Orbitron', monospace;
            font-size: 12px;
            font-weight: bold;
            margin-left: 4px;
            color: #fff;
        }
        .header select {
            padding: 3px 8px;
            font-size: 11px;
            font-weight: 600;
            border-radius: 6px;
            border: 1px solid rgba(255, 255, 255, 0.3);
            background: rgba(255, 255, 255, 0.95);
            color: #1a1a2e;
            cursor: pointer;
            transition: border-color 0.3s ease;
        }
        .header select:hover {
            border-color: rgba(255, 255, 255, 0.6);
        }
        .stats {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
            gap: 8px;
            padding: 8px 16px;
            background: rgba(26, 26, 46, 0.4);
            backdrop-filter: blur(5px);
            flex-shrink: 0;
        }
        .stat-card {
            background: rgba(255, 255, 255, 0.05);
            backdrop-filter: blur(10px);
            padding: 7px 10px;
            border-radius: 8px;
            text-align: center;
            border: 1px solid rgba(255, 107, 53, 0.2);
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.3);
            transition: border-color 0.3s ease;
            position: relative;
            overflow: hidden;
        }
        .stat-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: -100%;
            width: 100%;
            height: 100%;
            background: linear-gradient(90deg, transparent, rgba(255, 107, 53, 0.1), transparent);
            transition: left 0.5s ease;
        }
        .stat-card:hover { border-color: rgba(255, 107, 53, 0.5); }
        .stat-card:hover::before { left: 100%; }
        .stat-label {
            font-size: 10px;
            color: #9ca3af;
            margin-bottom: 3px;
            text-transform: uppercase;
            letter-spacing: 1px;
            font-weight: 600;
        }
        .stat-value {
            font-family: 'Orbitron', monospace;
            font-size: 18px;
            font-weight: 700;
            color: #e5e7eb;
            text-shadow: 0 0 8px rgba(255, 255, 255, 0.3);
        }
        .stat-value.bid { 
            color: #10b981;
            text-shadow: 0 0 10px rgba(16, 185, 129, 0.6);
        }
        .stat-value.ask { 
            color: #ef4444;
            text-shadow: 0 0 10px rgba(239, 68, 68, 0.6);
        }
        .stat-value.spread { 
            color: #ff6b35;
            text-shadow: 0 0 10px rgba(255, 107, 53, 0.6);
        }
        /* ── Live tab main layout ── */
        .live-main {
            display: flex;
            flex: 1;
            min-height: 0;
            overflow: hidden;
            gap: 8px;
            padding: 8px;
        }
        .order-book-side {
            flex: 1;
            min-width: 0;
            background: rgba(26, 26, 46, 0.4);
            backdrop-filter: blur(5px);
            border-radius: 10px;
            border: 1px solid rgba(255, 107, 53, 0.2);
            box-shadow: 0 4px 15px rgba(0, 0, 0, 0.3);
            display: flex;
            flex-direction: column;
            overflow: hidden;
            min-height: 0;
        }
        .order-book-header {
            padding: 8px 12px;
            font-family: 'Orbitron', monospace;
            font-weight: 700;
            text-align: center;
            font-size: 12px;
            letter-spacing: 2px;
            text-shadow: 0 0 10px rgba(255, 255, 255, 0.5);
            flex-shrink: 0;
        }
        .order-book-header.bids {
            background: linear-gradient(135deg, rgba(16, 185, 129, 0.9) 0%, rgba(5, 150, 105, 0.9) 100%);
            color: white;
            box-shadow: 0 0 20px rgba(16, 185, 129, 0.4);
        }
        .order-book-header.asks {
            background: linear-gradient(135deg, rgba(239, 68, 68, 0.9) 0%, rgba(220, 38, 38, 0.9) 100%);
            color: white;
            box-shadow: 0 0 20px rgba(239, 68, 68, 0.4);
        }
        /* ── Dark-mode scrollbar styles ── */
        ::-webkit-scrollbar {
            width: 6px;
            height: 6px;
        }
        ::-webkit-scrollbar-track {
            background: rgba(255, 255, 255, 0.03);
            border-radius: 3px;
        }
        ::-webkit-scrollbar-thumb {
            background: rgba(255, 107, 53, 0.35);
            border-radius: 3px;
            transition: background 0.2s ease;
        }
        ::-webkit-scrollbar-thumb:hover {
            background: rgba(255, 107, 53, 0.65);
        }
        ::-webkit-scrollbar-corner {
            background: transparent;
        }
        /* Firefox */
        .order-book-scroll,
        .trades-scroll,
        .data-section-scroll,
        .data-table-fill {
            scrollbar-width: thin;
            scrollbar-color: rgba(255, 107, 53, 0.35) rgba(255, 255, 255, 0.03);
        }
        .order-book-scroll {
            flex: 1;
            overflow-y: auto;
            min-height: 0;
        }
        .order-book-table {
            width: 100%;
            border-collapse: collapse;
        }
        .order-book-table th {
            padding: 7px 10px;
            text-align: right;
            background: rgba(255, 107, 53, 0.15);
            font-weight: 700;
            font-size: 10px;
            text-transform: uppercase;
            color: #ff6b35;
            letter-spacing: 1px;
            border-bottom: 2px solid rgba(255, 107, 53, 0.3);
            position: sticky;
            top: 0;
            z-index: 1;
        }
        .order-book-table th:first-child {
            text-align: left;
        }
        .order-book-table td {
            padding: 6px 10px;
            text-align: right;
            font-family: 'Courier New', monospace;
            font-size: 13px;
            border-bottom: 1px solid rgba(255, 107, 53, 0.08);
            color: #e5e7eb;
            transition: background 0.2s ease;
        }
        .order-book-table td:first-child {
            text-align: left;
        }
        .order-book-table tbody tr:hover {
            background: rgba(255, 107, 53, 0.1);
            box-shadow: inset 0 0 10px rgba(255, 107, 53, 0.2);
        }
        .price {
            font-weight: bold;
            color: #fff;
        }
        .size {
            color: #9ca3af;
        }
        /* ── Trades panel (right column in Live tab) ── */
        .trades-panel {
            flex: 1;
            min-width: 0;
            display: flex;
            flex-direction: column;
            padding: 8px;
            background: rgba(26, 26, 46, 0.3);
            border-left: 1px solid rgba(255, 107, 53, 0.15);
            overflow: hidden;
            min-height: 0;
        }
        .trades-header {
            font-family: 'Orbitron', monospace;
            font-size: 12px;
            font-weight: 700;
            margin-bottom: 6px;
            color: #ff6b35;
            text-shadow: 0 0 8px rgba(255, 107, 53, 0.5);
            letter-spacing: 1px;
            flex-shrink: 0;
        }
        .trades-scroll {
            flex: 1;
            overflow-y: auto;
            min-height: 0;
        }
        .trades-table {
            width: 100%;
            border-collapse: collapse;
            background: rgba(255, 255, 255, 0.03);
            border-radius: 8px;
            overflow: hidden;
            border: 1px solid rgba(255, 107, 53, 0.2);
        }
        .trades-table th {
            padding: 7px 8px;
            text-align: right;
            background: linear-gradient(135deg, rgba(255, 107, 53, 0.8) 0%, rgba(255, 140, 0, 0.8) 100%);
            color: white;
            font-weight: 700;
            font-size: 10px;
            text-transform: uppercase;
            letter-spacing: 1px;
            position: sticky;
            top: 0;
        }
        .trades-table th:first-child {
            text-align: left;
        }
        .trades-table td {
            padding: 6px 8px;
            text-align: right;
            font-family: 'Courier New', monospace;
            font-size: 12px;
            border-bottom: 1px solid rgba(255, 107, 53, 0.1);
            color: #e5e7eb;
        }
        .trades-table td:first-child {
            text-align: left;
        }
        .trades-table tbody tr:hover {
            background: rgba(255, 107, 53, 0.1);
        }
        .status {
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 4px 10px;
            border-radius: 20px;
            background: rgba(31, 41, 55, 0.6);
            border: 1px solid rgba(255, 255, 255, 0.2);
            color: #9ca3af;
            font-size: 11px;
            font-weight: 600;
            letter-spacing: 1px;
            text-transform: uppercase;
            white-space: nowrap;
        }
        .status::before {
            content: '';
            width: 7px;
            height: 7px;
            border-radius: 50%;
            background: #6b7280;
            flex-shrink: 0;
        }
        .status.connected {
            background: rgba(16, 185, 129, 0.2);
            border-color: rgba(16, 185, 129, 0.5);
            color: #6ee7b7;
        }
        .status.connected::before {
            background: #10b981;
            box-shadow: 0 0 6px rgba(16, 185, 129, 0.8);
            animation: pulse 2s ease infinite;
        }
        @keyframes pulse {
            0%, 100% { box-shadow: 0 0 4px rgba(16, 185, 129, 0.8); }
            50% { box-shadow: 0 0 10px rgba(16, 185, 129, 1); }
        }
        .status.error {
            background: rgba(239, 68, 68, 0.2);
            border-color: rgba(239, 68, 68, 0.5);
            color: #fca5a5;
        }
        .status.error::before {
            background: #ef4444;
        }
        /* ── Tab navigation ── */
        .tab-nav {
            display: flex;
            gap: 3px;
            padding: 5px 16px 0;
            margin-top: 6px;
            background: rgba(26, 26, 46, 0.6);
            border-bottom: 2px solid rgba(255, 107, 53, 0.3);
            flex-shrink: 0;
        }
        .tab-btn {
            padding: 5px 16px;
            border: none;
            border-radius: 6px 6px 0 0;
            background: rgba(255,255,255,0.06);
            color: #9ca3af;
            font-family: 'Rajdhani', sans-serif;
            font-size: 12px;
            font-weight: 600;
            letter-spacing: 1px;
            cursor: pointer;
            transition: all 0.25s;
        }
        .tab-btn:hover { background: rgba(255,107,53,0.15); color: #e5e7eb; }
        .tab-btn.active {
            background: linear-gradient(135deg, rgba(255,107,53,0.8), rgba(255,140,0,0.8));
            color: #fff;
            box-shadow: 0 -2px 12px rgba(255,107,53,0.4);
        }
        .tab-panel { display: none; flex-direction: column; flex: 1; overflow: hidden; min-height: 0; }
        .tab-panel.active { display: flex; }
        /* ── History chart ── */
        .history-section {
            padding: 12px 16px;
            flex: 1;
            display: flex;
            flex-direction: column;
            overflow: hidden;
            min-height: 0;
        }
        .history-controls { display: flex; gap: 12px; align-items: center; margin-bottom: 10px; flex-wrap: wrap; flex-shrink: 0; }
        .history-controls select, .history-controls button {
            padding: 6px 14px;
            border-radius: 8px;
            border: 1px solid rgba(255,107,53,0.4);
            background: rgba(255,255,255,0.08);
            color: #e5e7eb;
            font-family: 'Rajdhani', sans-serif;
            font-size: 13px;
            cursor: pointer;
        }
        .history-controls button {
            background: linear-gradient(135deg, rgba(255,107,53,0.7), rgba(255,140,0,0.7));
            font-weight: 700;
        }
        .history-controls button:hover { opacity: 0.85; }
        .chart-wrap {
            background: rgba(26,26,46,0.5);
            border: 1px solid rgba(255,107,53,0.2);
            border-radius: 12px;
            padding: 14px;
            position: relative;
            flex: 1;
            min-height: 0;
            display: flex;
            flex-direction: column;
        }
        .chart-wrap canvas { flex: 1; min-height: 0; }
        /* ── Generic data table ── */
        .data-section {
            padding: 12px 16px;
            flex: 1;
            display: flex;
            flex-direction: column;
            overflow: hidden;
            min-height: 0;
        }
        .data-section-scroll {
            padding: 12px 16px;
            flex: 1;
            display: flex;
            flex-direction: column;
            overflow-y: auto;
            overflow-x: hidden;
            min-height: 0;
        }
        .data-table-wrap { overflow-x: auto; flex-shrink: 0; }
        .data-table-fill { overflow-x: auto; flex: 1; overflow-y: auto; min-height: 0; }
        .data-table {
            width: 100%;
            border-collapse: collapse;
            background: rgba(255,255,255,0.03);
            border-radius: 12px;
            overflow: hidden;
            border: 1px solid rgba(255,107,53,0.2);
        }
        .data-table th {
            padding: 9px 12px;
            text-align: left;
            background: linear-gradient(135deg,rgba(255,107,53,0.8),rgba(255,140,0,0.8));
            color: #fff;
            font-size: 10px;
            text-transform: uppercase;
            letter-spacing: 1px;
            white-space: nowrap;
            position: sticky;
            top: 0;
            z-index: 1;
        }
        .data-table td {
            padding: 7px 12px;
            font-family: 'Courier New', monospace;
            font-size: 12px;
            border-bottom: 1px solid rgba(255,107,53,0.1);
            color: #e5e7eb;
        }
        .data-table tbody tr:hover { background: rgba(255,107,53,0.08); }
        /* ── Securities search ── */
        .search-bar {
            width: 100%;
            max-width: 420px;
            padding: 7px 14px;
            border-radius: 8px;
            border: 1px solid rgba(255,107,53,0.4);
            background: rgba(255,255,255,0.08);
            color: #e5e7eb;
            font-family: 'Rajdhani', sans-serif;
            font-size: 14px;
            margin-bottom: 10px;
            flex-shrink: 0;
        }
        .search-bar::placeholder { color: #6b7280; }
        .section-title {
            font-family: 'Orbitron', monospace;
            font-size: 16px;
            font-weight: 700;
            color: #ff6b35;
            margin-bottom: 10px;
            text-shadow: 0 0 8px rgba(255,107,53,0.4);
            flex-shrink: 0;
        }
        @media (max-width: 900px) {
            .live-main { flex-direction: column; padding: 6px; }
            .order-book-side { flex: none; height: 33%; }
            .trades-panel { flex: 1; border-left: none; border-top: 1px solid rgba(255,107,53,0.15); }
            .header h1 { font-size: 0.85em; }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="header-row">
                <h1>WOZINGA HFT PLATFORM</h1>
                <div class="header-controls">
                    <div class="menu-item">Markets: <span class="menu-item-value" id="totalMarkets">-</span></div>
                    <select id="symbolSelect">
                        <option value="BTC_USDT">BTC_USDT</option>
                        <option value="BTC_USD">BTC_USD</option>
                        <option value="SOL_BTC">SOL_BTC</option>
                        <option value="XRP_BTC">XRP_BTC</option>
                        <option value="DOGE_BTC">DOGE_BTC</option>
                    </select>
                    <div class="status" id="status">Connecting...</div>
                </div>
            </div>
        </div>

        <!-- Tab navigation -->
        <div class="tab-nav">
            <button class="tab-btn active" onclick="switchTab('live', this)">Live</button>
            <button class="tab-btn" onclick="switchTab('history', this)">History</button>
            <button class="tab-btn" onclick="switchTab('orders', this)">Orders</button>
            <button class="tab-btn" onclick="switchTab('positions', this)">Positions</button>
            <button class="tab-btn" onclick="switchTab('algo', this)">Algo</button>
            <button class="tab-btn" onclick="switchTab('backtest', this)">Backtest</button>
            <button class="tab-btn" onclick="switchTab('securities', this)">Securities</button>
        </div>

        <!-- ── LIVE TAB ── -->
        <div id="tab-live" class="tab-panel active">
            <div class="stats">
                <div class="stat-card">
                    <div class="stat-label">Best Bid</div>
                    <div class="stat-value bid" id="bestBid">-</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Best Ask</div>
                    <div class="stat-value ask" id="bestAsk">-</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Spread</div>
                    <div class="stat-value spread" id="spread">-</div>
                </div>
                <div class="stat-card">
                    <div class="stat-label">Trade Count</div>
                    <div class="stat-value" id="tradeCount">0</div>
                </div>
            </div>

            <div class="live-main">
                <div class="order-book-side">
                    <div class="order-book-header bids">BIDS</div>
                    <div class="order-book-scroll">
                        <table class="order-book-table">
                            <thead><tr><th>Price</th><th>Size</th></tr></thead>
                            <tbody id="bidsTable">
                                <tr><td colspan="2" style="text-align:center;padding:20px">Loading...</td></tr>
                            </tbody>
                        </table>
                    </div>
                </div>
                <div class="order-book-side">
                    <div class="order-book-header asks">ASKS</div>
                    <div class="order-book-scroll">
                        <table class="order-book-table">
                            <thead><tr><th>Price</th><th>Size</th></tr></thead>
                            <tbody id="asksTable">
                                <tr><td colspan="2" style="text-align:center;padding:20px">Loading...</td></tr>
                            </tbody>
                        </table>
                    </div>
                </div>
                <div class="trades-panel">
                    <div class="trades-header">Recent Trades</div>
                    <div class="trades-scroll">
                        <table class="trades-table">
                            <thead><tr><th>Price</th><th>Size</th><th>Time</th></tr></thead>
                            <tbody id="tradesTable">
                                <tr><td colspan="3" style="text-align:center;padding:20px">No trades yet</td></tr>
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>
        </div>

        <!-- ── HISTORY TAB ── -->
        <div id="tab-history" class="tab-panel">
            <div class="history-section">
                <div class="section-title">Bid / Ask History</div>
                <div class="history-controls">
                    <select id="histSymbolSelect"></select>
                    <button onclick="loadHistory()">Load</button>
                    <span id="histStatus" style="color:#9ca3af;font-size:13px"></span>
                </div>
                <div class="chart-wrap">
                    <canvas id="histChart"></canvas>
                </div>
            </div>
        </div>

        <!-- ── ORDERS TAB ── -->
        <div id="tab-orders" class="tab-panel">
            <div class="data-section">
                <div class="section-title">Order Execution History</div>
                <div class="data-table-fill">
                    <table class="data-table">
                        <thead>
                            <tr>
                                <th>ClOrdID</th>
                                <th>Symbol</th>
                                <th>Side</th>
                                <th>Qty</th>
                                <th>Price</th>
                                <th>Status</th>
                                <th>CumQty</th>
                                <th>AvgPx</th>
                                <th>Time</th>
                            </tr>
                        </thead>
                        <tbody id="ordersTable">
                            <tr><td colspan="9" style="text-align:center;padding:20px">Loading...</td></tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>

        <!-- ── POSITIONS TAB ── -->
        <div id="tab-positions" class="tab-panel">
            <div class="data-section">
                <div class="section-title">Live Positions &amp; P&amp;L</div>
                <div class="data-table-fill">
                    <table class="data-table">
                        <thead>
                            <tr>
                                <th>Symbol</th>
                                <th>Net Qty</th>
                                <th>Avg Entry</th>
                                <th>Last Mid</th>
                                <th>Unreal P&amp;L</th>
                                <th>Real P&amp;L</th>
                                <th>Total P&amp;L</th>
                                <th>Trades</th>
                            </tr>
                        </thead>
                        <tbody id="positionsTable">
                            <tr><td colspan="8" style="text-align:center;padding:20px">Loading…</td></tr>
                        </tbody>
                    </table>
                </div>
                <div id="positionsStatus" style="padding:6px 0;color:#9ca3af;font-size:12px;flex-shrink:0;"></div>
            </div>
        </div>

        <!-- ── ALGO TAB ── -->
        <div id="tab-algo" class="tab-panel">
            <div class="data-section-scroll">
                <div class="section-title">Trading Algorithm</div>

                <!-- Status banner -->
                <div id="algoStatusBanner" style="padding:12px 20px;border-radius:8px;margin-bottom:20px;font-weight:600;font-size:15px;background:rgba(75,85,99,0.5);color:#9ca3af;">
                    Loading algo status…
                </div>

                <!-- Controls row -->
                <div style="display:flex;gap:12px;flex-wrap:wrap;margin-bottom:24px;">
                    <button onclick="algoEnable()" style="padding:10px 24px;border-radius:8px;border:none;background:linear-gradient(135deg,#22c55e,#16a34a);color:white;font-weight:700;cursor:pointer;font-size:14px;">Enable Algo</button>
                    <button onclick="algoDisable()" style="padding:10px 24px;border-radius:8px;border:none;background:linear-gradient(135deg,#ef4444,#dc2626);color:white;font-weight:700;cursor:pointer;font-size:14px;">Disable Algo</button>
                    <button onclick="loadAlgoStatus()" style="padding:10px 24px;border-radius:8px;border:none;background:rgba(255,107,53,0.2);color:#ff6b35;font-weight:700;cursor:pointer;font-size:14px;border:1px solid rgba(255,107,53,0.3);">Refresh</button>
                </div>

                <!-- Config editor -->
                <div style="margin-bottom:24px;">
                    <div style="color:#ff6b35;font-weight:600;margin-bottom:12px;font-size:14px;text-transform:uppercase;letter-spacing:1px;">Configuration</div>
                    <div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:12px;">
                        <label style="display:flex;flex-direction:column;gap:4px;font-size:13px;color:#9ca3af;">
                            Symbol
                            <input id="algoCfgSymbol" type="text" style="padding:8px;background:rgba(31,41,55,0.8);border:1px solid rgba(255,107,53,0.3);border-radius:6px;color:#e5e7eb;font-size:14px;">
                        </label>
                        <label style="display:flex;flex-direction:column;gap:4px;font-size:13px;color:#9ca3af;">
                            Order Qty (base units)
                            <input id="algoCfgQty" type="number" step="0.0001" style="padding:8px;background:rgba(31,41,55,0.8);border:1px solid rgba(255,107,53,0.3);border-radius:6px;color:#e5e7eb;font-size:14px;">
                        </label>
                        <label style="display:flex;flex-direction:column;gap:4px;font-size:13px;color:#9ca3af;">
                            Min Spread (bps)
                            <input id="algoCfgSpread" type="number" step="0.1" style="padding:8px;background:rgba(31,41,55,0.8);border:1px solid rgba(255,107,53,0.3);border-radius:6px;color:#e5e7eb;font-size:14px;">
                        </label>
                        <label style="display:flex;flex-direction:column;gap:4px;font-size:13px;color:#9ca3af;">
                            Max Position (base units)
                            <input id="algoCfgMaxPos" type="number" step="0.001" style="padding:8px;background:rgba(31,41,55,0.8);border:1px solid rgba(255,107,53,0.3);border-radius:6px;color:#e5e7eb;font-size:14px;">
                        </label>
                        <label style="display:flex;flex-direction:column;gap:4px;font-size:13px;color:#9ca3af;">
                            Poll Interval (ms)
                            <input id="algoCfgPoll" type="number" step="100" style="padding:8px;background:rgba(31,41,55,0.8);border:1px solid rgba(255,107,53,0.3);border-radius:6px;color:#e5e7eb;font-size:14px;">
                        </label>
                        <label style="display:flex;flex-direction:column;gap:4px;font-size:13px;color:#9ca3af;">
                            Time-in-Force (0=Day,3=IOC,4=FOK)
                            <input id="algoCfgTIF" type="text" maxlength="1" style="padding:8px;background:rgba(31,41,55,0.8);border:1px solid rgba(255,107,53,0.3);border-radius:6px;color:#e5e7eb;font-size:14px;">
                        </label>
                    </div>
                    <button onclick="algoSaveConfig()" style="margin-top:14px;padding:10px 24px;border-radius:8px;border:none;background:linear-gradient(135deg,rgba(255,107,53,0.8),rgba(255,140,0,0.8));color:white;font-weight:700;cursor:pointer;font-size:14px;">Save Config</button>
                    <span id="algoCfgMsg" style="margin-left:12px;font-size:13px;color:#9ca3af;"></span>
                </div>

                <!-- Signal log -->
                <div>
                    <div style="color:#ff6b35;font-weight:600;margin-bottom:12px;font-size:14px;text-transform:uppercase;letter-spacing:1px;">Recent Signals</div>
                    <table class="data-table" style="font-size:13px;">
                        <thead>
                            <tr>
                                <th>Time</th>
                                <th>Symbol</th>
                                <th>Side</th>
                                <th>Price</th>
                                <th>Qty</th>
                                <th>Reason</th>
                            </tr>
                        </thead>
                        <tbody id="algoSignalLog">
                            <tr><td colspan="6" style="text-align:center;padding:20px;color:#9ca3af;">No signals yet</td></tr>
                        </tbody>
                    </table>
                    <div style="margin-top:8px;color:#6b7280;font-size:12px;">Signals fired: <span id="algoSigCount">0</span> | Orders placed: <span id="algoOrdCount">0</span></div>
                </div>
            </div>
        </div>

        <!-- ── BACKTEST TAB ── -->
        <div id="tab-backtest" class="tab-panel">
            <div class="data-section-scroll">
                <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:10px;">
                    <div style="display:flex;align-items:baseline;gap:14px;">
                        <div class="section-title" style="margin-bottom:0;">Strategy Backtester</div>
                        <span id="btStatus" style="color:#9ca3af;font-size:13px;"></span>
                    </div>
                    <button onclick="runBacktest()" id="btRunBtn" style="padding:10px 20px;border-radius:8px;border:none;background:linear-gradient(135deg,rgba(255,107,53,0.9),rgba(255,140,0,0.9));color:white;font-weight:700;cursor:pointer;font-size:14px;">Run Backtest</button>
                </div>

                <!-- Parameter form -->
                <div style="display:flex;flex-wrap:wrap;align-items:flex-end;gap:12px;margin-bottom:24px;">
                    <label style="display:flex;flex-direction:column;gap:4px;font-size:13px;color:#9ca3af;flex:1;min-width:120px;">
                        Symbol
                        <select id="btSymbol" style="padding:8px;background:rgba(31,41,55,0.8);border:1px solid rgba(255,107,53,0.3);border-radius:6px;color:#e5e7eb;font-size:14px;width:100%;"></select>
                    </label>
                    <label style="display:flex;flex-direction:column;gap:4px;font-size:13px;color:#9ca3af;flex:1;min-width:120px;">
                        Snapshots
                        <input id="btLimit" type="number" value="5000" min="100" max="50000" style="padding:8px;background:rgba(31,41,55,0.8);border:1px solid rgba(255,107,53,0.3);border-radius:6px;color:#e5e7eb;font-size:14px;width:100%;">
                    </label>
                    <label style="display:flex;flex-direction:column;gap:4px;font-size:13px;color:#9ca3af;flex:1;min-width:120px;">
                        Min Spread (bps)
                        <input id="btSpread" type="number" value="5" step="0.1" style="padding:8px;background:rgba(31,41,55,0.8);border:1px solid rgba(255,107,53,0.3);border-radius:6px;color:#e5e7eb;font-size:14px;width:100%;">
                    </label>
                    <label style="display:flex;flex-direction:column;gap:4px;font-size:13px;color:#9ca3af;flex:1;min-width:120px;">
                        Order Qty
                        <input id="btQty" type="number" value="0.001" step="0.0001" style="padding:8px;background:rgba(31,41,55,0.8);border:1px solid rgba(255,107,53,0.3);border-radius:6px;color:#e5e7eb;font-size:14px;width:100%;">
                    </label>
                    <label style="display:flex;flex-direction:column;gap:4px;font-size:13px;color:#9ca3af;flex:1;min-width:120px;">
                        Max Position
                        <input id="btMaxPos" type="number" value="0.01" step="0.001" style="padding:8px;background:rgba(31,41,55,0.8);border:1px solid rgba(255,107,53,0.3);border-radius:6px;color:#e5e7eb;font-size:14px;width:100%;">
                    </label>
                    <label style="display:flex;flex-direction:column;gap:4px;font-size:13px;color:#9ca3af;flex:1;min-width:120px;">
                        Tick Size
                        <input id="btTick" type="number" value="0.01" step="0.001" style="padding:8px;background:rgba(31,41,55,0.8);border:1px solid rgba(255,107,53,0.3);border-radius:6px;color:#e5e7eb;font-size:14px;width:100%;">
                    </label>
                </div>

                <!-- Summary stat cards -->
                <div id="btSummary" style="display:none;">
                    <div style="display:grid;grid-template-columns:repeat(6,1fr);gap:14px;margin-bottom:24px;">
                        <div class="stat-card"><div class="stat-label">Total P&amp;L</div><div class="stat-value" id="btStatPnl">-</div></div>
                        <div class="stat-card"><div class="stat-label">Total Trades</div><div class="stat-value" id="btStatTrades">-</div></div>
                        <div class="stat-card"><div class="stat-label">Win Rate</div><div class="stat-value" id="btStatWinRate">-</div></div>
                        <div class="stat-card"><div class="stat-label">Max Drawdown</div><div class="stat-value" id="btStatDrawdown">-</div></div>
                        <div class="stat-card"><div class="stat-label">Sharpe Ratio</div><div class="stat-value" id="btStatSharpe">-</div></div>
                        <div class="stat-card"><div class="stat-label">Final Position</div><div class="stat-value" id="btStatPos">-</div></div>
                    </div>

                    <!-- Price + signals chart -->
                    <div class="chart-wrap" style="margin-bottom:16px;height:260px;flex:none;">
                        <div style="color:#ff6b35;font-size:12px;font-weight:600;margin-bottom:6px;text-transform:uppercase;letter-spacing:1px;">Price &amp; Trade Signals</div>
                        <canvas id="btPriceChart"></canvas>
                    </div>

                    <!-- Equity curve chart -->
                    <div class="chart-wrap" style="margin-bottom:16px;height:260px;flex:none;">
                        <div style="color:#ff6b35;font-size:12px;font-weight:600;margin-bottom:6px;text-transform:uppercase;letter-spacing:1px;">Equity Curve (Cumulative P&amp;L)</div>
                        <canvas id="btEquityChart"></canvas>
                    </div>

                    <!-- Trade log table -->
                    <div>
                        <div style="color:#ff6b35;font-size:13px;font-weight:600;margin-bottom:8px;text-transform:uppercase;letter-spacing:1px;">Trade Log <span id="btTradeCount" style="font-weight:400;color:#9ca3af;"></span></div>
                        <div class="data-table-wrap">
                            <table class="data-table" style="font-size:13px;">
                                <thead>
                                    <tr>
                                        <th>Time</th>
                                        <th>Side</th>
                                        <th>Fill Price</th>
                                        <th>Qty</th>
                                        <th>Trade P&amp;L</th>
                                        <th>Cumul. P&amp;L</th>
                                        <th>Net Pos After</th>
                                    </tr>
                                </thead>
                                <tbody id="btTradeLog">
                                    <tr><td colspan="7" style="text-align:center;padding:20px;color:#9ca3af">Run a backtest to see trades</td></tr>
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- ── SECURITIES TAB ── -->
        <div id="tab-securities" class="tab-panel">
            <div class="data-section">
                <div class="section-title">Securities</div>
                <input class="search-bar" id="secSearch" type="text" placeholder="Search by symbol…" oninput="filterSecurities()">
                <div class="data-table-fill">
                    <table class="data-table">
                        <thead>
                            <tr>
                                <th>Symbol</th>
                                <th>Security ID</th>
                                <th>Exchange</th>
                                <th>Attributes</th>
                            </tr>
                        </thead>
                        <tbody id="securitiesTable">
                            <tr><td colspan="4" style="text-align:center;padding:20px">Loading...</td></tr>
                        </tbody>
                    </table>
                </div>
            </div>
        </div>

    </div>

    <script>
        // ── shared state ────────────────────────────────────────────────────────────
        let currentSymbol = 'BTC_USDT';
        let updateInterval = null;
        let histChartInstance = null;
        let allSecurities = [];

        // ── tab switching ────────────────────────────────────────────────────────────
        function switchTab(name, btn) {
            document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            document.getElementById('tab-' + name).classList.add('active');
            btn.classList.add('active');
            if (name === 'orders') loadOrders();
            if (name === 'positions') loadPositions();
            if (name === 'algo') loadAlgoStatus();
            if (name === 'backtest') initBacktestSymbols();
            if (name === 'securities') loadSecurities();
        }

        // ── symbol selector ──────────────────────────────────────────────────────────
        const symbolSelect = document.getElementById('symbolSelect');
        symbolSelect.addEventListener('change', e => {
            currentSymbol = e.target.value;
            updateOrderBook();
        });

        // ── helpers ──────────────────────────────────────────────────────────────────
        function formatNumber(num) {
            if (num === null || num === undefined || isNaN(num)) return '-';
            return num.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 8 });
        }

        function formatTime(timestamp) {
            if (!timestamp) return '-';
            // kdb+ timestamp is nanoseconds since 2000-01-01 (not Unix epoch)
            // Unix epoch offset: 946684800 seconds = 946684800000000000 nanoseconds
            const KDB_EPOCH_OFFSET_NS = 946684800000000000n;
            try {
                const ns = BigInt(Math.round(timestamp));
                const ms = Number((ns + KDB_EPOCH_OFFSET_NS) / 1000000n);
                return new Date(ms).toLocaleString();
            } catch(_) {
                return new Date(timestamp / 1000000).toLocaleTimeString();
            }
        }

        function formatKdbTime(ts) {
            // ts is either a Unix-ms integer (from Rust binary IPC conversion) or
            // a legacy ISO-8601 string "2024-02-25T10:30:00.000000000"
            if (!ts && ts !== 0) return '-';
            try {
                const ms = (typeof ts === 'number') ? ts : Date.parse(ts);
                return new Date(ms).toLocaleString();
            } catch(_) { return String(ts); }
        }

        // ── LIVE tab ─────────────────────────────────────────────────────────────────
        function updateTotalMarkets() {
            fetch('/api/symbols')
                .then(r => r.json())
                .then(data => {
                    document.getElementById('totalMarkets').textContent = data.symbols ? data.symbols.length : 0;
                    const sel = document.getElementById('symbolSelect');
                    const cur = sel.value;
                    sel.innerHTML = '';
                    const syms = (data.symbols && data.symbols.length > 0) ? data.symbols : ['BTC_USDT'];
                    syms.forEach(s => {
                        const o = document.createElement('option');
                        o.value = s; o.textContent = s;
                        if (s === cur) o.selected = true;
                        sel.appendChild(o);
                    });
                    // also populate history symbol selector
                    const hSel = document.getElementById('histSymbolSelect');
                    hSel.innerHTML = '';
                    syms.forEach(s => {
                        const o = document.createElement('option');
                        o.value = s; o.textContent = s;
                        if (s === currentSymbol) o.selected = true;
                        hSel.appendChild(o);
                    });
                })
                .catch(err => console.error('Error fetching symbols:', err));
        }

        function updateOrderBook() {
            fetch('/api/orderbook/' + currentSymbol)
                .then(r => { if (!r.ok) throw new Error('fetch failed'); return r.json(); })
                .then(data => {
                    document.getElementById('status').textContent = 'Connected';
                    document.getElementById('status').className = 'status connected';
                    document.getElementById('bestBid').textContent = formatNumber(data.best_bid);
                    document.getElementById('bestAsk').textContent = formatNumber(data.best_ask);
                    document.getElementById('spread').textContent = formatNumber(data.spread);
                    document.getElementById('tradeCount').textContent = data.trade_count || 0;

                    const bids = document.getElementById('bidsTable');
                    bids.innerHTML = data.bids && data.bids.length
                        ? data.bids.map(l => `<tr><td class="price">${formatNumber(l.price)}</td><td class="size">${formatNumber(l.size)}</td></tr>`).join('')
                        : '<tr><td colspan="2" style="text-align:center;padding:20px">No bids</td></tr>';

                    const asks = document.getElementById('asksTable');
                    asks.innerHTML = data.asks && data.asks.length
                        ? data.asks.map(l => `<tr><td class="price">${formatNumber(l.price)}</td><td class="size">${formatNumber(l.size)}</td></tr>`).join('')
                        : '<tr><td colspan="2" style="text-align:center;padding:20px">No asks</td></tr>';

                    const trades = document.getElementById('tradesTable');
                    trades.innerHTML = data.trades && data.trades.length
                        ? data.trades.map(t => `<tr><td class="price">${formatNumber(t.price)}</td><td class="size">${formatNumber(t.size)}</td><td>${formatTime(t.timestamp)}</td></tr>`).join('')
                        : '<tr><td colspan="3" style="text-align:center;padding:20px">No trades yet</td></tr>';
                })
                .catch(err => {
                    document.getElementById('status').textContent = 'Error: ' + err.message;
                    document.getElementById('status').className = 'status error';
                });
        }

        // ── HISTORY tab ──────────────────────────────────────────────────────────────
        function loadHistory() {
            const sym = document.getElementById('histSymbolSelect').value || currentSymbol;
            const statusEl = document.getElementById('histStatus');
            statusEl.textContent = 'Loading…';
            fetch('/api/history/' + sym)
                .then(r => r.json())
                .then(rows => {
                    if (!Array.isArray(rows) || rows.length === 0) {
                        statusEl.textContent = 'No snapshot data for ' + sym;
                        return;
                    }
                    statusEl.textContent = rows.length + ' snapshots';
                    const labels = rows.map(r => formatKdbTime(r.time));
                    const bids   = rows.map(r => r.best_bid);
                    const asks   = rows.map(r => r.best_ask);
                    if (histChartInstance) histChartInstance.destroy();
                    histChartInstance = new Chart(document.getElementById('histChart'), {
                        type: 'line',
                        data: {
                            labels,
                            datasets: [
                                { label: 'Best Bid', data: bids, borderColor: '#10b981', backgroundColor: 'rgba(16,185,129,0.08)', tension: 0.2, pointRadius: 0 },
                                { label: 'Best Ask', data: asks, borderColor: '#ef4444', backgroundColor: 'rgba(239,68,68,0.08)',   tension: 0.2, pointRadius: 0 }
                            ]
                        },
                        options: {
                            responsive: true,
                            interaction: { mode: 'index', intersect: false },
                            plugins: { legend: { labels: { color: '#e5e7eb' } } },
                            scales: {
                                x: { ticks: { color: '#9ca3af', maxTicksLimit: 10 }, grid: { color: 'rgba(255,255,255,0.05)' } },
                                y: { ticks: { color: '#9ca3af' }, grid: { color: 'rgba(255,255,255,0.05)' } }
                            }
                        }
                    });
                })
                .catch(err => { statusEl.textContent = 'Error: ' + err.message; });
        }

        // ── ORDERS tab ───────────────────────────────────────────────────────────────
        function loadOrders() {
            const tbody = document.getElementById('ordersTable');
            tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;padding:20px">Loading…</td></tr>';
            fetch('/api/orders/history')
                .then(r => r.json())
                .then(data => {
                    const orders = data.orders || [];
                    if (!orders.length) {
                        tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;padding:20px">No orders found</td></tr>';
                        return;
                    }
                    tbody.innerHTML = orders.map(o => `
                        <tr>
                            <td>${o.cl_ord_id || '-'}</td>
                            <td>${o.symbol || '-'}</td>
                            <td style="color:${o.side==='1'?'#10b981':'#ef4444'}">${o.side==='1'?'BUY':'SELL'}</td>
                            <td>${o.order_qty || '-'}</td>
                            <td>${o.price || '-'}</td>
                            <td>${o.ord_status || '-'}</td>
                            <td>${o.cum_qty || '-'}</td>
                            <td>${o.avg_px || '-'}</td>
                            <td>${o.transact_time || '-'}</td>
                        </tr>`).join('');
                })
                .catch(err => { tbody.innerHTML = '<tr><td colspan="9" style="text-align:center;color:#ef4444;padding:20px">Error: '+err.message+'</td></tr>'; });
        }

        // ── POSITIONS tab ─────────────────────────────────────────────────────────────
        function loadPositions() {
            const tbody = document.getElementById('positionsTable');
            const status = document.getElementById('positionsStatus');
            tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;padding:20px">Loading…</td></tr>';
            fetch('/api/positions')
                .then(r => r.json())
                .then(data => {
                    const positions = data.positions || [];
                    status.textContent = 'Last updated: ' + new Date().toLocaleTimeString();
                    if (!positions.length) {
                        tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;padding:20px;color:#9ca3af">No positions — place orders to see fills here</td></tr>';
                        return;
                    }
                    tbody.innerHTML = positions.map(p => {
                        const totalPnl = (p.unrealized_pnl || 0) + (p.realized_pnl || 0);
                        const pnlColor = v => v > 0 ? '#10b981' : v < 0 ? '#ef4444' : '#9ca3af';
                        const sideColor = p.net_qty > 0 ? '#10b981' : p.net_qty < 0 ? '#ef4444' : '#9ca3af';
                        const fmt = v => v === undefined || v === null ? '-' : Number(v).toFixed(6);
                        const fmtPnl = v => v === undefined || v === null ? '-' : (v >= 0 ? '+' : '') + Number(v).toFixed(4);
                        return `<tr>
                            <td style="font-weight:bold;color:#ff6b35">${p.symbol}</td>
                            <td style="color:${sideColor}">${fmt(p.net_qty)}</td>
                            <td>${fmt(p.avg_entry_px)}</td>
                            <td>${fmt(p.last_mid_px)}</td>
                            <td style="color:${pnlColor(p.unrealized_pnl)}">${fmtPnl(p.unrealized_pnl)}</td>
                            <td style="color:${pnlColor(p.realized_pnl)}">${fmtPnl(p.realized_pnl)}</td>
                            <td style="color:${pnlColor(totalPnl)};font-weight:bold">${fmtPnl(totalPnl)}</td>
                            <td>${p.trade_count}</td>
                        </tr>`;
                    }).join('');
                })
                .catch(err => { tbody.innerHTML = '<tr><td colspan="8" style="text-align:center;color:#ef4444;padding:20px">Error: '+err.message+'</td></tr>'; });
        }

        // ── ALGO tab ──────────────────────────────────────────────────────────────────
        let _algoCfgCache = null;

        function loadAlgoStatus() {
            fetch('/api/algo/status')
                .then(r => r.json())
                .then(data => {
                    const cfg = data.config || {};
                    _algoCfgCache = cfg;
                    const banner = document.getElementById('algoStatusBanner');
                    if (cfg.enabled) {
                        banner.style.background = 'rgba(34,197,94,0.15)';
                        banner.style.color = '#22c55e';
                        banner.textContent = '● ALGO ENABLED — trading ' + (cfg.symbol || '') + ' | qty=' + (cfg.order_qty || '') + ' | min_spread=' + (cfg.min_spread_bps || '') + 'bps';
                    } else {
                        banner.style.background = 'rgba(75,85,99,0.3)';
                        banner.style.color = '#9ca3af';
                        banner.textContent = '○ ALGO DISABLED — configure and enable to start trading';
                    }
                    // Populate config fields
                    document.getElementById('algoCfgSymbol').value = cfg.symbol || '';
                    document.getElementById('algoCfgQty').value = cfg.order_qty || '';
                    document.getElementById('algoCfgSpread').value = cfg.min_spread_bps || '';
                    document.getElementById('algoCfgMaxPos').value = cfg.max_position || '';
                    document.getElementById('algoCfgPoll').value = cfg.poll_interval_ms || '';
                    document.getElementById('algoCfgTIF').value = cfg.time_in_force || '';
                    // Counters
                    document.getElementById('algoSigCount').textContent = data.signals_fired || 0;
                    document.getElementById('algoOrdCount').textContent = data.orders_placed || 0;
                    // Signal log
                    const signals = data.recent_signals || [];
                    const logBody = document.getElementById('algoSignalLog');
                    if (!signals.length) {
                        logBody.innerHTML = '<tr><td colspan="6" style="text-align:center;padding:20px;color:#9ca3af">No signals yet</td></tr>';
                    } else {
                        logBody.innerHTML = signals.map(s => {
                            const t = new Date(s.timestamp_ms).toLocaleTimeString();
                            const sideLabel = s.side === '1' ? '<span style="color:#10b981">BUY</span>' : '<span style="color:#ef4444">SELL</span>';
                            return `<tr>
                                <td>${t}</td>
                                <td>${s.symbol}</td>
                                <td>${sideLabel}</td>
                                <td>${Number(s.price).toFixed(4)}</td>
                                <td>${s.qty}</td>
                                <td style="font-size:11px;max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${s.reason}">${s.reason}</td>
                            </tr>`;
                        }).join('');
                    }
                })
                .catch(err => { document.getElementById('algoStatusBanner').textContent = 'Error loading algo status: ' + err.message; });
        }

        function algoEnable() {
            fetch('/api/algo/enable', {method:'POST'}).then(() => loadAlgoStatus());
        }

        function algoDisable() {
            fetch('/api/algo/disable', {method:'POST'}).then(() => loadAlgoStatus());
        }

        function algoSaveConfig() {
            const msg = document.getElementById('algoCfgMsg');
            const cfg = Object.assign({}, _algoCfgCache || {}, {
                symbol:          document.getElementById('algoCfgSymbol').value,
                order_qty:       parseFloat(document.getElementById('algoCfgQty').value),
                min_spread_bps:  parseFloat(document.getElementById('algoCfgSpread').value),
                max_position:    parseFloat(document.getElementById('algoCfgMaxPos').value),
                poll_interval_ms:parseInt(document.getElementById('algoCfgPoll').value),
                time_in_force:   document.getElementById('algoCfgTIF').value,
            });
            fetch('/api/algo/config', {method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(cfg)})
                .then(r => r.json())
                .then(() => { msg.textContent = 'Saved ✓'; msg.style.color = '#22c55e'; setTimeout(() => { msg.textContent=''; }, 3000); loadAlgoStatus(); })
                .catch(err => { msg.textContent = 'Error: ' + err.message; msg.style.color = '#ef4444'; });
        }

        // ── BACKTEST tab ──────────────────────────────────────────────────────────────
        let btPriceChartInst = null;
        let btEquityChartInst = null;

        function initBacktestSymbols() {
            // Populate symbol selector from same source as other selectors
            const sel = document.getElementById('btSymbol');
            if (sel.options.length > 0) return; // already populated
            fetch('/api/symbols')
                .then(r => r.json())
                .then(data => {
                    const syms = data.symbols || [];
                    syms.forEach(s => {
                        const o = document.createElement('option');
                        o.value = s; o.textContent = s;
                        sel.appendChild(o);
                    });
                    if (!sel.value && syms.length) sel.value = syms[0];
                })
                .catch(() => {});
        }

        function runBacktest() {
            const btn = document.getElementById('btRunBtn');
            const status = document.getElementById('btStatus');
            const symbol   = document.getElementById('btSymbol').value;
            const limit    = document.getElementById('btLimit').value || 5000;
            const spread   = document.getElementById('btSpread').value || 5;
            const qty      = document.getElementById('btQty').value || 0.001;
            const maxPos   = document.getElementById('btMaxPos').value || 0.01;
            const tick     = document.getElementById('btTick').value || 0.01;

            if (!symbol) { status.textContent = 'Select a symbol first.'; return; }

            btn.disabled = true;
            btn.textContent = 'Running…';
            status.textContent = 'Fetching data and simulating…';
            document.getElementById('btSummary').style.display = 'none';

            const url = `/api/backtest?symbol=${encodeURIComponent(symbol)}&limit=${limit}&min_spread_bps=${spread}&order_qty=${qty}&max_position=${maxPos}&tick_size=${tick}`;

            fetch(url)
                .then(r => r.json())
                .then(data => {
                    btn.disabled = false;
                    btn.textContent = 'Run Backtest';
                    if (data.error) { status.textContent = 'Error: ' + data.error; return; }
                    renderBacktestResults(data, symbol);
                })
                .catch(err => {
                    btn.disabled = false;
                    btn.textContent = 'Run Backtest';
                    status.textContent = 'Error: ' + err.message;
                });
        }

        function renderBacktestResults(data, symbol) {
            const s = data.summary || {};
            const status = document.getElementById('btStatus');
            status.textContent = `Processed ${s.snapshots_processed || 0} snapshots for ${symbol}`;
            document.getElementById('btSummary').style.display = 'block';

            // Summary cards
            const pnlColor = v => v > 0 ? '#10b981' : v < 0 ? '#ef4444' : '#9ca3af';
            const fmtPnl = v => (v >= 0 ? '+' : '') + Number(v).toFixed(6);
            document.getElementById('btStatPnl').textContent = fmtPnl(s.total_pnl || 0);
            document.getElementById('btStatPnl').style.color = pnlColor(s.total_pnl || 0);
            document.getElementById('btStatTrades').textContent = s.total_trades || 0;
            document.getElementById('btStatWinRate').textContent = (s.win_rate_pct || 0).toFixed(1) + '%';
            document.getElementById('btStatDrawdown').textContent = fmtPnl(-(s.max_drawdown || 0));
            document.getElementById('btStatDrawdown').style.color = '#ef4444';
            document.getElementById('btStatSharpe').textContent = (s.sharpe_ratio || 0).toFixed(3);
            document.getElementById('btStatSharpe').style.color = pnlColor(s.sharpe_ratio || 0);
            document.getElementById('btStatPos').textContent = Number(s.final_position || 0).toFixed(6);

            // Trade log
            const trades = data.trades || [];
            document.getElementById('btTradeCount').textContent = `(${trades.length} trades shown)`;
            const logBody = document.getElementById('btTradeLog');
            if (!trades.length) {
                logBody.innerHTML = '<tr><td colspan="7" style="text-align:center;padding:20px;color:#9ca3af">No trades fired — try lowering min spread or increasing snapshots</td></tr>';
            } else {
                // Show last 200 trades to keep table fast
                const display = trades.slice(-200);
                logBody.innerHTML = display.map(t => {
                    const dt = new Date(t.timestamp_ms).toLocaleString();
                    const sideLabel = t.side === '1'
                        ? '<span style="color:#10b981">BUY</span>'
                        : '<span style="color:#ef4444">SELL</span>';
                    const pnlStr = t.pnl !== 0 ? fmtPnl(t.pnl) : '-';
                    const pnlStyle = t.pnl > 0 ? 'color:#10b981' : t.pnl < 0 ? 'color:#ef4444' : '';
                    return `<tr>
                        <td style="white-space:nowrap">${dt}</td>
                        <td>${sideLabel}</td>
                        <td>${Number(t.price).toFixed(4)}</td>
                        <td>${t.qty}</td>
                        <td style="${pnlStyle}">${pnlStr}</td>
                        <td style="${pnlColor(t.cumulative_pnl) !== '#9ca3af' ? 'color:'+pnlColor(t.cumulative_pnl) : ''}">${fmtPnl(t.cumulative_pnl)}</td>
                        <td>${Number(t.net_qty_after).toFixed(6)}</td>
                    </tr>`;
                }).join('');
            }

            // ── Price + signals chart ──────────────────────────────────────────
            const priceSeries = data.price_series || [];
            const priceLabels = priceSeries.map(p => new Date(p[0]).toLocaleTimeString());
            const bidData  = priceSeries.map(p => p[1]);
            const askData  = priceSeries.map(p => p[2]);

            // Create scatter datasets for buy/sell signals
            // Map each trade to the nearest price series index for x positioning
            const buyPoints  = [];
            const sellPoints = [];
            trades.forEach(t => {
                const label = new Date(t.timestamp_ms).toLocaleTimeString();
                const point = { x: label, y: t.price };
                if (t.side === '1') buyPoints.push(point);
                else sellPoints.push(point);
            });

            if (btPriceChartInst) btPriceChartInst.destroy();
            btPriceChartInst = new Chart(document.getElementById('btPriceChart'), {
                type: 'line',
                data: {
                    labels: priceLabels,
                    datasets: [
                        {
                            label: 'Best Bid',
                            data: bidData,
                            borderColor: '#10b981',
                            backgroundColor: 'rgba(16,185,129,0.05)',
                            tension: 0.2,
                            pointRadius: 0,
                            borderWidth: 1.5,
                        },
                        {
                            label: 'Best Ask',
                            data: askData,
                            borderColor: '#ef4444',
                            backgroundColor: 'rgba(239,68,68,0.05)',
                            tension: 0.2,
                            pointRadius: 0,
                            borderWidth: 1.5,
                        },
                        {
                            label: 'Buy',
                            data: buyPoints,
                            type: 'scatter',
                            borderColor: '#10b981',
                            backgroundColor: '#10b981',
                            pointStyle: 'triangle',
                            pointRadius: 6,
                            showLine: false,
                        },
                        {
                            label: 'Sell',
                            data: sellPoints,
                            type: 'scatter',
                            borderColor: '#ef4444',
                            backgroundColor: '#ef4444',
                            pointStyle: 'triangle',
                            rotation: 180,
                            pointRadius: 6,
                            showLine: false,
                        },
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    interaction: { mode: 'index', intersect: false },
                    plugins: { legend: { labels: { color: '#e5e7eb' } } },
                    scales: {
                        x: { ticks: { color: '#9ca3af', maxTicksLimit: 10 }, grid: { color: 'rgba(255,255,255,0.05)' } },
                        y: { ticks: { color: '#9ca3af' }, grid: { color: 'rgba(255,255,255,0.05)' } }
                    }
                }
            });

            // ── Equity curve chart ────────────────────────────────────────────
            const equityCurve = data.equity_curve || [];
            const eqLabels = equityCurve.map(p => new Date(p[0]).toLocaleTimeString());
            const eqData   = equityCurve.map(p => p[1]);

            if (btEquityChartInst) btEquityChartInst.destroy();
            btEquityChartInst = new Chart(document.getElementById('btEquityChart'), {
                type: 'line',
                data: {
                    labels: eqLabels,
                    datasets: [{
                        label: 'Cumulative P&L',
                        data: eqData,
                        borderColor: '#a78bfa',
                        backgroundColor: 'rgba(167,139,250,0.08)',
                        tension: 0.3,
                        pointRadius: 0,
                        fill: true,
                        borderWidth: 2,
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { labels: { color: '#e5e7eb' } } },
                    scales: {
                        x: { ticks: { color: '#9ca3af', maxTicksLimit: 10 }, grid: { color: 'rgba(255,255,255,0.05)' } },
                        y: { ticks: { color: '#9ca3af' }, grid: { color: 'rgba(255,255,255,0.05)' } }
                    }
                }
            });
        }

        // ── SECURITIES tab ───────────────────────────────────────────────────────────
        function loadSecurities() {
            const tbody = document.getElementById('securitiesTable');
            tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;padding:20px">Loading…</td></tr>';
            fetch('/api/securities')
                .then(r => r.json())
                .then(data => {
                    allSecurities = data.securities || [];
                    renderSecurities(allSecurities);
                })
                .catch(err => { tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;color:#ef4444;padding:20px">Error: '+err.message+'</td></tr>'; });
        }

        function renderSecurities(list) {
            const tbody = document.getElementById('securitiesTable');
            if (!list.length) {
                tbody.innerHTML = '<tr><td colspan="4" style="text-align:center;padding:20px">No securities found</td></tr>';
                return;
            }
            tbody.innerHTML = list.map(s => `
                <tr>
                    <td style="font-weight:bold;color:#ff6b35">${s.symbol || ''}</td>
                    <td>${s.security_id || ''}</td>
                    <td>${s.exchange || ''}</td>
                    <td style="font-size:11px;max-width:400px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${(s.attributes_json||'').replace(/"/g,'&quot;')}">${s.attributes_json || ''}</td>
                </tr>`).join('');
        }

        function filterSecurities() {
            const q = document.getElementById('secSearch').value.toLowerCase();
            renderSecurities(q ? allSecurities.filter(s => (s.symbol||'').toLowerCase().includes(q)) : allSecurities);
        }

        // ── boot ─────────────────────────────────────────────────────────────────────
        updateTotalMarkets();
        updateOrderBook();
        setInterval(updateTotalMarkets, 5000);
        updateInterval = setInterval(updateOrderBook, 500);
    </script>
</body>
</html>
"#;

    // Shared state for API endpoints
    #[derive(Clone)]
    struct AppState {
        order_session: Arc<FIXSession>,  // Order management session (has open_orders)
        market_data_session: Arc<FIXSession>,  // Market data session (has order books)
        market_data_sessions: Vec<(Arc<FIXSession>, String, u16)>,
        all_symbols: Vec<String>,  // All symbols to subscribe to
        kdb: Arc<KdbWriter>,  // kdb+ connection for historical queries
        algo: Arc<algo::TradingAlgo>,  // Trading algorithm
    }
    
    let all_symbols_list = vec![
        "BTC_USDT".to_string(),
        "BTC_USD".to_string(),
        "BTC_EUR".to_string(),
        "SOL_BTC".to_string(),
        "XRP_BTC".to_string(),
        "DOGE_BTC".to_string(),
        "ADA_BTC".to_string(),
        "LINK_BTC".to_string(),
        "AVAX_BTC".to_string(),
        "LTC_BTC".to_string(),
        "ATOM_BTC".to_string(),
        "ALGO_BTC".to_string(),
    ];
    
    let app_state = AppState {
        order_session: Arc::clone(&order_session),
        market_data_session: Arc::clone(&market_data_session),
        market_data_sessions: market_data_sessions.clone(),
        all_symbols: all_symbols_list.clone(),
        kdb: Arc::clone(&kdb_writer),
        algo: Arc::clone(&algo),
    };
    
    // API endpoint to get order book data
    async fn get_order_book_api(
        AxumPath(symbol): AxumPath<String>,
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> Result<Json<serde_json::Value>, StatusCode> {
        // Get order book snapshot from market data session (order books are stored there)
        let snapshot = state.market_data_session.get_order_book_snapshot(&symbol).await;
        
        if let Some(order_book) = snapshot {
            let best_bid = order_book.best_bid;
            let best_ask = order_book.best_ask;
            let spread = order_book.spread();
            
            // Get depth (top 20 levels)
            let bid_levels = order_book.get_depth(Side::Bid, 20);
            let ask_levels = order_book.get_depth(Side::Ask, 20);
            
            // Get recent trades
            let recent_trades = order_book.get_recent_trades(50);
            
            let response = serde_json::json!({
                "symbol": symbol,
                "best_bid": best_bid,
                "best_ask": best_ask,
                "spread": spread,
                "bids": bid_levels.iter().map(|(p, s)| {
                    serde_json::json!({"price": p, "size": s})
                }).collect::<Vec<_>>(),
                "asks": ask_levels.iter().map(|(p, s)| {
                    serde_json::json!({"price": p, "size": s})
                }).collect::<Vec<_>>(),
                "trades": recent_trades.iter().map(|(p, s, ts)| {
                    serde_json::json!({"price": p, "size": s, "timestamp": ts})
                }).collect::<Vec<_>>(),
                "trade_count": order_book.trade_count(),
                "timestamp": chrono::Utc::now().timestamp_millis(),
            });
            
            Ok(Json(response))
        } else {
            // Return empty order book
            let response = serde_json::json!({
                "symbol": symbol,
                "best_bid": 0.0,
                "best_ask": 0.0,
                "spread": 0.0,
                "bids": [],
                "asks": [],
                "trades": [],
                "trade_count": 0,
                "timestamp": chrono::Utc::now().timestamp_millis(),
            });
            Ok(Json(response))
        }
    }
    
    // API endpoint to list available symbols
    async fn list_symbols_api(
        axum::extract::State(_state): axum::extract::State<AppState>,
    ) -> Result<Json<serde_json::Value>, StatusCode> {
        // Return all subscribed symbols (even if they haven't received data yet)
        // This ensures the web interface shows all available markets
        let subscribed_symbols = vec![
            "BTC_USDT".to_string(),
            "BTC_USD".to_string(),
            "BTC_EUR".to_string(),
            "SOL_BTC".to_string(),
            "XRP_BTC".to_string(),
            "DOGE_BTC".to_string(),
            "ADA_BTC".to_string(),
            "LINK_BTC".to_string(),
            "AVAX_BTC".to_string(),
            "LTC_BTC".to_string(),
            "ATOM_BTC".to_string(),
            "ALGO_BTC".to_string(),
        ];
        
        Ok(Json(serde_json::json!({
            "symbols": subscribed_symbols
        })))
    }
    
    // API endpoint to get open orders (for console)
    async fn get_open_orders_api(
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> Result<Json<serde_json::Value>, StatusCode> {
        let open_orders = state.order_session.open_orders.get_open_orders();
        let orders_json: Vec<serde_json::Value> = open_orders.iter().map(|order| {
            serde_json::json!({
                "cl_ord_id": order.cl_ord_id,
                "order_id": order.order_id,
                "exec_id": order.exec_id,
                "symbol": order.symbol,
                "side": order.side,
                "order_qty": order.order_qty,
                "price": order.price,
                "ord_status": order.ord_status,
                "exec_type": order.exec_type,
                "cum_qty": order.cum_qty,
                "leaves_qty": order.leaves_qty,
                "last_px": order.last_px,
                "last_qty": order.last_qty,
                "avg_px": order.avg_px,
                "transact_time": order.transact_time,
                "order_create_time": order.order_create_time,
                "rejection_reason": order.rejection_reason,
            })
        }).collect();
        
        Ok(Json(serde_json::json!({
            "orders": orders_json,
            "count": open_orders.len()
        })))
    }
    
    // API endpoint to get all market data server statuses (for console)
    async fn get_all_market_data_status_api(
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> Result<Json<serde_json::Value>, StatusCode> {
        let mut statuses = Vec::new();
        
        for (session, description, port) in &state.market_data_sessions {
                    let is_connected = *session.is_connected.lock().await;
                    let connection_state = *session.connection_state.lock().await;
                    let is_ready = session.is_ready().await;
                    let seq_num = *session.seq_num.lock().await;
                    
                    let order_books = session.order_books.lock().await;
                    let symbol_count = order_books.len();
                    let total_trades: usize = order_books.values()
                        .map(|book| book.get_active().trade_count())
                        .sum();
                    let symbols: Vec<String> = order_books.keys().cloned().collect();
                    drop(order_books);
                    
                    let securities = session.securities.lock().await;
                    let securities_count = securities.len();
                    drop(securities);
                    
                    let state_str = match connection_state {
                        ConnectionState::Disconnected => "DISCONNECTED",
                        ConnectionState::Connecting => "CONNECTING",
                        ConnectionState::Connected => "CONNECTED",
                        ConnectionState::Logging => "LOGGING_IN",
                        ConnectionState::Ready => "READY",
                        ConnectionState::Disconnecting => "DISCONNECTING",
                    };
                    
                    statuses.push(serde_json::json!({
                        "description": description,
                        "port": port,
                        "sender_comp_id": session.sender_comp_id,
                        "target_comp_id": session.target_comp_id,
                        "is_connected": is_connected,
                        "is_ready": is_ready,
                        "connection_state": state_str,
                        "sequence_number": seq_num,
                        "order_books": symbol_count,
                        "total_trades": total_trades,
                        "symbols": symbols,
                        "securities_count": securities_count,
                    }));
                }
                
        Ok(Json(serde_json::json!({
            "servers": statuses
        })))
    }
    
    // API endpoint to subscribe to all symbols on a specific server
    async fn subscribe_all_symbols_api(
        AxumPath(port_str): AxumPath<String>,
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> Result<Json<serde_json::Value>, StatusCode> {
        let port: u16 = match port_str.parse() {
            Ok(p) => p,
            Err(_) => {
                return Ok(Json(serde_json::json!({
                    "error": format!("Invalid port: {}", port_str)
                })));
            }
        };
        
        // Find the session for this port
        let session_opt = state.market_data_sessions.iter()
            .find(|(_, _, p)| *p == port)
            .map(|(s, _, _)| Arc::clone(s));
        
        if let Some(session) = session_opt {
            if !session.is_ready().await {
                return Ok(Json(serde_json::json!({
                    "error": format!("Server on port {} is not ready", port)
                })));
            }
            
            let mut subscribed = Vec::new();
            let mut failed = Vec::new();
            
            // Subscribe to all symbols
            for symbol in &state.all_symbols {
                match session.market_data_request(symbol, 50).await {
                    Ok(_) => {
                        subscribed.push(symbol.clone());
                        // Small delay between requests
                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    }
                    Err(e) => {
                        failed.push(serde_json::json!([symbol.clone(), e.to_string()]));
                    }
                }
            }
            
            Ok(Json(serde_json::json!({
                "port": port,
                "subscribed": subscribed.len(),
                "failed": failed.len(),
                "subscribed_symbols": subscribed,
                "failed_symbols": failed
            })))
        } else {
            Ok(Json(serde_json::json!({
                "error": format!("No server found on port {}", port)
            })))
        }
    }
    
    fn escape_kdb_string(value: &str) -> String {
        value.replace('\\', "\\\\").replace('"', "\\\"")
    }

    // Build a WHERE predicate that safely handles symbols with punctuation by
    // constructing a symbol atom via `$"..."` instead of raw backtick literals.
    fn build_kdb_symbol_predicate(symbol: &str) -> String {
        format!("symbol=`$\"{}\"", escape_kdb_string(symbol))
    }

    // GET /api/history/:symbol — last 500 best-bid/ask snapshots for a symbol (from kdb+)
    async fn get_history_api(
        AxumPath(symbol): AxumPath<String>,
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> impl axum::response::IntoResponse {
        use axum::http::header;
        // Use binary IPC to avoid .j.j 'type errors with timestamp$ columns.
        // The K table is converted to JSON on the Rust side; "time" comes out as Unix ms integer.
        let symbol_predicate = build_kdb_symbol_predicate(&symbol);
        let q = format!(
            "neg[500] sublist select from orderbook_snapshots where {}",
            symbol_predicate
        );
        match state.kdb.query_table_to_json(&q).await {
            Ok(json) => (StatusCode::OK, [(header::CONTENT_TYPE, "application/json")], json),
            Err(e) => {
                let payload = serde_json::json!({
                    "error": format!("kdb+ query failed: {}", e),
                    "context": {
                        "symbol": symbol,
                        "where": symbol_predicate
                    }
                });
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    [(header::CONTENT_TYPE, "application/json")],
                    payload.to_string(),
                )
            }
        }
    }

    // GET /api/securities — all securities from kdb+
    async fn get_securities_api(
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> impl axum::response::IntoResponse {
        use axum::http::header;
        // Deduplicate: keep only the last record per symbol
        let q = "select last time, last security_id, last exchange, last attributes_json by symbol from securities";
        match state.kdb.query_json(q).await {
            Ok(raw_json) => {
                // .j.j on a keyed table returns an object; unwrap into array of records
                let securities_json = match serde_json::from_str::<serde_json::Value>(&raw_json) {
                    Ok(v) => {
                        // Keyed table result: object whose keys are symbols and values are row objects
                        let arr: Vec<serde_json::Value> = if let Some(obj) = v.as_object() {
                            obj.iter().map(|(sym, row)| {
                                let mut rec = row.clone();
                                if let Some(m) = rec.as_object_mut() {
                                    m.insert("symbol".to_string(), serde_json::Value::String(sym.clone()));
                                }
                                rec
                            }).collect()
                        } else if let Some(arr) = v.as_array() {
                            arr.clone()
                        } else {
                            vec![v]
                        };
                        serde_json::json!({ "securities": arr, "count": arr.len() })
                    }
                    Err(_) => serde_json::json!({ "securities": [], "error": raw_json }),
                };
                (StatusCode::OK, [(header::CONTENT_TYPE, "application/json")],
                 securities_json.to_string())
            }
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, [(header::CONTENT_TYPE, "application/json")],
                       format!("{{\"error\":\"{}\"}}", e)),
        }
    }

    // GET /api/orders/history — execution reports from orders/processed/*.json
    async fn get_orders_history_api(
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> Result<Json<serde_json::Value>, StatusCode> {
        // Fall back to open orders store for current orders
        let open_orders = state.order_session.open_orders.get_open_orders();
        let mut orders: Vec<serde_json::Value> = open_orders.iter().map(|o| serde_json::json!({
            "cl_ord_id": o.cl_ord_id, "order_id": o.order_id, "symbol": o.symbol,
            "side": o.side, "order_qty": o.order_qty, "price": o.price,
            "ord_status": o.ord_status, "exec_type": o.exec_type,
            "cum_qty": o.cum_qty, "leaves_qty": o.leaves_qty,
            "avg_px": o.avg_px, "transact_time": o.transact_time,
            "order_create_time": o.order_create_time, "rejection_reason": o.rejection_reason,
        })).collect();

        // Also load saved JSON files from orders/processed/
        if let Ok(mut dir) = tokio::fs::read_dir("orders/processed").await {
            while let Ok(Some(entry)) = dir.next_entry().await {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) == Some("json") {
                    if let Ok(contents) = tokio::fs::read_to_string(&path).await {
                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&contents) {
                            // Extract the execution_report section if present
                            let record = v.get("execution_report").cloned().unwrap_or(v);
                            orders.push(record);
                        }
                    }
                }
            }
        }

        Ok(Json(serde_json::json!({ "orders": orders, "count": orders.len() })))
    }

    // GET /api/positions — live position snapshot with unrealized P&L from order book
    async fn get_positions_api(
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> Result<Json<serde_json::Value>, StatusCode> {
        let mut positions = state.order_session.positions.snapshot();

        // Refresh unrealized P&L from live order book mid prices
        {
            let books = state.market_data_session.order_books.lock().await;
            for pos in positions.iter_mut() {
                if let Some(db) = books.get(&pos.symbol) {
                    let snap = db.get_snapshot();
                    let mid = snap.mid_price();
                    pos.refresh_unrealized(mid);
                }
            }
        }

        // Sort by absolute net_qty descending
        positions.sort_by(|a, b| b.net_qty.abs().partial_cmp(&a.net_qty.abs()).unwrap_or(std::cmp::Ordering::Equal));

        let positions_json: Vec<serde_json::Value> = positions.iter().map(|p| {
            serde_json::json!({
                "symbol": p.symbol,
                "net_qty": p.net_qty,
                "avg_entry_px": p.avg_entry_px,
                "realized_pnl": p.realized_pnl,
                "unrealized_pnl": p.unrealized_pnl,
                "last_mid_px": p.last_mid_px,
                "trade_count": p.trade_count
            })
        }).collect();

        Ok(Json(serde_json::json!({
            "positions": positions_json,
            "count": positions_json.len()
        })))
    }

    // GET /api/algo/status — current algo configuration and stats
    async fn get_algo_status_api(
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> Json<serde_json::Value> {
        let status = state.algo.status().await;
        Json(serde_json::to_value(&status).unwrap_or_default())
    }

    // POST /api/algo/enable — enable the trading algo
    async fn algo_enable_api(
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> Json<serde_json::Value> {
        let mut cfg = state.algo.config.lock().await.clone();
        cfg.enabled = true;
        state.algo.update_config(cfg).await;
        Json(serde_json::json!({"ok": true, "enabled": true}))
    }

    // POST /api/algo/disable — disable the trading algo
    async fn algo_disable_api(
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> Json<serde_json::Value> {
        let mut cfg = state.algo.config.lock().await.clone();
        cfg.enabled = false;
        state.algo.update_config(cfg).await;
        Json(serde_json::json!({"ok": true, "enabled": false}))
    }

    // POST /api/algo/config — update algo config fields
    async fn algo_config_api(
        axum::extract::State(state): axum::extract::State<AppState>,
        Json(new_cfg): Json<algo::AlgoConfig>,
    ) -> Json<serde_json::Value> {
        state.algo.update_config(new_cfg).await;
        Json(serde_json::json!({"ok": true}))
    }

    // Load historical snapshots from data/snapshots.csv.
    // Used as fallback when KDB+ has no in-memory snapshot data.
    // CSV columns: timestamp_ms,symbol,best_bid,best_ask
    fn load_snapshots_from_csv(symbol: &str, limit: usize) -> Vec<algo::Snapshot> {
        use std::io::BufRead;
        let file = match std::fs::File::open("data/snapshots.csv") {
            Ok(f) => f,
            Err(_) => return Vec::new(),
        };
        let reader = std::io::BufReader::new(file);
        let mut snapshots: Vec<algo::Snapshot> = Vec::new();
        let mut first_line = true;

        for line in reader.lines().flatten() {
            if first_line {
                first_line = false;
                continue; // skip header
            }
            let cols: Vec<&str> = line.splitn(4, ',').collect();
            if cols.len() < 4 {
                continue;
            }
            if cols[1] != symbol {
                continue;
            }
            let timestamp_ms: i64 = cols[0].parse().unwrap_or(0);
            let best_bid: f64   = cols[2].parse().unwrap_or(0.0);
            let best_ask: f64   = cols[3].parse().unwrap_or(0.0);
            if best_bid > 0.0 && best_ask > 0.0 && best_ask > best_bid {
                snapshots.push(algo::Snapshot { timestamp_ms, best_bid, best_ask });
            }
        }

        // Return the most recent `limit` rows (CSV is chronological).
        if snapshots.len() > limit {
            snapshots.drain(..snapshots.len() - limit);
        }
        snapshots
    }

    // GET /api/backtest — run backtest simulation against historical kdb+ snapshots
    // Query params: symbol, limit, min_spread_bps, order_qty, max_position, tick_size
    async fn get_backtest_api(
        axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> impl axum::response::IntoResponse {
        use axum::http::header;

        let symbol = params.get("symbol").cloned().unwrap_or_else(|| "BTC_USDT".to_string());
        let limit: usize = params
            .get("limit")
            .and_then(|v| v.parse().ok())
            .unwrap_or(5000)
            .clamp(1, 100_000);
        let min_spread_bps: f64 = params.get("min_spread_bps").and_then(|v| v.parse().ok()).unwrap_or(5.0);
        let order_qty: f64 = params.get("order_qty").and_then(|v| v.parse().ok()).unwrap_or(0.001);
        let max_position: f64 = params.get("max_position").and_then(|v| v.parse().ok()).unwrap_or(0.01);
        let tick_size: f64 = params.get("tick_size").and_then(|v| v.parse().ok()).unwrap_or(0.01);

        // Build an AlgoConfig for this backtest run (enabled flag doesn't matter here)
        let cfg = algo::AlgoConfig {
            enabled: false,
            symbol: symbol.clone(),
            order_qty,
            min_spread_bps,
            max_position,
            poll_interval_ms: 1000,
            tick_size,
            time_in_force: "3".to_string(),
        };

        // Query kdb+ for historical snapshots. On any error or empty result, fall back
        // to data/ticks.csv so backtests work even when kdb+ has no in-memory data.
        let symbol_predicate = build_kdb_symbol_predicate(&symbol);
        let q = format!(
            "neg[{}] sublist select time, best_bid, best_ask from orderbook_snapshots where {}",
            limit, symbol_predicate
        );
        let kdb_snapshots: Vec<algo::Snapshot> = match state.kdb.query_table_to_json(&q).await {
            Err(e) => {
                info!("📊 kdb+ query error ({}), using CSV fallback", e);
                Vec::new()
            }
            Ok(raw_json) => {
                match serde_json::from_str::<serde_json::Value>(&raw_json) {
                    Err(_) => Vec::new(),
                    Ok(parsed) => {
                        match parsed.as_array() {
                            None => Vec::new(),
                            Some(rows) => rows.iter().filter_map(|row| {
                                let timestamp_ms = row.get("time").and_then(|v| v.as_i64()).unwrap_or(0);
                                let best_bid = row.get("best_bid").and_then(|v| v.as_f64()).unwrap_or(0.0);
                                let best_ask = row.get("best_ask").and_then(|v| v.as_f64()).unwrap_or(0.0);
                                if best_bid > 0.0 && best_ask > 0.0 {
                                    Some(algo::Snapshot { timestamp_ms, best_bid, best_ask })
                                } else {
                                    None
                                }
                            }).collect()
                        }
                    }
                }
            }
        };

        // If KDB+ returned nothing (empty table or error), fall back to data/ticks.csv.
        let (snapshots, data_source) = if kdb_snapshots.is_empty() {
            let csv_snaps = load_snapshots_from_csv(&symbol, limit);
            (csv_snaps, "csv")
        } else {
            (kdb_snapshots, "kdb")
        };

        if snapshots.is_empty() {
            return (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                serde_json::json!({
                    "error": "No snapshot data found for this symbol",
                    "hint": "Run the server connected to live market data to accumulate snapshots. Data is auto-saved to data/ticks.csv every 60 seconds."
                }).to_string(),
            );
        }

        // Run the simulation
        info!("📊 Backtest: {} snapshots from {} for {}", snapshots.len(), data_source, symbol);
        let result = algo::run_backtest(&cfg, &snapshots);

        match serde_json::to_string(&result) {
            Ok(json) => (StatusCode::OK, [(header::CONTENT_TYPE, "application/json")], json),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(header::CONTENT_TYPE, "application/json")],
                format!("{{\"error\":\"Serialization failed: {}\"}}", e),
            ),
        }
    }

    // Build the router
    let html_page_clone = html_page.to_string();
    let app = Router::new()
        .route("/", get(move || {
            let html = html_page_clone.clone();
            async move {
                let mut headers = axum::http::HeaderMap::new();
                headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store, no-cache, must-revalidate"));
                (headers, Html(html))
            }
        }))
        .route("/api/orderbook/:symbol", get(get_order_book_api))
        .route("/api/symbols", get(list_symbols_api))
        .route("/api/open-orders", get(get_open_orders_api))
        .route("/api/market-data-status", get(get_all_market_data_status_api))
        .route("/api/subscribe-all-symbols/:port", post(subscribe_all_symbols_api))
        .route("/api/history/:symbol", get(get_history_api))
        .route("/api/securities", get(get_securities_api))
        .route("/api/orders/history", get(get_orders_history_api))
        .route("/api/positions", get(get_positions_api))
        .route("/api/algo/status", get(get_algo_status_api))
        .route("/api/algo/enable", post(algo_enable_api))
        .route("/api/algo/disable", post(algo_disable_api))
        .route("/api/algo/config", post(algo_config_api))
        .route("/api/backtest", get(get_backtest_api))
        .layer(CorsLayer::permissive())
        .with_state(app_state);
    
    // Parse bind address
    use std::net::SocketAddr;
    let addr: SocketAddr = bind_addr.parse().map_err(|e| anyhow!("Invalid bind address {}: {}", bind_addr, e))?;
    
    info!("🌐 Order Book Web Server starting...");
    info!("   Bind address: http://{}", bind_addr);
    info!("   Open http://{} in your browser to view the order book", addr);
    
    let listener = tokio::net::TcpListener::bind(&addr).await
        .map_err(|e| anyhow!("Failed to bind to {}: {}", addr, e))?;
    
    axum::serve(listener, app).await
        .map_err(|e| anyhow!("Web server error: {}", e))?;
    
    Ok(())
}

/// Management Console - Interactive menu for server management
async fn management_console(
    session: Arc<FIXSession>,
    market_data_sessions: Vec<(Arc<FIXSession>, String, u16)>, // (session, description, port)
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
) -> Result<()> {
    use tokio::io::{AsyncBufReadExt, BufReader};
    
    let stdin = tokio::io::stdin();
    let reader = BufReader::new(stdin);
    let mut lines = reader.lines();
    
    loop {
        // Clear screen and display menu
        print!("\x1B[2J\x1B[1;1H"); // ANSI escape codes to clear screen and move cursor to top
        println!("╔════════════════════════════════════════════════════════════╗");
        println!("║           FIX Server Management Console                    ║");
        println!("╚════════════════════════════════════════════════════════════╝");
        println!();
        println!("Menu Options:");
        println!("  1 -> Trade Table");
        println!("  2 -> Debug Market Data Servers");
        println!("  99 -> Exit");
        println!();
        print!("Select option: ");
        std::io::stdout().flush().map_err(|e| anyhow!("Failed to flush stdout: {}", e))?;
        
        // Wait for either user input or shutdown signal
        tokio::select! {
            line_result = lines.next_line() => {
                match line_result {
                    Ok(Some(line)) => {
                        let input = line.trim();
                        match input {
                            "1" => {
                                // Display Trade Table
                                display_trade_table(&session).await;
                                println!();
                                println!("Press Enter to return to menu...");
                                let _ = lines.next_line().await;
                            }
                            "2" => {
                                // Debug Market Data Servers
                                display_market_data_debug(&market_data_sessions).await;
                                println!();
                                println!("Press Enter to return to menu...");
                                let _ = lines.next_line().await;
                            }
                            "99" => {
                                println!();
                                println!("Shutting down server...");
                                // Send shutdown signal to all tasks
                                let _ = shutdown_tx.send(());
                                return Ok(());
                            }
                            _ => {
                                println!();
                                println!("Invalid option. Please select 1, 2, or 99.");
                                println!("Press Enter to continue...");
                                let _ = lines.next_line().await;
                            }
                        }
                    }
                    Ok(None) => {
                        // EOF - exit gracefully
                        return Ok(());
                    }
                    Err(e) => {
                        error!("Error reading input: {}", e);
                        sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
    }
}

/// Display Trade Table (Open Orders)
async fn display_trade_table(session: &FIXSession) {
    let open_orders = session.open_orders.get_open_orders();
    
    println!();
    println!("╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                                                                  TRADE TABLE                                                                                    ║");
    println!("╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣");
    
    if open_orders.is_empty() {
        println!("║  No open orders                                                                                                                                    ║");
    } else {
        // Header
        println!("║ {:<15} {:<15} {:<12} {:<12} {:<6} {:<12} {:<12} {:<10} {:<12} {:<12} {:<12} {:<20} ║",
            "ClOrdID", "OrderID", "Symbol", "Side", "Status", "OrderQty", "Price", "CumQty", "LeavesQty", "LastPx", "AvgPx", "TransactTime");
        println!("╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣");
        
        // Order rows
        for order in &open_orders {
            let side_str = match order.side.as_str() {
                "1" => "BUY",
                "2" => "SELL",
                _ => &order.side,
            };
            
            let status_str = match order.ord_status.as_str() {
                "0" => "NEW",
                "1" => "PARTIAL",
                "2" => "FILLED",
                "4" => "CANCELED",
                "8" => "REJECTED",
                _ => &order.ord_status,
            };
            
            // Truncate long fields for display
            let cl_ord_id = if order.cl_ord_id.len() > 15 { 
                format!("{}...", &order.cl_ord_id[..12]) 
            } else { 
                order.cl_ord_id.clone() 
            };
            
            let order_id = if order.order_id.len() > 15 { 
                format!("{}...", &order.order_id[..12]) 
            } else { 
                order.order_id.clone() 
            };
            
            let transact_time = if order.transact_time.len() > 20 {
                format!("{}...", &order.transact_time[..17])
            } else {
                order.transact_time.clone()
            };
            
            println!("║ {:<15} {:<15} {:<12} {:<12} {:<6} {:<12} {:<12} {:<10} {:<12} {:<12} {:<12} {:<20} ║",
                cl_ord_id,
                order_id,
                order.symbol,
                side_str,
                status_str,
                order.order_qty,
                order.price,
                order.cum_qty,
                order.leaves_qty,
                order.last_px,
                order.avg_px,
                transact_time
            );
        }
    }
    
    println!("╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝");
    println!();
    println!("Total Open Orders: {}", open_orders.len());
}

/// Display Market Data Servers Debug Information
async fn display_market_data_debug(sessions: &[(Arc<FIXSession>, String, u16)]) {
    println!();
    println!("╔════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╗");
    println!("║                                                    MARKET DATA SERVERS DEBUG                                                      ║");
    println!("╠════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╣");
    
    for (session, description, port) in sessions {
        // Get connection status
        let is_connected = *session.is_connected.lock().await;
        let connection_state = *session.connection_state.lock().await;
        let is_ready = session.is_ready().await;
        let seq_num = *session.seq_num.lock().await;
        
        // Get order book statistics
        let order_books = session.order_books.lock().await;
        let symbol_count = order_books.len();
        let total_trades: usize = order_books.values()
            .map(|book| book.get_active().trade_count())
            .sum();
        let symbols: Vec<String> = order_books.keys().cloned().collect();
        drop(order_books);
        
        // Get securities count
        let securities = session.securities.lock().await;
        let securities_count = securities.len();
        drop(securities);
        
        // Format connection state
        let state_str = match connection_state {
            ConnectionState::Disconnected => "DISCONNECTED",
            ConnectionState::Connecting => "CONNECTING",
            ConnectionState::Connected => "CONNECTED",
            ConnectionState::Logging => "LOGGING IN",
            ConnectionState::Ready => "READY",
            ConnectionState::Disconnecting => "DISCONNECTING",
        };
        
        let status_icon = if is_ready { "✅" } else if is_connected { "⚠️ " } else { "❌" };
        
        println!("║");
        println!("║ {} {} (Port {})", status_icon, description, port);
        println!("║   ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────");
        println!("║   Connection Status: {}", if is_ready { "READY" } else if is_connected { "CONNECTED" } else { "DISCONNECTED" });
        println!("║   State: {}", state_str);
        println!("║   Sender CompID: {}", session.sender_comp_id);
        println!("║   Target CompID: {}", session.target_comp_id);
        println!("║   Sequence Number: {}", seq_num);
        println!("║   Order Books: {} symbols", symbol_count);
        println!("║   Total Trades: {}", total_trades);
        println!("║   Securities: {}", securities_count);
        
        if !symbols.is_empty() {
            println!("║   Symbols: {}", symbols.join(", "));
        } else {
            println!("║   Symbols: (none)");
        }
        
        // Show top 5 order books with best bid/ask
        if symbol_count > 0 {
            println!("║");
            println!("║   Top Order Books:");
            let order_books = session.order_books.lock().await;
            let mut symbol_list: Vec<_> = order_books.iter().collect();
            symbol_list.sort_by_key(|(symbol, _)| symbol.clone());
            
            for (symbol, book) in symbol_list.iter().take(5) {
                let snapshot = book.get_snapshot();
                let bid = snapshot.best_bid;
                let ask = snapshot.best_ask;
                let spread = snapshot.spread();
                
                if bid > 0.0 && ask > 0.0 {
                    println!("║     {}: Bid={:.8}, Ask={:.8}, Spread={:.8}", 
                        symbol, bid, ask, spread);
                } else {
                    println!("║     {}: (no data)", symbol);
                }
            }
            if symbol_count > 5 {
                println!("║     ... and {} more", symbol_count - 5);
            }
            drop(order_books);
        }
    }
    
    println!("╚════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════════╝");
}


#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_default_env()
        .filter_level(log::LevelFilter::Info)
        .init();

    // Load configuration
    let config = load_config()?;
    info!("✅ Configuration loaded from config.toml");

    info!("╔════════════════════════════════════════════════════════════╗");
    info!("║  MULTI-CONNECTION FIX SERVER - ORDER + MARKET DATA           ║");
    info!("║  1. Order Management Gateway                                 ║");
    info!("║  2. Market Data Server (BTC)                                 ║");
    info!("║  3. Market Data Server (34405) - Symbols 0-9, A-D            ║");
    info!("║  4. Market Data Server (34406) - Symbols E-M                ║");
    info!("║  5. Market Data Server (34407) - Symbols N-Z                 ║");
    info!("╚════════════════════════════════════════════════════════════╝");
    info!("");
    info!("Order Management Gateway: {}", config.order_management.gateway_url);
    info!("  Sender: {} -> Target: {}", config.order_management.sender_comp_id, config.order_management.target_comp_id);
    info!("Market Data Server (BTC): {}", config.market_data_btc.gateway_url);
    info!("  Sender: {} -> Target: {}", config.market_data_btc.sender_comp_id, config.market_data_btc.target_comp_id);
    info!("Market Data Servers (Others): {}", config.market_data_others.base_url);
    info!("  Port {}: Symbols 0-9, A-D", config.market_data_others.port_34405);
    info!("  Port {}: Symbols E-M", config.market_data_others.port_34406);
    info!("  Port {}: Symbols N-Z", config.market_data_others.port_34407);
    info!("  Sender: {} -> Target: {}", config.market_data_others.sender_comp_id, config.market_data_others.target_comp_id);

    // Initialize kdb+ writer (connect to kdb+ server via IPC)
    // Default to localhost:5001 - can be configured via environment or config file
    let kdb_host = std::env::var("KDB_HOST").unwrap_or_else(|_| "localhost".to_string());
    let kdb_port: u16 = std::env::var("KDB_PORT")
        .unwrap_or_else(|_| "5001".to_string())
        .parse()
        .unwrap_or(5001);
    
    let kdb_writer = Arc::new(KdbWriter::new(kdb_host.clone(), kdb_port));
    kdb_writer.initialize().await
        .map_err(|e| {
            error!("❌ FATAL: Failed to initialize kdb+ writer ({}:{}): {}", kdb_host, kdb_port, e);
            error!("❌ Server cannot start without kdb+ connectivity.");
            error!("💡 To fix: Start kdb+ server with: q -p {}", kdb_port);
            error!("💡 Or set KDB_HOST and KDB_PORT environment variables if kdb+ is on a different host/port.");
            anyhow::anyhow!("kdb+ connection required but failed: {}", e)
        })?;
    info!("✅ kdb+ writer initialized. Connected to {}:{}", kdb_host, kdb_port);

    let session = Arc::new(FIXSession::new_with_credentials_and_kdb(
        &config.order_management.sender_comp_id, 
        &config.order_management.target_comp_id,
        &config.api.api_key,
        &config.api.api_secret,
        Arc::clone(&kdb_writer)
    ));
    let market_data_session = Arc::new(FIXSession::new_with_credentials_and_kdb(
        &config.market_data_btc.sender_comp_id, 
        &config.market_data_btc.target_comp_id,
        &config.api.api_key,
        &config.api.api_secret,
        Arc::clone(&kdb_writer)
    ));
    
    // Create additional market data sessions for ports 34405-34407 (symbol-based routing)
    let market_data_others_34405 = Arc::new(FIXSession::new_with_credentials_and_kdb(
        &config.market_data_others.sender_comp_id,
        &config.market_data_others.target_comp_id,
        &config.api.api_key,
        &config.api.api_secret,
        Arc::clone(&kdb_writer)
    ));
    let market_data_others_34406 = Arc::new(FIXSession::new_with_credentials_and_kdb(
        &config.market_data_others.sender_comp_id,
        &config.market_data_others.target_comp_id,
        &config.api.api_key,
        &config.api.api_secret,
        Arc::clone(&kdb_writer)
    ));
    let market_data_others_34407 = Arc::new(FIXSession::new_with_credentials_and_kdb(
        &config.market_data_others.sender_comp_id,
        &config.market_data_others.target_comp_id,
        &config.api.api_key,
        &config.api.api_secret,
        Arc::clone(&kdb_writer)
    ));

    // Create channel for order requests
    let (order_tx, order_rx) = mpsc::channel(100);

    // Order directory watcher
    let order_tx_for_watcher = order_tx.clone();
    let session_for_watcher = Arc::clone(&session);
    let orders_watcher_handle = tokio::spawn(async move {
        if let Err(e) = watch_orders_directory(order_tx_for_watcher, "orders", session_for_watcher).await {
            error!("Fatal error in orders directory watcher: {}", e);
        }
    });

    // Message loop with order handling
    let session_clone = Arc::clone(&session);
    let gateway_clone = config.order_management.gateway_url.clone();
    let heartbeat_interval = config.connection.heartbeat_interval;
    let max_reconnect = config.connection.max_reconnect_attempts;
    let reconnect_backoff = config.connection.reconnect_backoff_ms;
    let msg_handle = tokio::spawn(async move {
        if let Err(e) = message_loop(session_clone, gateway_clone, order_rx, heartbeat_interval, max_reconnect, reconnect_backoff).await {
            error!("Fatal error in message loop: {}", e);
        }
    });

    // Heartbeat loop for order gateway
    let session_clone = Arc::clone(&session);
    let heartbeat_interval_clone = heartbeat_interval;
    let heartbeat_handle = tokio::spawn(async move {
        if let Err(e) = heartbeat_loop(session_clone, heartbeat_interval_clone).await {
            error!("Fatal error in heartbeat loop: {}", e);
        }
    });

    // Market Data connection loop (separate from order gateway)
    let market_data_clone = Arc::clone(&market_data_session);
    let market_data_url = config.market_data_btc.gateway_url.clone();
    let market_data_handle = tokio::spawn(async move {
        if let Err(e) = market_data_loop(market_data_clone, market_data_url, heartbeat_interval, max_reconnect, reconnect_backoff, None).await {
            error!("Fatal error in market data loop: {}", e);
        }
    });

    // Market Data Heartbeat loop
    let market_data_clone = Arc::clone(&market_data_session);
    let market_data_heartbeat_handle = tokio::spawn(async move {
        if let Err(e) = heartbeat_loop(market_data_clone, heartbeat_interval).await {
            error!("Fatal error in market data heartbeat loop: {}", e);
        }
    });

    // Additional Market Data connections for ports 34405-34407
    // Port 34405: symbols starting with 0-9, A-D
    let market_data_34405_clone = Arc::clone(&market_data_others_34405);
    let market_data_34405_url = format!("{}:{}", config.market_data_others.base_url, config.market_data_others.port_34405);
    let config_34405 = config.market_data_others.clone();
    let market_data_34405_handle = tokio::spawn(async move {
        let filter: Box<dyn Fn(&str) -> bool + Send + Sync> = Box::new(move |symbol: &str| {
            let port = get_market_data_port_for_symbol(symbol, &config_34405);
            port == config_34405.port_34405
        });
        if let Err(e) = market_data_loop(market_data_34405_clone, market_data_34405_url, heartbeat_interval, max_reconnect, reconnect_backoff, Some(filter)).await {
            error!("Fatal error in market data loop (34405): {}", e);
        }
    });

    // Port 34406: symbols starting with E-M
    let market_data_34406_clone = Arc::clone(&market_data_others_34406);
    let market_data_34406_url = format!("{}:{}", config.market_data_others.base_url, config.market_data_others.port_34406);
    let config_34406 = config.market_data_others.clone();
    let market_data_34406_handle = tokio::spawn(async move {
        let filter: Box<dyn Fn(&str) -> bool + Send + Sync> = Box::new(move |symbol: &str| {
            let port = get_market_data_port_for_symbol(symbol, &config_34406);
            port == config_34406.port_34406
        });
        if let Err(e) = market_data_loop(market_data_34406_clone, market_data_34406_url, heartbeat_interval, max_reconnect, reconnect_backoff, Some(filter)).await {
            error!("Fatal error in market data loop (34406): {}", e);
        }
    });

    // Port 34407: symbols starting with N-Z
    let market_data_34407_clone = Arc::clone(&market_data_others_34407);
    let market_data_34407_url = format!("{}:{}", config.market_data_others.base_url, config.market_data_others.port_34407);
    let config_34407 = config.market_data_others.clone();
    let market_data_34407_handle = tokio::spawn(async move {
        let filter: Box<dyn Fn(&str) -> bool + Send + Sync> = Box::new(move |symbol: &str| {
            let port = get_market_data_port_for_symbol(symbol, &config_34407);
            port == config_34407.port_34407
        });
        if let Err(e) = market_data_loop(market_data_34407_clone, market_data_34407_url, heartbeat_interval, max_reconnect, reconnect_backoff, Some(filter)).await {
            error!("Fatal error in market data loop (34407): {}", e);
        }
    });

    // Additional Market Data Heartbeat loops
    let market_data_34405_clone = Arc::clone(&market_data_others_34405);
    let market_data_34405_heartbeat_handle = tokio::spawn(async move {
        if let Err(e) = heartbeat_loop(market_data_34405_clone, heartbeat_interval).await {
            error!("Fatal error in market data heartbeat loop (34405): {}", e);
        }
    });

    let market_data_34406_clone = Arc::clone(&market_data_others_34406);
    let market_data_34406_heartbeat_handle = tokio::spawn(async move {
        if let Err(e) = heartbeat_loop(market_data_34406_clone, heartbeat_interval).await {
            error!("Fatal error in market data heartbeat loop (34406): {}", e);
        }
    });

    let market_data_34407_clone = Arc::clone(&market_data_others_34407);
    let market_data_34407_heartbeat_handle = tokio::spawn(async move {
        if let Err(e) = heartbeat_loop(market_data_34407_clone, heartbeat_interval).await {
            error!("Fatal error in market data heartbeat loop (34407): {}", e);
        }
    });

    // Create broadcast channels for event-driven updates
    let (order_update_tx, _) = broadcast::channel::<()>(1000); // Signal execution report updates
    let (order_book_update_tx, _) = broadcast::channel::<String>(1000); // Signal order book updates (symbol)
    
    // Create shutdown channel for graceful shutdown
    let (shutdown_tx, shutdown_rx) = broadcast::channel::<()>(1);
    
    // Set update channels in sessions
    session.set_update_channels(order_update_tx.clone(), order_book_update_tx.clone()).await;
    market_data_session.set_update_channels(order_update_tx.clone(), order_book_update_tx.clone()).await;
    market_data_others_34405.set_update_channels(order_update_tx.clone(), order_book_update_tx.clone()).await;
    market_data_others_34406.set_update_channels(order_update_tx.clone(), order_book_update_tx.clone()).await;
    market_data_others_34407.set_update_channels(order_update_tx.clone(), order_book_update_tx.clone()).await;
    
    // Note: Management Console has been moved to a standalone program
    // Run: cargo run --bin fix_console
    
    // Web server — serves the trading UI and API endpoints on port 8080
    let order_session_for_web = Arc::clone(&session);
    let market_data_for_web = Arc::clone(&market_data_session);
    let market_data_sessions_for_web = vec![
        (Arc::clone(&market_data_session),        "BTC Market Data".to_string(),    config.market_data_btc.gateway_url.split(':').last().and_then(|p| p.parse().ok()).unwrap_or(34404_u16)),
        (Arc::clone(&market_data_others_34405),   "Others 34405".to_string(),       config.market_data_others.port_34405),
        (Arc::clone(&market_data_others_34406),   "Others 34406".to_string(),       config.market_data_others.port_34406),
        (Arc::clone(&market_data_others_34407),   "Others 34407".to_string(),       config.market_data_others.port_34407),
    ];
    // Trading Algo — starts disabled; enable via web UI or API (must be created before web server)
    let algo = Arc::new(algo::TradingAlgo::new(algo::AlgoConfig::default()));
    let algo_for_web = Arc::clone(&algo);
    let algo_order_session = Arc::clone(&session);
    let algo_market_session = Arc::clone(&market_data_session);
    let algo_positions = Arc::clone(&session.positions);
    let _algo_handle = tokio::spawn(async move {
        algo.run(algo_order_session, algo_market_session, algo_positions).await;
    });

    let kdb_for_web = Arc::clone(&kdb_writer);
    let _web_server_handle = tokio::spawn(async move {
        if let Err(e) = order_book_web_server(order_session_for_web, market_data_for_web, market_data_sessions_for_web, kdb_for_web, algo_for_web, "0.0.0.0:8080").await {
            error!("Fatal error in web server: {}", e);
        }
    });
    
    // Open Orders CSV Generator - event-driven (updates immediately on execution reports)
    let open_orders_store = Arc::clone(&session.open_orders);
    let csv_update_rx = order_update_tx.subscribe();
    let _csv_generator_handle = tokio::spawn(async move {
        if let Err(e) = open_orders_csv_generator_event_driven(open_orders_store, csv_update_rx).await {
            error!("Fatal error in open orders CSV generator: {}", e);
        }
    });

    // Auto-save tick data to data/ticks.csv every 60 seconds for backtest persistence.
    // Uses append mode so data accumulates across server sessions.
    let market_data_for_autosave = Arc::clone(&market_data_session);
    let _tick_autosave_handle = tokio::spawn(async move {
        let mut save_interval = interval(Duration::from_secs(60));
        save_interval.tick().await; // skip the immediate first tick
        loop {
            save_interval.tick().await;
            let _ = market_data_for_autosave.save_ticks_to_parquet("data/ticks.csv").await;
        }
    });

    // ZeroMQ Publisher for order book data - event-driven (publishes immediately on updates)
    let market_data_for_zmq = Arc::clone(&market_data_session);
    let zmq_update_rx = order_book_update_tx.subscribe();
    let zmq_handle = tokio::spawn(async move {
        if let Err(e) = zmq_order_book_publisher_event_driven(market_data_for_zmq, "tcp://0.0.0.0:5555", zmq_update_rx).await {
            error!("Fatal error in ZeroMQ publisher: {}", e);
        }
    });

    // Wait for initial connections and logons
    info!("Waiting for connections and logons to complete...");
    sleep(Duration::from_secs(3)).await;

    info!("");
    info!("🔄 Server running continuously - all services active:");
    info!("   ✓ Order Management Gateway connection");
    info!("   ✓ Market Data Server (34402) - BTC symbols");
    info!("   ✓ Market Data Server (34405) - Symbols 0-9, A-D");
    info!("   ✓ Market Data Server (34406) - Symbols E-M");
    info!("   ✓ Market Data Server (34407) - Symbols N-Z");
    info!("   ✓ ZeroMQ Publisher (tcp://0.0.0.0:5555) - Topics: symbol (e.g., BTC_USDT)");
    info!("   ✓ Orders directory watcher (orders/) - Processing JSON order files");
    info!("   ✓ Management Console - Run separately: cargo run --bin fix_console");
    info!("");
    info!("To use the management console, run: cargo run --bin fix_console");
    info!("Press Ctrl+C to stop the server.");

    // Keep the server running indefinitely
    // Wait for any of the critical tasks to fail (they will log errors and continue)
    // Or wait for console to request shutdown
    let mut shutdown_rx_main = shutdown_tx.subscribe();
    tokio::select! {
        _ = msg_handle => {
            error!("Order Management message loop exited");
        }
        _ = heartbeat_handle => {
            error!("Order Management heartbeat loop exited");
        }
        _ = market_data_handle => {
            error!("Market Data loop exited");
        }
        _ = market_data_heartbeat_handle => {
            error!("Market Data heartbeat loop exited");
        }
        _ = market_data_34405_handle => {
            error!("Market Data loop (34405) exited");
        }
        _ = market_data_34405_heartbeat_handle => {
            error!("Market Data heartbeat loop (34405) exited");
        }
        _ = market_data_34406_handle => {
            error!("Market Data loop (34406) exited");
        }
        _ = market_data_34406_heartbeat_handle => {
            error!("Market Data heartbeat loop (34406) exited");
        }
        _ = market_data_34407_handle => {
            error!("Market Data loop (34407) exited");
        }
        _ = market_data_34407_heartbeat_handle => {
            error!("Market Data heartbeat loop (34407) exited");
        }
        _ = zmq_handle => {
            error!("ZeroMQ publisher exited");
        }
        _ = orders_watcher_handle => {
            error!("Orders directory watcher exited");
        }
        _ = shutdown_rx_main.recv() => {
            info!("Shutdown signal received - shutting down server...");
        }
    }

    Ok(())
}
