mod fix_protocol;
mod order_book;
mod order_book_buffer;
mod kdb_integration;

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
use order_book::{OrderBook, Side, UpdateAction};
use order_book_buffer::DoubleBufferedOrderBook;
use arc_swap::ArcSwap;
use kdb_integration::KdbWriter;
use axum::{
    extract::Path as AxumPath,
    http::StatusCode,
    response::{Html, Json},
    routing::{get, post},
    Router,
};
use tower_http::cors::CorsLayer;

// #region agent log
const DEBUG_LOG_PATH: &str = "/home/ubuntu/fix_server/.cursor/debug.log";
fn debug_log(location: &str, message: &str, data: serde_json::Value, hypothesis_id: &str) {
    let ts = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis();
    let id = format!("log_{}_{}", ts, hypothesis_id);
    let line = serde_json::json!({
        "id": id,
        "timestamp": ts as u64,
        "location": location,
        "message": message,
        "data": data,
        "hypothesisId": hypothesis_id
    });
    if let Ok(mut f) = std::fs::OpenOptions::new().create(true).append(true).open(DEBUG_LOG_PATH) {
        let _ = writeln!(f, "{}", line.to_string());
        let _ = f.sync_all();
    }
}
// #endregion

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
                    // #region agent log
                    debug_log("main.rs:connect_with_retry", "connect_success", serde_json::json!({ "attempt": attempt, "gateway_url": gateway_url }), "H4");
                    // #endregion
                    info!("✅ Connected successfully to FIX gateway at attempt {}", attempt);
                    return Ok(());
                }
                Ok(Err(e)) => {
                    // #region agent log
                    debug_log("main.rs:connect_with_retry", "connect_fail", serde_json::json!({ "attempt": attempt, "err": e.to_string(), "gateway_url": gateway_url }), "H4");
                    // #endregion
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
                    // #region agent log
                    debug_log("main.rs:connect_with_retry", "connect_timeout", serde_json::json!({ "attempt": attempt, "gateway_url": gateway_url }), "H4");
                    // #endregion
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
    pub async fn get_order_book_snapshot(&self, symbol: &str) -> Option<Arc<OrderBook>> {
        let books = self.order_books.lock().await;
        books.get(symbol).map(|book| book.get_snapshot())
    }

    /// Get best bid/ask for a symbol (from snapshot, lock-free)
    pub async fn get_best_quote(&self, symbol: &str) -> Option<(f64, f64, f64)> {
        let books = self.order_books.lock().await;
        if let Some(book) = books.get(symbol) {
            let snapshot = book.get_snapshot();
            let bid = snapshot.best_bid();
            let ask = snapshot.best_ask();
            let spread = snapshot.spread();
            if bid > 0.0 && ask > 0.0 {
                Some((bid, ask, spread))
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
            let snapshot = book.get_snapshot();
            snapshot.get_depth(side, depth)
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

                    // Write snapshot to kdb+
                    let _ = self.kdb_writer.write_snapshot(&symbol, best_bid, best_ask, 
                        best_bid_size, best_ask_size, spread).await;

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
                    
                    // Write snapshot to kdb+ (periodically, not every update to reduce I/O)
                    // Only write if best bid/ask changed significantly or every N updates
                    let _ = self.kdb_writer.write_snapshot(&symbol, best_bid, best_ask, 
                        best_bid_size, best_ask_size, spread).await;
                    
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

    /// Save tick data to CSV file (compatible with Parquet tools)
    pub async fn save_ticks_to_parquet(&self, filename: &str) -> Result<()> {
        let mut ticks = self.tick_data.lock().await;
        let tick_count = ticks.len();

        if tick_count == 0 {
            info!("No tick data to save");
            return Ok(());
        }

        // Clone ticks and clear buffer to avoid duplicates on next save
        let ticks_clone = ticks.clone();
        ticks.clear(); // Clear buffer after cloning so next save only has new ticks
        drop(ticks); // Release lock before async operations
        
        let filename_owned = filename.to_string();

        std::thread::spawn(move || {
            match (|| -> Result<()> {
                let mut file = File::create(&filename_owned)?;

                // Write CSV header
                writeln!(file, "timestamp,symbol,bid,ask,bid_size,ask_size,last_price,last_size")?;

                // Write tick data rows
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
                    info!("✅ Saved {} ticks to {} (CSV format)", tick_count, filename_owned);
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
        let symbol = tick.symbol.clone();
        ticks.push(tick);
        let tick_count = ticks.len();
        
        if tick_count % 100 == 0 {
            info!("📊 Buffered {} ticks for {}", tick_count, symbol);
        }
        
        // Auto-save disabled - only order_book_current.csv is updated
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
                            // #region agent log
                            debug_log(
                                "main.rs:message_loop",
                                "receive_message_err",
                                serde_json::json!({ "err": e.to_string(), "loop_type": "order_management", "gateway_url": gateway_url }),
                                "H2_H3",
                            );
                            // #endregion
                            warn!("⚠️  Connection closed: {}", e);
                            connection_active = false;
                        }
                    }
                }
            }
        }

        // Wait before reconnecting
        // #region agent log
        debug_log("main.rs:message_loop", "connection_loop_exited", serde_json::json!({ "loop_type": "order_management" }), "H2_H3");
        // #endregion
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
                        // #region agent log
                        debug_log(
                            "main.rs:market_data_loop",
                            "receive_message_err",
                            serde_json::json!({ "err": err_str, "loop_type": "market_data", "gateway_url": gateway_url }),
                            "H2_H3",
                        );
                        // #endregion
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
                Ok(()) => {
                    // #region agent log
                    debug_log("main.rs:heartbeat_loop", "heartbeat_sent", serde_json::json!({}), "H1");
                    // #endregion
                }
                Err(e) => {
                    // #region agent log
                    debug_log("main.rs:heartbeat_loop", "heartbeat_error", serde_json::json!({ "err": e.to_string() }), "H1");
                    // #endregion
                    warn!("Heartbeat error: {}", e);
                }
            }
        } else {
            // #region agent log
            debug_log("main.rs:heartbeat_loop", "heartbeat_skipped", serde_json::json!({ "reason": "session_not_ready" }), "H1");
            // #endregion
            // Session not ready yet - skip this heartbeat
            // This prevents sending heartbeats before logon ACK is received
        }
    }
}

/// Generate CSV summary from OpenOrdersStore snapshot (lock-free)
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
                let best_bid = order_book.best_bid();
                let best_ask = order_book.best_ask();
                
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
            let best_bid = order_book.best_bid();
            let best_ask = order_book.best_ask();
            
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
    bind_addr: &str,
) -> Result<()> {
    use axum::extract::Query;
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
    <title>Order Book Viewer</title>
    <link href="https://fonts.googleapis.com/css2?family=Orbitron:wght@400;700;900&family=Rajdhani:wght@300;400;600;700&display=swap" rel="stylesheet">
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        body {
            font-family: 'Rajdhani', 'Segoe UI', sans-serif;
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
            background-attachment: fixed;
            color: #e5e7eb;
            padding: 20px;
            min-height: 100vh;
            position: relative;
            overflow-x: hidden;
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
            max-width: 1400px;
            margin: 0 auto;
            background: rgba(255, 255, 255, 0.03);
            backdrop-filter: blur(10px);
            border-radius: 20px;
            border: 1px solid rgba(255, 107, 53, 0.2);
            box-shadow: 
                0 8px 32px rgba(0, 0, 0, 0.3),
                0 0 0 1px rgba(255, 107, 53, 0.1),
                inset 0 1px 0 rgba(255, 255, 255, 0.1);
            overflow: hidden;
            position: relative;
            z-index: 1;
        }
        .header {
            background: linear-gradient(135deg, rgba(255, 107, 53, 0.9) 0%, rgba(255, 140, 0, 0.9) 100%);
            background-size: 200% 200%;
            animation: gradientShift 8s ease infinite;
            color: white;
            padding: 25px 30px;
            text-align: center;
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
        .header h1 {
            font-family: 'Orbitron', monospace;
            font-size: 2.0em;
            font-weight: 900;
            margin-bottom: 10px;
            text-shadow: 
                0 0 10px rgba(255, 255, 255, 0.5),
                0 0 20px rgba(255, 107, 53, 0.8),
                0 0 30px rgba(255, 107, 53, 0.6);
            letter-spacing: 2px;
            position: relative;
            z-index: 1;
        }
        .header-menu {
            display: flex;
            justify-content: center;
            align-items: center;
            gap: 30px;
            margin-top: 15px;
            flex-wrap: wrap;
            position: relative;
            z-index: 1;
        }
        .menu-item {
            background: rgba(255, 255, 255, 0.15);
            backdrop-filter: blur(10px);
            padding: 14px 28px;
            border-radius: 12px;
            font-size: 16px;
            font-weight: 600;
            cursor: default;
            transition: all 0.3s ease;
            border: 1px solid rgba(255, 255, 255, 0.2);
            box-shadow: 0 4px 15px rgba(0, 0, 0, 0.2);
        }
        .menu-item:hover {
            background: rgba(255, 255, 255, 0.25);
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(255, 107, 53, 0.4);
        }
        .menu-item-value {
            font-family: 'Orbitron', monospace;
            font-size: 26px;
            font-weight: bold;
            margin-left: 8px;
            color: #fff;
            text-shadow: 0 0 10px rgba(255, 255, 255, 0.8);
        }
        .header .symbol-selector {
            margin-top: 15px;
            position: relative;
            z-index: 1;
        }
        .header select {
            padding: 12px 24px;
            font-size: 16px;
            font-weight: 600;
            border-radius: 10px;
            border: 2px solid rgba(255, 255, 255, 0.3);
            background: rgba(255, 255, 255, 0.95);
            color: #1a1a2e;
            cursor: pointer;
            transition: all 0.3s ease;
            box-shadow: 0 4px 15px rgba(0, 0, 0, 0.2);
        }
        .header select:hover {
            border-color: rgba(255, 255, 255, 0.6);
            box-shadow: 0 6px 20px rgba(255, 107, 53, 0.4);
            transform: translateY(-2px);
        }
        .stats {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 25px;
            padding: 35px;
            background: rgba(26, 26, 46, 0.4);
            backdrop-filter: blur(5px);
        }
        .stat-card {
            background: rgba(255, 255, 255, 0.05);
            backdrop-filter: blur(10px);
            padding: 25px;
            border-radius: 15px;
            text-align: center;
            border: 1px solid rgba(255, 107, 53, 0.2);
            box-shadow: 
                0 4px 15px rgba(0, 0, 0, 0.3),
                inset 0 1px 0 rgba(255, 255, 255, 0.1);
            transition: all 0.3s ease;
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
        .stat-card:hover {
            transform: translateY(-5px);
            border-color: rgba(255, 107, 53, 0.5);
            box-shadow: 
                0 8px 25px rgba(255, 107, 53, 0.3),
                0 0 20px rgba(255, 107, 53, 0.2);
        }
        .stat-card:hover::before {
            left: 100%;
        }
        .stat-label {
            font-size: 13px;
            color: #9ca3af;
            margin-bottom: 12px;
            text-transform: uppercase;
            letter-spacing: 1px;
            font-weight: 600;
        }
        .stat-value {
            font-family: 'Orbitron', monospace;
            font-size: 32px;
            font-weight: 700;
            color: #e5e7eb;
            text-shadow: 0 0 10px rgba(255, 255, 255, 0.3);
        }
        .stat-value.bid { 
            color: #10b981;
            text-shadow: 0 0 15px rgba(16, 185, 129, 0.6);
        }
        .stat-value.ask { 
            color: #ef4444;
            text-shadow: 0 0 15px rgba(239, 68, 68, 0.6);
        }
        .stat-value.spread { 
            color: #ff6b35;
            text-shadow: 0 0 15px rgba(255, 107, 53, 0.6);
        }
        .order-book-container {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 25px;
            padding: 35px;
        }
        .order-book-side {
            background: rgba(26, 26, 46, 0.4);
            backdrop-filter: blur(5px);
            border-radius: 15px;
            overflow: hidden;
            border: 1px solid rgba(255, 107, 53, 0.2);
            box-shadow: 0 4px 15px rgba(0, 0, 0, 0.3);
        }
        .order-book-header {
            padding: 18px 20px;
            font-family: 'Orbitron', monospace;
            font-weight: 700;
            text-align: center;
            font-size: 18px;
            letter-spacing: 2px;
            text-shadow: 0 0 10px rgba(255, 255, 255, 0.5);
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
        .order-book-table {
            width: 100%;
            border-collapse: collapse;
        }
        .order-book-table th {
            padding: 14px;
            text-align: right;
            background: rgba(255, 107, 53, 0.15);
            font-weight: 700;
            font-size: 11px;
            text-transform: uppercase;
            color: #ff6b35;
            letter-spacing: 1px;
            border-bottom: 2px solid rgba(255, 107, 53, 0.3);
        }
        .order-book-table th:first-child {
            text-align: left;
        }
        .order-book-table td {
            padding: 12px 14px;
            text-align: right;
            font-family: 'Courier New', monospace;
            font-size: 14px;
            border-bottom: 1px solid rgba(255, 107, 53, 0.1);
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
        .trades-section {
            padding: 35px;
            background: rgba(26, 26, 46, 0.4);
            backdrop-filter: blur(5px);
        }
        .trades-header {
            font-family: 'Orbitron', monospace;
            font-size: 22px;
            font-weight: 700;
            margin-bottom: 25px;
            color: #ff6b35;
            text-shadow: 0 0 10px rgba(255, 107, 53, 0.5);
            letter-spacing: 1px;
        }
        .trades-table {
            width: 100%;
            border-collapse: collapse;
            background: rgba(255, 255, 255, 0.03);
            backdrop-filter: blur(10px);
            border-radius: 12px;
            overflow: hidden;
            border: 1px solid rgba(255, 107, 53, 0.2);
        }
        .trades-table th {
            padding: 14px;
            text-align: right;
            background: linear-gradient(135deg, rgba(255, 107, 53, 0.8) 0%, rgba(255, 140, 0, 0.8) 100%);
            color: white;
            font-weight: 700;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        .trades-table th:first-child {
            text-align: left;
        }
        .trades-table td {
            padding: 12px 14px;
            text-align: right;
            font-family: 'Courier New', monospace;
            font-size: 14px;
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
            padding: 20px 30px;
            background: rgba(31, 41, 55, 0.8);
            backdrop-filter: blur(10px);
            color: white;
            text-align: center;
            font-size: 14px;
            font-weight: 600;
            letter-spacing: 1px;
            border-top: 1px solid rgba(255, 107, 53, 0.2);
            text-transform: uppercase;
        }
        .status.connected {
            background: linear-gradient(135deg, rgba(16, 185, 129, 0.9) 0%, rgba(5, 150, 105, 0.9) 100%);
            box-shadow: 0 0 20px rgba(16, 185, 129, 0.4);
            animation: pulse 2s ease infinite;
        }
        @keyframes pulse {
            0%, 100% { box-shadow: 0 0 20px rgba(16, 185, 129, 0.4); }
            50% { box-shadow: 0 0 30px rgba(16, 185, 129, 0.6); }
        }
        .status.error {
            background: linear-gradient(135deg, rgba(239, 68, 68, 0.9) 0%, rgba(220, 38, 38, 0.9) 100%);
            box-shadow: 0 0 20px rgba(239, 68, 68, 0.4);
        }
        @media (max-width: 768px) {
            .order-book-container {
                grid-template-columns: 1fr;
            }
            .header h1 {
                font-size: 2em;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>HIGH FREQUENCY WOZINGA TRADING PLATFORM</h1>
            <div class="header-menu">
                <div class="menu-item">
                    Total Markets: <span class="menu-item-value" id="totalMarkets">-</span>
                </div>
            </div>
            <div class="symbol-selector">
                <select id="symbolSelect">
                    <option value="BTC_USDT">BTC_USDT</option>
                    <option value="BTC_USD">BTC_USD</option>
                    <option value="SOL_BTC">SOL_BTC</option>
                    <option value="XRP_BTC">XRP_BTC</option>
                    <option value="DOGE_BTC">DOGE_BTC</option>
                </select>
            </div>
        </div>
        
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
        
        <div class="trades-section">
            <div class="trades-header">Recent Trades</div>
            <table class="trades-table">
                <thead>
                    <tr>
                        <th>Price</th>
                        <th>Size</th>
                        <th>Time</th>
                    </tr>
                </thead>
                <tbody id="tradesTable">
                    <tr><td colspan="3" style="text-align: center; padding: 20px;">No trades yet</td></tr>
                </tbody>
            </table>
        </div>
        
        <div class="order-book-container">
            <div class="order-book-side">
                <div class="order-book-header bids">BIDS</div>
                <table class="order-book-table">
                    <thead>
                        <tr>
                            <th>Price</th>
                            <th>Size</th>
                        </tr>
                    </thead>
                    <tbody id="bidsTable">
                        <tr><td colspan="2" style="text-align: center; padding: 20px;">Loading...</td></tr>
                    </tbody>
                </table>
            </div>
            
            <div class="order-book-side">
                <div class="order-book-header asks">ASKS</div>
                <table class="order-book-table">
                    <thead>
                        <tr>
                            <th>Price</th>
                            <th>Size</th>
                        </tr>
                    </thead>
                    <tbody id="asksTable">
                        <tr><td colspan="2" style="text-align: center; padding: 20px;">Loading...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
        
        <div class="status" id="status">Connecting...</div>
    </div>
    
    <script>
        let currentSymbol = 'BTC_USDT';
        let updateInterval = null;
        
        const symbolSelect = document.getElementById('symbolSelect');
        symbolSelect.addEventListener('change', (e) => {
            currentSymbol = e.target.value;
            updateOrderBook();
        });
        
        function formatNumber(num) {
            if (num === null || num === undefined || isNaN(num)) return '-';
            return num.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 8 });
        }
        
        function formatTime(timestamp) {
            if (!timestamp) return '-';
            const date = new Date(timestamp / 1000000); // Convert nanoseconds to milliseconds
            return date.toLocaleTimeString();
        }
        
        function updateTotalMarkets() {
            fetch('/api/symbols')
                .then(response => {
                    if (!response.ok) {
                        throw new Error('Failed to fetch symbols');
                    }
                    return response.json();
                })
                .then(data => {
                    const totalMarkets = data.symbols ? data.symbols.length : 0;
                    document.getElementById('totalMarkets').textContent = totalMarkets;
                    
                    // Update symbol selector with all available symbols
                    const symbolSelect = document.getElementById('symbolSelect');
                    const currentValue = symbolSelect.value;
                    symbolSelect.innerHTML = '';
                    if (data.symbols && data.symbols.length > 0) {
                        // If current symbol is not in the list, switch to first available
                        const hasCurrentSymbol = data.symbols.includes(currentValue);
                        const symbolToSelect = hasCurrentSymbol ? currentValue : data.symbols[0];
                        
                        data.symbols.forEach(symbol => {
                            const option = document.createElement('option');
                            option.value = symbol;
                            option.textContent = symbol;
                            if (symbol === symbolToSelect) {
                                option.selected = true;
                            }
                            symbolSelect.appendChild(option);
                        });
                        
                        // Update currentSymbol if we switched
                        if (!hasCurrentSymbol && symbolToSelect) {
                            currentSymbol = symbolToSelect;
                            updateOrderBook();
                        }
                    } else {
                        const option = document.createElement('option');
                        option.value = 'BTC_USDT';
                        option.textContent = 'BTC_USDT';
                        symbolSelect.appendChild(option);
                    }
                })
                .catch(error => {
                    console.error('Error fetching symbols:', error);
                });
        }
        
        function updateOrderBook() {
            fetch(`/api/orderbook/${currentSymbol}`)
                .then(response => {
                    if (!response.ok) {
                        throw new Error('Failed to fetch order book');
                    }
                    return response.json();
                })
                .then(data => {
                    document.getElementById('status').textContent = 'Connected';
                    document.getElementById('status').className = 'status connected';
                    
                    // Update stats
                    document.getElementById('bestBid').textContent = formatNumber(data.best_bid);
                    document.getElementById('bestAsk').textContent = formatNumber(data.best_ask);
                    document.getElementById('spread').textContent = formatNumber(data.spread);
                    document.getElementById('tradeCount').textContent = data.trade_count || 0;
                    
                    // Update bids table
                    const bidsTable = document.getElementById('bidsTable');
                    if (data.bids && data.bids.length > 0) {
                        bidsTable.innerHTML = data.bids.map(level => `
                            <tr>
                                <td class="price">${formatNumber(level.price)}</td>
                                <td class="size">${formatNumber(level.size)}</td>
                            </tr>
                        `).join('');
                    } else {
                        bidsTable.innerHTML = '<tr><td colspan="2" style="text-align: center; padding: 20px;">No bids</td></tr>';
                    }
                    
                    // Update asks table
                    const asksTable = document.getElementById('asksTable');
                    if (data.asks && data.asks.length > 0) {
                        asksTable.innerHTML = data.asks.map(level => `
                            <tr>
                                <td class="price">${formatNumber(level.price)}</td>
                                <td class="size">${formatNumber(level.size)}</td>
                            </tr>
                        `).join('');
                    } else {
                        asksTable.innerHTML = '<tr><td colspan="2" style="text-align: center; padding: 20px;">No asks</td></tr>';
                    }
                    
                    // Update trades table
                    const tradesTable = document.getElementById('tradesTable');
                    if (data.trades && data.trades.length > 0) {
                        tradesTable.innerHTML = data.trades.map(trade => `
                            <tr>
                                <td class="price">${formatNumber(trade.price)}</td>
                                <td class="size">${formatNumber(trade.size)}</td>
                                <td>${formatTime(trade.timestamp)}</td>
                            </tr>
                        `).join('');
                    } else {
                        tradesTable.innerHTML = '<tr><td colspan="3" style="text-align: center; padding: 20px;">No trades yet</td></tr>';
                    }
                })
                .catch(error => {
                    console.error('Error:', error);
                    document.getElementById('status').textContent = 'Error: ' + error.message;
                    document.getElementById('status').className = 'status error';
                });
        }
        
        // Initial load
        updateTotalMarkets();
        updateOrderBook();
        
        // Update total markets every 5 seconds (less frequent)
        setInterval(updateTotalMarkets, 5000);
        
        // Update order book every 500ms
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
    };
    
    // API endpoint to get order book data
    async fn get_order_book_api(
        AxumPath(symbol): AxumPath<String>,
        axum::extract::State(state): axum::extract::State<AppState>,
    ) -> Result<Json<serde_json::Value>, StatusCode> {
        // Get order book snapshot from market data session (order books are stored there)
        let snapshot = state.market_data_session.get_order_book_snapshot(&symbol).await;
        
        if let Some(order_book) = snapshot {
            let best_bid = order_book.best_bid();
            let best_ask = order_book.best_ask();
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
    
    // Build the router
    let html_page_clone = html_page.to_string();
    let app = Router::new()
        .route("/", get(move || {
            let html = html_page_clone.clone();
            async move { Html(html) }
        }))
        .route("/api/orderbook/:symbol", get(get_order_book_api))
        .route("/api/symbols", get(list_symbols_api))
        .route("/api/open-orders", get(get_open_orders_api))
        .route("/api/market-data-status", get(get_all_market_data_status_api))
        .route("/api/subscribe-all-symbols/:port", post(subscribe_all_symbols_api))
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
                let bid = snapshot.best_bid();
                let ask = snapshot.best_ask();
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
    
    // Web server disabled - using kdb+ for data access instead
    // Uncomment the code below to re-enable the web server:
    /*
    let order_session_for_web = Arc::clone(&session);
    let market_data_for_web = Arc::clone(&market_data_session);
    let market_data_sessions_for_web = vec![
        Arc::clone(&market_data_session),
        Arc::clone(&market_data_others_34405),
        Arc::clone(&market_data_others_34406),
        Arc::clone(&market_data_others_34407),
    ];
    let web_server_handle = tokio::spawn(async move {
        if let Err(e) = order_book_web_server(order_session_for_web, market_data_for_web, market_data_sessions_for_web, "0.0.0.0:8080").await {
            error!("Fatal error in web server: {}", e);
        }
    });
    */
    
    // Open Orders CSV Generator - event-driven (updates immediately on execution reports)
    let open_orders_store = Arc::clone(&session.open_orders);
    let csv_update_rx = order_update_tx.subscribe();
    let _csv_generator_handle = tokio::spawn(async move {
        if let Err(e) = open_orders_csv_generator_event_driven(open_orders_store, csv_update_rx).await {
            error!("Fatal error in open orders CSV generator: {}", e);
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

    // Web server disabled - using kdb+ for data access instead
    // Console queries kdb+ directly via IPC

    // Wait for initial connections and logons
    info!("Waiting for connections and logons to complete...");
    sleep(Duration::from_secs(3)).await;

    // Send a single test order and verify successful placement
    info!("📊 Submitting single test order...");
    info!("");

    let test_order = OrderRequest {
        cl_ord_id: "TEST_ORDER_001".to_string(),
        symbol: "BTC_USDT".to_string(),  // Correct format with underscore
        side: '1',  // Buy
        qty: 0.001,
        price: Some(45000.0),
        ord_type: None,
        time_in_force: None,
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

    info!("Test Order Details:");
    info!("  Client Order ID: {}", test_order.cl_ord_id);
    info!("  Symbol: {}", test_order.symbol);
    info!("  Side: {} (1=Buy, 2=Sell)", test_order.side);
    info!("  Quantity: {}", test_order.qty);
    if let Some(price) = test_order.price {
        info!("  Price: {}", price);
    } else {
        info!("  Price: N/A (MARKET order)");
    }
    info!("");

    if order_tx.send(test_order.clone()).await.is_err() {
        error!("Failed to send test order to queue");
    } else {
        info!("✅ Test order queued for submission");
    }

    // Wait a bit for order to be processed
    info!("Waiting for order processing (5 seconds)...");
    sleep(Duration::from_secs(5)).await;

    // Send order status request to check order status
    info!("");
    info!("📋 Requesting order status...");
    if let Err(e) = session.order_status_request(&test_order.cl_ord_id, &test_order.symbol).await {
        error!("Failed to send order status request: {}", e);
    }

    // Wait for status response (30 seconds)
    info!("Waiting for status response (30 seconds)...");
    sleep(Duration::from_secs(30)).await;

    // Wait for market data responses (SecurityListRequest is sent immediately after logon in market_data_loop)
    info!("");
    info!("🔗 Waiting for market data server responses...");
    sleep(Duration::from_secs(20)).await;

    info!("");
    info!("═══════════════════════════════════════════════════════════");
    info!("Order latency test complete.");
    info!("═══════════════════════════════════════════════════════════");
    
    // Wait for order book to be populated before placing test orders
    info!("");
    info!("⏳ Waiting for order book to be populated...");
    let mut order_book_ready = false;
    for _ in 0..30 {
        // Check if order book has data
        let books = market_data_session.order_books.lock().await;
        if let Some(book) = books.get("BTC_USDT") {
            let bid = book.get_active().best_bid();
            let ask = book.get_active().best_ask();
            if bid > 0.0 && ask > 0.0 {
                info!("✅ Order book ready: Bid={:.2}, Ask={:.2}", bid, ask);
                order_book_ready = true;
                drop(books);
                break;
            }
        }
        drop(books);
        sleep(Duration::from_secs(1)).await;
    }
    
    if order_book_ready {
        // Place small test orders to generate trades
        info!("");
        if let Err(e) = market_data_session.place_test_orders_for_trades(Arc::clone(&session), "BTC_USDT", 4).await {
            warn!("Failed to place test orders: {}", e);
        }
        
        // Wait for trades to appear
        info!("");
        info!("⏳ Waiting 10 seconds for trades to appear in order book...");
        sleep(Duration::from_secs(10)).await;
        
        // Check trade count
        let books = market_data_session.order_books.lock().await;
        if let Some(book) = books.get("BTC_USDT") {
            let trade_count = book.get_active().trade_count();
            info!("📊 Current trade count in order book: {}", trade_count);
            if trade_count > 0 {
                let recent_trades = book.get_active().get_recent_trades(10);
                info!("   Recent trades:");
                for (price, size, timestamp) in recent_trades {
                    info!("     Price: {:.2}, Size: {}, Time: {}", price, size, timestamp);
                }
            }
        }
    } else {
        warn!("⚠️  Order book not ready, skipping test order placement");
    }
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
