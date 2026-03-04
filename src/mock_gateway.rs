use anyhow::Result;
use log::info;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{interval, Duration};

use crate::fix_protocol::{FIXMessage, tags};

/// Fake securities returned by the SecurityList response.
const MOCK_SYMBOLS: &[&str] = &["BTC_USDT", "ETH_USDT", "SOL_USDT"];

/// Mock FIX Gateway for testing.
/// Handles all message types needed for end-to-end integration tests:
///   A  – Logon
///   0  – Heartbeat
///   D  – NewOrderSingle  → ExecutionReport (New)
///   F  – CancelOrder     → ExecutionReport (Canceled)
///   x  – SecurityListRequest → SecurityList (y)
///   V  – MarketDataRequest   → MarketDataFullRefresh (W)
/// After a MarketDataRequest the gateway also emits incremental refreshes (X)
/// on a 500ms timer to simulate a live feed.
pub async fn start_mock_gateway(addr: &str) -> Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("Mock FIX Gateway listening on {}", addr);

    loop {
        let (socket, peer_addr) = listener.accept().await?;
        info!("New connection from {}", peer_addr);
        tokio::spawn(async move {
            if let Err(e) = handle_client(socket).await {
                log::error!("Client error: {}", e);
            }
        });
    }
}

async fn handle_client(socket: TcpStream) -> Result<()> {
    let (mut reader, writer) = tokio::io::split(socket);
    let writer = std::sync::Arc::new(tokio::sync::Mutex::new(writer));

    // Ticker task — emits incremental refreshes for subscribed symbols.
    // The channel receives a list of subscribed symbols when a V message arrives.
    let (md_tx, mut md_rx) = tokio::sync::mpsc::channel::<Vec<String>>(4);
    let writer_ticker = writer.clone();
    tokio::spawn(async move {
        let mut subscribed: Vec<String> = Vec::new();
        let mut tick = interval(Duration::from_millis(500));
        let mut seq: u32 = 100;
        loop {
            tokio::select! {
                Some(syms) = md_rx.recv() => { subscribed = syms; }
                _ = tick.tick() => {
                    if subscribed.is_empty() { continue; }
                    for sym in &subscribed {
                        let msg = build_incremental_refresh(sym, seq);
                        seq += 1;
                        if let Ok(encoded) = msg.encode() {
                            let mut w = writer_ticker.lock().await;
                            let _ = w.write_all(&encoded).await;
                            let _ = w.flush().await;
                        }
                    }
                }
            }
        }
    });

    let mut buffer = vec![0u8; 8192];
    let mut seq: u32 = 1;

    loop {
        let n = reader.read(&mut buffer).await?;
        if n == 0 {
            info!("Client disconnected");
            return Ok(());
        }

        match FIXMessage::decode(&buffer[..n]) {
            Ok(msg) => {
                info!("Mock GW received msg_type={}", msg.msg_type);
                seq += 1;

                let response_opt: Option<Vec<FIXMessage>> = match msg.msg_type {
                    // ── Logon ──────────────────────────────────────────────────
                    'A' => {
                        let mut resp = FIXMessage::new('A', "UAT1.CDC.UD", "UAT1.WOZINGA.UD.00", seq);
                        resp.set_field(tags::ENCRYPT_METHOD, "0");
                        resp.set_field(tags::HEARTBEAT_INT, "30");
                        Some(vec![resp])
                    }

                    // ── Heartbeat ──────────────────────────────────────────────
                    '0' => {
                        let resp = FIXMessage::new('0', "UAT1.CDC.UD", "UAT1.WOZINGA.UD.00", seq);
                        Some(vec![resp])
                    }

                    // ── NewOrderSingle → ExecutionReport (New) ─────────────────
                    'D' => {
                        let mut resp = FIXMessage::new('8', "UAT1.CDC.UD", "UAT1.WOZINGA.UD.00", seq);
                        if let Some(id) = msg.get_field(tags::CL_ORD_ID) {
                            resp.set_field(tags::CL_ORD_ID, id);
                            resp.set_field(tags::ORDER_ID, &format!("SRV_{}", id));
                        }
                        if let Some(sym) = msg.get_field(tags::SYMBOL) {
                            resp.set_field(tags::SYMBOL, sym);
                        }
                        resp.set_field(tags::EXEC_ID, &format!("EXEC_{}", seq));
                        resp.set_field(tags::EXEC_STATUS, "0"); // 0 = New
                        resp.set_field(151, "0");  // LeavesQty
                        Some(vec![resp])
                    }

                    // ── CancelOrder → ExecutionReport (Canceled) ───────────────
                    'F' => {
                        let mut resp = FIXMessage::new('8', "UAT1.CDC.UD", "UAT1.WOZINGA.UD.00", seq);
                        if let Some(id) = msg.get_field(tags::CL_ORD_ID) {
                            resp.set_field(tags::CL_ORD_ID, id);
                        }
                        if let Some(orig) = msg.get_field(tags::ORIG_CL_ORD_ID) {
                            resp.set_field(tags::ORIG_CL_ORD_ID, orig);
                            resp.set_field(tags::ORDER_ID, &format!("SRV_{}", orig));
                        }
                        if let Some(sym) = msg.get_field(tags::SYMBOL) {
                            resp.set_field(tags::SYMBOL, sym);
                        }
                        resp.set_field(tags::EXEC_ID, &format!("EXEC_{}", seq));
                        resp.set_field(tags::EXEC_STATUS, "4"); // 4 = Canceled
                        resp.set_field(151, "0");  // LeavesQty
                        Some(vec![resp])
                    }

                    // ── SecurityListRequest → SecurityList ─────────────────────
                    'x' => {
                        let mut resp = FIXMessage::new('y', "UAT1.CDC.MD", "UAT1.WOZINGA.MD.00", seq);
                        if let Some(req_id) = msg.get_field(tags::SECURITY_REQ_ID) {
                            resp.set_field(tags::SECURITY_REQ_ID, req_id);
                        }
                        resp.set_field(560, "0");  // SecurityRequestResult = Valid request
                        resp.set_field(146, &MOCK_SYMBOLS.len().to_string()); // NoRelatedSym
                        for sym in MOCK_SYMBOLS {
                            resp.set_field(tags::SYMBOL, sym);
                        }
                        Some(vec![resp])
                    }

                    // ── MarketDataRequest → Full Refresh + subscribe ticker ────
                    'V' => {
                        // Determine which symbols were requested (repeating group tag 146)
                        let groups = msg.get_repeating_group(146, 55);
                        let requested: Vec<String> = if groups.is_empty() {
                            MOCK_SYMBOLS.iter().map(|s| s.to_string()).collect()
                        } else {
                            groups.iter()
                                .filter_map(|g| g.get(&55).cloned())
                                .collect()
                        };

                        // Send a Full Refresh (W) for each requested symbol
                        let mut responses = Vec::new();
                        for sym in &requested {
                            responses.push(build_full_refresh(sym, seq));
                            seq += 1;
                        }

                        // Start incremental refresh ticker for these symbols
                        let _ = md_tx.try_send(requested);
                        Some(responses)
                    }

                    // ── Logout ─────────────────────────────────────────────────
                    '5' => {
                        let resp = FIXMessage::new('5', "UAT1.CDC.UD", "UAT1.WOZINGA.UD.00", seq);
                        let encoded = resp.encode()?;
                        let mut w = writer.lock().await;
                        w.write_all(&encoded).await?;
                        w.flush().await?;
                        return Ok(());
                    }

                    other => {
                        info!("Mock GW: unhandled msg_type={}", other);
                        None
                    }
                };

                if let Some(responses) = response_opt {
                    let mut w = writer.lock().await;
                    for resp in responses {
                        let encoded = resp.encode()?;
                        w.write_all(&encoded).await?;
                    }
                    w.flush().await?;
                }
            }
            Err(e) => {
                log::error!("Failed to decode FIX message: {}", e);
            }
        }
    }
}

/// Build a MarketDataFullRefresh (W) with two bid levels and two ask levels.
fn build_full_refresh(symbol: &str, seq: u32) -> FIXMessage {
    let mut msg = FIXMessage::new('W', "UAT1.CDC.MD", "UAT1.WOZINGA.MD.00", seq);
    msg.set_field(tags::SYMBOL, symbol);
    msg.set_field(tags::MARKET_DATA_REQ_ID, "MD_REQ_1");

    // NoMDEntries = 4
    msg.set_field(tags::NO_MD_ENTRIES, "4");

    // Bid 1
    msg.set_field(tags::MD_ENTRY_TYPE, "0");   // 0 = Bid
    msg.set_field(tags::MD_ENTRY_PX, "50000.00");
    msg.set_field(tags::MD_ENTRY_SIZE, "1.0");
    // Bid 2
    msg.set_field(tags::MD_ENTRY_TYPE, "0");
    msg.set_field(tags::MD_ENTRY_PX, "49990.00");
    msg.set_field(tags::MD_ENTRY_SIZE, "2.0");
    // Ask 1
    msg.set_field(tags::MD_ENTRY_TYPE, "1");   // 1 = Ask
    msg.set_field(tags::MD_ENTRY_PX, "50010.00");
    msg.set_field(tags::MD_ENTRY_SIZE, "1.0");
    // Ask 2
    msg.set_field(tags::MD_ENTRY_TYPE, "1");
    msg.set_field(tags::MD_ENTRY_PX, "50020.00");
    msg.set_field(tags::MD_ENTRY_SIZE, "2.0");

    msg
}

/// Build a MarketDataIncrementalRefresh (X) with a single updated bid level.
fn build_incremental_refresh(symbol: &str, seq: u32) -> FIXMessage {
    use std::time::{SystemTime, UNIX_EPOCH};
    let jitter = (SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_millis() % 10) as f64;
    let new_px = 50000.0 + jitter;

    let mut msg = FIXMessage::new('X', "UAT1.CDC.MD", "UAT1.WOZINGA.MD.00", seq);
    msg.set_field(tags::MARKET_DATA_REQ_ID, "MD_REQ_1");
    msg.set_field(tags::NO_MD_ENTRIES, "1");
    msg.set_field(279, "1");               // MDUpdateAction = Change
    msg.set_field(tags::MD_ENTRY_TYPE, "0"); // Bid
    msg.set_field(tags::SYMBOL, symbol);
    msg.set_field(tags::MD_ENTRY_PX, &format!("{:.2}", new_px));
    msg.set_field(tags::MD_ENTRY_SIZE, "1.5");
    msg
}
