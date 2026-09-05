// Mock FIX counterparty used by integration tests (see #[cfg(test)] in
// main.rs) to exercise the real `FIXSession` against a real TCP/FIX
// exchange without touching the sandbox venue. Supports the message types
// the acceptance criteria require coverage for:
//   - Logon / Heartbeat / Logout (session lifecycle)
//   - NewOrderSingle -> ExecutionReport(New)
//   - OrderCancelRequest -> ExecutionReport(Canceled)
//   - OrderCancelReplaceRequest -> ExecutionReport(Replaced)
//   - MarketDataRequest -> MarketDataSnapshotFullRefresh, then an optional
//     stream of MarketDataIncrementalRefresh updates (with an optional
//     induced sequence gap, to exercise AC2 recovery end-to-end)
//   - ResendRequest -> SequenceReset-GapFill + a fresh full snapshot
//     (this venue's actual recovery mechanism: re-snapshot rather than
//     replay individual historical deltas)
// It can also simulate a mid-session disconnect (AC4) by closing the socket
// after a configured number of inbound messages.

use anyhow::Result;
use log::info;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

use crate::fix_protocol::{tags, FIXMessage};

#[derive(Clone, Debug)]
pub struct MockGatewayConfig {
    /// CompID the mock identifies itself as (becomes TargetCompID from the client's POV).
    pub venue_comp_id: String,
    /// How many MarketDataIncrementalRefresh ticks to stream per subscribed symbol after the snapshot.
    pub md_ticks_to_stream: u32,
    /// If Some(n), the mock silently skips transmitting the n-th incremental tick
    /// (while still consuming its MsgSeqNum) to induce a sequence gap for AC2 testing.
    pub induce_gap_at_tick: Option<u32>,
    /// If Some(n), the mock closes the connection after processing n inbound
    /// application messages (Logon doesn't count), to exercise reconnect (AC4).
    pub disconnect_after_messages: Option<u32>,
}

impl Default for MockGatewayConfig {
    fn default() -> Self {
        MockGatewayConfig {
            venue_comp_id: "MOCK.VENUE".to_string(),
            md_ticks_to_stream: 0,
            induce_gap_at_tick: None,
            disconnect_after_messages: None,
        }
    }
}

/// Start the mock gateway on `addr` (e.g. "127.0.0.1:0" to let the OS pick a
/// free port) and return the port it actually bound to.
pub async fn start_mock_gateway(addr: &str, config: MockGatewayConfig) -> Result<u16> {
    let listener = TcpListener::bind(addr).await?;
    let port = listener.local_addr()?.port();
    info!("Mock FIX Gateway listening on {}", listener.local_addr()?);

    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((socket, peer_addr)) => {
                    info!("Mock gateway: new connection from {}", peer_addr);
                    let cfg = config.clone();
                    tokio::spawn(async move {
                        if let Err(e) = handle_client(socket, cfg).await {
                            log::debug!("Mock gateway client session ended: {}", e);
                        }
                    });
                }
                Err(e) => {
                    log::error!("Mock gateway accept error: {}", e);
                    return;
                }
            }
        }
    });

    Ok(port)
}

#[derive(Clone)]
struct OpenOrder {
    symbol: String,
    side: String,
    price: String,
    qty: String,
}

async fn handle_client(mut socket: TcpStream, config: MockGatewayConfig) -> Result<()> {
    let mut buffer = Vec::new();
    let mut read_buf = [0u8; 4096];
    let out_seq = Arc::new(Mutex::new(0u32));
    let orders: Arc<Mutex<HashMap<String, OpenOrder>>> = Arc::new(Mutex::new(HashMap::new()));
    let mut app_messages_seen: u32 = 0;
    // The induced gap fires at most once per connection: a resubscribe
    // triggered by the client's own recovery flow shouldn't re-trigger it,
    // or recovery would never converge.
    let gap_already_used = Arc::new(std::sync::atomic::AtomicBool::new(false));

    loop {
        let n = socket.read(&mut read_buf).await?;
        if n == 0 {
            info!("Mock gateway: client disconnected");
            return Ok(());
        }
        buffer.extend_from_slice(&read_buf[..n]);

        // Drain as many complete messages as are buffered.
        while let Some((msg, consumed)) = try_extract(&buffer) {
            buffer.drain(..consumed);

            if msg.msg_type != 'A' && msg.msg_type != '0' {
                app_messages_seen += 1;
            }

            match msg.msg_type {
                'A' => {
                    let resp = next_message(&out_seq, &config, 'A').await;
                    let mut resp = resp;
                    resp.set_field(tags::ENCRYPT_METHOD, "0");
                    resp.set_field(tags::HEARTBEAT_INT, "30");
                    send(&mut socket, &resp).await?;
                }
                '0' => {
                    let resp = next_message(&out_seq, &config, '0').await;
                    send(&mut socket, &resp).await?;
                }
                'D' => {
                    let cl_ord_id = msg.get_field(tags::CL_ORD_ID).unwrap_or_default().to_string();
                    let symbol = msg.get_field(tags::SYMBOL).unwrap_or_default().to_string();
                    let side = msg.get_field(tags::SIDE).unwrap_or_default().to_string();
                    let price = msg.get_field(tags::PRICE).unwrap_or_default().to_string();
                    let qty = msg.get_field(tags::ORDER_QTY).unwrap_or_default().to_string();

                    orders.lock().await.insert(
                        cl_ord_id.clone(),
                        OpenOrder { symbol: symbol.clone(), side: side.clone(), price: price.clone(), qty: qty.clone() },
                    );

                    let mut resp = next_message(&out_seq, &config, '8').await;
                    resp.set_field(tags::CL_ORD_ID, &cl_ord_id);
                    resp.set_field(tags::ORDER_ID, &format!("SRV_{}", cl_ord_id));
                    resp.set_field(tags::EXEC_ID, &format!("EXEC_{}", cl_ord_id));
                    resp.set_field(150, "0"); // ExecType = New
                    resp.set_field(tags::ORD_STATUS, "0"); // OrdStatus = New
                    resp.set_field(tags::SYMBOL, &symbol);
                    resp.set_field(tags::SIDE, &side);
                    resp.set_field(tags::PRICE, &price);
                    resp.set_field(tags::ORDER_QTY, &qty);
                    resp.set_field(tags::LEAVES_QTY, &qty);
                    resp.set_field(14, "0"); // CumQty
                    send(&mut socket, &resp).await?;
                }
                'F' => {
                    let cl_ord_id = msg.get_field(tags::CL_ORD_ID).unwrap_or_default().to_string();
                    let orig = msg.get_field(tags::ORIG_CL_ORD_ID).unwrap_or_default().to_string();
                    let order = orders.lock().await.get(&orig).cloned();

                    let mut resp = next_message(&out_seq, &config, '8').await;
                    resp.set_field(tags::CL_ORD_ID, &cl_ord_id);
                    resp.set_field(tags::ORIG_CL_ORD_ID, &orig);
                    resp.set_field(tags::ORDER_ID, &format!("SRV_{}", orig));
                    resp.set_field(tags::EXEC_ID, &format!("EXEC_{}", cl_ord_id));
                    resp.set_field(150, "4"); // ExecType = Canceled
                    resp.set_field(tags::ORD_STATUS, "4"); // OrdStatus = Canceled
                    resp.set_field(tags::LEAVES_QTY, "0");
                    if let Some(o) = order {
                        resp.set_field(tags::SYMBOL, &o.symbol);
                        resp.set_field(tags::SIDE, &o.side);
                    }
                    orders.lock().await.remove(&orig);
                    send(&mut socket, &resp).await?;
                }
                'G' => {
                    // OrderCancelReplaceRequest: new ClOrdID (11) replaces OrigClOrdID (41).
                    let cl_ord_id = msg.get_field(tags::CL_ORD_ID).unwrap_or_default().to_string();
                    let orig = msg.get_field(tags::ORIG_CL_ORD_ID).unwrap_or_default().to_string();
                    let new_price = msg.get_field(tags::PRICE).map(|s| s.to_string());
                    let new_qty = msg.get_field(tags::ORDER_QTY).map(|s| s.to_string());

                    let mut orders_guard = orders.lock().await;
                    let existing = orders_guard.remove(&orig);
                    let (symbol, side, price, qty) = match existing {
                        Some(o) => (
                            o.symbol,
                            o.side,
                            new_price.unwrap_or(o.price),
                            new_qty.unwrap_or(o.qty),
                        ),
                        None => (
                            msg.get_field(tags::SYMBOL).unwrap_or_default().to_string(),
                            msg.get_field(tags::SIDE).unwrap_or_default().to_string(),
                            new_price.unwrap_or_default(),
                            new_qty.unwrap_or_default(),
                        ),
                    };
                    orders_guard.insert(
                        cl_ord_id.clone(),
                        OpenOrder { symbol: symbol.clone(), side: side.clone(), price: price.clone(), qty: qty.clone() },
                    );
                    drop(orders_guard);

                    let mut resp = next_message(&out_seq, &config, '8').await;
                    resp.set_field(tags::CL_ORD_ID, &cl_ord_id);
                    resp.set_field(tags::ORIG_CL_ORD_ID, &orig);
                    resp.set_field(tags::ORDER_ID, &format!("SRV_{}", cl_ord_id));
                    resp.set_field(tags::EXEC_ID, &format!("EXEC_{}", cl_ord_id));
                    resp.set_field(150, "5"); // ExecType = Replaced
                    resp.set_field(tags::ORD_STATUS, "5"); // OrdStatus = Replaced
                    resp.set_field(tags::SYMBOL, &symbol);
                    resp.set_field(tags::SIDE, &side);
                    resp.set_field(tags::PRICE, &price);
                    resp.set_field(tags::ORDER_QTY, &qty);
                    resp.set_field(tags::LEAVES_QTY, &qty);
                    resp.set_field(14, "0");
                    send(&mut socket, &resp).await?;
                }
                'V' => {
                    let symbol = msg.get_field(tags::SYMBOL).unwrap_or("UNKNOWN").to_string();
                    let md_req_id = msg.get_field(tags::MARKET_DATA_REQ_ID).unwrap_or("mdreq").to_string();

                    send_snapshot(&mut socket, &out_seq, &config, &symbol, &md_req_id, 100.0, 100.5).await?;

                    if config.md_ticks_to_stream > 0 {
                        stream_incrementals(&mut socket, &out_seq, &config, &symbol, &md_req_id, &gap_already_used).await?;
                    }
                }
                '2' => {
                    // ResendRequest: acknowledge the gap with SequenceReset-GapFill,
                    // then recover the caller with a fresh full snapshot per known symbol
                    // (this venue's real recovery path is re-snapshot, not per-message replay).
                    let begin = msg.get_field(tags::BEGIN_SEQ_NO).and_then(|s| s.parse::<u32>().ok()).unwrap_or(1);
                    let mut reset = next_message(&out_seq, &config, '4').await;
                    let new_seq = { *out_seq.lock().await + 1 };
                    reset.set_field(tags::BEGIN_SEQ_NO, &begin.to_string());
                    reset.set_field(tags::NEW_SEQ_NO, &new_seq.to_string());
                    reset.set_field(tags::GAP_FILL_FLAG, "Y");
                    send(&mut socket, &reset).await?;

                    send_snapshot(&mut socket, &out_seq, &config, "BTC_USDT", "resync", 100.0, 100.5).await?;
                }
                '5' => {
                    let resp = next_message(&out_seq, &config, '5').await;
                    send(&mut socket, &resp).await?;
                    return Ok(());
                }
                other => {
                    info!("Mock gateway: unhandled message type '{}', ignoring", other);
                }
            }

            if let Some(limit) = config.disconnect_after_messages {
                if app_messages_seen >= limit {
                    info!("Mock gateway: simulating disconnect after {} messages", app_messages_seen);
                    return Ok(());
                }
            }
        }
    }
}

async fn next_message(out_seq: &Arc<Mutex<u32>>, config: &MockGatewayConfig, msg_type: char) -> FIXMessage {
    let mut seq = out_seq.lock().await;
    *seq += 1;
    FIXMessage::new(msg_type, &config.venue_comp_id, "CLIENT", *seq)
}

async fn send(socket: &mut TcpStream, msg: &FIXMessage) -> Result<()> {
    let encoded = msg.encode()?;
    socket.write_all(&encoded).await?;
    socket.flush().await?;
    Ok(())
}

async fn send_snapshot(
    socket: &mut TcpStream,
    out_seq: &Arc<Mutex<u32>>,
    config: &MockGatewayConfig,
    symbol: &str,
    md_req_id: &str,
    bid: f64,
    ask: f64,
) -> Result<()> {
    let mut snapshot = next_message(out_seq, config, 'W').await;
    snapshot.set_field(tags::MARKET_DATA_REQ_ID, md_req_id);
    snapshot.set_field(tags::SYMBOL, symbol);
    snapshot.set_field(268, "2"); // NoMDEntries
    snapshot.set_field(269, "0"); // MDEntryType = Bid
    snapshot.set_field(270, &bid.to_string());
    snapshot.set_field(271, "1.0");
    snapshot.set_field(269, "1"); // MDEntryType = Ask
    snapshot.set_field(270, &ask.to_string());
    snapshot.set_field(271, "1.0");
    send(socket, &snapshot).await
}

/// Stream `config.md_ticks_to_stream` incremental updates. If
/// `config.induce_gap_at_tick` is set, that tick's MsgSeqNum is consumed but
/// never transmitted, producing a real, observable gap on the wire.
async fn stream_incrementals(
    socket: &mut TcpStream,
    out_seq: &Arc<Mutex<u32>>,
    config: &MockGatewayConfig,
    symbol: &str,
    md_req_id: &str,
    gap_already_used: &Arc<std::sync::atomic::AtomicBool>,
) -> Result<()> {
    for tick in 1..=config.md_ticks_to_stream {
        let mut msg = next_message(out_seq, config, 'X').await;

        if config.induce_gap_at_tick == Some(tick)
            && gap_already_used
                .compare_exchange(false, true, std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst)
                .is_ok()
        {
            // Consume the sequence number silently — the client will see a
            // jump in MsgSeqNum on the next message it does receive.
            continue;
        }

        msg.set_field(tags::MARKET_DATA_REQ_ID, md_req_id);
        msg.set_field(tags::SYMBOL, symbol);
        msg.set_field(268, "1");
        msg.set_field(279, "1"); // MDUpdateAction = Change
        msg.set_field(269, "0"); // MDEntryType = Bid
        msg.set_field(270, &(100.0 + tick as f64 * 0.01).to_string());
        msg.set_field(271, "1.0");
        send(socket, &msg).await?;
        sleep(Duration::from_micros(200)).await;
    }
    Ok(())
}

/// Extract one complete FIX message (returns the message plus the number of
/// bytes it consumed from `buf`) if `buf` currently holds one, else `None`.
fn try_extract(buf: &[u8]) -> Option<(FIXMessage, usize)> {
    let s = String::from_utf8_lossy(buf);
    if !s.starts_with("8=") {
        return None;
    }
    let pos = s.find("9=")?;
    let after = &s[pos + 2..];
    let end = after.find('\x01')?;
    let body_len: usize = after[..end].parse().ok()?;
    let body_start = pos + s[pos..].find('\x01')? + 1;
    let checksum_search_start = body_start + body_len;
    let last_10 = s.rfind("10=")?;
    if last_10 < checksum_search_start {
        return None;
    }
    let checksum_end = s[last_10..].find('\x01')?;
    let message_end = last_10 + checksum_end + 1;
    if buf.len() < message_end {
        return None;
    }
    let msg = FIXMessage::decode(&buf[..message_end]).ok()?;
    Some((msg, message_end))
}
