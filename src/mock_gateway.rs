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
use tokio::time::{sleep, Duration, Instant};

use crate::fix_protocol::{tags, FIXMessage};

#[derive(Clone)]
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
    /// If Some(id), the mock sends a TestRequest (35=1) carrying this TestReqID
    /// immediately after Logon, and hangs up unless the client answers with a
    /// Heartbeat echoing it — which is what the live venue does, and what was
    /// disconnecting every session every 15-25 seconds.
    pub test_request_after_logon: Option<String>,
    /// Optional recorder of symbols requested via MarketDataRequest (35=V).
    pub md_requested_symbols: Option<Arc<Mutex<Vec<String>>>>,
    /// Optional counter of inbound NewOrderSingle (35=D). Incremented before ExecReport.
    pub orders_received: Option<Arc<Mutex<u64>>>,
    /// Optional two-sided snapshot BBO (prices + sizes) sent on 35=V.
    pub snapshot_bbo: Option<MockBbo>,
    /// Optional second 35=W after the snapshot, delayed so paper SC can rest quotes first.
    pub followup_bbo: Option<MockBbo>,
}

#[derive(Clone, Debug)]
pub struct MockBbo {
    pub bid: f64,
    pub ask: f64,
    pub bid_size: f64,
    pub ask_size: f64,
}

impl Default for MockGatewayConfig {
    fn default() -> Self {
        MockGatewayConfig {
            venue_comp_id: "MOCK.VENUE".to_string(),
            md_ticks_to_stream: 0,
            induce_gap_at_tick: None,
            disconnect_after_messages: None,
            test_request_after_logon: None,
            md_requested_symbols: None,
            orders_received: None,
            snapshot_bbo: None,
            followup_bbo: None,
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
    // Set when we send a TestRequest; cleared by a Heartbeat echoing its
    // TestReqID. If it is still set when the grace period expires we hang up,
    // which is how the live venue behaves toward an unresponsive session.
    let mut awaiting_test_req_id: Option<String> = None;
    let probe_grace = Duration::from_secs(2);
    let mut probe_elapsed = Duration::ZERO;
    let mut probe_due_at: Option<Instant> = None;

    loop {
        let n = match tokio::time::timeout(Duration::from_millis(250), socket.read(&mut read_buf)).await {
            Ok(r) => r?,
            Err(_) => {
                // Read timed out. Fire a scheduled probe if it is due.
                if let Some(due) = probe_due_at {
                    if Instant::now() >= due {
                        probe_due_at = None;
                        if let Some(id) = config.test_request_after_logon.clone() {
                            let mut probe = next_message(&out_seq, &config, '1').await;
                            probe.set_field(tags::TEST_REQ_ID, &id);
                            send(&mut socket, &probe).await?;
                            awaiting_test_req_id = Some(id);
                            probe_elapsed = Duration::ZERO;
                        }
                    }
                }
                // If a TestRequest is outstanding past its grace period, the
                // client never answered: drop the session.
                if awaiting_test_req_id.is_some() {
                    probe_elapsed += probe_grace.min(Duration::from_millis(250));
                    if probe_elapsed >= probe_grace {
                        info!("Mock gateway: TestRequest unanswered - closing session");
                        return Ok(());
                    }
                }
                continue;
            }
        };
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

                    // Schedule the probe rather than sending it inline: a real
                    // venue probes an established session, and firing mid-logon
                    // makes the client's sequencer see a gap it cannot yet act on.
                    if config.test_request_after_logon.is_some() {
                        probe_due_at = Some(Instant::now() + Duration::from_millis(500));
                    }
                }
                '0' => {
                    // A Heartbeat echoing our TestReqID clears the probe. One that
                    // does not is ignored, exactly as a real venue would: the
                    // session stays unproven and gets hung up on below.
                    if let Some(expected) = awaiting_test_req_id.clone() {
                        if msg.get_field(tags::TEST_REQ_ID) == Some(expected.as_str()) {
                            awaiting_test_req_id = None;
                        }
                    }
                    let resp = next_message(&out_seq, &config, '0').await;
                    send(&mut socket, &resp).await?;
                }
                'D' => {
                    if let Some(counter) = &config.orders_received {
                        *counter.lock().await += 1;
                    }
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

                    if let Some(rec) = &config.md_requested_symbols {
                        rec.lock().await.push(symbol.clone());
                    }

                    let (bid, ask, bid_sz, ask_sz) = match &config.snapshot_bbo {
                        Some(b) => (b.bid, b.ask, b.bid_size, b.ask_size),
                        None => (100.0, 100.5, 1.0, 1.0),
                    };
                    send_snapshot(&mut socket, &out_seq, &config, &symbol, &md_req_id, bid, ask, bid_sz, ask_sz).await?;

                    if let Some(follow) = &config.followup_bbo {
                        // Delay so a paper SpreadCapture instance can rest quotes on the
                        // first book before the crossing snapshot arrives.
                        sleep(Duration::from_millis(200)).await;
                        send_snapshot(
                            &mut socket,
                            &out_seq,
                            &config,
                            &symbol,
                            &md_req_id,
                            follow.bid,
                            follow.ask,
                            follow.bid_size,
                            follow.ask_size,
                        )
                        .await?;
                    }

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

                    send_snapshot(&mut socket, &out_seq, &config, "BTC_USDT", "resync", 100.0, 100.5, 1.0, 1.0).await?;
                }
                '5' => {
                    let resp = next_message(&out_seq, &config, '5').await;
                    send(&mut socket, &resp).await?;
                    return Ok(());
                }
                'H' => {
                    // OrderStatusRequest. Lookup ClOrdID in the mock venue book.
                    let cl_ord_id = msg.get_field(tags::CL_ORD_ID).unwrap_or_default().to_string();
                    let order = orders.lock().await.get(&cl_ord_id).cloned();

                    let mut resp = next_message(&out_seq, &config, '8').await;
                    resp.set_field(tags::CL_ORD_ID, &cl_ord_id); // MUST echo 11

                    if let Some(o) = order {
                        // Found — still working. 150=I must NOT be 8/4/2; LeavesQty must be > 0.
                        resp.set_field(tags::ORDER_ID, &format!("SRV_{}", cl_ord_id));
                        resp.set_field(tags::EXEC_ID, &format!("EXEC_{}", cl_ord_id));
                        resp.set_field(150, "I"); // ExecType = Order Status
                        resp.set_field(tags::ORD_STATUS, "0"); // OrdStatus = New
                        resp.set_field(tags::SYMBOL, &o.symbol);
                        resp.set_field(tags::SIDE, &o.side);
                        resp.set_field(tags::PRICE, &o.price);
                        resp.set_field(tags::ORDER_QTY, &o.qty);
                        resp.set_field(tags::LEAVES_QTY, &o.qty);
                        resp.set_field(14, "0"); // CumQty — same as 'D' New ack
                    } else {
                        // Not found — unknown to venue. Eviction key is 150=8 + LeavesQty=0 + tag 11.
                        resp.set_field(150, "8"); // ExecType = Rejected
                        resp.set_field(tags::ORD_STATUS, "8"); // OrdStatus = Rejected
                        resp.set_field(tags::LEAVES_QTY, "0");
                        resp.set_field(tags::TEXT, "Unknown order");
                    }
                    send(&mut socket, &resp).await?;
                }
                other => {
                    info!("Mock gateway: unhandled message type '{}', sending BusinessMessageReject", other);
                    let ref_seq = msg.get_field(tags::MSG_SEQ_NUM).unwrap_or("0");
                    let mut reject = next_message(&out_seq, &config, 'j').await;
                    reject.set_field(45, ref_seq);             // RefSeqNum
                    reject.set_field(372, &other.to_string()); // RefMsgType
                    reject.set_field(380, "3");                // Unsupported Message Type
                    send(&mut socket, &reject).await?;
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
    bid_size: f64,
    ask_size: f64,
) -> Result<()> {
    let mut snapshot = next_message(out_seq, config, 'W').await;
    snapshot.set_field(tags::MARKET_DATA_REQ_ID, md_req_id);
    snapshot.set_field(tags::SYMBOL, symbol);
    snapshot.set_field(268, "2"); // NoMDEntries
    snapshot.set_field(269, "0"); // MDEntryType = Bid
    snapshot.set_field(270, &bid.to_string());
    snapshot.set_field(271, &bid_size.to_string());
    snapshot.set_field(269, "1"); // MDEntryType = Ask
    snapshot.set_field(270, &ask.to_string());
    snapshot.set_field(271, &ask_size.to_string());
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
    // Frame exactly one FIX message from the front of the buffer, using
    // BodyLength (9). Locating the checksum with rfind("10=") over the whole
    // buffer consumes every coalesced frame as one message — reconcile sends
    // sequential 35=H and those land in a single TCP read.
    const SOH: u8 = 0x01;
    const TRAILER_LEN: usize = 7; // "10=" + 3 digits + SOH
    if buf.len() < 10 || !buf.starts_with(b"8=") {
        return None;
    }
    let p1 = buf.iter().position(|&b| b == SOH)?;
    if buf.len() < p1 + 3 || &buf[p1 + 1..p1 + 3] != b"9=" {
        return None;
    }
    let rel = buf[p1 + 3..].iter().position(|&b| b == SOH)?;
    let p2 = p1 + 3 + rel;
    let body_len: usize = std::str::from_utf8(&buf[p1 + 3..p2]).ok()?.parse().ok()?;
    let trailer_start = p2 + 1 + body_len;
    let message_end = trailer_start + TRAILER_LEN;
    if buf.len() < message_end {
        return None;
    }
    if &buf[trailer_start..trailer_start + 3] != b"10=" || buf[message_end - 1] != SOH {
        return None;
    }
    let msg = FIXMessage::decode(&buf[..message_end]).ok()?;
    Some((msg, message_end))
}
