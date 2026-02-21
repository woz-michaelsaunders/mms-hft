use anyhow::Result;
use log::info;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::fix_protocol::{FIXMessage, tags};

/// Mock FIX Gateway for testing
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

async fn handle_client(mut socket: TcpStream) -> Result<()> {
    let mut buffer = vec![0; 4096];

    loop {
        let n = socket.read(&mut buffer).await?;
        if n == 0 {
            info!("Client disconnected");
            return Ok(());
        }

        buffer.truncate(n);

        // Parse incoming message
        match FIXMessage::decode(&buffer) {
            Ok(msg) => {
                info!("Received message type: {}", msg.msg_type);

                // Send appropriate response
                let response = match msg.msg_type {
                    'A' => {
                        // Logon request - send logon response
                        let mut resp = FIXMessage::new(
                            'A',
                            "UAT1.CDC.UD",
                            "UAT1.WOZINGA.UD.00",
                            msg.seq_num,
                        );
                        resp.set_field(tags::ENCRYPT_METHOD, "0");
                        resp.set_field(tags::HEARTBEAT_INT, "30");
                        resp
                    }
                    '0' => {
                        // Heartbeat - send heartbeat response
                        FIXMessage::new(
                            '0',
                            "UAT1.CDC.UD",
                            "UAT1.WOZINGA.UD.00",
                            msg.seq_num,
                        )
                    }
                    'D' => {
                        // New Order Single - send execution report
                        let mut resp = FIXMessage::new(
                            '8',
                            "UAT1.CDC.UD",
                            "UAT1.WOZINGA.UD.00",
                            msg.seq_num,
                        );
                        if let Some(cl_ord_id) = msg.get_field(tags::CL_ORD_ID) {
                            resp.set_field(tags::CL_ORD_ID, cl_ord_id);
                            resp.set_field(tags::ORDER_ID, &format!("SRV_{}", cl_ord_id));
                        }
                        resp.set_field(tags::EXEC_ID, "1");
                        resp.set_field(tags::EXEC_STATUS, "0"); // New
                        resp
                    }
                    '5' => {
                        // Logout
                        info!("Client logout request");
                        let resp = FIXMessage::new(
                            '5',
                            "UAT1.CDC.UD",
                            "UAT1.WOZINGA.UD.00",
                            msg.seq_num,
                        );
                        socket.write_all(&resp.encode()?).await?;
                        return Ok(());
                    }
                    _ => {
                        info!("Unknown message type: {}", msg.msg_type);
                        continue;
                    }
                };

                // Send response
                let encoded = response.encode()?;
                socket.write_all(&encoded).await?;
                socket.flush().await?;
                info!("Sent response for message type: {}", response.msg_type);
            }
            Err(e) => {
                log::error!("Failed to decode message: {}", e);
            }
        }
    }
}
