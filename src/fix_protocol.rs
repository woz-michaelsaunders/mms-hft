use anyhow::{anyhow, Result};
use chrono::Utc;
use std::collections::HashMap;

/// FIX Message types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    Logon = 65,                     // A
    Logout = 53,                    // 5
    Heartbeat = 48,                 // 0
    TestRequest = 49,               // 1
    ResendRequest = 50,             // 2
    Reject = 51,                    // 3
    SequenceReset = 52,             // 4
    NewOrderSingle = 68,            // D
    ExecutionReport = 56,           // 8
    CancelOrder = 70,               // F
    CancelReplaceRequest = 71,      // G
    CancelReject = 89,              // Y
    OrderStatusRequest = 72,        // H
    MarketDataRequest = 86,         // V
    MarketDataSnapshotFullRefresh = 87, // W
    MarketDataIncrementalRefresh = 88,  // X
    BusinessMessageReject = 106,    // j
}

impl MessageType {
    pub fn from_char(c: char) -> Option<Self> {
        match c {
            'A' => Some(MessageType::Logon),
            '5' => Some(MessageType::Logout),
            '0' => Some(MessageType::Heartbeat),
            '1' => Some(MessageType::TestRequest),
            '2' => Some(MessageType::ResendRequest),
            '3' => Some(MessageType::Reject),
            '4' => Some(MessageType::SequenceReset),
            '8' => Some(MessageType::ExecutionReport),
            'D' => Some(MessageType::NewOrderSingle),
            'F' => Some(MessageType::CancelOrder),
            'G' => Some(MessageType::CancelReplaceRequest),
            'Y' => Some(MessageType::CancelReject),
            'H' => Some(MessageType::OrderStatusRequest),
            'V' => Some(MessageType::MarketDataRequest),
            'W' => Some(MessageType::MarketDataSnapshotFullRefresh),
            'X' => Some(MessageType::MarketDataIncrementalRefresh),
            'j' => Some(MessageType::BusinessMessageReject),
            _ => None,
        }
    }

    pub fn as_char(&self) -> char {
        (*self as u8) as char
    }
}

/// FIX Field Tags
pub mod tags {
    pub const BEGIN_STRING: usize = 8;
    pub const BODY_LENGTH: usize = 9;
    pub const MSG_TYPE: usize = 35;
    pub const SENDER_COMP_ID: usize = 49;
    pub const TARGET_COMP_ID: usize = 56;
    pub const MSG_SEQ_NUM: usize = 34;
    pub const SENDING_TIME: usize = 52;
    pub const TRANSACT_TIME: usize = 60;
    pub const ENCRYPT_METHOD: usize = 98;
    pub const HEARTBEAT_INT: usize = 108;
    pub const CHECKSUM: usize = 10;
    pub const TEXT: usize = 58;

    // Order fields
    pub const CL_ORD_ID: usize = 11;
    pub const ORIG_CL_ORD_ID: usize = 41;
    pub const SYMBOL: usize = 55;
    pub const SIDE: usize = 54;
    pub const ORDER_QTY: usize = 38;
    pub const PRICE: usize = 44;
    pub const ORD_TYPE: usize = 40;
    pub const TIME_IN_FORCE: usize = 59;
    pub const ORDER_ID: usize = 37;
    pub const EXEC_ID: usize = 17;
    pub const LAST_QTY: usize = 32;
    pub const LAST_PX: usize = 31;
    pub const LEAVES_QTY: usize = 151;
    pub const EXEC_STATUS: usize = 150;
    pub const EXCHANGE_ID: usize = 100;
    /// OrdStatus (39) — the order's current state as understood by the venue.
    /// Distinct from ExecType (150/EXEC_STATUS above), which describes *this* execution report.
    pub const ORD_STATUS: usize = 39;

    // Session-level sequencing / recovery fields
    pub const BEGIN_SEQ_NO: usize = 7;   // ResendRequest
    pub const END_SEQ_NO: usize = 16;    // ResendRequest (0 = "to infinity")
    pub const NEW_SEQ_NO: usize = 36;    // SequenceReset
    pub const GAP_FILL_FLAG: usize = 123; // SequenceReset
    pub const POSS_DUP_FLAG: usize = 43;  // PossDupFlag

    // SecurityListRequest fields
    pub const SECURITY_REQ_ID: usize = 320;
    pub const SECURITY_LIST_REQUEST_TYPE: usize = 559;
    pub const SUBSCRIPTION_REQUEST_TYPE: usize = 263;

    // MarketDataRequest fields
    pub const MARKET_DATA_REQ_ID: usize = 262;
    pub const NO_MD_ENTRIES: usize = 268;
    pub const MD_ENTRY_TYPE: usize = 269;
    pub const MD_ENTRY_PX: usize = 270;
    pub const MD_ENTRY_SIZE: usize = 271;
    pub const NO_MD_ENTRY_TYPES: usize = 267;
    pub const MARKET_DEPTH: usize = 264;
}

pub struct FIXMessage {
    pub fields: HashMap<usize, String>,
    pub msg_type: char,
    pub seq_num: u32,
    pub field_order: Vec<usize>, // Track the order fields appeared in the raw message
    pub fields_ordered: Vec<(usize, String)>, // All fields in order (including repeating groups)
}

impl FIXMessage {
    pub fn new(msg_type: char, sender: &str, target: &str, seq_num: u32) -> Self {
        let mut fields = HashMap::new();
        fields.insert(tags::BEGIN_STRING, "FIX.4.4".to_string());
        fields.insert(tags::MSG_TYPE, msg_type.to_string());
        fields.insert(tags::SENDER_COMP_ID, sender.to_string());
        fields.insert(tags::TARGET_COMP_ID, target.to_string());
        fields.insert(tags::MSG_SEQ_NUM, seq_num.to_string());
        fields.insert(tags::SENDING_TIME, Self::timestamp());

        FIXMessage {
            fields,
            msg_type,
            seq_num,
            field_order: vec![],
            fields_ordered: vec![],
        }
    }

    pub fn timestamp() -> String {
        Utc::now().format("%Y%m%d-%H:%M:%S%.3f").to_string()
    }

    pub fn set_field(&mut self, tag: usize, value: &str) {
        self.fields.insert(tag, value.to_string());
        // Also track in fields_ordered for proper encoding with repeating groups
        // For repeating fields, we DON'T remove the previous entry - this allows multiple values
        self.fields_ordered.push((tag, value.to_string()));
    }

    pub fn get_field(&self, tag: usize) -> Option<&str> {
        self.fields.get(&tag).map(|s| s.as_str())
    }

    /// Return the nth occurrence (0-indexed) of a tag from `fields_ordered`.
    /// Use this instead of `get_field` when parsing repeating groups manually,
    /// since `get_field` only returns the last value for duplicate tags.
    pub fn get_field_nth(&self, tag: usize, n: usize) -> Option<&str> {
        self.fields_ordered
            .iter()
            .filter(|(t, _)| *t == tag)
            .nth(n)
            .map(|(_, v)| v.as_str())
    }

    /// Extract repeating group instances based on a group count tag and starting tag
    /// For example, SecurityListResponse has tag 146 (NoRelatedSym) = 26, then 26 instances of tag 55 (Symbol) follow
    pub fn get_repeating_group(&self, count_tag: usize, group_start_tag: usize) -> Vec<HashMap<usize, String>> {
        let mut groups = Vec::new();

        // Get the count of group instances
        let group_count = match self.fields.get(&count_tag).and_then(|s| s.parse::<usize>().ok()) {
            Some(count) => count,
            None => return groups,
        };

        // Find the position of the count tag in the ordered list
        let mut start_idx = None;
        for (i, (tag, _)) in self.fields_ordered.iter().enumerate() {
            if *tag == count_tag {
                start_idx = Some(i + 1); // Start after the count tag
                break;
            }
        }

        let start_idx = match start_idx {
            Some(idx) => idx,
            None => return groups,
        };

        // Extract group instances
        let mut current_group = HashMap::new();
        let mut group_instance_count = 0;

        for (tag, value) in &self.fields_ordered[start_idx..] {
            // If we see the group start tag, check if we need to start a new group
            if *tag == group_start_tag {
                // If we already have a group, save it and start a new one
                if !current_group.is_empty() {
                    groups.push(current_group);
                    current_group = HashMap::new();
                    group_instance_count += 1;
                    if group_instance_count >= group_count {
                        break;
                    }
                }
                // Start the new group with this tag
                current_group.insert(*tag, value.clone());
            } else {
                // Add field to current group
                current_group.insert(*tag, value.clone());
            }
        }

        // Don't forget the last group
        if !current_group.is_empty() && group_instance_count < group_count {
            groups.push(current_group);
        }

        groups
    }

    /// Encode FIX message to bytes
    /// FIX message structure: 8=FIX.4.4|9=<body_length>|<body>|10=<checksum>|
    /// Body length = length of everything between tag 9 and tag 10 (checksum), NOT including tag 10
    pub fn encode(&self) -> Result<Vec<u8>> {
        // Build body with proper field ordering
        // Standard Header order: 35 (MsgType), 49 (SenderCompID), 56 (TargetCompID), 34 (MsgSeqNum), 52 (SendingTime)
        // Then other fields
        let mut body = String::new();

        // Add standard header fields first in correct order
        if let Some(value) = self.fields.get(&tags::MSG_TYPE) {
            body.push_str(&format!("{}={}\x01", tags::MSG_TYPE, value));
        }
        if let Some(value) = self.fields.get(&tags::SENDER_COMP_ID) {
            body.push_str(&format!("{}={}\x01", tags::SENDER_COMP_ID, value));
        }
        if let Some(value) = self.fields.get(&tags::TARGET_COMP_ID) {
            body.push_str(&format!("{}={}\x01", tags::TARGET_COMP_ID, value));
        }
        if let Some(value) = self.fields.get(&tags::MSG_SEQ_NUM) {
            body.push_str(&format!("{}={}\x01", tags::MSG_SEQ_NUM, value));
        }
        if let Some(value) = self.fields.get(&tags::SENDING_TIME) {
            body.push_str(&format!("{}={}\x01", tags::SENDING_TIME, value));
        }

        // If fields_ordered is populated, use it (preserves repeating groups)
        // Otherwise fall back to HashMap-based encoding
        if !self.fields_ordered.is_empty() {
            // Use fields_ordered to preserve exact field order including repeating groups
            let processed_tags = vec![
                tags::BEGIN_STRING,
                tags::BODY_LENGTH,
                tags::MSG_TYPE,
                tags::MSG_SEQ_NUM,
                tags::SENDER_COMP_ID,
                tags::TARGET_COMP_ID,
                tags::SENDING_TIME,
                tags::CHECKSUM,
            ];

            // Add fields from fields_ordered (skipping standard header and special tags)
            for (tag, value) in &self.fields_ordered {
                if !processed_tags.contains(tag) {
                    body.push_str(&format!("{}={}\x01", tag, value));
                }
            }
        } else {
            // Add remaining fields (skip standard header fields and special tags)
            let processed_tags = vec![
                tags::BEGIN_STRING,
                tags::BODY_LENGTH,
                tags::MSG_TYPE,
                tags::MSG_SEQ_NUM,
                tags::SENDER_COMP_ID,
                tags::TARGET_COMP_ID,
                tags::SENDING_TIME,
                tags::CHECKSUM,
            ];

            // Iterate in standard numeric order for remaining fields
            let mut remaining_tags: Vec<_> = self.fields
                .iter()
                .filter(|(tag, _)| !processed_tags.contains(tag))
                .map(|(tag, _)| *tag)
                .collect();
            remaining_tags.sort();

            for tag in remaining_tags {
                if let Some(value) = self.fields.get(&tag) {
                    body.push_str(&format!("{}={}\x01", tag, value));
                }
            }
        }

        // Body length = length of everything between tag 9 and tag 10 (checksum)
        // This is just the body length, NOT including the checksum field
        let body_length = body.len();

        // Build complete message: 8=FIX.4.4|9=<body_length>|<body>|10=<checksum>|
        let mut message = Vec::<u8>::new();
        
        // Begin string (tag 8) - MUST be first field
        message.extend_from_slice(format!("{}={}\x01", tags::BEGIN_STRING, "FIX.4.4").as_bytes());
        
        // Body length (tag 9) - MUST be second field
        message.extend_from_slice(format!("{}={}\x01", tags::BODY_LENGTH, body_length).as_bytes());
        
        // Body (everything after tag 9, before tag 10)
        message.extend_from_slice(body.as_bytes());

        // Calculate checksum (sum of all bytes from after tag 8 up to but not including tag 10)
        // The checksum is calculated over: tag 9 + body (everything except tag 8 and tag 10)
        let checksum = Self::calculate_checksum(&message);
        
        // Checksum (tag 10) - MUST be last field
        message.extend_from_slice(format!("{}={}\x01", tags::CHECKSUM, checksum).as_bytes());

        Ok(message)
    }

    /// Parse FIX message from bytes
    pub fn decode(data: &[u8]) -> Result<Self> {
        let message_str = String::from_utf8(data.to_vec())?;
        let mut fields = HashMap::new();
        let mut fields_ordered = Vec::new();

        for field in message_str.split('\x01') {
            if field.is_empty() {
                continue;
            }

            let parts: Vec<&str> = field.splitn(2, '=').collect();
            if parts.len() != 2 {
                continue;
            }

            if let Ok(tag) = parts[0].parse::<usize>() {
                let value = parts[1].to_string();
                fields_ordered.push((tag, value.clone()));
                fields.insert(tag, value);
            }
        }

        let msg_type = fields
            .get(&tags::MSG_TYPE)
            .and_then(|s| s.chars().next())
            .ok_or_else(|| anyhow!("Missing MSG_TYPE"))?;

        let seq_num = fields
            .get(&tags::MSG_SEQ_NUM)
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(0);

        Ok(FIXMessage {
            fields,
            msg_type,
            seq_num,
            field_order: vec![],
            fields_ordered,
        })
    }

    /// Calculate FIX checksum
    /// Checksum is calculated over all bytes from tag 9 (BodyLength) onwards, up to but not including tag 10
    /// The checksum value is (sum of all bytes) % 256, formatted as 3 digits
    fn calculate_checksum(data: &[u8]) -> String {
        let sum: u32 = data.iter().map(|&b| b as u32).sum();
        let checksum = (sum % 256) as u8;
        format!("{:03}", checksum)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_creation() {
        let msg = FIXMessage::new('A', "SENDER", "TARGET", 1);
        assert_eq!(msg.msg_type, 'A');
        assert_eq!(msg.seq_num, 1);
    }

    #[test]
    fn test_encode_decode() -> Result<()> {
        let mut msg = FIXMessage::new('A', "SENDER", "TARGET", 1);
        msg.set_field(tags::HEARTBEAT_INT, "30");

        let encoded = msg.encode()?;
        let decoded = FIXMessage::decode(&encoded)?;

        assert_eq!(decoded.msg_type, 'A');
        assert_eq!(decoded.seq_num, 1);
        Ok(())
    }
}
