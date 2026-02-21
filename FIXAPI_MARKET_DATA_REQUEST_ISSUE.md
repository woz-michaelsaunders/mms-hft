# Crypto.com FIX API - MarketDataRequest Rejection Issue

## Issue Summary
MarketDataRequest (35=V) messages are being rejected with Business Message Reject (35=j) indicating "Required tags validation failure" with BusinessRejectRefID pointing to tag 266.

## Environment
- **FIX Gateway**: UAT Market Data Server
- **Endpoint**: tcp://uat1-fix-md-f18a.crypto.local:34402 (172.31.32.227:34402)
- **Sender CompID**: UAT1.WOZINGA_BTC.MD.00
- **Target CompID**: UAT1.CDC.MD
- **FIX Version**: FIX.4.4

## MarketDataRequest Message (35=V)

### Message Format (Readable)
```
8=FIX.4.4|9=175|35=V|49=UAT1.WOZINGA_BTC.MD.00|56=UAT1.CDC.MD|34=4|52=20251129-19:09:35.972|262=book.BTC_USDT.1|263=1|264=1|265=0|267=2|269=0|269=1|146=1|55=BTC_USDT|60=20251129-19:09:35.972|10=234|
```

### Field Breakdown
| Tag | Name | Value | Description |
|-----|------|-------|-------------|
| 8 | BeginString | FIX.4.4 | FIX Protocol Version |
| 9 | BodyLength | 175 | Message body length |
| 35 | MsgType | V | MarketDataRequest |
| 49 | SenderCompID | UAT1.WOZINGA_BTC.MD.00 | Sender Company ID |
| 56 | TargetCompID | UAT1.CDC.MD | Target Company ID |
| 34 | MsgSeqNum | 4 | Message Sequence Number |
| 52 | SendingTime | 20251129-19:09:35.972 | Sending Timestamp |
| 262 | MDReqID | book.BTC_USDT.1 | Market Data Request ID (format: book.SYMBOL.DEPTH) |
| 263 | SubscriptionRequestType | 1 | 1 = Snapshot + Subscribe |
| 264 | MarketDepth | 1 | 1 = Top of Book |
| 265 | MDUpdateType | 0 | 0 = Full Refresh |
| 267 | NoMDEntryTypes | 2 | Number of MDEntryType entries (Bid and Ask) |
| 269 | MDEntryType | 0 | First entry: 0 = Bid |
| 269 | MDEntryType | 1 | Second entry: 1 = Ask |
| 146 | NoRelatedSym | 1 | Number of related symbols |
| 55 | Symbol | BTC_USDT | Trading pair symbol |
| 60 | TransactTime | 20251129-19:09:35.972 | Transaction Time |
| 10 | CheckSum | 234 | Message checksum |

### Raw Binary Message (Hex)
The message is sent as binary FIX format with 0x01 (SOH) as field separator.

## Rejection Response (35=j)

### Rejection Details
| Tag | Name | Value | Description |
|-----|------|-------|-------------|
| 35 | MsgType | j | Business Message Reject |
| 372 | RefMsgType | V | Rejected Message Type (MarketDataRequest) |
| 45 | RefSeqNum | 4 | Sequence Number of Rejected Message |
| 379 | BusinessRejectRefID | 266 | Business Reject Reference ID (points to tag 266) |
| 380 | BusinessRejectReason | 13 | Business Reject Reason (Custom Crypto.com code) |
| 58 | Text | Required tags validation failure. | Rejection reason text |

### Full Rejection Message Fields
```
Tag 8: FIX.4.4
Tag 9: 139
Tag 35: j
Tag 45: 4
Tag 34: 3
Tag 49: UAT1.CDC.MD
Tag 56: UAT1.WOZINGA_BTC.MD.00
Tag 52: 20251129-19:05:57.415
Tag 372: V
Tag 379: 266
Tag 380: 13
Tag 58: Required tags validation failure.
```

## Questions for Crypto.com Support

1. **Tag 266 (MDEntrySize)**: The BusinessRejectRefID (379) points to tag 266. However, per FIX 4.4 specification, MDEntrySize (266) is NOT part of MarketDataRequest (35=V) - it only appears in MarketDataSnapshotFullRefresh (35=W) and MarketDataIncrementalRefresh (35=X) responses. Should tag 266 be included in the request?

2. **BusinessRejectReason 13**: What does the custom reject reason code 13 indicate? This is not in the standard FIX reject reason codes.

3. **Field Order**: Does Crypto.com require a specific field order within the repeating group (NoMDEntryTypes)? Currently sending: 267=2, 269=0, 269=1

4. **MDUpdateType**: Should MDUpdateType (265) be 0 (Full Refresh) or 1 (Incremental Refresh) for initial subscription?

5. **Symbol Format**: Is "BTC_USDT" the correct symbol format for the BTC/USDT pair on the BTC-related market data server (port 34402)?

## Additional Context

- Session is properly authenticated (Logon 35=A successful)
- SecurityListRequest (35=x) works correctly and returns 26 securities including BTC_USDT
- Heartbeat messages (35=0) are being exchanged successfully
- The rejection occurs immediately after sending MarketDataRequest

## Requested Information

Please provide:
1. The exact required field structure for MarketDataRequest (35=V)
2. Whether tag 266 (MDEntrySize) should be included in the request
3. The meaning of BusinessRejectReason code 13
4. Any Crypto.com-specific requirements for MarketDataRequest that differ from FIX 4.4 standard


