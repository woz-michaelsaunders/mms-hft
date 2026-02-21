# Orders Directory

Place order files (`.json`) in this directory to be processed by the FIX server.

## Order File Format

Each order file should be a JSON file with the following structure:

```json
{
  "cl_ord_id": "ORDER_001",
  "symbol": "BTC_USDT",
  "side": "1",
  "qty": 0.001,
  "price": 45000.0
}
```

## Field Descriptions

- **cl_ord_id** (string): Client Order ID - unique identifier for this order
- **symbol** (string): Trading pair symbol (e.g., "BTC_USDT", "ETH_USDT")
- **side** (string): Order side - "1" for Buy, "2" for Sell
- **qty** (number): Order quantity (e.g., 0.001 for 0.001 BTC)
- **price** (number): Limit price (e.g., 45000.0 for $45,000)

## Processing

- Order files are checked every 100ms
- Valid orders are sent to the FIX gateway
- Processed files are moved to `processed/` subdirectory
- Invalid files are moved to `processed/` with `.error` suffix

## Example Files

See `../order_template.json` for a complete example.


