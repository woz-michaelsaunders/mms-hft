# Cancel Order Template

## File Format

Place cancel order requests in JSON files in the `orders/` directory. The file will be processed and moved to `orders/processed/` after execution.

## Required Fields

| Field | Type | Description | FIX Tag |
|-------|------|-------------|---------|
| `cl_ord_id` | string | Unique identifier for this cancel request (must be unique) | 11 (ClOrdID) |
| `orig_cl_ord_id` | string | The ClOrdID of the original order you want to cancel | 41 (OrigClOrdID) |
| `symbol` | string | Trading pair symbol (e.g., "BTC_USDT") | 55 (Symbol) |
| `side` | string | Side of the order: "1" for Buy, "2" for Sell | 54 (Side) |

## Example

```json
{
  "cl_ord_id": "CANCEL_ORDER_001",
  "orig_cl_ord_id": "ORDER_001",
  "symbol": "BTC_USDT",
  "side": "1"
}
```

## Notes

- The `cl_ord_id` must be unique for each cancel request
- The `orig_cl_ord_id` must match the `cl_ord_id` of the original order you want to cancel
- The `side` must match the side of the original order ("1" for Buy, "2" for Sell)
- The `symbol` must match the symbol of the original order

## Response

After sending the cancel request, you will receive an ExecutionReport (35=8) with:
- ExecType (150): "6" = Response to a successful OrderCancelRequest
- OrdStatus (39): "4" = Cancelled (if successful)

If the cancel fails, you'll receive:
- ExecType (150): "9" = Cancel Reject
- OrdRejReason (103): Reason code for rejection
- Text (58): Description of rejection reason

