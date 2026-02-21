# ExecutionReport (35=8) - Field Reference

## Required Fields

| Tag | Name | Required | Description |
|-----|------|----------|-------------|
| 35 | MsgType | Y | Always "8" for ExecutionReport |
| 11 | ClOrdID | Y | Client-selected order ID |
| 37 | OrderID | Y | Server-assigned order ID (Numeric) |
| 17 | ExecID | Y | Unique execution ID. Equal to Fill ID if this message was the result of a fill (Numeric) |
| 55 | Symbol | Y | Symbol name (e.g., BTC_USDT) |
| 54 | Side | Y | "1": buy; "2": sell |
| 38 | OrderQty | Y | Original order quantity |
| 150 | ExecType | Y | Reason for this message (see ExecType values below) |
| 39 | OrdStatus | Y | Order status (Please refer to standard FIX spec) |
| 14 | CumQty | Y | Quantity of order that has already been filled |
| 151 | LeavesQty | Y | Quantity of order that is still open |
| 13 | CommType | Y | Always 3 (absolute) |

## Conditional Fields

| Tag | Name | Required | Description |
|-----|------|----------|-------------|
| 41 | OrigClOrdID | Depends | ClOrdID <11> of the previous non-rejected order. i.e. Response from OrderCancelReplaceRequest(35=G) or OrderCancelRequest(35=F) |
| 44 | Price | N | Original order price |
| 31 | LastPx | Y* | Fill price. Only present if this message was the result of a fill |
| 32 | LastQty | Y* | Fill quantity. Only present if this message was the result of a fill |
| 6 | AvgPx | Y* | Average fill price for all fills in order. Only present if this message was the result of a fill |
| 12 | Commission | N | Fee for trade. Only present if this message was the result of a fill |
| 2643 | CommissionCurrency | N | Fee currency for trade. Only present if this message was the result of a fill |
| 18 | ExecInst | N | Instructions for order handling on exchange. If more than one instruction is applicable to an order, this field can contain multiple instructions separated by space |
| 19 | ExecRefID | N | For CDC internal reference only |
| 636 | WorkingIndicator | N | Indicates if the order is currently being worked. Y or N |
| 60 | TransactTime | N | Time of the order update. Only present on order updates. It is stamped in response flow [no matter there were trades or not]; our system stamp this time when there are updates for an order (so it is called update_time on Web Socket) |
| 6616 | OrderCreateTime | N | TransactTime when order is created with 39=0\|150=0. It is stamped in request flow; when our system confirmed and accepted the order (i.e. it is called create_time on Web Socket) |
| 851 | LastLiquidityInd | N | "2": taker fill; "1": maker fill. Only present if this message was the result of a fill |
| 103 | OrdRejReason | N | Reason the order was rejected (see below). Only present on rejected NewOrderSingle (D) requests |
| 58 | Text | N | Description of the reason the order was rejected (e.g., Too many requests). Only present on rejected NewOrderSingle (D) requests |
| 544 | CashMargin | N | `1`: messages corresponds to an spot order. `2`: messages corresponds to an margin order. `3`: messages correspond to liquidation |
| 880 | TrdMatchID | N | Trade match ID (string of number) |
| 10060 | TradeTransactTime | N | TransactTime when order is getting partial filled or fully filled. It is stamped in response flow [only when there has trade happened]; i.e. the trade time / order matching time of when the trade happened (i.e. the time of this order being matched with another order) [i.e. the transaction_time in Web Socket] |
| 20101 | MatchCount | N | Number of orders matched for this trade execution. If it is Maker's Order, value is always 1. If it is Taker's Order, it is the number of orders matched for this trade execution |
| 20102 | MatchIndex | N | Only appears if it is Maker's order. It represents which order entry of corresponding price level was matched. This value is 0 base. If the matched order is on the top of the queue, it is shown 0 |

## ExecType (150) Values

The ExecType (150) field indicates the reason why this ExecutionReport was sent.

| ExecType | Description |
|----------|-------------|
| 0 | New order |
| 4 | Order cancelled |
| A | Response to a successful NewOrderSingle (D) request |
| 8 | Response to a rejected NewOrderSingle (D) request |
| 6 | Response to a successful OrderCancelRequest (F) request |
| I | Response to a OrderStatusRequest (H) |
| F | Trade |

## OrdRejReason (103) Values

Reason the order was rejected (only present on rejected NewOrderSingle (D) requests).

| Code | Description |
|------|-------------|
| 0 | Broker/Exchange Option |
| 1 | Unknown Symbol |
| 2 | Exchange Closed |
| 3 | Order Exceeds Limit |
| 4 | Too Late To Enter |
| 5 | Unknown Order |
| 6 | Duplicate Order |
| 7 | Duplicate Of A Verbally Communicated Order |
| 8 | Stale Order |
| 9 | Trade Along Required |
| 10 | Invalid Investor ID |
| 11 | Unsupported Order Characteristic |
| 12 | Surveillance Option |
| 13 | Incorrect Quantity |
| 14 | Incorrect Allocated Quantity |
| 15 | Unknown Account(s) |
| 99 | Other |

## Notes

- Fields marked with Y* are required only when the message is the result of a fill
- TransactTime (60) is the update time (stamped in response flow)
- OrderCreateTime (6616) is the create time (stamped in request flow when order is accepted)
- TradeTransactTime (10060) is the trade/matching time (only when a trade occurred)

