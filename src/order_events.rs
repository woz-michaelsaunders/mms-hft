// Typed order-event mapping for FIX ExecutionReport (35=8) messages.
//
// The FIX wire only gives us two loosely-related char codes — ExecType (150)
// describing *this* report, and OrdStatus (39) describing the order's
// resulting state. Strategy code shouldn't have to know FIX char codes at
// all, so this module collapses both into one typed, exhaustively-matched
// `OrderEventType` that the rest of the engine can switch on.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderEventType {
    New,
    PartialFill,
    Fill,
    Canceled,
    Replaced,
    Rejected,
    /// Anything else the venue sends (e.g. Expired, Suspended, PendingCancel)
    /// that doesn't map to one of the typed events above. Callers should log
    /// and ignore rather than treat it as a book-affecting event.
    Other,
}

/// Map ExecType (FIX tag 150) and OrdStatus (FIX tag 39) from an
/// ExecutionReport into a single typed event.
///
/// ExecType is authoritative for what *this* report represents; OrdStatus is
/// used as a fallback when ExecType is missing/unrecognized (some venues are
/// inconsistent about populating both) and to disambiguate PartialFill vs
/// Fill when a venue only sets OrdStatus correctly.
pub fn map_exec_type(exec_type: &str, ord_status: &str) -> OrderEventType {
    match exec_type {
        "0" => OrderEventType::New,
        "1" => OrderEventType::PartialFill,
        "2" => OrderEventType::Fill,
        "4" => OrderEventType::Canceled,
        "5" => OrderEventType::Replaced,
        "8" => OrderEventType::Rejected,
        _ => match ord_status {
            "0" => OrderEventType::New,
            "1" => OrderEventType::PartialFill,
            "2" => OrderEventType::Fill,
            "4" => OrderEventType::Canceled,
            "5" => OrderEventType::Replaced,
            "8" => OrderEventType::Rejected,
            _ => OrderEventType::Other,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exec_type_is_authoritative() {
        assert_eq!(map_exec_type("0", "0"), OrderEventType::New);
        assert_eq!(map_exec_type("1", "1"), OrderEventType::PartialFill);
        assert_eq!(map_exec_type("2", "2"), OrderEventType::Fill);
        assert_eq!(map_exec_type("4", "4"), OrderEventType::Canceled);
        assert_eq!(map_exec_type("5", "5"), OrderEventType::Replaced);
        assert_eq!(map_exec_type("8", "8"), OrderEventType::Rejected);
    }

    #[test]
    fn falls_back_to_ord_status_when_exec_type_unrecognized() {
        // Some venues send a custom/unknown ExecType but a standard OrdStatus.
        assert_eq!(map_exec_type("C", "4"), OrderEventType::Canceled);
        assert_eq!(map_exec_type("", "2"), OrderEventType::Fill);
    }

    #[test]
    fn unknown_both_maps_to_other_not_a_panic() {
        assert_eq!(map_exec_type("Z", "Z"), OrderEventType::Other);
        assert_eq!(map_exec_type("", ""), OrderEventType::Other);
    }
}
