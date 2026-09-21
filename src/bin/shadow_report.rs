//! Daily shadow-vs-backtest comparison. No FIX / UAT connection.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process;

use chrono::NaiveDate;
use fix_server::{algo, kdb_integration, kdb_query};

const SPAWN_SYMBOL: &str = "BTC_USDT";
const SNAPSHOT_ROW_CAP: usize = 500_000;
const NOTE: &str = "Delta is evidence, not a pass/fail. Shadow samples at poll_interval_ms on the live book; backtest walks every clean kdb BBO snapshot. MarketImbalance paper fills on the current poll; run_backtest signals on prev and marks at cur.";

struct Cli {
    date: NaiveDate,
    fills: PathBuf,
    out_dir: PathBuf,
}

#[derive(Debug, Clone, serde::Serialize)]
struct ShadowVsBacktestReport {
    date: String,
    timezone: String,
    symbol: String,
    fills_path: String,
    snapshot_count: usize,
    note: String,
    strategies: StrategyPair,
}

#[derive(Debug, Clone, serde::Serialize)]
struct StrategyPair {
    spread_capture: StrategyCompare,
    market_imbalance: StrategyCompare,
}

#[derive(Debug, Clone, serde::Serialize)]
struct StrategyCompare {
    shadow_fill_count: u64,
    shadow_total_pnl: f64,
    backtest_fill_count: usize,
    backtest_total_pnl: f64,
    delta_fill_count: i64,
    delta_total_pnl: f64,
}

fn print_help() {
    println!("USAGE: shadow_report --date YYYY-MM-DD [--fills PATH] [--out-dir DIR]");
    println!();
    println!("Compare one UTC calendar day's shadow fills against run_backtest over the");
    println!("identical kdb orderbook_snapshots window. Does not connect to FIX.");
    println!();
    println!("Exit 0 = report written; exit 1 = no file written.");
    println!("Env: KDB_HOST (default localhost), KDB_PORT (default 5001)");
}

fn parse_args(args: &[String]) -> Result<Cli, String> {
    let mut date: Option<NaiveDate> = None;
    let mut fills = PathBuf::from("data/shadow_fills.jsonl");
    let mut out_dir = PathBuf::from("reports");
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--date" => {
                i += 1;
                let raw = args
                    .get(i)
                    .ok_or_else(|| {
                        "shadow_report: --date YYYY-MM-DD is required (UTC calendar day)".to_string()
                    })?;
                let parsed = NaiveDate::parse_from_str(raw, "%Y-%m-%d").map_err(|_| {
                    "shadow_report: --date YYYY-MM-DD is required (UTC calendar day)".to_string()
                })?;
                date = Some(parsed);
            }
            "--fills" => {
                i += 1;
                let raw = args.get(i).ok_or_else(|| {
                    "shadow_report: --fills/--out-dir must be non-empty".to_string()
                })?;
                if raw.trim().is_empty() {
                    return Err("shadow_report: --fills/--out-dir must be non-empty".to_string());
                }
                fills = PathBuf::from(raw);
            }
            "--out-dir" => {
                i += 1;
                let raw = args.get(i).ok_or_else(|| {
                    "shadow_report: --fills/--out-dir must be non-empty".to_string()
                })?;
                if raw.trim().is_empty() {
                    return Err("shadow_report: --fills/--out-dir must be non-empty".to_string());
                }
                out_dir = PathBuf::from(raw);
            }
            other => {
                return Err(format!(
                    "shadow_report: unknown argument: {}\nUSAGE: shadow_report --date YYYY-MM-DD [--fills PATH] [--out-dir DIR]",
                    other
                ));
            }
        }
        i += 1;
    }
    let date = date.ok_or_else(|| {
        "shadow_report: --date YYYY-MM-DD is required (UTC calendar day)".to_string()
    })?;
    Ok(Cli {
        date,
        fills,
        out_dir,
    })
}

fn load_day_fills(
    path: &Path,
    start_ms: i64,
    end_ms: i64,
    symbol: &str,
) -> (Vec<algo::ShadowFill>, Vec<algo::ShadowFill>) {
    let mut sc = Vec::new();
    let mut mi = Vec::new();
    let bytes = match fs::read(path) {
        Ok(b) => b,
        Err(_) => return (sc, mi),
    };
    let text = String::from_utf8_lossy(&bytes);
    for line in text.split('\n') {
        if line.trim().is_empty() {
            continue;
        }
        let row: algo::ShadowFill = match serde_json::from_str(line) {
            Ok(r) => r,
            Err(_) => continue,
        };
        if row.ts_ms < start_ms || row.ts_ms >= end_ms {
            continue;
        }
        if !kdb_query::is_allowlisted_symbol(&row.symbol) {
            continue;
        }
        if row.symbol != symbol {
            continue;
        }
        match row.strategy {
            algo::StrategyType::SpreadCapture => sc.push(row),
            algo::StrategyType::MarketImbalance => mi.push(row),
        }
    }
    (sc, mi)
}

fn shadow_total_pnl(rows: &[algo::ShadowFill]) -> f64 {
    if rows.is_empty() {
        return 0.0;
    }
    let mut best_ts = i64::MIN;
    let mut pnl = 0.0;
    for r in rows {
        if r.ts_ms >= best_ts {
            best_ts = r.ts_ms;
            pnl = r.cumulative_pnl;
        }
    }
    pnl
}

fn compare(shadow: &[algo::ShadowFill], bt: &algo::BacktestResult) -> StrategyCompare {
    let shadow_fill_count = shadow.len() as u64;
    let shadow_total_pnl = shadow_total_pnl(shadow);
    let backtest_fill_count = bt.summary.total_trades;
    let backtest_total_pnl = bt.summary.total_pnl;
    StrategyCompare {
        shadow_fill_count,
        shadow_total_pnl,
        backtest_fill_count,
        backtest_total_pnl,
        delta_fill_count: shadow_fill_count as i64 - backtest_fill_count as i64,
        delta_total_pnl: shadow_total_pnl - backtest_total_pnl,
    }
}

fn fail(msg: &str) -> ! {
    eprintln!("{}", msg);
    process::exit(1);
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "-h" || a == "--help") {
        print_help();
        process::exit(0);
    }
    let cli = match parse_args(&args) {
        Ok(c) => c,
        Err(e) => fail(&e),
    };

    let host = std::env::var("KDB_HOST").unwrap_or_else(|_| "localhost".to_string());
    let port: u16 = match std::env::var("KDB_PORT") {
        Ok(s) => match s.parse() {
            Ok(p) => p,
            Err(_) => fail("shadow_report: KDB_PORT must be an integer"),
        },
        Err(_) => 5001,
    };

    let start = cli
        .date
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc();
    let end = cli
        .date
        .succ_opt()
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc();
    let start_ms = start.timestamp_millis();
    let end_ms = end.timestamp_millis();

    let kdb_lo = match kdb_query::unix_ms_to_kdb_ns(start_ms) {
        Some(v) => v,
        None => fail("shadow_report: --date YYYY-MM-DD is required (UTC calendar day)"),
    };
    let kdb_hi = match kdb_query::unix_ms_to_kdb_ns(end_ms - 1) {
        Some(v) => v,
        None => fail("shadow_report: --date YYYY-MM-DD is required (UTC calendar day)"),
    };

    let symbol = SPAWN_SYMBOL.to_string();
    if !kdb_query::is_allowlisted_symbol(&symbol) {
        fail("shadow_report: spawn symbol failed allowlist");
    }

    let (sc_fills, mi_fills) = load_day_fills(&cli.fills, start_ms, end_ms, &symbol);

    let pred = kdb_query::kdb_symbol_predicate(&symbol);
    let q = format!(
        "select time, best_bid, best_ask, best_bid_size, best_ask_size from orderbook_snapshots where {}, time within ({};{})",
        pred,
        kdb_query::kdb_ts_atom(kdb_lo),
        kdb_query::kdb_ts_atom(kdb_hi)
    );

    let date_s = cli.date.format("%Y-%m-%d").to_string();
    let raw = match kdb_integration::query_table_once(&host, port, &q).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "shadow_report: kdb query failed for {} ({}:{}): {}",
                date_s, host, port, e
            );
            eprintln!("No report written. Shadow fills were not compared.");
            process::exit(1);
        }
    };
    let snaps = kdb_query::snapshots_from_kdb_json(&raw);
    if snaps.is_empty() {
        eprintln!(
            "shadow_report: no orderbook_snapshots for {} on {} UTC",
            symbol, date_s
        );
        eprintln!("No report written.");
        process::exit(1);
    }
    if snaps.len() > SNAPSHOT_ROW_CAP {
        eprintln!(
            "shadow_report: snapshot window too large (n={})",
            snaps.len()
        );
        process::exit(1);
    }

    let mut cfg_sc = algo::AlgoConfig::default();
    cfg_sc.strategy = algo::StrategyType::SpreadCapture;
    cfg_sc.symbol = symbol.clone();
    let mut cfg_mi = cfg_sc.clone();
    cfg_mi.strategy = algo::StrategyType::MarketImbalance;
    let bt_sc = algo::run_backtest(&cfg_sc, &snaps);
    let bt_mi = algo::run_backtest(&cfg_mi, &snaps);

    let report = ShadowVsBacktestReport {
        date: date_s.clone(),
        timezone: "UTC".to_string(),
        symbol: symbol.clone(),
        fills_path: cli.fills.to_string_lossy().into_owned(),
        snapshot_count: snaps.len(),
        note: NOTE.to_string(),
        strategies: StrategyPair {
            spread_capture: compare(&sc_fills, &bt_sc),
            market_imbalance: compare(&mi_fills, &bt_mi),
        },
    };

    if let Err(e) = fs::create_dir_all(&cli.out_dir) {
        fail(&format!("shadow_report: failed to create out-dir: {}", e));
    }
    let dest = cli
        .out_dir
        .join(format!("shadow_vs_backtest_{}.json", date_s));
    let tmp = cli
        .out_dir
        .join(format!("shadow_vs_backtest_{}.json.tmp", date_s));
    let body = match serde_json::to_string_pretty(&report) {
        Ok(s) => s,
        Err(e) => {
            let _ = fs::remove_file(&tmp);
            fail(&format!("shadow_report: serialize failed: {}", e));
        }
    };
    match fs::File::create(&tmp).and_then(|mut f| {
        f.write_all(body.as_bytes())?;
        f.write_all(b"\n")?;
        f.flush()
    }) {
        Ok(()) => {}
        Err(e) => {
            let _ = fs::remove_file(&tmp);
            fail(&format!("shadow_report: write failed: {}", e));
        }
    }
    if let Err(e) = fs::rename(&tmp, &dest) {
        let _ = fs::remove_file(&tmp);
        fail(&format!("shadow_report: rename failed: {}", e));
    }

    let sc = &report.strategies.spread_capture;
    let mi = &report.strategies.market_imbalance;
    println!("Wrote {}", dest.display());
    println!(
        "spread_capture fills={} pnl={:.6} d_fills={:+} d_pnl={:+.6}",
        sc.shadow_fill_count, sc.shadow_total_pnl, sc.delta_fill_count, sc.delta_total_pnl
    );
    println!(
        "market_imbalance fills={} pnl={:.6} d_fills={:+} d_pnl={:+.6}",
        mi.shadow_fill_count, mi.shadow_total_pnl, mi.delta_fill_count, mi.delta_total_pnl
    );
}
