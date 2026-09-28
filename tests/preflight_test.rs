//! Integration tests for `fix_server --preflight`.
//! Spawns the real binary. Local listeners only; synthetic env names.

use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn bin() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_fix_server"));
    cmd.env("RUST_LOG", "error");
    cmd
}

fn isolated_dir() -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "hft-preflight-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_config(dir: &Path, host: &str, port: u16, key_env: &str, secret_env: &str) -> PathBuf {
    let path = dir.join("preflight.toml");
    let body = format!(
        r#"
[connection]
heartbeat_interval = 30
max_reconnect_attempts = 10
reconnect_backoff_ms = 1000

[credentials.TEST]
api_key_env = "{key_env}"
api_secret_env = "{secret_env}"

[[sessions]]
name = "s1"
role = "market_data"
host = "{host}"
port = {port}
sender_comp_id = "PF.TEST.SENDER"
target_comp_id = "PF.TEST.TARGET"
credentials_ref = "TEST"
symbol_routing = "all"
"#
    );
    std::fs::write(&path, body).unwrap();
    path
}

fn unused_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

fn encode_fix_logon_ack() -> Vec<u8> {
    // 35=A Logon. Body = fields after 9= until 10=.
    let soh = 0x01u8;
    let body = format!(
        "35=A{soh}49=PF.TEST.TARGET{soh}56=PF.TEST.SENDER{soh}34=1{soh}52=20200101-00:00:00.000{soh}98=0{soh}108=30{soh}",
        soh = soh as char
    );
    let mut msg = Vec::new();
    msg.extend_from_slice(b"8=FIX.4.4");
    msg.push(soh);
    let body_len = body.len();
    msg.extend_from_slice(format!("9={body_len}").as_bytes());
    msg.push(soh);
    msg.extend_from_slice(body.as_bytes());
    let sum: u32 = msg.iter().map(|&b| b as u32).sum();
    let checksum = (sum % 256) as u8;
    msg.extend_from_slice(format!("10={checksum:03}").as_bytes());
    msg.push(soh);
    msg
}

fn table_row_for(stdout: &str, session: &str) -> Vec<String> {
    let line = stdout
        .lines()
        .find(|l| l.split_whitespace().next() == Some(session))
        .unwrap_or_else(|| panic!("no table row for {session:?} in:\n{stdout}"));
    line.split_whitespace().map(|s| s.to_string()).collect()
}

fn assert_no_files_under_data(dir: &Path) {
    let data = dir.join("data");
    match std::fs::read_dir(&data) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => panic!("failed to read {}: {e}", data.display()),
        Ok(_) => {
            let mut files = Vec::new();
            fn collect(dir: &Path, files: &mut Vec<PathBuf>) {
                if let Ok(rd) = std::fs::read_dir(dir) {
                    for ent in rd.flatten() {
                        let p = ent.path();
                        if p.is_dir() {
                            collect(&p, files);
                        } else {
                            files.push(p);
                        }
                    }
                }
            }
            collect(&data, &mut files);
            assert!(
                files.is_empty(),
                "sandbox data/ must contain no files, found {files:?}"
            );
        }
    }
}

const USAGE: &str = "USAGE: fix_server [--config PATH] | --preflight [--config PATH] [--logon-check] [--timeout-secs N] | --help";

#[test]
fn help_prints_ui_c2_and_exits_0() {
    let out = bin().arg("--help").output().unwrap();
    assert_eq!(out.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.is_empty(), "help stderr not empty: {stderr}");
    assert!(stdout.contains("LIVE server."), "{stdout}");
    assert!(stdout.contains("Deploy GATE."), "{stdout}");
    assert!(stdout.contains("141=Y"), "{stdout}");
    assert!(stdout.contains("fix_server --help"), "{stdout}");
    assert!(
        stdout.contains("HFT-042 cutover"),
        "{stdout}"
    );
}

#[test]
fn logon_check_without_preflight_is_usage_error() {
    let dir = isolated_dir();
    let out = bin()
        .current_dir(&dir)
        .args(["--logon-check"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(1));
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stdout.is_empty(), "usage error must not print table: {stdout}");
    assert!(
        stderr.contains("fix_server: --logon-check requires --preflight"),
        "{stderr}"
    );
    assert!(stderr.contains(USAGE), "{stderr}");
    assert!(!stderr.contains(" starting"), "must not start live: {stderr}");
}

#[test]
fn unknown_flag_and_timeout_zero_are_usage_errors() {
    let dir = isolated_dir();
    let unknown = bin()
        .current_dir(&dir)
        .args(["--preflight", "--nope"])
        .output()
        .unwrap();
    assert_eq!(unknown.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&unknown.stderr);
    assert!(
        stderr.contains("fix_server: unknown argument: --nope"),
        "{stderr}"
    );
    assert!(stderr.contains(USAGE), "{stderr}");
    assert!(!stderr.contains(" starting"), "{stderr}");

    let zero = bin()
        .current_dir(&dir)
        .args(["--preflight", "--timeout-secs", "0"])
        .output()
        .unwrap();
    assert_eq!(zero.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&zero.stderr);
    assert!(
        stderr.contains("fix_server: --timeout-secs must be a positive integer"),
        "{stderr}"
    );
    assert!(String::from_utf8_lossy(&zero.stdout).is_empty());
}

#[test]
fn listener_reachable_true_logon_skipped_exit_0() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let dir = isolated_dir();
    let key = format!("HFT_PF_KEY_OK_{port}");
    let secret = format!("HFT_PF_SECRET_OK_{port}");
    let cfg = write_config(&dir, "127.0.0.1", port, &key, &secret);

    let out = bin()
        .current_dir(&dir)
        .env(&key, "dummy-key")
        .env(&secret, "dummy-secret")
        .args([
            "--preflight",
            "--config",
            cfg.to_str().unwrap(),
            "--timeout-secs",
            "2",
        ])
        .output()
        .unwrap();
    drop(listener);

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(0), "stdout={stdout}\nstderr={stderr}");
    let cols = table_row_for(&stdout, "s1");
    assert_eq!(cols[1], "market_data");
    assert_eq!(cols[2], format!("127.0.0.1:{port}"));
    assert_eq!(cols[3], "ok");
    assert_eq!(cols[4], "true");
    assert_eq!(cols[5], "skipped");
    assert!(stdout.lines().next().unwrap().contains("session"));
    assert!(!stderr.contains(" starting"), "{stderr}");
    assert_no_files_under_data(&dir);
}

#[test]
fn closed_port_reachable_false_exit_1() {
    let port = unused_port();
    let dir = isolated_dir();
    let key = format!("HFT_PF_KEY_CLOSED_{port}");
    let secret = format!("HFT_PF_SECRET_CLOSED_{port}");
    let cfg = write_config(&dir, "127.0.0.1", port, &key, &secret);

    let out = bin()
        .current_dir(&dir)
        .env(&key, "dummy-key")
        .env(&secret, "dummy-secret")
        .args([
            "--preflight",
            "--config",
            cfg.to_str().unwrap(),
            "--timeout-secs",
            "1",
        ])
        .output()
        .unwrap();

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(1), "stdout={stdout}\nstderr={stderr}");
    let cols = table_row_for(&stdout, "s1");
    assert_eq!(cols[3], "ok");
    assert_eq!(cols[4], "false");
    assert_eq!(cols[5], "skipped");
    assert!(
        stderr.contains(&format!("session 's1' 127.0.0.1:{port}:")),
        "{stderr}"
    );
    assert_no_files_under_data(&dir);
}

#[test]
fn unset_env_names_exact_var_creds_fail_exit_1() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let dir = isolated_dir();
    let key = format!("HFT_PF_KEY_UNSET_{port}");
    let secret = format!("HFT_PF_SECRET_UNSET_{port}");
    let cfg = write_config(&dir, "127.0.0.1", port, &key, &secret);

    let out = bin()
        .current_dir(&dir)
        .env_remove(&key)
        .env_remove(&secret)
        .args([
            "--preflight",
            "--config",
            cfg.to_str().unwrap(),
            "--timeout-secs",
            "2",
        ])
        .output()
        .unwrap();
    drop(listener);

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(1), "stdout={stdout}\nstderr={stderr}");
    assert!(
        stderr.contains(&format!(
            "environment variable {key} (credentials.TEST api_key_env) is not set"
        )),
        "{stderr}"
    );
    assert!(
        !stderr.contains(&format!(
            "environment variable {secret} (credentials.TEST api_secret_env) is not set"
        )),
        "must print only the first missing var per ref: {stderr}"
    );
    let cols = table_row_for(&stdout, "s1");
    assert_eq!(cols[3], "fail");
    assert_eq!(cols[5], "skipped");
}

#[test]
fn unset_env_with_logon_check_still_no_fix_and_exits_1() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let dir = isolated_dir();
    let key = format!("HFT_PF_KEY_UNSET_LC_{port}");
    let secret = format!("HFT_PF_SECRET_UNSET_LC_{port}");
    let cfg = write_config(&dir, "127.0.0.1", port, &key, &secret);

    let out = bin()
        .current_dir(&dir)
        .env_remove(&key)
        .env_remove(&secret)
        .args([
            "--preflight",
            "--logon-check",
            "--config",
            cfg.to_str().unwrap(),
            "--timeout-secs",
            "2",
        ])
        .output()
        .unwrap();
    drop(listener);

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(1), "stdout={stdout}\nstderr={stderr}");
    assert!(
        stderr.contains(&format!(
            "environment variable {key} (credentials.TEST api_key_env) is not set"
        )),
        "{stderr}"
    );
    let cols = table_row_for(&stdout, "s1");
    assert_eq!(cols[3], "fail");
    assert_eq!(cols[5], "fail");
    // creds=fail path never constructs FIXSession, so no seqnum file.
    let seq = dir.join("data/seqnum_PF_TEST_SENDER.txt");
    assert!(!seq.exists(), "unset creds must not send FIX: {seq:?}");
    assert_no_files_under_data(&dir);
}

#[test]
fn logon_check_echo_a_reports_ok() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(false).unwrap();
    let port = listener.local_addr().unwrap().port();
    let ack = encode_fix_logon_ack();
    thread::spawn(move || {
        if let Ok((mut sock, _)) = listener.accept() {
            let _ = sock.set_read_timeout(Some(Duration::from_secs(2)));
            let mut buf = [0u8; 4096];
            let _ = sock.read(&mut buf);
            let _ = sock.write_all(&ack);
            let _ = sock.flush();
            // Stay up for logout / close.
            thread::sleep(Duration::from_secs(2));
        }
    });

    let dir = isolated_dir();
    let key = format!("HFT_PF_KEY_LOGON_{port}");
    let secret = format!("HFT_PF_SECRET_LOGON_{port}");
    let cfg = write_config(&dir, "127.0.0.1", port, &key, &secret);

    let out = bin()
        .current_dir(&dir)
        .env(&key, "dummy-key")
        .env(&secret, "dummy-secret")
        .args([
            "--preflight",
            "--logon-check",
            "--config",
            cfg.to_str().unwrap(),
            "--timeout-secs",
            "3",
        ])
        .output()
        .unwrap();

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let cols = table_row_for(&stdout, "s1");
    assert_eq!(cols[4], "true", "stdout={stdout}\nstderr={stderr}");
    assert_eq!(cols[5], "ok", "stdout={stdout}\nstderr={stderr}");
    assert_eq!(out.status.code(), Some(0), "stdout={stdout}\nstderr={stderr}");
    assert_no_files_under_data(&dir);
}

#[test]
fn logon_check_closed_port_exit_1() {
    let port = unused_port();
    let dir = isolated_dir();
    let key = format!("HFT_PF_KEY_LC_CLOSED_{port}");
    let secret = format!("HFT_PF_SECRET_LC_CLOSED_{port}");
    let cfg = write_config(&dir, "127.0.0.1", port, &key, &secret);

    let out = bin()
        .current_dir(&dir)
        .env(&key, "dummy-key")
        .env(&secret, "dummy-secret")
        .args([
            "--preflight",
            "--logon-check",
            "--config",
            cfg.to_str().unwrap(),
            "--timeout-secs",
            "1",
        ])
        .output()
        .unwrap();

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(1), "stdout={stdout}\nstderr={stderr}");
    let cols = table_row_for(&stdout, "s1");
    assert_eq!(cols[4], "false");
    assert_eq!(cols[5], "fail");
    assert!(
        stderr.contains(&format!("session 's1' 127.0.0.1:{port}:")),
        "{stderr}"
    );
    assert_no_files_under_data(&dir);
}

#[test]
fn logon_check_tcp_up_no_ack_no_data_files_exit_1() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(false).unwrap();
    let port = listener.local_addr().unwrap().port();
    thread::spawn(move || {
        if let Ok((mut sock, _)) = listener.accept() {
            let _ = sock.set_read_timeout(Some(Duration::from_secs(4)));
            let mut buf = [0u8; 4096];
            let _ = sock.read(&mut buf);
            // Reachable TCP, but never a logon ack (the failed-logon persist bug).
            thread::sleep(Duration::from_secs(4));
        }
    });

    let dir = isolated_dir();
    let key = format!("HFT_PF_KEY_NOACK_{port}");
    let secret = format!("HFT_PF_SECRET_NOACK_{port}");
    let cfg = write_config(&dir, "127.0.0.1", port, &key, &secret);

    let out = bin()
        .current_dir(&dir)
        .env(&key, "dummy-key")
        .env(&secret, "dummy-secret")
        .args([
            "--preflight",
            "--logon-check",
            "--config",
            cfg.to_str().unwrap(),
            "--timeout-secs",
            "2",
        ])
        .output()
        .unwrap();

    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(1), "stdout={stdout}\nstderr={stderr}");
    let cols = table_row_for(&stdout, "s1");
    assert_eq!(cols[4], "true", "stdout={stdout}\nstderr={stderr}");
    assert_eq!(cols[5], "fail", "stdout={stdout}\nstderr={stderr}");
    assert!(
        stderr.contains("session 's1' logon: timed out after"),
        "{stderr}"
    );
    assert_no_files_under_data(&dir);
}
