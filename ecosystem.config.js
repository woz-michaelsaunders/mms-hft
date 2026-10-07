const fs = require("fs");

// Credentials live in /home/ubuntu/.fix_server.env (chmod 600), never in this
// file and never in config.toml. HFT-042 moved them out of [api].
function envFile(path) {
  const out = {};
  try {
    for (const line of fs.readFileSync(path, "utf8").split("\n")) {
      const m = line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
      if (m) out[m[1]] = m[2];
    }
  } catch (e) {
    console.error("WARNING: could not read " + path + ": " + e.message);
  }
  return out;
}
const fixServerEnv = envFile("/home/ubuntu/.fix_server.env");
// Dashboard credentials (HFT-058). 0600, never in this file.
const mgmtServerEnv = envFile("/home/ubuntu/.mgmt-server.env");

module.exports = {
  apps: [
    {
      name: "fix-server",
      script: "/home/ubuntu/fix_server/target/release/fix_server",
      cwd: "/home/ubuntu/fix_server",
      autorestart: true,
      watch: false,
      max_restarts: 10,
      restart_delay: 3000,
      env: {
        RUST_LOG: "info",
        ...fixServerEnv,
      },
      log_date_format: "YYYY-MM-DD HH:mm:ss",
      error_file: "/home/ubuntu/logs/fix-server-error.log",
      out_file: "/home/ubuntu/logs/fix-server-out.log",
    },
    {
      // PRODUCTION market data, READ ONLY. No order-management session in its
      // config, and main.rs gates the algo runtime and web server on having
      // BOTH an OM and a primary MD session — so this process structurally
      // cannot trade. Runs from its own cwd (separate data/) against its own
      // kdb+ on 5002, so production ticks never mix with UAT.
      name: "fix-server-prod",
      // Deployed binary, NOT a hand-built copy: the deploy swaps this file, so
      // production no longer drifts from UAT. Restart it after a deploy to pick
      // up the new build (the poller only restarts fix-server).
      script: "/home/ubuntu/fix_server/target/release/fix_server",
      args: "--config /home/ubuntu/fix_server_prod/config.toml",
      cwd: "/home/ubuntu/fix_server_prod",
      autorestart: true,
      watch: false,
      max_restarts: 10,
      restart_delay: 3000,
      env: {
        // warn, not info: at info this instance wrote ~2.3 GB/hour and
        // pm2-logrotate cycled every ~3 minutes, so only ~15 minutes of
        // history survived. On 2026-10-02 that destroyed the one warning
        // that would have explained why BTCUSD-PERP was not being captured
        // ("CAPTURE_SYMBOLS: dropping invalid token") before it could be
        // read. Production market data is far busier than UAT; UAT stays
        // at info.
        RUST_LOG: "warn",
        KDB_PORT: "5002",
        // UAT owns 5555 and drop_copy owns 5556/5557; a clash kills this process.
        ZMQ_BIND: "tcp://127.0.0.1:5565",
        CAPTURE_SYMBOLS: "BTC_USDT,BTC_USD,BTCUSD-PERP",
        ...envFile("/home/ubuntu/.fix_server.prd3main.env"),
      },
      log_date_format: "YYYY-MM-DD HH:mm:ss",
      error_file: "/home/ubuntu/logs/fix-server-prod-error.log",
      out_file: "/home/ubuntu/logs/fix-server-prod-out.log",
    },
    {
      name: "drop-copy",
      script: "/home/ubuntu/drop_copy/target/release/drop_copy",
      cwd: "/home/ubuntu/drop_copy",
      autorestart: true,
      watch: false,
      max_restarts: 10,
      restart_delay: 3000,
      env: {
        RUST_LOG: "info",
      },
      log_date_format: "YYYY-MM-DD HH:mm:ss",
      error_file: "/home/ubuntu/logs/drop-copy-error.log",
      out_file: "/home/ubuntu/logs/drop-copy-out.log",
    },
    {
      name: "mgmt-server",
      script: "app.py",
      env: { ...mgmtServerEnv },
      interpreter: "python3",
      cwd: "/home/ubuntu/mgmt-server",
      autorestart: true,
      watch: false,
      max_restarts: 10,
      restart_delay: 3000,
      log_date_format: "YYYY-MM-DD HH:mm:ss",
      error_file: "/home/ubuntu/logs/mgmt-server-error.log",
      out_file: "/home/ubuntu/logs/mgmt-server-out.log",
    },
    {
      name: "cloudflare-tunnel",
      script: "cloudflared",
      interpreter: "none",
      args: "tunnel --config /home/ubuntu/.cloudflared/config.yml run",
      autorestart: true,
      watch: false,
      max_restarts: 10,
      restart_delay: 3000,
      log_date_format: "YYYY-MM-DD HH:mm:ss",
      error_file: "/home/ubuntu/logs/cloudflare-tunnel-error.log",
      out_file: "/home/ubuntu/logs/cloudflare-tunnel-out.log",
    },
  ],
};
