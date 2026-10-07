"""
Database Monitoring Web Server
Read-only web interface to monitor kdb+ database tables without interfering with fix_server
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
import os
import json
from datetime import datetime, timedelta
import sys
import time
import logging
import requests as _requests

# Setup logging
from logging_config import loggers
app_logger = loggers["app"]
error_logger = loggers["error"]
kdb_logger = loggers["kdb"]
access_logger = loggers["access"]

# Try to use qpython, fallback to IPC client
try:
    # Try importing qpython (may have compatibility issues)
    import qpython
    from qpython.qconnection import QConnection
    HAS_QPATHON = True
    app_logger.info("Using qpython for kdb+ connections")
except (ImportError, SyntaxError, AttributeError):
    # Fallback: use custom IPC client
    HAS_QPATHON = False
    from kdb_ipc import KdbIPCClient
    app_logger.warning("qpython not available, using custom IPC client")

app = FastAPI(title="Database Monitor", description="Read-only kdb+ database table monitor")

# CORS. allow_origins=["*"] together with allow_credentials=True is the
# combination that lets any site on the internet make credentialed calls to
# this server, so origins are now an explicit allow-list and default to empty
# (same-origin only). Set MGMT_CORS_ORIGINS to a comma-separated list if a
# genuine cross-origin caller appears.
_cors_origins = [o.strip() for o in os.getenv("MGMT_CORS_ORIGINS", "").split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# HTTP Basic auth (HFT-058)
#
# This server is published to the internet at hft.wozinga.com through the
# cloudflared tunnel, and it proxies straight to the fix_server API --
# including POST /proxy/algo/config. On 2026-10-07 an unauthenticated GET of
# https://hft.wozinga.com/proxy/algo/status returned the live algo config,
# and the matching POST would have let anyone alter max_position and the
# other risk parameters.
#
# Fail CLOSED: with no password configured every request is refused. A
# dashboard that is down is recoverable; one that is open is not.
# ---------------------------------------------------------------------------
import base64 as _base64
import hmac as _hmac

_MGMT_USER = os.getenv("MGMT_AUTH_USER", "wozinga")
_MGMT_PASSWORD = os.getenv("MGMT_AUTH_PASSWORD", "")
if not _MGMT_PASSWORD:
    app_logger.error(
        "MGMT_AUTH_PASSWORD is not set - refusing every request. "
        "Set it in /home/ubuntu/.mgmt-server.env and reload with --update-env."
    )


def _unauthorized(detail: str) -> JSONResponse:
    return JSONResponse(
        status_code=401,
        content={"detail": detail},
        headers={"WWW-Authenticate": 'Basic realm="wozinga-hft", charset="UTF-8"'},
    )


@app.middleware("http")
async def require_basic_auth(request: Request, call_next):
    if not _MGMT_PASSWORD:
        return JSONResponse(
            status_code=503,
            content={"detail": "server misconfigured: no MGMT_AUTH_PASSWORD set"},
        )

    header = request.headers.get("authorization", "")
    if not header.lower().startswith("basic "):
        return _unauthorized("authentication required")

    try:
        decoded = _base64.b64decode(header.split(" ", 1)[1], validate=True).decode("utf-8")
        user, _, password = decoded.partition(":")
    except Exception:
        return _unauthorized("malformed credentials")

    # compare_digest on both halves, and always both, so the comparison does
    # not leak which half was wrong through timing.
    ok_user = _hmac.compare_digest(user, _MGMT_USER)
    ok_pass = _hmac.compare_digest(password, _MGMT_PASSWORD)
    if not (ok_user and ok_pass):
        access_logger.warning("auth failed from %s for %s", request.client.host if request.client else "?", request.url.path)
        return _unauthorized("invalid credentials")

    return await call_next(request)

# Add access logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all HTTP requests to access log"""
    start_time = time.time()
    
    # Process request
    response = await call_next(request)
    
    # Calculate processing time
    process_time = time.time() - start_time
    
    # Log request
    access_logger.info(
        f"{request.client.host}:{request.client.port} - "
        f'"{request.method} {request.url.path} HTTP/1.1" '
        f"{response.status_code} - {process_time:.3f}s"
    )
    
    return response

# Configuration
KDB_HOST = os.getenv("KDB_HOST", "localhost")
KDB_PORT = int(os.getenv("KDB_PORT", "5001"))
MONITOR_PORT = int(os.getenv("MONITOR_PORT", "8081"))
FIX_SERVER_URL = os.getenv("FIX_SERVER_URL", "http://localhost:8080")

# Response models
class TableInfo(BaseModel):
    name: str
    row_count: int
    columns: List[Dict[str, str]]

class TableData(BaseModel):
    columns: List[str]
    rows: List[List[Any]]
    total_rows: int
    page: int
    page_size: int

def get_kdb_connection():
    """Get kdb+ database connection"""
    if HAS_QPATHON:
        try:
            q = QConnection(host=KDB_HOST, port=KDB_PORT, timeout=5)
            q.open()
            return q
        except Exception as e:
            raise HTTPException(
                status_code=503,
                detail=f"Failed to connect to kdb+ at {KDB_HOST}:{KDB_PORT}: {str(e)}"
            )
    else:
        # Use subprocess to execute q commands
        import subprocess
        return subprocess

def convert_kdb_value(value):
    """Convert kdb+ value to Python native type"""
    if value is None:
        return None
    
    # Handle kdb+ timestamp (numpy datetime64)
    if hasattr(value, 'dtype') and 'datetime64' in str(value.dtype):
        try:
            # Convert to Python datetime
            ts = value.astype('datetime64[ns]')
            dt = datetime.utcfromtimestamp(ts.astype('int64') / 1e9)
            return dt.isoformat() + 'Z'
        except:
            return str(value)
    
    # Handle numpy types
    if hasattr(value, 'item'):
        try:
            return value.item()
        except:
            pass
    
    # Handle bytes/string
    if isinstance(value, bytes):
        try:
            return value.decode('utf-8')
        except:
            return str(value)
    
    # Handle lists
    if isinstance(value, (list, tuple)):
        return [convert_kdb_value(v) for v in value]
    
    return value

@app.get("/", response_class=HTMLResponse)
async def root():
    """Serve the main HTML page"""
    html_content = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Database Monitor - kdb+</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        
        .header {
            background: white;
            border-radius: 12px;
            padding: 30px;
            margin-bottom: 20px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        
        .header h1 {
            color: #333;
            margin-bottom: 10px;
            font-size: 2em;
        }
        
        .header p {
            color: #666;
            font-size: 1.1em;
        }
        
        .header .connection-info {
            color: #888;
            font-size: 0.9em;
            margin-top: 5px;
        }
        
        .status {
            display: inline-block;
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 0.9em;
            margin-top: 10px;
        }
        
        .status.connected {
            background: #10b981;
            color: white;
        }
        
        .status.disconnected {
            background: #ef4444;
            color: white;
        }
        
        .tables-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }
        
        .table-card {
            background: white;
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            cursor: pointer;
            transition: transform 0.2s, box-shadow 0.2s;
        }
        
        .table-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 6px 12px rgba(0, 0, 0, 0.15);
        }
        
        .table-card h3 {
            color: #333;
            margin-bottom: 10px;
            font-size: 1.3em;
        }
        
        .table-card .row-count {
            color: #666;
            font-size: 0.9em;
            margin-bottom: 15px;
            transition: color 0.3s;
        }
        
        .table-card.updated .row-count {
            color: #10b981;
            font-weight: 600;
            animation: pulse 0.5s;
        }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.7; }
        }
        
        .table-card .columns {
            color: #888;
            font-size: 0.85em;
            max-height: 100px;
            overflow-y: auto;
        }
        
        .table-card .columns span {
            display: inline-block;
            background: #f3f4f6;
            padding: 3px 8px;
            border-radius: 4px;
            margin: 3px;
        }
        
        .table-viewer {
            background: white;
            border-radius: 12px;
            padding: 30px;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            display: none;
        }
        
        .table-viewer.active {
            display: block;
        }
        
        .table-viewer-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            padding-bottom: 15px;
            border-bottom: 2px solid #e5e7eb;
        }
        
        .table-viewer-header h2 {
            color: #333;
        }
        
        .close-btn {
            background: #ef4444;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 6px;
            cursor: pointer;
            font-size: 1em;
            transition: background 0.2s;
        }
        
        .close-btn:hover {
            background: #dc2626;
        }
        
        .table-controls {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
            flex-wrap: wrap;
            gap: 10px;
        }
        
        .pagination {
            display: flex;
            gap: 10px;
            align-items: center;
        }
        
        .pagination button {
            background: #667eea;
            color: white;
            border: none;
            padding: 8px 16px;
            border-radius: 6px;
            cursor: pointer;
            font-size: 0.9em;
            transition: background 0.2s;
        }
        
        .pagination button:hover:not(:disabled) {
            background: #5568d3;
        }
        
        .pagination button:disabled {
            background: #d1d5db;
            cursor: not-allowed;
        }
        
        .page-size-select {
            padding: 8px 12px;
            border-radius: 6px;
            border: 1px solid #d1d5db;
            font-size: 0.9em;
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
            margin-top: 15px;
            overflow-x: auto;
            display: block;
            max-height: 600px;
            overflow-y: auto;
        }
        
        thead {
            position: sticky;
            top: 0;
            background: #667eea;
            color: white;
        }
        
        th, td {
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #e5e7eb;
        }
        
        th {
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.85em;
            letter-spacing: 0.5px;
        }
        
        tbody tr:hover {
            background: #f9fafb;
        }
        
        .loading {
            text-align: center;
            padding: 40px;
            color: #666;
        }
        
        .error {
            background: #fee2e2;
            color: #991b1b;
            padding: 15px;
            border-radius: 6px;
            margin: 20px 0;
        }
        
        .refresh-btn {
            background: #10b981;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 6px;
            cursor: pointer;
            font-size: 1em;
            transition: background 0.2s;
        }
        
        .refresh-btn:hover {
            background: #059669;
        }

        .tab-bar {
            display: flex;
            gap: 8px;
            margin-top: 15px;
        }
        .tab-btn {
            padding: 8px 20px;
            border: none;
            border-radius: 6px 6px 0 0;
            cursor: pointer;
            font-size: 0.95em;
            background: #e5e7eb;
            color: #555;
            transition: background 0.2s, color 0.2s;
        }
        .tab-btn.active {
            background: #667eea;
            color: white;
        }
        .tab-panel { display: none; }
        .tab-panel.active { display: block; }

        .trading-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(360px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }
        .trading-card {
            background: white;
            border-radius: 12px;
            padding: 20px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }
        .trading-card h3 {
            color: #333;
            margin-bottom: 12px;
            font-size: 1.15em;
            border-bottom: 2px solid #667eea;
            padding-bottom: 6px;
        }
        .trading-card table { max-height: 300px; }
        .pnl-pos { color: #10b981; font-weight: 600; }
        .pnl-neg { color: #ef4444; font-weight: 600; }
        .signal-row { font-size: 0.8em; color: #555; padding: 4px 0; border-bottom: 1px solid #f3f4f6; }
        .badge {
            display: inline-block;
            padding: 3px 10px;
            border-radius: 12px;
            font-size: 0.8em;
            font-weight: 600;
        }
        .badge-green { background: #d1fae5; color: #065f46; }
        .badge-red { background: #fee2e2; color: #991b1b; }
        .badge-gray { background: #f3f4f6; color: #374151; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 HFT Monitor</h1>
            <p>kdb+ database &amp; live trading dashboard</p>
            <div class="connection-info">kdb+ server: <strong>""" + f"{KDB_HOST}:{KDB_PORT}" + """</strong> &nbsp;|&nbsp; fix_server: <strong>""" + f"{os.getenv('FIX_SERVER_URL', 'http://localhost:8080')}" + """</strong></div>
            <div class="tab-bar">
                <button class="tab-btn active" id="tab-db" onclick="switchTab('db')">📋 Database</button>
                <button class="tab-btn" id="tab-trading" onclick="switchTab('trading')">📈 Trading</button>
                <button class="tab-btn" id="tab-graph" onclick="switchTab('graph')">📊 Graph</button>
                <button class="tab-btn" id="tab-trades" onclick="switchTab('trades')">🎯 Trades</button>
            </div>
        </div>

        <div id="error-container"></div>

        <!-- Database tab -->
        <div class="tab-panel active" id="panel-db">
            <div style="margin-bottom:12px;display:flex;align-items:center;gap:10px;">
                <button class="refresh-btn" onclick="loadTables()">🔄 Refresh</button>
                <button class="refresh-btn" id="auto-refresh-btn" onclick="toggleRealTime()">⚡ Enable Real-Time</button>
                <span id="status" class="status" style="font-size:0.85em;padding:3px 10px;border-radius:12px;background:#e2e8f0;color:#555;">Connecting...</span>
            </div>
            <div class="tables-grid" id="tables-grid">
                <div class="loading">Loading tables...</div>
            </div>
            <div class="table-viewer" id="table-viewer">
                <div class="table-viewer-header">
                    <h2 id="viewer-title">Table Data</h2>
                    <button class="close-btn" onclick="closeViewer()">Close</button>
                </div>
                <div class="table-controls">
                    <div class="pagination">
                        <button id="prev-btn" onclick="changePage(-1)">← Previous</button>
                        <span id="page-info">Page 1</span>
                        <button id="next-btn" onclick="changePage(1)">Next →</button>
                    </div>
                    <select class="page-size-select" id="page-size" onchange="changePageSize()">
                        <option value="50">50 rows</option>
                        <option value="100" selected>100 rows</option>
                        <option value="200">200 rows</option>
                        <option value="500">500 rows</option>
                    </select>
                </div>
                <div id="table-content">
                    <div class="loading">Loading table data...</div>
                </div>
            </div>
        </div>

        <!-- Trading tab -->
        <div class="tab-panel" id="panel-trading">
            <div style="margin-bottom:12px;">
                <button class="refresh-btn" onclick="loadTradingData()">🔄 Refresh</button>
                <button class="refresh-btn" id="trading-auto-btn" onclick="toggleTradingRealTime()" style="margin-left:10px;">⚡ Enable Real-Time</button>
                <span id="trading-last-update" style="margin-left:15px;color:#fff;font-size:0.85em;"></span>
            </div>
            <div class="trading-grid">
                <div class="trading-card" style="grid-column: span 2;">
                    <h3>Positions &amp; P&amp;L</h3>
                    <div id="positions-content"><div class="loading">Loading...</div></div>
                </div>
                <div class="trading-card">
                    <h3>Algo Control</h3>
                    <div id="algo-content"><div class="loading">Loading...</div></div>
                    <div style="margin-top:14px;border-top:1px solid #e2e8f0;padding-top:14px;">
                        <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-bottom:10px;">
                            <div>
                                <label style="font-size:0.78em;color:#666;display:block;margin-bottom:3px;">Strategy</label>
                                <select id="cfg-strategy" style="width:100%;padding:5px 8px;border-radius:6px;border:1px solid #ccc;font-size:0.9em;">
                                    <option value="spread_capture">Spread Capture</option>
                                    <option value="market_imbalance">Market Imbalance</option>
                                </select>
                            </div>
                            <div>
                                <label style="font-size:0.78em;color:#666;display:block;margin-bottom:3px;">Symbol</label>
                                <select id="cfg-symbol" style="width:100%;padding:5px 8px;border-radius:6px;border:1px solid #ccc;font-size:0.9em;">
                                    <option value="BTC_USDT">BTC_USDT</option>
                                    <option value="BTC_USD">BTC_USD</option>
                                </select>
                            </div>
                            <div>
                                <label style="font-size:0.78em;color:#666;display:block;margin-bottom:3px;">Order Qty</label>
                                <input id="cfg-order-qty" type="number" step="0.0001" value="0.001" style="width:100%;padding:5px 8px;border-radius:6px;border:1px solid #ccc;font-size:0.9em;box-sizing:border-box;">
                            </div>
                            <div>
                                <label style="font-size:0.78em;color:#666;display:block;margin-bottom:3px;">Max Position</label>
                                <input id="cfg-max-pos" type="number" step="0.001" value="0.01" style="width:100%;padding:5px 8px;border-radius:6px;border:1px solid #ccc;font-size:0.9em;box-sizing:border-box;">
                            </div>
                            <div>
                                <label style="font-size:0.78em;color:#666;display:block;margin-bottom:3px;">Imbalance Threshold</label>
                                <input id="cfg-imbal-thresh" type="number" step="0.01" min="0.5" max="1.0" value="0.6" style="width:100%;padding:5px 8px;border-radius:6px;border:1px solid #ccc;font-size:0.9em;box-sizing:border-box;">
                            </div>
                            <div>
                                <label style="font-size:0.78em;color:#666;display:block;margin-bottom:3px;">Depth Levels</label>
                                <input id="cfg-depth" type="number" step="1" min="1" max="20" value="5" style="width:100%;padding:5px 8px;border-radius:6px;border:1px solid #ccc;font-size:0.9em;box-sizing:border-box;">
                            </div>
                            <div>
                                <label style="font-size:0.78em;color:#666;display:block;margin-bottom:3px;">Min Spread (bps)</label>
                                <input id="cfg-min-spread" type="number" step="0.5" value="5.0" style="width:100%;padding:5px 8px;border-radius:6px;border:1px solid #ccc;font-size:0.9em;box-sizing:border-box;">
                            </div>
                            <div>
                                <label style="font-size:0.78em;color:#666;display:block;margin-bottom:3px;">Signal Cooldown (ms)</label>
                                <input id="cfg-cooldown" type="number" step="100" value="500" style="width:100%;padding:5px 8px;border-radius:6px;border:1px solid #ccc;font-size:0.9em;box-sizing:border-box;">
                            </div>
                        </div>
                        <div style="display:flex;gap:8px;flex-wrap:wrap;">
                            <button onclick="applyAlgoConfig(true)"  style="flex:1;padding:8px;background:#22c55e;color:#fff;border:none;border-radius:6px;cursor:pointer;font-weight:600;font-size:0.9em;">▶ Enable</button>
                            <button onclick="applyAlgoConfig(false)" style="flex:1;padding:8px;background:#ef4444;color:#fff;border:none;border-radius:6px;cursor:pointer;font-weight:600;font-size:0.9em;">■ Disable</button>
                            <button onclick="applyAlgoConfig(null)"  style="flex:1;padding:8px;background:#6366f1;color:#fff;border:none;border-radius:6px;cursor:pointer;font-weight:600;font-size:0.9em;">⚙ Apply Config</button>
                        </div>
                        <div id="algo-apply-status" style="margin-top:8px;font-size:0.85em;min-height:1.2em;"></div>
                    </div>
                </div>
                <div class="trading-card" style="grid-column: span 2;">
                    <h3>Open Orders</h3>
                    <div id="open-orders-content"><div class="loading">Loading...</div></div>
                </div>
                <div class="trading-card" style="grid-column: span 2;">
                    <div style="display:flex;align-items:center;gap:10px;margin-bottom:10px;">
                        <h3 style="margin:0;">Order History</h3>
                        <button onclick="loadOrderHistory()" style="padding:4px 10px;border-radius:6px;border:none;background:#6366f1;color:#fff;font-size:0.82em;cursor:pointer;">↻ Refresh</button>
                        <select id="order-history-filter" onchange="loadOrderHistory()" style="padding:4px 8px;border-radius:6px;border:1px solid #ccc;font-size:0.85em;">
                            <option value="">All statuses</option>
                            <option value="F">Filled</option>
                            <option value="1">Partial</option>
                            <option value="0">New</option>
                            <option value="4">Cancelled</option>
                            <option value="8">Rejected</option>
                        </select>
                    </div>
                    <div id="order-history-content" style="max-height:300px;overflow-y:auto;"><div class="loading">Loading...</div></div>
                </div>
                <div class="trading-card" style="grid-column: span 2;">
                    <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:10px;">
                        <h3 style="margin:0;">Logs</h3>
                        <select id="log-service" onchange="loadLogs()" style="padding:4px 8px;border-radius:6px;border:1px solid #ccc;font-size:0.85em;">
                            <option value="fix-server">fix-server</option>
                            <option value="drop-copy">drop-copy</option>
                            <option value="mgmt-server">mgmt-server</option>
                            <option value="cloudflare">cloudflare</option>
                        </select>
                        <input id="log-filter" type="text" placeholder="Filter..." onkeyup="loadLogs()"
                            style="padding:4px 8px;border-radius:6px;border:1px solid #ccc;font-size:0.85em;width:140px;">
                        <label style="font-size:0.82em;color:#555;display:flex;align-items:center;gap:4px;">
                            <input type="checkbox" id="log-verbose" onchange="loadLogs()"> Verbose
                        </label>
                        <button onclick="loadLogs()" style="padding:4px 10px;border-radius:6px;border:none;background:#6366f1;color:#fff;font-size:0.82em;cursor:pointer;">↻ Refresh</button>
                        <button id="log-auto-btn" onclick="toggleLogRealTime()" style="padding:4px 10px;border-radius:6px;border:none;background:#48bb78;color:#fff;font-size:0.82em;cursor:pointer;">⚡ Live</button>
                    </div>
                    <div id="logs-content" style="background:#0d1117;border-radius:8px;padding:12px;height:320px;overflow-y:auto;font-family:monospace;font-size:0.78em;line-height:1.5;color:#c9d1d9;">
                        <span style="color:#666;">Loading logs...</span>
                    </div>
                </div>
            </div>
        </div>

        <!-- Graph tab -->
        <div class="tab-panel" id="panel-graph">
            <div style="margin-bottom:12px;display:flex;align-items:center;gap:10px;flex-wrap:wrap;">
                <select id="graph-symbol" onchange="loadChart()" style="padding:6px 12px;border-radius:6px;border:1px solid #ccc;font-size:0.95em;">
                    <option value="BTC_USDT">BTC_USDT</option>
                    <option value="BTC_USD">BTC_USD</option>
                </select>
                <select id="graph-field" onchange="loadChart()" style="padding:6px 12px;border-radius:6px;border:1px solid #ccc;font-size:0.95em;">
                    <option value="mid">Mid Price</option>
                    <option value="best_bid">Best Bid</option>
                    <option value="best_ask">Best Ask</option>
                    <option value="spread">Spread</option>
                </select>
                <select id="graph-limit" onchange="loadChart()" style="padding:6px 12px;border-radius:6px;border:1px solid #ccc;font-size:0.95em;">
                    <option value="500">Last 500</option>
                    <option value="1000">Last 1000</option>
                    <option value="5000">Last 5000</option>
                    <option value="0">All</option>
                </select>
                <button class="refresh-btn" onclick="loadChart()">🔄 Refresh</button>
                <button class="refresh-btn" id="graph-auto-btn" onclick="toggleGraphRealTime()">⚡ Enable Real-Time</button>
                <span id="graph-last-update" style="color:#fff;font-size:0.85em;"></span>
            </div>
            <div style="background:#1a1a2e;border-radius:12px;padding:16px;position:relative;">
                <canvas id="btcChart" style="width:100%;height:420px;"></canvas>
                <div id="graph-loading" style="display:none;position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);color:#aaa;font-size:1.1em;">Loading chart data...</div>
                <div id="graph-error" style="display:none;color:#ff6b6b;text-align:center;padding:40px;font-size:1em;"></div>
            </div>
            <div id="graph-stats" style="display:flex;gap:20px;margin-top:12px;flex-wrap:wrap;"></div>
        </div>

        <!-- Trades tab -->
        <div class="tab-panel" id="panel-trades">
            <div style="margin-bottom:12px;display:flex;align-items:center;gap:10px;flex-wrap:wrap;">
                <select id="trades-symbol" onchange="loadTradesChart()" style="padding:6px 12px;border-radius:6px;border:1px solid #ccc;font-size:0.95em;">
                    <option value="BTC_USD">BTC_USD</option>
                    <option value="BTC_USDT">BTC_USDT</option>
                </select>
                <select id="trades-limit" onchange="loadTradesChart()" style="padding:6px 12px;border-radius:6px;border:1px solid #ccc;font-size:0.95em;">
                    <option value="500">Last 500 ticks</option>
                    <option value="1000">Last 1000 ticks</option>
                    <option value="0">All</option>
                </select>
                <button class="refresh-btn" onclick="loadTradesChart()">🔄 Refresh</button>
                <button class="refresh-btn" id="trades-auto-btn" onclick="toggleTradesRealTime()">⚡ Enable Real-Time</button>
                <span id="trades-last-update" style="color:#fff;font-size:0.85em;"></span>
            </div>
            <div id="trades-chart-container" style="background:#131722;border-radius:12px;height:480px;position:relative;">
                <div id="trades-chart" style="width:100%;height:480px;"></div>
                <div id="trades-loading" style="display:none;position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);color:#aaa;font-size:1.1em;">Loading...</div>
            </div>
            <div id="trades-legend" style="display:flex;gap:24px;margin-top:10px;flex-wrap:wrap;font-size:0.85em;color:#ccc;padding:0 4px;"></div>
        </div>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/lightweight-charts@4.1.3/dist/lightweight-charts.standalone.production.js"></script>
    <script>
        let currentTable = null;
        let currentPage = 1;
        let pageSize = 100;
        let totalRows = 0;
        
        async function loadTables() {
            const statusEl = document.getElementById('status');
            try {
                statusEl.textContent = 'Connecting...';
                statusEl.className = 'status';
                
                const response = await fetch('/api/tables');
                
                // Check HTTP response status first
                if (!response.ok) {
                    const errorText = await response.text();
                    showError(`HTTP ${response.status}: ${errorText}`);
                    statusEl.textContent = 'Error';
                    statusEl.className = 'status disconnected';
                    return;
                }
                
                const data = await response.json();
                
                // Debug: log response
                console.log('API Response:', data);
                console.log('Has error field:', 'error' in data);
                if ('error' in data) {
                    console.log('Error value:', data.error);
                }
                
                // Check for error in response data (only if error field exists and is non-empty)
                if (data.error !== undefined && data.error !== null && String(data.error).trim() !== '') {
                    showError(data.error);
                    statusEl.textContent = 'Error';
                    statusEl.className = 'status disconnected';
                    return;
                }
                
                // Success - update status
                statusEl.textContent = 'Connected';
                statusEl.className = 'status connected';
                
                const grid = document.getElementById('tables-grid');
                grid.innerHTML = '';
                
                if (data.tables.length === 0) {
                    grid.innerHTML = '<div class="error">No tables found in the database.</div>';
                    return;
                }
                
                // Store current state for change detection
                const newState = {};
                data.tables.forEach(table => {
                    newState[table.name] = {
                        row_count: table.row_count,
                        columns: table.columns.length
                    };
                });
                
                // Update last known state (only if real-time is enabled, otherwise initialize)
                if (Object.keys(lastTableState).length === 0 || !realTimeEnabled) {
                    lastTableState = newState;
                }
                
                data.tables.forEach(table => {
                    const card = document.createElement('div');
                    card.className = 'table-card';
                    card.onclick = () => openTable(table.name);
                    
                    const columnsHtml = table.columns.map(col => 
                        `<span>${col.name} (${col.type})</span>`
                    ).join('');
                    
                    // Highlight if row count changed (for real-time updates)
                    const rowCountEl = document.createElement('div');
                    rowCountEl.className = 'row-count';
                    rowCountEl.textContent = `${table.row_count.toLocaleString()} rows`;
                    
                    card.innerHTML = `
                        <h3>${table.name}</h3>
                        <div class="row-count">${table.row_count.toLocaleString()} rows</div>
                        <div class="columns">${columnsHtml}</div>
                    `;
                    
                    grid.appendChild(card);
                });
            } catch (error) {
                showError('Failed to load tables: ' + error.message);
                const s = document.getElementById('status');
                if (s) { s.textContent = 'Error'; s.className = 'status disconnected'; }
            }
        }
        
        async function openTable(tableName) {
            currentTable = tableName;
            currentPage = 1;
            document.getElementById('table-viewer').classList.add('active');
            document.getElementById('viewer-title').textContent = `Table: ${tableName}`;
            await loadTableData();
        }
        
        async function loadTableData() {
            try {
                const response = await fetch(
                    `/api/table/${currentTable}?page=${currentPage}&page_size=${pageSize}`
                );
                const data = await response.json();
                
                if (data.error) {
                    document.getElementById('table-content').innerHTML = 
                        `<div class="error">${data.error}</div>`;
                    return;
                }
                
                totalRows = data.total_rows;
                updatePagination();
                
                if (data.rows.length === 0) {
                    document.getElementById('table-content').innerHTML = 
                        '<div class="loading">No data available</div>';
                    return;
                }
                
                let html = '<table><thead><tr>';
                data.columns.forEach(col => {
                    html += `<th>${col}</th>`;
                });
                html += '</tr></thead><tbody>';
                
                data.rows.forEach(row => {
                    html += '<tr>';
                    row.forEach(cell => {
                        const value = cell === null ? '<em>NULL</em>' : 
                                     typeof cell === 'object' ? JSON.stringify(cell) : 
                                     String(cell);
                        html += `<td>${escapeHtml(value)}</td>`;
                    });
                    html += '</tr>';
                });
                
                html += '</tbody></table>';
                document.getElementById('table-content').innerHTML = html;
            } catch (error) {
                document.getElementById('table-content').innerHTML = 
                    `<div class="error">Failed to load table data: ${error.message}</div>`;
            }
        }
        
        function updatePagination() {
            const totalPages = Math.ceil(totalRows / pageSize);
            document.getElementById('page-info').textContent = 
                `Page ${currentPage} of ${totalPages} (${totalRows.toLocaleString()} total rows)`;
            
            document.getElementById('prev-btn').disabled = currentPage <= 1;
            document.getElementById('next-btn').disabled = currentPage >= totalPages;
        }
        
        function changePage(delta) {
            const totalPages = Math.ceil(totalRows / pageSize);
            const newPage = currentPage + delta;
            if (newPage >= 1 && newPage <= totalPages) {
                currentPage = newPage;
                loadTableData();
            }
        }
        
        function changePageSize() {
            pageSize = parseInt(document.getElementById('page-size').value);
            currentPage = 1;
            loadTableData();
        }
        
        function closeViewer() {
            document.getElementById('table-viewer').classList.remove('active');
        }
        
        function showError(message) {
            const container = document.getElementById('error-container');
            container.innerHTML = `<div class="error">${message}</div>`;
            setTimeout(() => {
                container.innerHTML = '';
            }, 5000);
        }
        
        function escapeHtml(text) {
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }
        
        // Real-time monitoring configuration
        let realTimeEnabled = false;
        let realTimeInterval = null;
        let lastTableState = {}; // Store last known state for change detection
        const REAL_TIME_INTERVAL_MS = 2000; // 2 seconds for near real-time
        
        function toggleRealTime() {
            realTimeEnabled = !realTimeEnabled;
            const btn = document.getElementById('auto-refresh-btn');
            
            if (realTimeEnabled) {
                // Start real-time monitoring with change detection
                realTimeInterval = setInterval(async () => {
                    await checkForChanges();
                }, REAL_TIME_INTERVAL_MS);
                btn.textContent = '⏸️ Stop Real-Time';
                btn.style.background = '#ef4444';
                console.log('Real-time monitoring enabled (2s interval)');
            } else {
                // Stop real-time monitoring
                if (realTimeInterval) {
                    clearInterval(realTimeInterval);
                    realTimeInterval = null;
                }
                btn.textContent = '⚡ Enable Real-Time';
                btn.style.background = '#10b981';
                console.log('Real-time monitoring disabled');
            }
        }
        
        async function checkForChanges() {
            try {
                const response = await fetch('/api/tables');
                if (!response.ok) return;
                
                const data = await response.json();
                if (data.error || !data.tables) return;
                
                // Check for changes in row counts
                let hasChanges = false;
                const currentState = {};
                
                data.tables.forEach(table => {
                    currentState[table.name] = {
                        row_count: table.row_count,
                        columns: table.columns.length
                    };
                    
                    // Compare with last known state
                    if (lastTableState[table.name]) {
                        const last = lastTableState[table.name];
                        if (last.row_count !== table.row_count) {
                            hasChanges = true;
                            console.log(`Change detected in ${table.name}: ${last.row_count} → ${table.row_count} rows`);
                        }
                    } else {
                        // First time, consider it a change
                        hasChanges = true;
                    }
                });
                
                // Update if there are changes
                if (hasChanges) {
                    lastTableState = currentState;
                    await loadTables(); // Refresh the display
                    
                    // If a table viewer is open, refresh it too
                    if (currentTable) {
                        await loadTableData();
                    }
                }
            } catch (error) {
                console.error('Error checking for changes:', error);
            }
        }
        
        // Load tables on page load
        loadTables();

        // ── Tab switching ──────────────────────────────────────────────
        let activeTab = 'db';
        let tradingInterval = null;

        function switchTab(tab) {
            activeTab = tab;
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
            document.getElementById('tab-' + tab).classList.add('active');
            document.getElementById('panel-' + tab).classList.add('active');
            if (tab === 'trading') loadTradingData();
            if (tab === 'graph') loadChart();
            if (tab === 'trades') loadTradesChart();
        }

        // ── Graph tab ─────────────────────────────────────────────────
        let btcChart = null;
        let graphInterval = null;

        async function loadChart() {
            const symbol = document.getElementById('graph-symbol').value;
            const field  = document.getElementById('graph-field').value;
            const limit  = document.getElementById('graph-limit').value;
            const loadEl = document.getElementById('graph-loading');
            const errEl  = document.getElementById('graph-error');
            loadEl.style.display = 'block';
            errEl.style.display  = 'none';
            try {
                const res = await fetch(`/api/chart?symbol=${symbol}&limit=${limit}`);
                if (!res.ok) throw new Error(`HTTP ${res.status}`);
                const data = await res.json();
                if (data.error) throw new Error(data.error);
                renderChart(data.rows, field, symbol);
                renderStats(data.rows, field);
                document.getElementById('graph-last-update').textContent =
                    'Updated: ' + new Date().toLocaleTimeString();
            } catch(e) {
                errEl.textContent = 'Failed to load chart: ' + e.message;
                errEl.style.display = 'block';
            } finally {
                loadEl.style.display = 'none';
            }
        }

        function renderChart(rows, field, symbol) {
            const labels = rows.map(r => {
                const d = new Date(r.ts_ms);
                return d.toLocaleTimeString();
            });
            const values = rows.map(r =>
                field === 'mid' ? (r.best_bid + r.best_ask) / 2 : r[field]
            );
            const label = field === 'mid' ? 'Mid Price' :
                          field === 'best_bid' ? 'Best Bid' :
                          field === 'best_ask' ? 'Best Ask' : 'Spread';
            const color = field === 'spread' ? '#f59e0b' : '#3b82f6';

            if (btcChart) { btcChart.destroy(); btcChart = null; }
            const ctx = document.getElementById('btcChart').getContext('2d');
            btcChart = new Chart(ctx, {
                type: 'line',
                data: {
                    labels,
                    datasets: [{
                        label: `${symbol} ${label}`,
                        data: values,
                        borderColor: color,
                        backgroundColor: color + '18',
                        borderWidth: 1.5,
                        pointRadius: rows.length < 100 ? 3 : 0,
                        fill: true,
                        tension: 0.1,
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    animation: false,
                    plugins: {
                        legend: { labels: { color: '#ccc' } },
                        tooltip: {
                            callbacks: {
                                label: ctx => `${label}: ${ctx.parsed.y.toFixed(2)}`
                            }
                        }
                    },
                    scales: {
                        x: {
                            ticks: { color: '#aaa', maxTicksLimit: 12, maxRotation: 0 },
                            grid:  { color: '#2a2a4a' }
                        },
                        y: {
                            ticks: { color: '#aaa', callback: v => v.toFixed(2) },
                            grid:  { color: '#2a2a4a' }
                        }
                    }
                }
            });
        }

        function renderStats(rows, field) {
            if (!rows.length) return;
            const vals = rows.map(r =>
                field === 'mid' ? (r.best_bid + r.best_ask) / 2 : r[field]);
            const min = Math.min(...vals), max = Math.max(...vals);
            const last = vals[vals.length - 1];
            const first = vals[0];
            const pct = ((last - first) / first * 100).toFixed(3);
            const color = pct >= 0 ? '#22c55e' : '#ef4444';
            document.getElementById('graph-stats').innerHTML = `
                <div style="background:#fff2;border-radius:8px;padding:10px 18px;color:#fff;">
                    <div style="font-size:0.75em;color:#aaa;">LAST</div>
                    <div style="font-size:1.3em;font-weight:700;">${last.toFixed(2)}</div>
                </div>
                <div style="background:#fff2;border-radius:8px;padding:10px 18px;color:#fff;">
                    <div style="font-size:0.75em;color:#aaa;">CHANGE</div>
                    <div style="font-size:1.3em;font-weight:700;color:${color};">${pct >= 0 ? '+' : ''}${pct}%</div>
                </div>
                <div style="background:#fff2;border-radius:8px;padding:10px 18px;color:#fff;">
                    <div style="font-size:0.75em;color:#aaa;">HIGH</div>
                    <div style="font-size:1.3em;font-weight:700;">${max.toFixed(2)}</div>
                </div>
                <div style="background:#fff2;border-radius:8px;padding:10px 18px;color:#fff;">
                    <div style="font-size:0.75em;color:#aaa;">LOW</div>
                    <div style="font-size:1.3em;font-weight:700;">${min.toFixed(2)}</div>
                </div>
                <div style="background:#fff2;border-radius:8px;padding:10px 18px;color:#fff;">
                    <div style="font-size:0.75em;color:#aaa;">POINTS</div>
                    <div style="font-size:1.3em;font-weight:700;">${rows.length.toLocaleString()}</div>
                </div>`;
        }

        function toggleGraphRealTime() {
            const btn = document.getElementById('graph-auto-btn');
            if (graphInterval) {
                clearInterval(graphInterval);
                graphInterval = null;
                btn.textContent = '⚡ Enable Real-Time';
                btn.style.background = '#48bb78';
            } else {
                loadChart();
                graphInterval = setInterval(loadChart, 3000);
                btn.textContent = '⏸ Stop Real-Time';
                btn.style.background = '#ed8936';
            }
        }

        // ── Trading dashboard ─────────────────────────────────────────
        let tradingFailCount = 0;
        async function loadTradingData() {
            try {
                await Promise.all([loadPositions(), loadAlgoStatus(), loadOpenOrders(), loadOrderHistory(), loadLogs()]);
                tradingFailCount = 0;
                const el = document.getElementById('trading-last-update');
                if (el) el.textContent = 'Last updated: ' + new Date().toLocaleTimeString();
                const banner = document.getElementById('trading-error-banner');
                if (banner) banner.style.display = 'none';
            } catch(e) {
                tradingFailCount++;
                if (tradingFailCount >= 3) {
                    let banner = document.getElementById('trading-error-banner');
                    if (!banner) {
                        banner = document.createElement('div');
                        banner.id = 'trading-error-banner';
                        banner.style.cssText = 'background:#7f1d1d;color:#fca5a5;padding:8px 16px;border-radius:8px;margin-bottom:10px;font-size:0.9em;';
                        document.getElementById('panel-trading').prepend(banner);
                    }
                    banner.style.display = 'block';
                    banner.textContent = 'fix_server connection issue — retrying... (last error: ' + e.message + ')';
                }
            }
        }

        async function loadPositions() {
            const el = document.getElementById('positions-content');
            try {
                const [rPos, rHist] = await Promise.all([
                    fetch('/proxy/positions'),
                    fetch('/proxy/orders/history')
                ]);
                if (!rPos.ok) { el.innerHTML = '<div class="error">fix_server unreachable</div>'; return; }
                const data = await rPos.json();
                const positions = data.positions || data;
                if (!positions.length) { el.innerHTML = '<p style="color:#888">No open positions</p>'; return; }

                // tally fees from fills: commission is in base currency (BTC), convert to quote (USDT)
                const feesBySymbol = {};
                if (rHist.ok) {
                    const hdata = await rHist.json();
                    for (const o of (hdata.orders || [])) {
                        if (!o.commission || !o.avg_px) continue;
                        const comm = parseFloat(o.commission);   // negative = paid
                        const px   = parseFloat(o.avg_px);
                        if (!comm || !px) continue;
                        const sym = o.symbol || '';
                        const feeUsdt = Math.abs(comm) * px;     // BTC fee × fill price = USDT
                        feesBySymbol[sym] = (feesBySymbol[sym] || 0) + feeUsdt;
                    }
                }

                const c = (v, pos, neg) => v > 0 ? pos : v < 0 ? neg : '#6b7280';
                const fmt = v => (v >= 0 ? '+' : '') + v.toFixed(4);
                const cell = (v, note) => '<span style="color:' + c(v,'#10b981','#ef4444') + ';font-weight:600">' + fmt(v) + '</span>' + (note ? '<br><span style="font-size:0.75em;color:#9ca3af">' + note + '</span>' : '');

                let html = '<table style="width:100%"><thead><tr>' +
                    '<th>Symbol</th><th>Qty</th><th>Avg Entry</th><th>Last Mid</th>' +
                    '<th title="Price movement on open position">Unrealized P&L</th>' +
                    '<th title="Closed trade P&L (price only, no fees)">Realized P&L</th>' +
                    '<th title="Fees paid on visible fills (partial — API shows last 30 orders)">Est. Fees Paid</th>' +
                    '<th title="Realized P&L minus estimated fees">Net Realized</th>' +
                    '<th>Trades</th></tr></thead><tbody>';

                for (const p of positions) {
                    const upnl  = parseFloat(p.unrealized_pnl || 0);
                    const rpnl  = parseFloat(p.realized_pnl   || 0);
                    const fees  = feesBySymbol[p.symbol] || 0;
                    const net   = rpnl - fees;
                    const mid   = p.last_mid_px ? parseFloat(p.last_mid_px).toFixed(2) : '-';
                    html += '<tr>' +
                        '<td><strong>' + escapeHtml(p.symbol) + '</strong></td>' +
                        '<td>' + parseFloat(p.net_qty).toFixed(6) + '</td>' +
                        '<td>' + parseFloat(p.avg_entry_px).toFixed(2) + '</td>' +
                        '<td>' + mid + '</td>' +
                        '<td>' + cell(upnl) + '</td>' +
                        '<td>' + cell(rpnl) + '</td>' +
                        '<td style="color:#f59e0b;font-weight:600">-' + fees.toFixed(4) + (fees === 0 ? ' <span style="font-size:0.7em;color:#9ca3af">(no fills)</span>' : '') + '</td>' +
                        '<td>' + cell(net, fees > 0 ? '' : '') + '</td>' +
                        '<td>' + (p.trade_count || 0) + '</td>' +
                        '</tr>';
                }
                html += '</tbody></table>';

                // summary bar
                const totUpnl = positions.reduce((s,p) => s + parseFloat(p.unrealized_pnl||0), 0);
                const totRpnl = positions.reduce((s,p) => s + parseFloat(p.realized_pnl||0), 0);
                const totFees = Object.values(feesBySymbol).reduce((s,v) => s+v, 0);
                const totNet  = totRpnl - totFees;
                html += '<div style="display:flex;gap:24px;margin-top:12px;padding:10px 4px;border-top:1px solid #e5e7eb;flex-wrap:wrap;font-size:0.9em;">' +
                    '<span>Unrealized: <strong style="color:' + c(totUpnl,'#10b981','#ef4444') + '">' + fmt(totUpnl) + ' USDT</strong></span>' +
                    '<span>Realized (gross): <strong style="color:' + c(totRpnl,'#10b981','#ef4444') + '">' + fmt(totRpnl) + ' USDT</strong></span>' +
                    '<span>Fees: <strong style="color:#f59e0b">-' + totFees.toFixed(4) + ' USDT</strong></span>' +
                    '<span style="border-left:2px solid #e5e7eb;padding-left:20px">Net Realized: <strong style="color:' + c(totNet,'#10b981','#ef4444') + '">' + fmt(totNet) + ' USDT</strong></span>' +
                    '</div>';

                el.innerHTML = html;
            } catch(e) { el.innerHTML = '<div class="error">' + e.message + '</div>'; }
        }

        async function loadAlgoStatus() {
            const el = document.getElementById('algo-content');
            try {
                const r = await fetch('/proxy/algo/status');
                if (!r.ok) { el.innerHTML = '<div class="error">fix_server unreachable</div>'; return; }
                const d = await r.json();
                const cfg = d.config || {};
                const enabled = cfg.enabled;
                const signals = d.recent_signals || [];

                // Populate form fields from live config
                const set = (id, val) => { const el = document.getElementById(id); if (el && val !== undefined) el.value = val; };
                set('cfg-strategy',     cfg.strategy || 'spread_capture');
                set('cfg-symbol',       cfg.symbol || 'BTC_USDT');
                set('cfg-order-qty',    cfg.order_qty);
                set('cfg-max-pos',      cfg.max_position);
                set('cfg-imbal-thresh', cfg.imbalance_threshold);
                set('cfg-depth',        cfg.imbalance_depth_levels);
                set('cfg-min-spread',   cfg.min_spread_bps);
                set('cfg-cooldown',     cfg.min_signal_interval_ms);

                let html = `
                    <div style="margin-bottom:10px">
                        <span class="badge ${enabled ? 'badge-green' : 'badge-gray'}">${enabled ? '● ENABLED' : '○ DISABLED'}</span>
                        &nbsp; <span style="font-size:0.85em;color:#666">Strategy: <strong>${(cfg.strategy || 'spread_capture').replace('_',' ').toUpperCase()}</strong></span>
                    </div>
                    <div style="font-size:0.85em;color:#555;margin-bottom:8px">
                        Symbol: <strong>${cfg.symbol || '–'}</strong> &nbsp;
                        Qty: <strong>${cfg.order_qty || '–'}</strong> &nbsp;
                        MaxPos: <strong>${cfg.max_position || '–'}</strong> &nbsp;
                        ImbalThresh: <strong>${cfg.imbalance_threshold || '–'}</strong>
                    </div>
                    <div style="font-size:0.8em;color:#888;margin-bottom:6px">Signals fired: ${d.signals_fired || 0} &nbsp; Orders placed: ${d.orders_placed || 0}</div>`;
                if (signals.length) {
                    html += '<div style="font-size:0.8em;color:#555;margin-bottom:4px;font-weight:600">Recent signals:</div>';
                    for (const s of signals.slice(0, 6)) {
                        const side = s.side === '1' ? '<span class="badge badge-green">BUY</span>' : '<span class="badge badge-red">SELL</span>';
                        html += `<div class="signal-row">${side} ${escapeHtml(s.symbol)} qty=${s.qty} px=${parseFloat(s.price).toFixed(2)} — <span style="color:#999">${escapeHtml((s.reason||'').slice(0,60))}</span></div>`;
                    }
                } else {
                    html += '<div style="color:#888;font-size:0.85em">No recent signals</div>';
                }
                el.innerHTML = html;
            } catch(e) { el.innerHTML = `<div class="error">${e.message}</div>`; }
        }

        async function applyAlgoConfig(enabled) {
            const statusEl = document.getElementById('algo-apply-status');
            statusEl.textContent = 'Applying...';
            statusEl.style.color = '#666';
            try {
                // Fetch current config to preserve fields not shown in the form
                const cur = await fetch('/proxy/algo/status');
                const curData = await cur.json();
                const base = curData.config || {};

                // Merge form values over current config
                const body = Object.assign({}, base, {
                    strategy:               document.getElementById('cfg-strategy').value,
                    symbol:                 document.getElementById('cfg-symbol').value,
                    order_qty:              parseFloat(document.getElementById('cfg-order-qty').value),
                    max_position:           parseFloat(document.getElementById('cfg-max-pos').value),
                    imbalance_threshold:    parseFloat(document.getElementById('cfg-imbal-thresh').value),
                    imbalance_depth_levels: parseInt(document.getElementById('cfg-depth').value),
                    min_spread_bps:         parseFloat(document.getElementById('cfg-min-spread').value),
                    min_signal_interval_ms: parseInt(document.getElementById('cfg-cooldown').value),
                });
                if (enabled !== null) body.enabled = enabled;

                const r = await fetch('/proxy/algo/config', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(body),
                });
                if (!r.ok) {
                    const txt = await r.text();
                    throw new Error(`HTTP ${r.status}: ${txt}`);
                }
                statusEl.textContent = '✓ Applied';
                statusEl.style.color = '#22c55e';
                setTimeout(() => { statusEl.textContent = ''; }, 3000);
                await loadAlgoStatus();
            } catch(e) {
                statusEl.textContent = '✗ ' + e.message;
                statusEl.style.color = '#ef4444';
            }
        }

        const ORD_STATUS = {'0':'New','1':'Partial','2':'Filled','F':'Filled','4':'Cancelled','8':'Rejected'};
        const ORD_STATUS_COLOR = {'0':'#6366f1','1':'#f59e0b','2':'#10b981','F':'#10b981','4':'#6b7280','8':'#ef4444'};
        let currentMidPx = {};
        async function loadOrderHistory() {
            const el = document.getElementById('order-history-content');
            if (!el) return;
            const filter = document.getElementById('order-history-filter')?.value || '';
            try {
                // fetch order history and current positions in parallel
                const [rh, rp] = await Promise.all([
                    fetch('/proxy/orders/history'),
                    fetch('/proxy/positions')
                ]);
                if (!rh.ok) { el.innerHTML = '<div class="error">fix_server unreachable</div>'; return; }
                const hdata = await rh.json();
                if (rp.ok) {
                    const pdata = await rp.json();
                    for (const p of (pdata.positions || [])) {
                        currentMidPx[p.symbol] = parseFloat(p.last_mid_px || 0);
                    }
                }
                let orders = (hdata.orders || hdata).filter(o => o.cl_ord_id);
                if (filter) orders = orders.filter(o => (o.ord_status||o.exec_status) === filter);
                if (!orders.length) { el.innerHTML = '<p style="color:#888">No orders found</p>'; return; }
                // summary row for fills
                const fills = orders.filter(o => (o.ord_status||'') === 'F' || (o.exec_type||'') === 'F');
                let summaryHtml = '';
                if (fills.length) {
                    let totalPnl = 0;
                    for (const o of fills) {
                        const avgPx = parseFloat(o.avg_px || 0);
                        const qty = parseFloat(o.cum_qty || 0);
                        const mid = currentMidPx[o.symbol] || 0;
                        if (avgPx > 0 && qty > 0 && mid > 0) {
                            const pnl = o.side === '1' ? (mid - avgPx) * qty : (avgPx - mid) * qty;
                            totalPnl += pnl;
                        }
                    }
                    const pnlColor = totalPnl >= 0 ? '#10b981' : '#ef4444';
                    summaryHtml = '<div style="padding:8px 4px;margin-bottom:8px;font-size:0.9em;border-bottom:1px solid #e5e7eb">' +
                        fills.length + ' filled order(s) &nbsp;|&nbsp; ' +
                        'Floating P&L vs mid: <strong style="color:' + pnlColor + '">' +
                        (totalPnl >= 0 ? '+' : '') + totalPnl.toFixed(4) + ' USDT</strong></div>';
                }
                let html = summaryHtml + '<table><thead><tr><th>Time</th><th>ClOrdID</th><th>Symbol</th><th>Side</th><th>Fill Px</th><th>Qty</th><th>P&L vs Mid</th><th>Commission</th><th>Status</th></tr></thead><tbody>';
                for (const o of orders) {
                    const isFill = (o.ord_status||'') === 'F' || (o.exec_type||'') === 'F';
                    const side = o.side === '1' ? '<span style="color:#10b981">BUY</span>' : '<span style="color:#ef4444">SELL</span>';
                    const st = o.ord_status || o.exec_status || '?';
                    const stLabel = ORD_STATUS[st] || st;
                    const stColor = ORD_STATUS_COLOR[st] || '#888';
                    const t = o.transact_time || o.order_create_time || '';
                    const tFmt = t ? t.replace('T',' ').substring(0,19) : '-';
                    const avgPx = parseFloat(o.avg_px || 0);
                    const cumQty = parseFloat(o.cum_qty || 0);
                    const mid = currentMidPx[o.symbol] || 0;
                    let pnlCell = '-';
                    if (isFill && avgPx > 0 && cumQty > 0 && mid > 0) {
                        const pnl = o.side === '1' ? (mid - avgPx) * cumQty : (avgPx - mid) * cumQty;
                        const pnlColor = pnl >= 0 ? '#10b981' : '#ef4444';
                        pnlCell = '<span style="color:' + pnlColor + ';font-weight:600">' + (pnl >= 0 ? '+' : '') + pnl.toFixed(4) + '</span>';
                    }
                    const comm = o.commission ? parseFloat(o.commission).toFixed(6) + ' ' + (o.commission_currency||'') : '-';
                    html += '<tr>' +
                        '<td style="font-size:0.8em;white-space:nowrap">' + escapeHtml(tFmt) + '</td>' +
                        '<td style="font-size:0.8em">' + escapeHtml(o.cl_ord_id||'-') + '</td>' +
                        '<td><strong>' + escapeHtml(o.symbol||'-') + '</strong></td>' +
                        '<td>' + side + '</td>' +
                        '<td>' + (avgPx > 0 ? avgPx.toFixed(2) : (o.price||'-')) + '</td>' +
                        '<td>' + (cumQty > 0 ? cumQty : (o.order_qty||'-')) + '</td>' +
                        '<td>' + pnlCell + '</td>' +
                        '<td style="font-size:0.8em;color:#888">' + comm + '</td>' +
                        '<td><span style="color:' + stColor + ';font-weight:600">' + stLabel + '</span></td>' +
                        '</tr>';
                }
                html += '</tbody></table>';
                el.innerHTML = html;
            } catch(e) { el.innerHTML = '<div class="error">' + e.message + '</div>'; }
        }

        async function loadOpenOrders() {
            const el = document.getElementById('open-orders-content');
            try {
                const r = await fetch('/proxy/open-orders');
                if (!r.ok) { el.innerHTML = '<div class="error">fix_server unreachable</div>'; return; }
                const orders = await r.json();
                if (!orders.length) { el.innerHTML = '<p style="color:#888">No open orders</p>'; return; }
                let html = '<table><thead><tr><th>ClOrdID</th><th>Symbol</th><th>Side</th><th>Qty</th><th>Price</th><th>Leaves</th><th>Status</th></tr></thead><tbody>';
                for (const o of orders) {
                    const side = o.side === '1' ? '<span class="badge badge-green">BUY</span>' : '<span class="badge badge-red">SELL</span>';
                    html += `<tr>
                        <td style="font-size:0.8em">${escapeHtml(o.cl_ord_id)}</td>
                        <td><strong>${escapeHtml(o.symbol)}</strong></td>
                        <td>${side}</td>
                        <td>${o.order_qty}</td>
                        <td>${o.price}</td>
                        <td>${o.leaves_qty}</td>
                        <td><span class="badge badge-gray">${escapeHtml(o.ord_status)}</span></td>
                    </tr>`;
                }
                html += '</tbody></table>';
                el.innerHTML = html;
            } catch(e) { el.innerHTML = `<div class="error">${e.message}</div>`; }
        }

        // ── Trades chart (Lightweight Charts) ──────────────────────────
        let twChart = null;
        let twSeries = null;
        let tradesRealTimeInterval = null;

        async function loadTradesChart() {
            const container = document.getElementById('trades-chart');
            const loading = document.getElementById('trades-loading');
            const legend = document.getElementById('trades-legend');
            if (!container) return;
            loading.style.display = 'block';
            const symbol = document.getElementById('trades-symbol').value;
            const limit = document.getElementById('trades-limit').value;
            try {
                const [rPrice, rOrders] = await Promise.all([
                    fetch('/api/chart?symbol=' + symbol + '&limit=' + limit),
                    fetch('/proxy/orders/history')
                ]);
                const priceData = await rPrice.json();
                const orderData = rOrders.ok ? await rOrders.json() : {orders:[]};
                const rows = priceData.rows || [];
                if (!rows.length) {
                    container.innerHTML = '<div style="color:#888;text-align:center;padding-top:180px;font-size:1em;">No price data for ' + symbol + '</div>';
                    loading.style.display = 'none';
                    return;
                }
                // build price series — skip rows with zero bid/ask (partial incremental updates)
                const bySecond = {};
                for (const r of rows) {
                    if (r.best_bid <= 0 || r.best_ask <= 0) continue;
                    if (r.best_ask > r.best_bid * 1.05) continue; // skip clearly bad spreads (>5%)
                    const t = Math.floor(r.ts_ms / 1000);
                    const mid = (r.best_bid + r.best_ask) / 2;
                    bySecond[t] = mid;
                }
                const priceSeries = Object.keys(bySecond).sort((a,b)=>a-b)
                    .map(t => ({time: parseInt(t), value: bySecond[t]}));
                // build markers from filled orders
                const fills = (orderData.orders || []).filter(o => (o.exec_type==='F' || o.ord_status==='F') && o.avg_px && parseFloat(o.avg_px)>0);
                const markers = [];
                for (const o of fills) {
                    const ts = o.transact_time || o.trade_transact_time || '';
                    if (!ts) continue;
                    // format: 20251201-10:26:26.627... -> 2025-12-01T10:26:26Z
                    const ds = ts.substring(0,4)+'-'+ts.substring(4,6)+'-'+ts.substring(6,8)+'T'+ts.substring(9,17)+'Z';
                    const t = Math.floor(new Date(ds).getTime() / 1000);
                    if (isNaN(t)) continue;
                    const isBuy = o.side === '1';
                    markers.push({
                        time: t,
                        position: isBuy ? 'belowBar' : 'aboveBar',
                        color: isBuy ? '#26a69a' : '#ef5350',
                        shape: isBuy ? 'arrowUp' : 'arrowDown',
                        text: (isBuy ? 'B ' : 'S ') + parseFloat(o.avg_px).toFixed(0) + ' x' + parseFloat(o.cum_qty||0)
                    });
                }
                markers.sort((a,b) => a.time - b.time);
                // (re)create chart
                if (twChart) { twChart.remove(); twChart = null; twSeries = null; }
                container.innerHTML = '';
                const chartH = 480;
                const chartW = container.clientWidth || window.innerWidth - 80 || 1200;
                twChart = LightweightCharts.createChart(container, {
                    width: chartW,
                    height: chartH,
                    layout: { background: {color:'#131722'}, textColor: '#d1d4dc' },
                    grid: { vertLines: {color:'#1e2130'}, horzLines: {color:'#1e2130'} },
                    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
                    rightPriceScale: { borderColor: '#2a2e39' },
                    timeScale: { borderColor: '#2a2e39', timeVisible: true, secondsVisible: false }
                });
                twSeries = twChart.addLineSeries({
                    color: '#2962ff',
                    lineWidth: 2,
                    priceLineVisible: true,
                    lastValueVisible: true
                });
                twSeries.setData(priceSeries);
                if (markers.length) twSeries.setMarkers(markers);
                twChart.timeScale().fitContent();
                // legend
                const last = priceSeries[priceSeries.length-1];
                const first = priceSeries[0];
                const chg = last && first ? ((last.value - first.value)/first.value*100).toFixed(2) : '?';
                const chgColor = parseFloat(chg) >= 0 ? '#26a69a' : '#ef5350';
                legend.innerHTML =
                    '<span>' + symbol + '</span>' +
                    '<span>Last: <strong>' + (last ? last.value.toFixed(2) : '-') + '</strong></span>' +
                    '<span>Change: <strong style="color:' + chgColor + '">' + (parseFloat(chg)>=0?'+':'') + chg + '%</strong></span>' +
                    '<span>' + priceSeries.length + ' ticks</span>' +
                    (fills.length ? '<span style="color:#26a69a">▲ BUY (' + fills.filter(o=>o.side==="1").length + ')</span>' : '') +
                    (fills.filter(o=>o.side==="2").length ? '<span style="color:#ef5350">▼ SELL (' + fills.filter(o=>o.side==="2").length + ')</span>' : '');
                document.getElementById('trades-last-update').textContent = 'Updated: ' + new Date().toLocaleTimeString();
                // resize observer
                const ro = new ResizeObserver(() => { if (twChart) twChart.applyOptions({width: container.clientWidth}); });
                ro.observe(container);
            } catch(e) {
                container.innerHTML = '<div style="color:#ef5350;padding:20px">' + e.message + '</div>';
            }
            loading.style.display = 'none';
        }

        function toggleTradesRealTime() {
            const btn = document.getElementById('trades-auto-btn');
            if (tradesRealTimeInterval) {
                clearInterval(tradesRealTimeInterval);
                tradesRealTimeInterval = null;
                btn.textContent = 'Enable Real-Time';
                btn.style.background = '#10b981';
            } else {
                loadTradesChart();
                tradesRealTimeInterval = setInterval(loadTradesChart, 5000);
                btn.textContent = 'Stop Real-Time';
                btn.style.background = '#ef4444';
            }
        }

        let logInterval = null;
        async function loadLogs() {
            const el = document.getElementById('logs-content');
            if (!el) return;
            const service = document.getElementById('log-service')?.value || 'fix-server';
            const filter = document.getElementById('log-filter')?.value || '';
            const verbose = document.getElementById('log-verbose')?.checked || false;
            try {
                let url = `/api/logs?service=${encodeURIComponent(service)}&lines=80`;
                if (filter) url += `&filter=${encodeURIComponent(filter)}`;
                if (verbose) url += `&verbose=1`;
                const r = await fetch(url);
                const d = await r.json();
                if (d.lines && d.lines.length > 0) {
                    el.textContent = d.lines.join('\\n');
                } else {
                    el.textContent = '(no log lines)';
                }
                el.scrollTop = el.scrollHeight;
            } catch(e) { el.textContent = `Error: ${e.message}`; }
        }

        function toggleLogRealTime() {
            const btn = document.getElementById('log-auto-btn');
            if (logInterval) {
                clearInterval(logInterval);
                logInterval = null;
                btn.textContent = '⚡ Live';
                btn.style.background = '#10b981';
            } else {
                loadLogs();
                logInterval = setInterval(loadLogs, 3000);
                btn.textContent = '⏸ Stop';
                btn.style.background = '#ef4444';
            }
        }

        function toggleTradingRealTime() {
            const btn = document.getElementById('trading-auto-btn');
            if (tradingInterval) {
                clearInterval(tradingInterval);
                tradingInterval = null;
                btn.textContent = '⚡ Enable Real-Time';
                btn.style.background = '#10b981';
            } else {
                loadTradingData();
                tradingInterval = setInterval(loadTradingData, 2000);
                btn.textContent = '⏸ Stop Real-Time';
                btn.style.background = '#ef4444';
            }
        }

        // ── Auto-start real-time on page load ─────────────────────────
        (function() {
            // DB tab: enable real-time row-change detection
            realTimeEnabled = true;
            realTimeInterval = setInterval(checkForChanges, REAL_TIME_INTERVAL_MS);
            const dbBtn = document.getElementById('auto-refresh-btn');
            if (dbBtn) { dbBtn.textContent = '⏸️ Stop Real-Time'; dbBtn.style.background = '#ef4444'; }

            // Trading tab: start polling immediately (visible even when on DB tab)
            loadTradingData();
            tradingInterval = setInterval(loadTradingData, 2000);
            const tradeBtn = document.getElementById('trading-auto-btn');
            if (tradeBtn) { tradeBtn.textContent = '⏸ Stop Real-Time'; tradeBtn.style.background = '#ef4444'; }
        })();
    </script>
</body>
</html>
    """
    return HTMLResponse(content=html_content)

def execute_q_query(query: str):
    """Execute a q query using available method"""
    kdb_logger.debug(f"Executing query: {query}")
    start_time = time.time()
    
    if HAS_QPATHON:
        q = None
        try:
            q = QConnection(host=KDB_HOST, port=KDB_PORT, timeout=5)
            q.open()
            result = q(query)
            q.close()
            elapsed = time.time() - start_time
            kdb_logger.info(f"Query executed successfully in {elapsed:.3f}s: {query[:100]}")
            return result
        except Exception as e:
            if q:
                try:
                    q.close()
                except:
                    pass
            elapsed = time.time() - start_time
            kdb_logger.error(f"Query failed after {elapsed:.3f}s: {query[:100]} - {e}")
            raise e
    else:
        # Use custom IPC client directly (q command doesn't work well for remote connections)
        client = None
        try:
            client = KdbIPCClient(host=KDB_HOST, port=KDB_PORT, timeout=10)
            client.connect()
            result = client.execute(query)
            client.close()
            elapsed = time.time() - start_time
            kdb_logger.info(f"Query executed successfully via IPC in {elapsed:.3f}s: {query[:100]}")
            return result
        except Exception as e:
            if client:
                try:
                    client.close()
                except:
                    pass
            elapsed = time.time() - start_time
            error_logger.error(f"IPC client failed after {elapsed:.3f}s: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=503,
                detail=f"Failed to connect to kdb+ at {KDB_HOST}:{KDB_PORT}: {str(e)}"
            )

@app.get("/api/tables")
async def get_tables():
    """Get list of all tables in the kdb+ database"""
    # kdb+ char type codes → readable names
    KTYPE = {
        'b': 'boolean', 'g': 'guid', 'x': 'byte', 'h': 'short',
        'i': 'int',     'j': 'long', 'e': 'real', 'f': 'float',
        'c': 'char',    's': 'symbol', 'p': 'timestamp', 'm': 'month',
        'd': 'date',    'z': 'datetime', 'n': 'timespan', 'u': 'minute',
        'v': 'second',  't': 'time',
    }
    try:
        # Use tables[] — returns only actual tables, not functions/variables
        tables_result = execute_q_query('tables[]')
        if isinstance(tables_result, str):
            table_names = [t.strip() for t in tables_result.split() if t.strip()]
        elif isinstance(tables_result, list):
            table_names = [str(t) for t in tables_result]
        else:
            table_names = []

        table_info = []
        for table_name in table_names:
            try:
                row_count = int(execute_q_query(f'count {table_name}') or 0)

                meta_result = execute_q_query(f'meta {table_name}')
                columns = []
                if isinstance(meta_result, list):
                    # list of dicts: [{'c': 'colname', 't': 'f', ...}, ...]
                    for row in meta_result:
                        if isinstance(row, dict):
                            columns.append({
                                "name": str(row.get('c', '')),
                                "type": KTYPE.get(str(row.get('t', '')), str(row.get('t', 'unknown'))),
                            })

                table_info.append({"name": table_name, "row_count": row_count, "columns": columns})
            except Exception as e:
                error_logger.error(f"Error processing table {table_name}: {e}")
                table_info.append({"name": table_name, "row_count": 0, "columns": []})
        
        app_logger.info(f"Retrieved {len(table_info)} tables")
        return {"tables": table_info}
    except Exception as e:
        error_logger.error(f"Error getting tables: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

@app.get("/api/table/{table_name}")
async def get_table_data(table_name: str, page: int = 1, page_size: int = 100):
    """Get paginated data from a specific kdb+ table"""
    try:
        # Validate table exists
        tables_result = execute_q_query('tables[]')
        # Parse kdb+ symbol list format: `table1`table2`table3
        if hasattr(tables_result, 'tolist'):
            table_names = [str(t) for t in tables_result.tolist()]
        elif isinstance(tables_result, (list, tuple)):
            table_names = [str(t) for t in tables_result]
        elif isinstance(tables_result, str):
            cleaned = tables_result.strip()
            if cleaned.startswith('`') or '`' in cleaned:
                parts = cleaned.split('`')
                table_names = [p.strip() for p in parts if p.strip()]
            else:
                table_names = [t.strip() for t in cleaned.replace(',', ' ').split() if t.strip()]
        else:
            table_names = [str(tables_result)] if tables_result else []
        
        if table_name not in table_names:
            raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")
        
        # Get total row count
        count_result = execute_q_query(f'count {table_name}')
        if isinstance(count_result, str):
            total_rows = int(count_result.strip()) if count_result.strip().isdigit() else 0
        else:
            total_rows = int(count_result) if count_result is not None else 0
        
        # Get column names from meta
        meta_result = execute_q_query(f'meta {table_name}')
        columns = []
        if meta_result is not None:
            if hasattr(meta_result, 'c'):
                col_names = meta_result.c.tolist() if hasattr(meta_result.c, 'tolist') else list(meta_result.c)
                columns = [str(c) for c in col_names]
            elif isinstance(meta_result, str):
                # Parse table format: "c | t f a\n---| ---\ncol1 | type ..."
                lines = meta_result.strip().split('\n')
                if len(lines) >= 2:
                    # Skip header line and separator
                    for line in lines[2:]:
                        parts = line.split('|')
                        if len(parts) >= 1:
                            col_name = parts[0].strip()
                            if col_name:
                                columns.append(col_name)
        
        # Get paginated data using kdb+ select with limit/offset
        offset = (page - 1) * page_size
        limit = page_size
        
        # kdb+ pagination: use take (#) and drop (_) operators
        if total_rows == 0:
            query = f'select from {table_name}'
        elif offset == 0:
            # Get first N rows
            query = f'{limit}#select from {table_name}'
        else:
            # Get rows from offset to offset+limit
            query = f'{limit}#{offset}_select from {table_name}'
        
        result = execute_q_query(query)
        
        # Convert result to list of lists
        rows = []
        if result is not None and result != '':
            # Result is a formatted table string from kdb+
            if isinstance(result, str):
                # Parse kdb+ table format
                lines = result.strip().split('\n')
                if len(lines) > 2:
                    # Skip header line and separator line
                    data_lines = lines[2:]
                    for line in data_lines:
                        if line.strip():
                            # Split by whitespace (kdb+ tables are space-aligned)
                            # More robust: split on multiple spaces
                            parts = [p.strip() for p in line.split() if p.strip()]
                            if parts and len(parts) == len(columns):
                                rows.append(parts)
                            elif parts:
                                # Try splitting by fixed positions or handle differently
                                # For now, just use what we have
                                row_data = parts[:len(columns)]
                                # Pad if needed
                                while len(row_data) < len(columns):
                                    row_data.append('')
                                rows.append(row_data[:len(columns)])
            elif hasattr(result, 'values'):
                # pandas DataFrame
                rows = result.values.tolist()
            elif hasattr(result, 'tolist'):
                rows = result.tolist()
            elif isinstance(result, (list, tuple)):
                rows = list(result)
            
            # Convert each cell value
            converted_rows = []
            for row in rows:
                converted_row = [convert_kdb_value(cell) for cell in row]
                converted_rows.append(converted_row)
            rows = converted_rows
        
        return {
            "columns": columns,
            "rows": rows,
            "total_rows": total_rows,
            "page": page,
            "page_size": page_size
        }
    except HTTPException:
        raise
    except Exception as e:
        error_logger.error(f"Error getting table data for {table_name}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

LOG_FILES = {
    "fix-server":       "/home/ubuntu/logs/fix-server-error.log",
    "drop-copy":        "/home/ubuntu/logs/drop-copy-error.log",
    "mgmt-server":      "/home/ubuntu/logs/mgmt-server-error.log",
    "cloudflare":       "/home/ubuntu/logs/cloudflare-tunnel-error.log",
}

# Lines containing these strings are too verbose to show by default
NOISE_PATTERNS = [
    "Buffered order book level",
    "write_order_book_level called",
    "Calling write_order_book_level",
    "Processing Ask update",
    "Processing Bid update",
    "New Ask (type:",
    "New Bid (type:",
    "Price:", "Size:",
    "ZeroMQ received update signal",
    "Checking order books",
    "Found order book for",
    "Order book snapshot:",
    "Publishing ZeroMQ",
    "Sent bids message",
    "Sent asks message",
    "Sent trades message",
    "Buffered snapshot",
    "kdb_integration",
]

@app.get("/api/logs")
async def get_logs(service: str = "fix-server", lines: int = 200, filter: str = "", verbose: bool = False):
    """Return the last N lines from a pm2 service log, with noise filtered out."""
    import subprocess
    log_path = LOG_FILES.get(service)
    if not log_path:
        raise HTTPException(status_code=400, detail=f"Unknown service: {service}")
    try:
        # Tail more lines to account for noise filtering
        result = subprocess.run(
            ["tail", "-n", str(lines * 10 if not verbose else lines), log_path],
            capture_output=True, text=True, timeout=5
        )
        raw_lines = result.stdout.splitlines()

        # Strip pm2 timestamp prefix: "2026-05-06 20:28:55: "
        import re
        cleaned = []
        for line in raw_lines:
            line = re.sub(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}: ', '', line)
            if not verbose and any(n in line for n in NOISE_PATTERNS):
                continue
            if filter and filter.lower() not in line.lower():
                continue
            cleaned.append(line)

        return {"lines": cleaned[-lines:], "service": service, "total": len(cleaned)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def _proxy(path: str, retries: int = 2):
    """Forward a GET request to fix_server and return the JSON response. Retries on timeout."""
    last_err = None
    for attempt in range(retries):
        try:
            resp = _requests.get(f"{FIX_SERVER_URL}{path}", timeout=5)
            resp.raise_for_status()
            return JSONResponse(content=resp.json())
        except _requests.ConnectionError as e:
            raise HTTPException(status_code=503, detail=f"fix_server unreachable at {FIX_SERVER_URL}")
        except _requests.Timeout as e:
            last_err = e
            continue  # retry
        except _requests.HTTPError as e:
            raise HTTPException(status_code=502, detail=str(e))
    raise HTTPException(status_code=504, detail="fix_server timed out after retries")


@app.get("/api/chart")
async def get_chart_data(symbol: str = "BTC_USDT", limit: int = 500):
    """Return time-series orderbook snapshot data for charting."""
    # kdb+ timestamps are nanoseconds since 2000-01-01; add 946684800000 ms to get Unix ms
    KDB_EPOCH_OFFSET_MS = 946684800000
    try:
        q = f"select from orderbook_snapshots where symbol=`{symbol}"
        if limit > 0:
            q = f"select[-{limit}] from orderbook_snapshots where symbol=`{symbol}"
        result = execute_q_query(q)
        rows = []
        if isinstance(result, list):
            for r in result:
                if not isinstance(r, dict):
                    continue
                ts_ns = r.get("time", 0)
                ts_ms = int(ts_ns / 1_000_000) + KDB_EPOCH_OFFSET_MS
                rows.append({
                    "ts_ms":        ts_ms,
                    "best_bid":     float(r.get("best_bid", 0) or 0),
                    "best_ask":     float(r.get("best_ask", 0) or 0),
                    "best_bid_size": float(r.get("best_bid_size", 0) or 0),
                    "best_ask_size": float(r.get("best_ask_size", 0) or 0),
                    "spread":       float(r.get("spread", 0) or 0),
                })
        return {"rows": rows, "symbol": symbol, "count": len(rows)}
    except Exception as e:
        return {"rows": [], "error": str(e), "symbol": symbol, "count": 0}


@app.post("/proxy/algo/config")
async def proxy_algo_config(request: Request):
    """Forward a POST to fix_server /api/algo/config."""
    try:
        body = await request.json()
        resp = _requests.post(
            f"{FIX_SERVER_URL}/api/algo/config",
            json=body,
            timeout=3,
        )
        resp.raise_for_status()
        return JSONResponse(content=resp.json())
    except _requests.ConnectionError:
        raise HTTPException(status_code=503, detail=f"fix_server unreachable at {FIX_SERVER_URL}")
    except _requests.Timeout:
        raise HTTPException(status_code=504, detail="fix_server timed out")
    except _requests.HTTPError as e:
        raise HTTPException(status_code=502, detail=str(e))


@app.get("/proxy/positions")
def proxy_positions():
    """Proxy: live positions with unrealized P&L from fix_server."""
    return _proxy("/api/positions")


@app.get("/proxy/algo/status")
def proxy_algo_status():
    """Proxy: algo config, signal counts, and recent signals from fix_server."""
    return _proxy("/api/algo/status")


@app.get("/proxy/open-orders")
def proxy_open_orders():
    """Proxy: current open orders from fix_server."""
    return _proxy("/api/open-orders")


@app.get("/proxy/orders/history")
def proxy_orders_history():
    """Proxy: order history from fix_server."""
    return _proxy("/api/orders/history")


if __name__ == "__main__":
    import uvicorn
    app_logger.info(f"Starting HFT Monitor on port {MONITOR_PORT}")
    app_logger.info(f"Connecting to kdb+ at {KDB_HOST}:{KDB_PORT}")
    app_logger.info(f"Proxying fix_server at {FIX_SERVER_URL}")
    uvicorn.run(app, host="0.0.0.0", port=MONITOR_PORT, log_config=None)  # We handle our own logging
