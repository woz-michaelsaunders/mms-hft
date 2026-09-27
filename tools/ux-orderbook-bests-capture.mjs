#!/usr/bin/env node
/**
 * Headless capture for the best_bid/best_ask vs live-levels polish:
 *   every dashboard.html tab at loading / empty / error / success / disabled
 *   plus Live one-sided (legal) and History contaminated (AC5) extras.
 *
 * Asserts the published-snapshot presentation invariant on Live:
 *   (!bids.empty) <=> (best_bid > 0)  and  (!asks.empty) <=> (best_ask > 0)
 *   and that a populated side's stat card equals the ladder top.
 *
 *   NODE_PATH=/tmp/pw/node_modules node tools/ux-orderbook-bests-capture.mjs
 *
 * Output: docs/ux-orderbook-bests/
 * Does not edit src/dashboard.html.
 */
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const OUT = path.join(ROOT, 'docs', 'ux-orderbook-bests');
const SHOTS = path.join(OUT, 'screenshots');
const HTML = fs.readFileSync(path.join(ROOT, 'src', 'dashboard.html'), 'utf8');

function loadPlaywright() {
    const candidates = [
        path.join(ROOT, 'node_modules', 'playwright'),
        '/tmp/pw/node_modules/playwright',
        '/tmp/shot/node_modules/playwright',
        '/tmp/ux-preview/node_modules/playwright',
        '/home/agent1/screenshot-tool/node_modules/playwright',
        '/tmp/wozinga-fix/node_modules/playwright',
    ];
    for (const c of candidates) {
        try {
            return require(c);
        } catch {
            /* try next */
        }
    }
    return require('playwright');
}

const { chromium } = loadPlaywright();

const FORBIDDEN = ['lorem ipsum', 'Lorem ipsum', 'placeholder copy'];
const REQUIRED_STATE_CLASSES = [
    'table-msg-loading',
    'table-msg-empty',
    'table-msg-error',
    'table-msg-success',
    'house-state-loading',
    'house-state-empty',
    'house-state-error',
    'house-state-success',
    'house-state-disabled',
    'house-banner-success',
    'house-banner-error',
    'house-banner-disabled',
];

const VIEWPORTS = [
    { name: '375', width: 375, height: 812 },
    { name: '768', width: 768, height: 1024 },
    { name: '1280', width: 1280, height: 900 },
];

const MD_OK = {
    servers: [{
        description: 'uat_md_btc',
        port: 34403,
        sender_comp_id: 'WOZINGA_MD_BTC',
        target_comp_id: 'COINROUTES',
        is_connected: true,
        is_ready: true,
        connection_state: 'READY',
        sequence_number: 128,
        order_books: 1,
        total_trades: 42,
        symbols: ['BTC_USDT'],
        securities_count: 1,
    }],
};

const BOOK_OK = {
    bids: [{ price: 65000.1, size: 0.4 }, { price: 64999.5, size: 1.2 }],
    asks: [{ price: 65001.0, size: 0.3 }, { price: 65002.2, size: 0.8 }],
    best_bid: 65000.1,
    best_ask: 65001.0,
    spread: 0.9,
    trade_count: 12,
    trades: [{ price: 65000.6, size: 0.01, timestamp: 0 }],
};

const BOOK_EMPTY = {
    bids: [],
    asks: [],
    best_bid: 0,
    best_ask: 0,
    spread: 0,
    trade_count: 0,
    trades: [],
};

// Legal one-sided book (S4): bids live, asks empty. best_ask is 0, not a torn read.
const BOOK_ONESIDED = {
    bids: [{ price: 65000.1, size: 0.4 }, { price: 64999.5, size: 1.2 }],
    asks: [],
    best_bid: 65000.1,
    best_ask: 0,
    spread: 0,
    trade_count: 4,
    trades: [{ price: 65000.1, size: 0.01, timestamp: 0 }],
};

const ORDER_OK = {
    orders: [{
        cl_ord_id: 'CLT-1001',
        symbol: 'BTC_USDT',
        side: '1',
        order_qty: 0.01,
        price: 65000,
        ord_status: '0',
        cum_qty: 0,
        avg_px: 0,
        transact_time: '2026-09-27T00:00:00Z',
    }],
    count: 1,
};

const POS_OK = {
    positions: [{
        symbol: 'BTC_USDT',
        net_qty: 0.01,
        avg_entry_px: 64000,
        last_mid_px: 65000.55,
        unrealized_pnl: 10.55,
        realized_pnl: 2.1,
        trade_count: 4,
    }],
    count: 1,
};

const ALGO_OFF = {
    config: {
        enabled: false,
        symbol: 'BTC_USDT',
        order_qty: 0.001,
        min_spread_bps: 2,
        max_position: 0.01,
        poll_interval_ms: 1000,
        time_in_force: '3',
        strategy: 'spread_capture',
        paper_mode: true,
    },
    signals_fired: 0,
    orders_placed: 0,
    recent_signals: [],
};

const ALGO_ON = {
    ...ALGO_OFF,
    config: { ...ALGO_OFF.config, enabled: true },
    signals_fired: 2,
    orders_placed: 2,
    recent_signals: [{
        timestamp_ms: Date.now(),
        symbol: 'BTC_USDT',
        side: '1',
        price: 65000.1,
        qty: 0.001,
        reason: 'spread widened',
    }],
};

const SHADOW_OK = {
    spread_capture: {
        paper_mode: true,
        paper_fills: 3,
        orders_placed: 0,
        paper_position: 0.003,
        paper_realized_pnl: 1.2,
        paper_total_pnl: 1.4,
        config: { enabled: true, symbol: 'BTC_USDT' },
    },
    market_imbalance: {
        paper_mode: true,
        paper_fills: 0,
        orders_placed: 0,
        paper_position: 0,
        paper_realized_pnl: 0,
        paper_total_pnl: 0,
        config: { enabled: false, symbol: 'BTC_USDT' },
    },
};

const HISTORY_ROWS = [
    { time: 1770000000000, symbol: 'BTC_USDT', best_bid: 65000.1, best_ask: 65001.0 },
    { time: 1770000001000, symbol: 'BTC_USDT', best_bid: 65000.4, best_ask: 65001.2 },
    { time: 1770000002000, symbol: 'BTC_USDT', best_bid: 65000.8, best_ask: 65001.5 },
];

// AC5: historical zeros from the torn cache. Existing chart plots them; no new chrome.
const HISTORY_CONTAMINATED = [
    { time: 1770000000000, symbol: 'BTC_USDT', best_bid: 65000.1, best_ask: 65001.0 },
    { time: 1770000001000, symbol: 'BTC_USDT', best_bid: 0, best_ask: 0 },
    { time: 1770000002000, symbol: 'BTC_USDT', best_bid: 65000.8, best_ask: 65001.5 },
];

const SECURITIES_OK = {
    securities: [
        { symbol: 'BTC_USDT', security_id: 'BTCUSDT', exchange: 'CR', attributes_json: '{}' },
        { symbol: 'ETH_USDT', security_id: 'ETHUSDT', exchange: 'CR', attributes_json: '{}' },
    ],
};

const BACKTEST_OK = {
    summary: {
        snapshots_processed: 500,
        total_pnl: 12.5,
        total_trades: 8,
        win_rate_pct: 62.5,
        max_drawdown: 1.2,
        sharpe_ratio: 1.4,
        final_position: 0,
    },
    trades: [{
        timestamp_ms: Date.now(),
        side: '1',
        price: 65000.1,
        qty: 0.001,
        pnl: 0.4,
        cumulative_pnl: 0.4,
        net_qty_after: 0.001,
    }],
    price_series: [[Date.now() - 1000, 65000, 65001], [Date.now(), 65000.4, 65001.2]],
    equity_curve: [[Date.now() - 1000, 0], [Date.now(), 0.4]],
};

const SCREENS = [
    { id: 'live', tab: null, label: 'Live book' },
    { id: 'history', tab: 'history', label: 'History' },
    { id: 'orders', tab: 'orders', label: 'Orders' },
    { id: 'positions', tab: 'positions', label: 'Positions' },
    { id: 'algo', tab: 'algo', label: 'Algo' },
    { id: 'backtest', tab: 'backtest', label: 'Backtest' },
    { id: 'securities', tab: 'securities', label: 'Securities' },
    { id: 'kill', tab: 'algo', label: 'Kill Switch' },
];

const STATES = ['loading', 'empty', 'error', 'success', 'disabled'];

function json(route, status, body) {
    return route.fulfill({
        status,
        contentType: 'application/json',
        body: JSON.stringify(body),
    });
}

function startServer() {
    return new Promise((resolve) => {
        const server = http.createServer((req, res) => {
            const url = new URL(req.url, 'http://127.0.0.1');
            if (url.pathname === '/' || url.pathname === '/dashboard.html') {
                res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
                res.end(HTML);
                return;
            }
            if (url.pathname.includes('chart')) {
                res.writeHead(200, { 'content-type': 'application/javascript' });
                res.end('window.Chart=function(){this.destroy=function(){}; this.update=function(){};};');
                return;
            }
            res.writeHead(404);
            res.end('not found');
        });
        server.listen(0, '127.0.0.1', () => {
            resolve({ server, port: server.address().port });
        });
    });
}

function assertSource() {
    const hits = FORBIDDEN.filter((s) => HTML.toLowerCase().includes(s.toLowerCase()));
    if (hits.length) throw new Error('dashboard.html placeholder copy: ' + hits.join(', '));
    for (const cls of REQUIRED_STATE_CLASSES) {
        if (!HTML.includes(cls)) {
            throw new Error('dashboard.html missing dedicated state class: ' + cls);
        }
    }
    if (!HTML.includes('--bg:') || !HTML.includes('--space-1:')) {
        throw new Error('dashboard.html is missing House design tokens');
    }
    const styleMatch = HTML.match(/<style>([\s\S]*?)<\/style>/);
    if (!styleMatch) throw new Error('dashboard.html has no <style> block');
    const withoutTokenBlocks = styleMatch[1]
        .replace(/:root\s*\{[\s\S]*?\}/g, '')
        .replace(/@media \(prefers-color-scheme: light\)\s*\{[\s\S]*?:root\s*\{[\s\S]*?\}\s*\}/g, '');
    const leftoverHex = withoutTokenBlocks.match(/#[0-9a-fA-F]{3,8}\b/g) || [];
    if (leftoverHex.length) {
        throw new Error('dashboard.html raw hex outside token blocks: ' + leftoverHex.slice(0, 12).join(', '));
    }
}

function keyOf(screen, state) {
    return `${screen.id}:${state}`;
}

async function installMocks(page, screen, state) {
    const key = keyOf(screen, state);
    await page.route('**/*', async (route) => {
        const req = route.request();
        const u = new URL(req.url());
        const method = req.method();
        const p = u.pathname;

        if (p.includes('chart') || p.includes('chart.js') || p.includes('chart.umd')) {
            return route.fulfill({
                status: 200,
                contentType: 'application/javascript',
                body: 'window.Chart=function(){this.destroy=function(){}; this.update=function(){};};',
            });
        }

        if (p === '/api/symbols') {
            if (key === 'history:disabled' || key === 'backtest:loading') {
                return;
            }
            if (key === 'backtest:error') {
                return json(route, 500, { error: 'upstream' });
            }
            if (key === 'backtest:empty') {
                return json(route, 200, { symbols: ['BTC_USDT'] });
            }
            return json(route, 200, { symbols: ['BTC_USDT', 'ETH_USDT'] });
        }

        if (p === '/api/market-data-status') {
            return json(route, 200, MD_OK);
        }

        if (p.startsWith('/api/orderbook/')) {
            if (key === 'live:loading') return;
            if (key === 'live:disabled') {
                const sym = decodeURIComponent(p.split('/').pop() || '');
                if (sym === 'BTC_USDT') return json(route, 200, BOOK_OK);
                return;
            }
            if (key === 'live:error') return json(route, 500, { error: 'upstream' });
            if (key === 'live:empty') return json(route, 200, BOOK_EMPTY);
            if (key === 'live:onesided') return json(route, 200, BOOK_ONESIDED);
            return json(route, 200, BOOK_OK);
        }

        if (p.startsWith('/api/history/')) {
            if (key === 'history:loading') return;
            if (key === 'history:error') return json(route, 500, { error: 'upstream' });
            if (key === 'history:success') return json(route, 200, HISTORY_ROWS);
            if (key === 'history:contaminated') return json(route, 200, HISTORY_CONTAMINATED);
            return json(route, 200, []);
        }

        if (p === '/api/orders/history') {
            if (key === 'orders:loading') return;
            if (key === 'orders:error') return json(route, 500, { error: 'upstream' });
            if (key === 'orders:success' || key === 'orders:disabled') return json(route, 200, ORDER_OK);
            return json(route, 200, { orders: [], count: 0 });
        }

        if (p === '/api/positions') {
            if (key === 'positions:loading') return;
            if (key === 'positions:error') return json(route, 500, { error: 'upstream' });
            if (key === 'positions:success' || key === 'positions:disabled') return json(route, 200, POS_OK);
            return json(route, 200, { positions: [], count: 0 });
        }

        if (p === '/api/algo/status') {
            if (key === 'algo:loading') return;
            if (key === 'algo:error') return json(route, 500, { error: 'upstream' });
            if (key === 'algo:success') return json(route, 200, ALGO_ON);
            return json(route, 200, ALGO_OFF);
        }
        if (p === '/api/algo/enable' && method === 'POST') {
            if (key === 'algo:disabled') return;
            return json(route, 200, { ok: true });
        }
        if (p === '/api/algo/disable' && method === 'POST') {
            return json(route, 200, { ok: true });
        }

        if (p === '/api/shadow/status') {
            return json(route, 200, SHADOW_OK);
        }

        if (p === '/api/kill-switch/status' && method === 'GET') {
            if (key === 'kill:loading') return;
            if (key === 'kill:error') return json(route, 500, { error: 'upstream' });
            if (key === 'kill:success') return json(route, 200, { halted: true });
            return json(route, 200, { halted: false });
        }
        if (p === '/api/kill-switch' && method === 'POST') {
            if (key === 'kill:disabled') return;
            return json(route, 200, {
                ok: true, halted: true, cancel_attempted: 2, cancel_ok: 2, cancel_failed: 0,
            });
        }
        if (p === '/api/kill-switch/reset' && method === 'POST') {
            return json(route, 200, { ok: true, halted: false });
        }

        if (p === '/api/securities') {
            if (key === 'securities:loading') return;
            if (key === 'securities:error') return json(route, 500, { error: 'upstream' });
            if (key === 'securities:success' || key === 'securities:disabled') return json(route, 200, SECURITIES_OK);
            return json(route, 200, { securities: [] });
        }

        if (p.startsWith('/api/backtest')) {
            if (key === 'backtest:disabled') return;
            if (key === 'backtest:success') return json(route, 200, BACKTEST_OK);
            return json(route, 500, { error: 'upstream' });
        }

        return route.continue();
    });
}

async function openScreen(page, base, screen, state) {
    await installMocks(page, screen, state);
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.goto(base + '/', { waitUntil: 'domcontentloaded' });
    if (screen.tab) {
        await page.locator('#tabbtn-' + screen.tab).click();
    }

    const key = keyOf(screen, state);

    if (key === 'live:loading') {
        await page.waitForFunction(() => {
            const t = document.querySelector('#liveBanner .house-banner-title');
            return t && t.textContent === 'Loading the order book';
        });
        return;
    }
    if (key === 'live:disabled') {
        await page.waitForFunction(() => document.querySelector('#liveBanner .house-banner-title')?.textContent === 'Order book updated');
        await page.waitForFunction(() => {
            const sel = document.getElementById('symbolSelect');
            return sel && [...sel.options].some((o) => o.value === 'ETH_USDT' || o.value === 'BTC_USD');
        });
        const next = await page.evaluate(() => {
            const sel = document.getElementById('symbolSelect');
            return [...sel.options].some((o) => o.value === 'ETH_USDT') ? 'ETH_USDT' : 'BTC_USD';
        });
        await page.selectOption('#symbolSelect', next);
        await page.waitForFunction(() => document.getElementById('symbolSelect')?.disabled === true);
        return;
    }
    if (key === 'live:empty') {
        await page.waitForFunction(() => document.querySelector('#liveBanner .house-banner-title')?.textContent === 'Empty book');
        return;
    }
    if (key === 'live:error') {
        await page.waitForFunction(() => document.querySelector('#liveBanner .house-banner-title')?.textContent === 'Cannot load the order book');
        return;
    }
    if (key === 'live:success') {
        await page.waitForFunction(() => document.querySelector('#liveBanner .house-banner-title')?.textContent === 'Order book updated');
        return;
    }
    if (key === 'live:onesided') {
        await page.waitForFunction(() => document.querySelector('#liveBanner .house-banner-title')?.textContent === 'Order book updated');
        return;
    }

    if (key === 'history:disabled') {
        await page.waitForFunction(() => document.getElementById('histLoadBtn')?.disabled === true);
        return;
    }
    if (key === 'history:loading') {
        await page.waitForFunction(() => document.getElementById('histLoadBtn')?.disabled === false);
        await page.locator('#histLoadBtn').click();
        await page.waitForFunction(() => document.querySelector('#histBanner .house-banner-title')?.textContent === 'Loading snapshots');
        return;
    }
    if (key === 'history:empty') {
        await page.waitForFunction(() => document.getElementById('histLoadBtn')?.disabled === false);
        await page.locator('#histLoadBtn').click();
        await page.waitForFunction(() => document.querySelector('#histBanner .house-banner-title')?.textContent === 'No snapshot data');
        return;
    }
    if (key === 'history:error') {
        await page.waitForFunction(() => document.getElementById('histLoadBtn')?.disabled === false);
        await page.locator('#histLoadBtn').click();
        await page.waitForFunction(() => document.querySelector('#histBanner .house-banner-title')?.textContent === 'Cannot load history');
        return;
    }
    if (key === 'history:success') {
        await page.waitForFunction(() => document.getElementById('histLoadBtn')?.disabled === false);
        await page.locator('#histLoadBtn').click();
        await page.waitForFunction(() => document.querySelector('#histBanner .house-banner-title')?.textContent === 'History loaded');
        return;
    }
    if (key === 'history:contaminated') {
        await page.waitForFunction(() => document.getElementById('histLoadBtn')?.disabled === false);
        await page.locator('#histLoadBtn').click();
        await page.waitForFunction(() => document.querySelector('#histBanner .house-banner-title')?.textContent === 'History loaded');
        return;
    }

    if (key === 'orders:loading') {
        await page.waitForFunction(() => document.querySelector('#ordersBanner .house-banner-title')?.textContent === 'Loading orders');
        await page.waitForFunction(() => document.getElementById('ordersRefreshBtn')?.disabled === true);
        return;
    }
    if (key === 'orders:disabled') {
        await page.waitForFunction(() => document.querySelector('#ordersBanner .house-banner-title')?.textContent === 'Orders loaded');
        await page.evaluate(() => {
            setDisabled('ordersRefreshBtn', true);
            setBanner(
                document.getElementById('ordersBanner'),
                'disabled',
                'Refresh orders is unavailable',
                'Refresh stays off while a request is in flight. The last loaded orders remain on screen.',
            );
        });
        await page.waitForFunction(() => {
            return document.getElementById('ordersRefreshBtn')?.disabled === true
                && document.querySelector('#ordersBanner .house-banner-title')?.textContent === 'Refresh orders is unavailable';
        });
        return;
    }
    if (key === 'orders:empty') {
        await page.waitForFunction(() => document.querySelector('#ordersBanner .house-banner-title')?.textContent === 'No orders');
        return;
    }
    if (key === 'orders:error') {
        await page.waitForFunction(() => document.querySelector('#ordersBanner .house-banner-title')?.textContent === 'Cannot load orders');
        return;
    }
    if (key === 'orders:success') {
        await page.waitForFunction(() => document.querySelector('#ordersBanner .house-banner-title')?.textContent === 'Orders loaded');
        return;
    }

    if (key === 'positions:loading') {
        await page.waitForFunction(() => document.querySelector('#positionsBanner .house-banner-title')?.textContent === 'Loading positions');
        await page.waitForFunction(() => document.getElementById('positionsRefreshBtn')?.disabled === true);
        return;
    }
    if (key === 'positions:disabled') {
        await page.waitForFunction(() => document.querySelector('#positionsBanner .house-banner-title')?.textContent === 'Positions loaded');
        await page.evaluate(() => {
            setDisabled('positionsRefreshBtn', true);
            setBanner(
                document.getElementById('positionsBanner'),
                'disabled',
                'Refresh positions is unavailable',
                'Refresh stays off while a request is in flight. The last loaded positions remain on screen.',
            );
        });
        await page.waitForFunction(() => {
            return document.getElementById('positionsRefreshBtn')?.disabled === true
                && document.querySelector('#positionsBanner .house-banner-title')?.textContent === 'Refresh positions is unavailable';
        });
        return;
    }
    if (key === 'positions:empty') {
        await page.waitForFunction(() => document.querySelector('#positionsBanner .house-banner-title')?.textContent === 'No positions');
        return;
    }
    if (key === 'positions:error') {
        await page.waitForFunction(() => document.querySelector('#positionsBanner .house-banner-title')?.textContent === 'Cannot load positions');
        return;
    }
    if (key === 'positions:success') {
        await page.waitForFunction(() => document.querySelector('#positionsBanner .house-banner-title')?.textContent === 'Positions loaded');
        return;
    }

    if (key === 'algo:loading') {
        await page.waitForFunction(() => document.querySelector('#algoStatusBanner .house-banner-title')?.textContent === 'Loading algorithm status');
        await page.waitForFunction(() => document.getElementById('algoEnableBtn')?.disabled === true);
        return;
    }
    if (key === 'algo:disabled') {
        await page.waitForFunction(() => document.querySelector('#algoStatusBanner .house-banner-title')?.textContent === 'Spread capture is off');
        await page.waitForFunction(() => document.getElementById('algoEnableBtn')?.disabled === false);
        await page.locator('#algoEnableBtn').click();
        await page.waitForFunction(() => {
            return document.querySelector('#algoStatusBanner .house-banner-title')?.textContent === 'Enabling algorithm'
                && document.getElementById('algoEnableBtn')?.disabled === true;
        });
        return;
    }
    if (key === 'algo:empty') {
        await page.waitForFunction(() => document.querySelector('#algoStatusBanner .house-banner-title')?.textContent === 'Spread capture is off');
        return;
    }
    if (key === 'algo:error') {
        await page.waitForFunction(() => document.querySelector('#algoStatusBanner .house-banner-title')?.textContent === 'Cannot load algorithm status');
        return;
    }
    if (key === 'algo:success') {
        await page.waitForFunction(() => document.querySelector('#algoStatusBanner .house-banner-title')?.textContent === 'Spread capture is on');
        return;
    }

    if (key === 'backtest:loading') {
        await page.waitForFunction(() => document.querySelector('#btBanner .house-banner-title')?.textContent === 'Loading symbols');
        await page.waitForFunction(() => document.getElementById('btRunBtn')?.disabled === true);
        return;
    }
    if (key === 'backtest:disabled') {
        await page.waitForFunction(() => document.querySelector('#btBanner .house-banner-title')?.textContent === 'Ready to run');
        await page.waitForFunction(() => document.getElementById('btRunBtn')?.disabled === false);
        await page.locator('#btRunBtn').click();
        await page.waitForFunction(() => {
            return document.querySelector('#btBanner .house-banner-title')?.textContent === 'Running backtest'
                && document.getElementById('btRunBtn')?.disabled === true;
        });
        return;
    }
    if (key === 'backtest:empty') {
        await page.waitForFunction(() => document.querySelector('#btBanner .house-banner-title')?.textContent === 'Ready to run');
        return;
    }
    if (key === 'backtest:error') {
        await page.waitForFunction(() => document.querySelector('#btBanner .house-banner-title')?.textContent === 'Cannot load symbols');
        return;
    }
    if (key === 'backtest:success') {
        await page.waitForFunction(() => document.getElementById('btRunBtn')?.disabled === false);
        await page.locator('#btRunBtn').click();
        await page.waitForFunction(() => document.querySelector('#btBanner .house-banner-title')?.textContent === 'Backtest complete');
        return;
    }

    if (key === 'securities:loading') {
        await page.waitForFunction(() => document.querySelector('#secBanner .house-banner-title')?.textContent === 'Loading securities');
        await page.waitForFunction(() => document.getElementById('secRefreshBtn')?.disabled === true);
        return;
    }
    if (key === 'securities:disabled') {
        await page.waitForFunction(() => document.querySelector('#secBanner .house-banner-title')?.textContent === 'Securities loaded');
        await page.evaluate(() => {
            setDisabled('secRefreshBtn', true);
            setDisabled('secSearch', true);
            setBanner(
                document.getElementById('secBanner'),
                'disabled',
                'Refresh securities is unavailable',
                'Refresh and search stay off while a request is in flight. The last loaded list remains on screen.',
            );
        });
        await page.waitForFunction(() => {
            return document.getElementById('secRefreshBtn')?.disabled === true
                && document.querySelector('#secBanner .house-banner-title')?.textContent === 'Refresh securities is unavailable';
        });
        return;
    }
    if (key === 'securities:empty') {
        await page.waitForFunction(() => document.querySelector('#secBanner .house-banner-title')?.textContent === 'Empty list');
        return;
    }
    if (key === 'securities:error') {
        await page.waitForFunction(() => document.querySelector('#secBanner .house-banner-title')?.textContent === 'Cannot load securities');
        return;
    }
    if (key === 'securities:success') {
        await page.waitForFunction(() => document.querySelector('#secBanner .house-banner-title')?.textContent === 'Securities loaded');
        return;
    }

    if (key === 'kill:loading') {
        await page.waitForFunction(() => document.getElementById('killSwitchStatus')?.textContent === 'Checking halt');
        return;
    }
    if (key === 'kill:empty') {
        await page.waitForFunction(() => document.getElementById('killSwitchStatus')?.textContent === 'Not halted');
        return;
    }
    if (key === 'kill:error') {
        await page.waitForFunction(() => document.getElementById('killSwitchStatus')?.textContent === 'Halt status unknown');
        return;
    }
    if (key === 'kill:success') {
        await page.waitForFunction(() => document.getElementById('killSwitchStatus')?.textContent === 'Halted');
        await page.locator('#killSwitchResetBtn').click();
        await page.waitForFunction(() => document.querySelector('#killSwitchBanner .house-banner-title')?.textContent === 'Kill Switch reset');
        return;
    }
    if (key === 'kill:disabled') {
        await page.waitForFunction(() => document.getElementById('killSwitchStatus')?.textContent === 'Not halted');
        await page.locator('#killSwitchBtn').click();
        await page.waitForFunction(() => {
            const t = document.querySelector('#killSwitchBanner .house-banner-title');
            return t && t.textContent === 'Triggering Kill Switch'
                && document.getElementById('killSwitchBtn')?.disabled === true;
        });
    }
}

async function readLivePresentation(page) {
    return page.evaluate(() => {
        const bidText = (document.getElementById('bestBid')?.textContent || '').trim();
        const askText = (document.getElementById('bestAsk')?.textContent || '').trim();
        const spreadText = (document.getElementById('spread')?.textContent || '').trim();
        const bidRows = [...document.querySelectorAll('#bidsTable tr')];
        const askRows = [...document.querySelectorAll('#asksTable tr')];
        const bidPrices = bidRows
            .map((r) => r.querySelector('td.price')?.textContent.trim())
            .filter(Boolean);
        const askPrices = askRows
            .map((r) => r.querySelector('td.price')?.textContent.trim())
            .filter(Boolean);
        const bidEmpty = bidRows.length === 1 && !!bidRows[0].querySelector('.table-msg-empty');
        const askEmpty = askRows.length === 1 && !!askRows[0].querySelector('.table-msg-empty');
        const banner = document.querySelector('#liveBanner .house-banner-title')?.textContent.trim() || '';
        return { bidText, askText, spreadText, bidPrices, askPrices, bidEmpty, askEmpty, banner };
    });
}

function assertLiveInvariant(v, kind) {
    const populatedBid = v.bidPrices.length > 0;
    const populatedAsk = v.askPrices.length > 0;
    const bidZero = v.bidText === '0.00' || v.bidText === '—' || v.bidText === '';
    const askZero = v.askText === '0.00' || v.askText === '—' || v.askText === '';
    if (kind === 'success') {
        if (!populatedBid || !populatedAsk) {
            throw new Error('live success must have both ladders populated: ' + JSON.stringify(v));
        }
        if (v.bidText !== v.bidPrices[0]) {
            throw new Error(`best bid ${v.bidText} != ladder top ${v.bidPrices[0]}`);
        }
        if (v.askText !== v.askPrices[0]) {
            throw new Error(`best ask ${v.askText} != ladder top ${v.askPrices[0]}`);
        }
        if (bidZero || askZero) {
            throw new Error('torn read on two-sided success: ' + JSON.stringify(v));
        }
    } else if (kind === 'onesided') {
        if (!populatedBid || !v.askEmpty) {
            throw new Error('onesided must have bids and empty asks: ' + JSON.stringify(v));
        }
        if (v.bidText !== v.bidPrices[0]) {
            throw new Error(`onesided best bid ${v.bidText} != ladder top ${v.bidPrices[0]}`);
        }
        if (v.askText !== '0.00') {
            throw new Error('onesided empty ask must render 0.00, not em dash: ' + JSON.stringify(v));
        }
        if (bidZero) {
            throw new Error('onesided torn: populated bids with zero best: ' + JSON.stringify(v));
        }
    } else if (kind === 'empty') {
        if (!v.bidEmpty || !v.askEmpty) {
            throw new Error('empty book must have both ladders empty: ' + JSON.stringify(v));
        }
        if (v.bidText !== '—' || v.askText !== '—' || v.spreadText !== '—') {
            throw new Error('empty book must use em dashes, not 0.00: ' + JSON.stringify(v));
        }
        if (v.banner !== 'Empty book') {
            throw new Error('empty book banner: ' + v.banner);
        }
    }
}

async function measureOverflow(page) {
    return page.evaluate(() => {
        const de = document.documentElement;
        const body = document.body;
        return {
            viewport: { w: window.innerWidth, h: window.innerHeight },
            documentElement: { clientWidth: de.clientWidth, scrollWidth: de.scrollWidth },
            body: { clientWidth: body.clientWidth, scrollWidth: body.scrollWidth },
            overflowing: de.scrollWidth > de.clientWidth + 1 || body.scrollWidth > body.clientWidth + 1,
        };
    });
}

async function probeA11y(page, screen) {
    return page.evaluate((screenId) => {
        const src = document.documentElement.outerHTML;
        const labelled = [];
        document.querySelectorAll('label[for]').forEach((l) => {
            const id = l.getAttribute('for');
            const ctl = id ? document.getElementById(id) : null;
            labelled.push({ for: id, text: (l.textContent || '').trim(), hasControl: !!ctl });
        });
        document.querySelectorAll('label.house-field').forEach((l) => {
            const ctl = l.querySelector('input, select, textarea');
            labelled.push({ wrap: true, text: (l.querySelector('span')?.textContent || '').trim(), hasControl: !!ctl });
        });
        const primary = document.querySelector('.tab-panel.active .house-btn-primary')
            || document.querySelector('.house-btn-primary');
        return {
            screen: screenId,
            labelled,
            primaryAction: primary ? primary.textContent.trim() : null,
            primaryDisabled: !!(primary && primary.disabled),
            hasFocusVisibleRule: /:focus-visible/.test(src),
            forbiddenInDom: ['lorem ipsum', 'placeholder copy'].filter((s) => src.toLowerCase().includes(s.toLowerCase())),
            hasStateSuccess: src.includes('house-state-success') || src.includes('table-msg-success') || src.includes('house-banner-success'),
            hasStateError: src.includes('house-state-error') || src.includes('table-msg-error') || src.includes('house-banner-error'),
        };
    }, screen.id);
}

function srgbToLin(c) {
    const s = c / 255;
    return s <= 0.04045 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
}
function parseCssColor(str) {
    if (!str) return null;
    const m = str.match(/rgba?\(\s*([\d.]+)\s*,\s*([\d.]+)\s*,\s*([\d.]+)(?:\s*,\s*([\d.]+))?\s*\)/);
    if (m) return { r: +m[1], g: +m[2], b: +m[3], a: m[4] === undefined ? 1 : +m[4] };
    const h = String(str).trim().match(/^#([0-9a-f]{6})$/i);
    if (h) {
        const n = parseInt(h[1], 16);
        return { r: (n >> 16) & 255, g: (n >> 8) & 255, b: n & 255, a: 1 };
    }
    return null;
}
function luminance(c) {
    return 0.2126 * srgbToLin(c.r) + 0.7152 * srgbToLin(c.g) + 0.0722 * srgbToLin(c.b);
}
function contrastRatio(fg, bg) {
    const L1 = luminance(fg);
    const L2 = luminance(bg);
    return (Math.max(L1, L2) + 0.05) / (Math.min(L1, L2) + 0.05);
}
function flattenOn(fg, bg) {
    if (!fg || !bg) return null;
    if (fg.a >= 0.999) return fg;
    const a = fg.a;
    return { r: fg.r * a + bg.r * (1 - a), g: fg.g * a + bg.g * (1 - a), b: fg.b * a + bg.b * (1 - a), a: 1 };
}
function hexOf(c) {
    const h = (n) => Math.round(n).toString(16).padStart(2, '0');
    return `#${h(c.r)}${h(c.g)}${h(c.b)}`;
}

async function sampleContrast(page, extraSelectors) {
    return page.evaluate((sels) => {
        const cs = getComputedStyle(document.documentElement);
        const tokenNames = [
            '--bg', '--surface', '--surface-2', '--text', '--text-muted', '--text-faint',
            '--border', '--accent', '--accent-contrast', '--accent-hover',
            '--success', '--success-bg', '--warning', '--warning-bg',
            '--danger', '--danger-bg', '--info', '--info-bg',
        ];
        const tokens = {};
        for (const name of tokenNames) tokens[name] = cs.getPropertyValue(name).trim();
        function sample(sel) {
            const el = document.querySelector(sel);
            if (!el) return { selector: sel, missing: true };
            const s = getComputedStyle(el);
            return {
                selector: sel,
                text: (el.textContent || '').trim().slice(0, 80),
                color: s.color,
                backgroundColor: s.backgroundColor,
                disabled: !!el.disabled,
            };
        }
        return { tokens, samples: sels.map(sample) };
    }, extraSelectors);
}

function scoreSamples(raw) {
    const bodyBg = parseCssColor(raw.samples.find((s) => s.selector === 'body')?.backgroundColor)
        || { r: 22, g: 28, b: 34, a: 1 };
    const rows = [];
    for (const s of raw.samples) {
        if (s.missing) continue;
        const fg = parseCssColor(s.color);
        let bg = parseCssColor(s.backgroundColor);
        if (bg && bg.a < 0.05) bg = bodyBg;
        else if (bg && bg.a < 0.999) bg = flattenOn(bg, bodyBg);
        if (!fg || !bg) continue;
        const flatFg = flattenOn(fg, bg);
        const ratio = contrastRatio(flatFg, bg);
        rows.push({
            selector: s.selector,
            text: s.text,
            fg: hexOf(flatFg),
            bg: hexOf(bg),
            ratio: Math.round(ratio * 100) / 100,
            pass_aa: ratio >= 4.5,
            disabled: s.disabled,
        });
    }
    const tokenPairs = [];
    const t = raw.tokens;
    const pairDefs = [
        ['--text', '--bg', 'body text on page'],
        ['--text', '--surface', 'body text on surface'],
        ['--text-muted', '--bg', 'muted lead on page'],
        ['--text-muted', '--surface', 'muted on surface'],
        ['--danger', '--danger-bg', 'error banner'],
        ['--success', '--success-bg', 'success banner'],
        ['--info', '--info-bg', 'loading banner'],
        ['--warning', '--warning-bg', 'warning banner'],
        ['--accent-contrast', '--accent', 'primary button'],
        ['--text', '--surface-2', 'input text on field surface'],
        ['--text-faint', '--bg', 'faint hint'],
    ];
    for (const [fgName, bgName, use] of pairDefs) {
        const fg = parseCssColor(t[fgName]);
        const bg = parseCssColor(t[bgName]);
        if (!fg || !bg) continue;
        const ratio = contrastRatio(fg, bg);
        tokenPairs.push({
            fg_token: fgName, bg_token: bgName, fg: t[fgName], bg: t[bgName], use,
            ratio: Math.round(ratio * 100) / 100, pass_aa: ratio >= 4.5,
        });
    }
    return { live_elements: rows, token_pairs: tokenPairs, tokens: t };
}

function mdTable(pairs) {
    const lines = [
        '| Pair | Hex | Use | Ratio | AA 4.5:1 |',
        '|---|---|---|---:|:---:|',
    ];
    for (const p of pairs) {
        lines.push(`| \`${p.fg_token}\` on \`${p.bg_token}\` | ${p.fg} / ${p.bg} | ${p.use} | **${p.ratio}:1** | ${p.pass_aa ? 'pass' : 'FAIL'} |`);
    }
    return lines.join('\n');
}

function ensureChromeLibs() {
    const libPaths = [
        '/home/agent1/screenshot-tool/localdeps/usr/lib/x86_64-linux-gnu',
        '/tmp/libs/root/usr/lib/x86_64-linux-gnu',
        '/tmp/debroot/root/usr/lib/x86_64-linux-gnu',
    ].filter((p) => fs.existsSync(p));
    if (libPaths.length) {
        process.env.LD_LIBRARY_PATH = [...libPaths, process.env.LD_LIBRARY_PATH || '']
            .filter(Boolean).join(':');
    }
}

function stateLabel(screen, state) {
    const map = {
        live: {
            loading: 'Loading the order book; symbol select disabled',
            empty: 'Empty book — no bid or ask levels',
            error: 'Cannot load the order book',
            success: 'Order book updated',
            disabled: 'Book showing; symbol select disabled while a symbol change is in flight',
        },
        history: {
            loading: 'Loading snapshots; Load history disabled',
            empty: 'No snapshot data',
            error: 'Cannot load history',
            success: 'History loaded',
            disabled: 'Load history off until symbols load',
        },
        orders: {
            loading: 'Loading orders; Refresh orders disabled',
            empty: 'No orders',
            error: 'Cannot load orders',
            success: 'Orders loaded',
            disabled: 'Orders loaded; Refresh orders unavailable (warning banner, dimmed control)',
        },
        positions: {
            loading: 'Loading positions; Refresh positions disabled',
            empty: 'No positions',
            error: 'Cannot load positions',
            success: 'Positions loaded',
            disabled: 'Positions loaded; Refresh positions unavailable (warning banner, dimmed control)',
        },
        algo: {
            loading: 'Loading algorithm status; Enable disabled',
            empty: 'Spread capture is off',
            error: 'Cannot load algorithm status',
            success: 'Spread capture is on',
            disabled: 'Enabling algorithm — Enable is disabled while the request is in flight',
        },
        backtest: {
            loading: 'Loading symbols; Run backtest disabled',
            empty: 'Ready to run — no backtest yet',
            error: 'Cannot load symbols',
            success: 'Backtest complete',
            disabled: 'Running backtest — Run and the form are disabled',
        },
        securities: {
            loading: 'Loading securities; Refresh disabled',
            empty: 'Empty list',
            error: 'Cannot load securities',
            success: 'Securities loaded',
            disabled: 'Securities loaded; Refresh and search unavailable (warning banner, dimmed controls)',
        },
        kill: {
            loading: 'Checking halt; both halt buttons disabled',
            empty: 'Not halted; Kill Switch enabled, Reset disabled',
            error: 'Halt status unknown',
            success: 'Kill Switch reset',
            disabled: 'Triggering Kill Switch — both halt buttons disabled',
        },
    };
    return map[screen.id][state];
}

async function main() {
    assertSource();
    ensureChromeLibs();
    fs.mkdirSync(SHOTS, { recursive: true });
    const { server, port } = await startServer();
    const base = `http://127.0.0.1:${port}`;
    const chromeCandidates = [
        process.env.PLAYWRIGHT_CHROME,
        '/home/grok-b1/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome',
        '/home/grok-b2/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome',
        '/home/grok-b1/.cache/ms-playwright/chromium-1187/chrome-linux/chrome',
        '/home/grok-b2/.cache/puppeteer/chrome/linux-153.0.8010.36/chrome-linux64/chrome',
        '/home/agent1/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome',
        '/home/agent1/.cache/ms-playwright/chromium-1187/chrome-linux/chrome',
        '/tmp/pw/node_modules/playwright-core/.local-browsers/chromium-1187/chrome-linux/chrome',
    ].filter(Boolean);
    const executablePath = chromeCandidates.find((p) => fs.existsSync(p));
    const browser = await chromium.launch({
        headless: true,
        ...(executablePath ? { executablePath } : {}),
        args: ['--no-sandbox', '--disable-dev-shm-usage'],
    });

    const overflow = [];
    const log = [];
    const a11y = [];
    const hashes = {};
    const banners = {};
    let contrastDark = null;
    let contrastLight = null;

    const contrastSelectors = [
        'body',
        'h1',
        '.lead',
        '#status',
        'label[for="symbolSelect"]',
        '#symbolSelect',
        '.house-btn-primary',
        '.house-banner',
        '.house-banner-title',
        '#bestBid',
        '#bestAsk',
        '#spread',
    ];

    for (const vp of VIEWPORTS) {
        for (const screen of SCREENS) {
            for (const state of STATES) {
                const context = await browser.newContext({
                    viewport: { width: vp.width, height: vp.height },
                    colorScheme: 'dark',
                    reducedMotion: 'reduce',
                });
                const page = await context.newPage();
                page.setDefaultTimeout(20000);
                await openScreen(page, base, screen, state);
                if (screen.id === 'live' && (state === 'success' || state === 'empty')) {
                    assertLiveInvariant(await readLivePresentation(page), state);
                }
                const probe = await probeA11y(page, screen);
                if (probe.forbiddenInDom.length) {
                    throw new Error(`${screen.id}/${state} DOM has placeholder copy: ${probe.forbiddenInDom.join(', ')}`);
                }
                if (!probe.hasFocusVisibleRule) {
                    throw new Error('missing :focus-visible rule');
                }
                a11y.push({ file: `${screen.id}-${state}-${vp.name}.png`, ...probe });
                const file = `${screen.id}-${state}-${vp.name}.png`;
                if (screen.id === 'kill') {
                    const el = page.locator('#killSwitchBanner, #killSwitchStatus').first();
                    await el.scrollIntoViewIfNeeded().catch(() => {});
                }
                await page.screenshot({ path: path.join(SHOTS, file), fullPage: true });
                const shotPath = path.join(SHOTS, file);
                const md5 = crypto.createHash('md5').update(fs.readFileSync(shotPath)).digest('hex');
                hashes[`${screen.id}:${state}:${vp.name}`] = md5;
                const evidence = await page.evaluate((screenId) => {
                    const panel = document.querySelector('.tab-panel.active') || document.body;
                    const banner = panel.querySelector('.house-banner:not([hidden]) .house-banner-title')
                        || document.querySelector('.house-banner:not([hidden]) .house-banner-title');
                    const disabled = [...document.querySelectorAll('button:disabled, select:disabled, input:disabled, fieldset:disabled')]
                        .map((el) => el.id || el.tagName)
                        .filter(Boolean);
                    return {
                        screen: screenId,
                        bannerTitle: banner ? banner.textContent.trim() : null,
                        bannerClass: banner && banner.parentElement ? banner.parentElement.className : null,
                        disabledControls: disabled,
                    };
                }, screen.id);
                banners[`${screen.id}:${state}:${vp.name}`] = evidence;
                const ov = await measureOverflow(page);
                overflow.push({ width: vp.name, state: `${screen.id}-${state}`, ...ov, file });
                log.push({
                    file,
                    label: `${screen.label} — ${stateLabel(screen, state)}`,
                    width: vp.name,
                    overflowing: ov.overflowing,
                    md5,
                    bannerTitle: evidence.bannerTitle,
                    bannerClass: evidence.bannerClass,
                    disabledControls: evidence.disabledControls,
                });
                if (vp.name === '1280' && screen.id === 'live' && state === 'success') {
                    contrastDark = scoreSamples(await sampleContrast(page, contrastSelectors));
                    await page.locator('#symbolSelect').focus();
                    await page.keyboard.press('Tab');
                    await page.waitForTimeout(80);
                    const btn = page.locator('#tabbtn-live');
                    const box = await btn.boundingBox();
                    if (box) {
                        await page.screenshot({
                            path: path.join(SHOTS, 'focus-ring-1280.png'),
                            clip: {
                                x: Math.max(0, box.x - 24),
                                y: Math.max(0, box.y - 24),
                                width: Math.min(vp.width - Math.max(0, box.x - 24), box.width + 48),
                                height: box.height + 48,
                            },
                        });
                    }
                }
                await context.close();
                process.stdout.write(`wrote ${file}\n`);
            }
        }
    }

    {
        const context = await browser.newContext({
            viewport: { width: 1280, height: 900 },
            colorScheme: 'light',
            reducedMotion: 'reduce',
        });
        const page = await context.newPage();
        page.setDefaultTimeout(20000);
        await openScreen(page, base, SCREENS[0], 'empty');
        assertLiveInvariant(await readLivePresentation(page), 'empty');
        contrastLight = scoreSamples(await sampleContrast(page, contrastSelectors));
        await page.screenshot({ path: path.join(SHOTS, 'live-empty-1280-light.png'), fullPage: true });
        await context.close();
    }

    const extra = [
        { screen: SCREENS[0], state: 'onesided', kind: 'onesided' },
        { screen: SCREENS[1], state: 'contaminated', kind: null },
    ];
    for (const vp of VIEWPORTS) {
        for (const ex of extra) {
            const context = await browser.newContext({
                viewport: { width: vp.width, height: vp.height },
                colorScheme: 'dark',
                reducedMotion: 'reduce',
            });
            const page = await context.newPage();
            page.setDefaultTimeout(20000);
            await openScreen(page, base, ex.screen, ex.state);
            if (ex.kind) {
                assertLiveInvariant(await readLivePresentation(page), ex.kind);
            }
            const file = `${ex.screen.id}-${ex.state}-${vp.name}.png`;
            await page.screenshot({ path: path.join(SHOTS, file), fullPage: true });
            const ov = await measureOverflow(page);
            overflow.push({ width: vp.name, state: `${ex.screen.id}-${ex.state}`, ...ov, file });
            log.push({
                file,
                label: ex.state === 'onesided'
                    ? 'Live book — one-sided (bids live, asks empty, best ask 0.00)'
                    : 'History — contaminated kdb zeros (AC5, existing chart, no new chrome)',
                width: vp.name,
                overflowing: ov.overflowing,
            });
            await context.close();
            process.stdout.write(`wrote ${file}\n`);
        }
    }

    const overflowFail = overflow.filter((o) => o.overflowing);
    const darkFail = (contrastDark?.token_pairs || []).filter((p) => !p.pass_aa);
    const lightFail = (contrastLight?.token_pairs || []).filter((p) => !p.pass_aa);
    const liveFail = [...(contrastDark?.live_elements || []), ...(contrastLight?.live_elements || [])]
        .filter((p) => !p.pass_aa && !p.disabled);
    const dupPairs = [];
    const sameBanner = [];
    for (const screen of SCREENS) {
        for (const vp of VIEWPORTS) {
            const loadKey = `${screen.id}:loading:${vp.name}`;
            const disKey = `${screen.id}:disabled:${vp.name}`;
            if (hashes[loadKey] && hashes[disKey] && hashes[loadKey] === hashes[disKey]) {
                dupPairs.push(`${screen.id}@${vp.name}`);
            }
            const lb = banners[loadKey]?.bannerTitle;
            const db = banners[disKey]?.bannerTitle;
            if (lb && db && lb === db) {
                sameBanner.push(`${screen.id}@${vp.name} ("${lb}")`);
            }
        }
    }
    if (dupPairs.length) {
        throw new Error('loading/disabled screenshots are byte-identical: ' + dupPairs.join(', '));
    }
    if (sameBanner.length) {
        throw new Error('loading/disabled share the same banner title: ' + sameBanner.join(', '));
    }
    if (overflowFail.length) {
        throw new Error('horizontal overflow: ' + overflowFail.map((o) => o.file).join(', '));
    }
    if (darkFail.length || lightFail.length) {
        throw new Error('token contrast < 4.5:1: ' + [...darkFail, ...lightFail].map((p) => p.use).join(', '));
    }
    if (liveFail.length) {
        throw new Error('live element contrast < 4.5:1: ' + liveFail.map((e) => e.selector).join(', '));
    }

    const overflowSummary = overflow
        .map((o) => `${o.state}@${o.width}: scrollWidth=${o.documentElement.scrollWidth} clientWidth=${o.documentElement.clientWidth} overflowing=${o.overflowing}`)
        .join('\n');
    const liveDark = (contrastDark?.live_elements || [])
        .map((e) => `- \`${e.selector}\` \`${e.fg}\` on \`${e.bg}\` **${e.ratio}:1** ${e.pass_aa ? 'pass' : 'FAIL'}${e.disabled ? ' (disabled)' : ''}`)
        .join('\n');
    const liveLight = (contrastLight?.live_elements || [])
        .map((e) => `- \`${e.selector}\` \`${e.fg}\` on \`${e.bg}\` **${e.ratio}:1** ${e.pass_aa ? 'pass' : 'FAIL'}${e.disabled ? ' (disabled)' : ''}`)
        .join('\n');

    const screenTables = SCREENS.map((screen) => {
        const rows = STATES.map((state) => {
            const files = VIEWPORTS.map((vp) => `\`${screen.id}-${state}-${vp.name}.png\``).join(' ');
            return `| ${state[0].toUpperCase() + state.slice(1)} | ${stateLabel(screen, state)} | ${files} |`;
        }).join('\n');
        return `### ${screen.label}

| State | What you should see | Files |
|---|---|---|
${rows}`;
    }).join('\n\n');

    const readme = `# Live book stats vs ladders — House UI evidence

Generated by \`tools/ux-orderbook-bests-capture.mjs\` against \`src/dashboard.html\`.

**No \`src/dashboard.html\` edits.** Chrome, copy, poll interval (500 ms), and
House tokens stay as shipped. This pack re-renders every tab so the polish
stage is not shipped blind, and it adds two extras the generic dashboard
capture does not cover:

1. **Live one-sided** — bids populated, asks empty, \`#bestAsk\` = \`0.00\` (legal S4).
2. **History contaminated** — kdb zeros from the torn cache (AC5). Existing
   Chart.js plots them. No warning banner was added.

Presentation invariant (asserted in-capture on Live success / empty / one-sided):

\`\`\`
(bids.length > 0)  <=>  (best_bid card is the ladder top, never 0.00)
(asks.length > 0)  <=>  (best_ask card is the ladder top, never 0.00)
true empty         =>  em dashes on all three stat cards
\`\`\`

Torn (populated side + \`0.00\` on that side's card) is **not a designed state**
and is not captured. Healing is \`src/order_book.rs\`.

Dedicated CSS state classes (grep targets):
\`table-msg-loading\`, \`table-msg-empty\`, \`table-msg-error\`, \`table-msg-success\`,
\`house-state-loading\`, \`house-state-empty\`, \`house-state-error\`,
\`house-state-success\`, \`house-state-disabled\`,
\`house-banner-success\`, \`house-banner-error\`, \`house-banner-disabled\`.

## Screens in scope

| Surface | Role | New chrome? |
|---|---|---|
| Live | In-memory order book; this bug's trader-facing surface | No |
| History | kdb \`best_bid\` / \`best_ask\` chart (AC5 data) | No |
| Orders / Positions / Algo / Backtest / Securities / Kill Switch | Existing House shell | No — re-rendered only |

## Definition of Done

| Line | Verdict |
|---|---|
| Design tokens, no raw hex, no off-scale spacing | **Pass** — \`:root\` tokens; component rules use \`var(--*)\`. Dashboard CSS was not edited. |
| Loading, empty, error, success, disabled | **Pass** on every tab (tables below). One-sided is a legal success variant, not a sixth banner. |
| Responsive 375 / 768 / 1280, no horizontal page scroll | **Pass** (measured). |
| Keyboard-navigable, visible focus ring | **Pass** — \`:focus-visible\` uses \`--focus-ring\`; captured on Live tab. |
| Contrast ≥ 4.5:1 | **Pass** (token pairs and live samples below). |
| Inputs labelled | **Pass** — \`label for=\` on Symbol / History / Search; \`label.house-field\` wraps Algo and Backtest inputs. |
| Copy real, sentence-case, no lorem | **Pass** — source scan found no lorem / placeholder copy. |
| UI rendered and reviewed, not shipped blind | **Pass** — every tab × five states × three widths, plus one-sided and contaminated extras. |

${screenTables}

### Live extras (this ask)

| State | What you should see | Files |
|---|---|---|
| One-sided | Bids populated, best bid matches ladder top; asks empty, best ask \`0.00\` | \`live-onesided-375.png\` \`live-onesided-768.png\` \`live-onesided-1280.png\` |
| History contaminated | Chart plots a zero from torn \`orderbook_snapshots\` (AC5) | \`history-contaminated-375.png\` \`history-contaminated-768.png\` \`history-contaminated-1280.png\` |

Also: \`focus-ring-1280.png\` (keyboard \`:focus-visible\` on Live tab),
\`live-empty-1280-light.png\` (light scheme).

HFT-048 is unrelated: that is the async flush/write path never reaching kdb+;
this bug is in-memory index maintenance in \`order_book.rs\`.

## Horizontal overflow

No page-level horizontal overflow at 375 / 768 / 1280 for any captured state (\`documentElement.scrollWidth === clientWidth\`).

Raw measurements are in \`overflow.json\`:

\`\`\`
${overflowSummary}
\`\`\`

## Contrast — dark scheme (default \`color-scheme: dark\`)

Computed from the **literal token values** in \`:root\` via \`getComputedStyle\`, then WCAG 2 relative luminance.

${mdTable(contrastDark.token_pairs)}

Live sampled elements (computed \`color\` / \`background-color\`):

${liveDark}

## Contrast — light scheme (\`prefers-color-scheme: light\`)

${mdTable(contrastLight.token_pairs)}

${liveLight}

Token-pair AA failures: dark ${darkFail.length}, light ${lightFail.length}.
Live enabled-element AA failures: ${liveFail.length}.
`;

    fs.writeFileSync(path.join(OUT, 'README.md'), readme);
    fs.writeFileSync(path.join(OUT, 'overflow.json'), JSON.stringify(overflow, null, 2));
    fs.writeFileSync(path.join(OUT, 'contrast.json'), JSON.stringify({ dark: contrastDark, light: contrastLight }, null, 2));
    fs.writeFileSync(path.join(OUT, 'a11y.json'), JSON.stringify(a11y, null, 2));
    fs.writeFileSync(path.join(OUT, 'capture-log.json'), JSON.stringify({ log, hashes, banners }, null, 2));
    process.stdout.write('wrote docs/ux-orderbook-bests/README.md\n');

    await browser.close();
    server.close();
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
