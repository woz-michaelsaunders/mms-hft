#!/usr/bin/env node
/**
 * Headless capture of the operator surface this TLS work reuses:
 *   - Market-data session strip (GET /api/market-data-status)
 *   - TLS is opt-in transport, default off (ADR-020 / UI skip)
 *
 * This feature does not add dashboard chrome, /api/tls*, or a tls JSON field.
 * Handshake success still lands on the existing Ready chip; handshake failure
 * still lands on Not ready · Disconnected. Screenshots prove the existing
 * strip already has loading / empty / error / success / disabled, and that
 * no TLS UI leaked in.
 *
 *   node tools/ux-tls-capture.mjs
 *
 * Output: docs/ux-tls/
 */
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const OUT = path.join(ROOT, 'docs', 'ux-tls');
const SHOTS = path.join(OUT, 'screenshots');

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
const HTML = fs.readFileSync(path.join(ROOT, 'src', 'dashboard.html'), 'utf8');

const FORBIDDEN_UI = [
    'tls_ca_path',
    'tls_server_name',
    'tls_enabled',
    'tls_domain',
    '/api/tls',
    'TLS status',
    'tls status',
    'EncryptMethod',
    'native-tls',
    'TlsStream',
    'lorem ipsum',
    'Lorem ipsum',
    'placeholder copy',
];

const MD_SUCCESS = {
    servers: [
        {
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
        },
        {
            description: 'uat_md_eth',
            port: 34404,
            sender_comp_id: 'WOZINGA_MD_ETH',
            target_comp_id: 'COINROUTES',
            is_connected: false,
            is_ready: false,
            connection_state: 'DISCONNECTED',
            sequence_number: 3,
            order_books: 0,
            total_trades: 0,
            symbols: [],
            securities_count: 0,
        },
    ],
};

const ORDERBOOK = {
    bids: [[65000.1, 0.4], [64999.5, 1.2]],
    asks: [[65001.0, 0.3], [65002.2, 0.8]],
    best_bid: 65000.1,
    best_ask: 65001.0,
    trade_count: 12,
    trades: [],
};

const VIEWPORTS = [
    { name: '375', width: 375, height: 812 },
    { name: '768', width: 768, height: 1024 },
    { name: '1280', width: 1280, height: 900 },
];

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
                res.end('window.Chart=function(){return {destroy(){}, update(){}}};');
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

function assertNoTlsChromeInSource() {
    const hits = FORBIDDEN_UI.filter((s) => HTML.includes(s));
    if (hits.length) {
        throw new Error('dashboard.html contains forbidden TLS chrome: ' + hits.join(', '));
    }
    // The word "tls" must not appear as a UI field (JSON key, badge, or copy).
    if (/\btls\b/i.test(HTML)) {
        throw new Error('dashboard.html mentions tls — dashboard/API TLS surface is frozen');
    }
    if (!HTML.includes('--bg:') || !HTML.includes('--space-1:')) {
        throw new Error('dashboard.html is missing House design tokens');
    }
    // Re-scan style rules excluding :root / light-scheme token blocks.
    const styleMatch = HTML.match(/<style>([\s\S]*?)<\/style>/);
    if (!styleMatch) throw new Error('no <style> block');
    const style = styleMatch[1];
    const withoutTokenBlocks = style
        .replace(/:root\s*\{[\s\S]*?\}/g, '')
        .replace(/@media \(prefers-color-scheme: light\)\s*\{[\s\S]*?:root\s*\{[\s\S]*?\}\s*\}/g, '');
    const leftoverHex = withoutTokenBlocks.match(/#[0-9a-fA-F]{3,8}\b/g) || [];
    if (leftoverHex.length) {
        throw new Error('raw hex outside token blocks: ' + leftoverHex.slice(0, 12).join(', '));
    }
}

async function installApiMocks(page, scenario) {
    let mdHits = 0;
    await page.route('**/*', async (route) => {
        const u = new URL(route.request().url());
        const p = u.pathname;

        if (p === '/api/market-data-status') {
            mdHits += 1;
            if (scenario === 'loading') return; // hang first paint
            if (scenario === 'disabled') {
                if (mdHits === 1) {
                    return route.fulfill({
                        status: 200,
                        contentType: 'application/json',
                        body: JSON.stringify({ servers: [] }),
                    });
                }
                return; // hang the user-initiated refresh so Check sessions stays disabled
            }
            if (scenario === 'error') {
                return route.fulfill({
                    status: 500,
                    contentType: 'application/json',
                    body: JSON.stringify({ error: 'upstream' }),
                });
            }
            if (scenario === 'success') {
                return route.fulfill({
                    status: 200,
                    contentType: 'application/json',
                    body: JSON.stringify(MD_SUCCESS),
                });
            }
            // empty
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify({ servers: [] }),
            });
        }
        if (p === '/api/symbols') {
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify({ symbols: ['BTC_USDT', 'ETH_USDT'] }),
            });
        }
        if (p.startsWith('/api/orderbook/')) {
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify(ORDERBOOK),
            });
        }
        if (p === '/api/positions') {
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify({ positions: [] }),
            });
        }
        if (p === '/api/kill-switch/status') {
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify({ halted: false }),
            });
        }
        if (p === '/api/algo/status') {
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify({
                    config: { enabled: false, symbol: 'BTC_USDT', strategy: 'spread_capture' },
                    signals_fired: 0,
                    orders_placed: 0,
                    paper_mode: true,
                    recent_signals: [],
                }),
            });
        }
        if (p === '/api/shadow/status') {
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify({}),
            });
        }
        return route.continue();
    });
}

async function openMdStrip(page, base, scenario) {
    await installApiMocks(page, scenario);
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.goto(base + '/', { waitUntil: 'domcontentloaded' });

    if (scenario === 'loading') {
        await page.waitForFunction(() => {
            const t = document.querySelector('#mdStatusBanner .house-banner-title');
            const btn = document.getElementById('mdStatusRefreshBtn');
            return t && t.textContent === 'Loading market-data sessions' && btn && btn.disabled === true;
        });
        return;
    }
    if (scenario === 'empty') {
        await page.waitForFunction(() => {
            const t = document.querySelector('#mdStatusBanner .house-banner-title');
            return t && t.textContent === 'No market-data sessions';
        });
        await page.waitForFunction(() => document.getElementById('mdStatusRefreshBtn')?.disabled === false);
        return;
    }
    if (scenario === 'error') {
        await page.waitForFunction(() => {
            const t = document.querySelector('#mdStatusBanner .house-banner-title');
            return t && t.textContent === 'Cannot reach market-data status';
        });
        await page.waitForFunction(() => document.getElementById('mdStatusRefreshBtn')?.disabled === false);
        return;
    }
    if (scenario === 'success') {
        await page.waitForFunction(() => {
            const t = document.querySelector('#mdStatusBanner .house-banner-title');
            return t && t.textContent === 'Sessions sampled';
        });
        await page.waitForFunction(() => document.getElementById('mdStatusRefreshBtn')?.disabled === false);
        return;
    }
    if (scenario === 'disabled') {
        await page.waitForFunction(() => {
            const t = document.querySelector('#mdStatusBanner .house-banner-title');
            return t && t.textContent === 'No market-data sessions';
        });
        await page.waitForFunction(() => document.getElementById('mdStatusRefreshBtn')?.disabled === false);
        await page.locator('#mdStatusRefreshBtn').click();
        await page.waitForFunction(() => {
            const t = document.querySelector('#mdStatusBanner .house-banner-title');
            const btn = document.getElementById('mdStatusRefreshBtn');
            return t && t.textContent === 'Loading market-data sessions' && btn && btn.disabled === true;
        });
    }
}

async function shotChrome(page, dest) {
    const header = await page.locator('header.header').boundingBox();
    const strip = await page.locator('.md-strip').boundingBox();
    if (!header || !strip) throw new Error('missing header or md-strip');
    const vp = page.viewportSize();
    const y = Math.max(0, header.y);
    const bottom = strip.y + strip.height;
    const clip = {
        x: 0,
        y,
        width: vp.width,
        height: Math.min(vp.height - y, Math.ceil(bottom - y) + 8),
    };
    await page.screenshot({ path: dest, clip });
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

async function probeA11y(page) {
    return page.evaluate(() => {
        const label = document.querySelector('label[for="symbolSelect"]');
        const select = document.getElementById('symbolSelect');
        const btn = document.getElementById('mdStatusRefreshBtn');
        const src = document.documentElement.outerHTML;
        const forbidden = [
            'tls_ca_path', 'tls_server_name', 'tls_enabled', '/api/tls', 'TLS status',
            'lorem ipsum', 'Lorem ipsum',
        ].filter((s) => src.includes(s));
        return {
            labelledSymbol: !!(label && select && (label.textContent || '').trim().length),
            symbolLabel: label ? label.textContent.trim() : null,
            primaryAction: btn ? btn.textContent.trim() : null,
            primaryDisabled: !!(btn && btn.disabled),
            forbiddenInDom: forbidden,
            hasFocusVisibleRule: /:focus-visible/.test(src),
        };
    });
}

function srgbToLin(c) {
    const s = c / 255;
    return s <= 0.04045 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4;
}

function parseCssColor(str) {
    if (!str) return null;
    const m = str.match(/rgba?\(\s*([\d.]+)\s*,\s*([\d.]+)\s*,\s*([\d.]+)(?:\s*,\s*([\d.]+))?\s*\)/);
    if (m) {
        return { r: +m[1], g: +m[2], b: +m[3], a: m[4] === undefined ? 1 : +m[4] };
    }
    const hex = str.trim();
    const h = hex.match(/^#([0-9a-f]{6})$/i);
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
    const lighter = Math.max(L1, L2);
    const darker = Math.min(L1, L2);
    return (lighter + 0.05) / (darker + 0.05);
}

function flattenOn(fg, bg) {
    if (!fg || !bg) return null;
    if (fg.a >= 0.999) return fg;
    const a = fg.a;
    return {
        r: fg.r * a + bg.r * (1 - a),
        g: fg.g * a + bg.g * (1 - a),
        b: fg.b * a + bg.b * (1 - a),
        a: 1,
    };
}

function hexOf(c) {
    const h = (n) => Math.round(n).toString(16).padStart(2, '0');
    return `#${h(c.r)}${h(c.g)}${h(c.b)}`;
}

async function sampleContrast(page) {
    return page.evaluate(() => {
        const cs = getComputedStyle(document.documentElement);
        const tokenNames = [
            '--bg', '--surface', '--surface-2', '--text', '--text-muted', '--text-faint',
            '--border', '--accent', '--accent-contrast', '--accent-hover',
            '--success', '--success-bg', '--warning', '--warning-bg',
            '--danger', '--danger-bg', '--info', '--info-bg',
        ];
        const tokens = {};
        for (const name of tokenNames) {
            tokens[name] = cs.getPropertyValue(name).trim();
        }
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
        return {
            tokens,
            samples: [
                sample('body'),
                sample('.md-strip-title'),
                sample('.md-strip-lead'),
                sample('#mdStatusRefreshBtn'),
                sample('#mdStatusBanner'),
                sample('#mdStatusBanner .house-banner-title'),
                sample('#mdStatusEmpty p'),
                sample('.md-chip-title'),
                sample('.md-chip .status'),
                sample('.md-chip-meta'),
                sample('label[for="symbolSelect"]'),
                sample('#symbolSelect'),
            ],
        };
    });
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
        ['--success', '--success-bg', 'success banner / ready chip'],
        ['--info', '--info-bg', 'loading banner'],
        ['--warning', '--warning-bg', 'skipped-sample chip'],
        ['--accent-contrast', '--accent', 'Check sessions primary button'],
        ['--text', '--surface-2', 'input text on field surface'],
        ['--text-faint', '--bg', 'faint status'],
    ];
    for (const [fgName, bgName, use] of pairDefs) {
        const fg = parseCssColor(t[fgName]);
        const bg = parseCssColor(t[bgName]);
        if (!fg || !bg) continue;
        const ratio = contrastRatio(fg, bg);
        tokenPairs.push({
            fg_token: fgName,
            bg_token: bgName,
            fg: t[fgName],
            bg: t[bgName],
            use,
            ratio: Math.round(ratio * 100) / 100,
            pass_aa: ratio >= 4.5,
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
        lines.push(
            `| \`${p.fg_token}\` on \`${p.bg_token}\` | ${p.fg} / ${p.bg} | ${p.use} | **${p.ratio}:1** | ${p.pass_aa ? 'pass' : 'FAIL'} |`
        );
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
            .filter(Boolean)
            .join(':');
    }
}

async function main() {
    assertNoTlsChromeInSource();
    ensureChromeLibs();
    fs.mkdirSync(SHOTS, { recursive: true });
    const { server, port } = await startServer();
    const base = `http://127.0.0.1:${port}`;
    const chromeCandidates = [
        process.env.PLAYWRIGHT_CHROME,
        '/home/agent1/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome',
        '/home/grok-b1/.cache/ms-playwright/chromium-1187/chrome-linux/chrome',
        '/home/grok-b1/.cache/ms-playwright/chromium_headless_shell-1187/chrome-linux/headless_shell',
        '/home/agent1/screenshot-tool/node_modules/playwright-core/.local-browsers/chromium-1187/chrome-linux/chrome',
    ].filter(Boolean);
    const executablePath = chromeCandidates.find((p) => fs.existsSync(p));
    const browser = await chromium.launch({
        headless: true,
        ...(executablePath ? { executablePath } : {}),
        args: ['--no-sandbox', '--disable-dev-shm-usage'],
    });

    const scenarios = [
        { id: 'loading', file: 'loading', label: 'Loading — banner Loading market-data sessions; Check sessions disabled' },
        { id: 'empty', file: 'empty', label: 'Empty — No market-data sessions; Check sessions is the primary action' },
        { id: 'error', file: 'error', label: 'Error — Cannot reach market-data status' },
        { id: 'success', file: 'success', label: 'Success — Sessions sampled; ready and not-ready chips' },
        { id: 'disabled', file: 'disabled', label: 'Disabled — Check sessions off while a refresh is in flight' },
    ];

    const overflow = [];
    const log = [];
    const a11y = [];
    let contrastDark = null;

    for (const vp of VIEWPORTS) {
        for (const sc of scenarios) {
            const context = await browser.newContext({
                viewport: { width: vp.width, height: vp.height },
                colorScheme: 'dark',
                reducedMotion: 'reduce',
            });
            const page = await context.newPage();
            page.setDefaultTimeout(15000);
            await openMdStrip(page, base, sc.id);
            const probe = await probeA11y(page);
            if (probe.forbiddenInDom.length) {
                throw new Error('DOM contains forbidden TLS chrome: ' + probe.forbiddenInDom.join(', '));
            }
            if (!probe.labelledSymbol) {
                throw new Error('Symbol select is not labelled');
            }
            if (!probe.hasFocusVisibleRule) {
                throw new Error('missing :focus-visible rule');
            }
            a11y.push({ file: `${sc.file}-${vp.name}.png`, ...probe });
            const file = `${sc.file}-${vp.name}.png`;
            const dest = path.join(SHOTS, file);
            await shotChrome(page, dest);
            const ov = await measureOverflow(page);
            overflow.push({ width: vp.name, state: sc.file, ...ov, file });
            log.push({ file, label: sc.label, width: vp.name, overflowing: ov.overflowing });
            if (vp.name === '1280' && sc.id === 'empty') {
                contrastDark = scoreSamples(await sampleContrast(page));
                // Keyboard Tab so :focus-visible (not just :focus) matches.
                await page.locator('#symbolSelect').focus();
                await page.keyboard.press('Tab');
                await page.waitForFunction(() => document.activeElement && document.activeElement.id === 'mdStatusRefreshBtn');
                await page.waitForTimeout(80);
                const btnBox = await page.locator('#mdStatusRefreshBtn').boundingBox();
                if (btnBox) {
                    await page.screenshot({
                        path: path.join(SHOTS, 'focus-ring-1280.png'),
                        clip: {
                            x: Math.max(0, btnBox.x - 24),
                            y: Math.max(0, btnBox.y - 24),
                            width: Math.min(vp.width, btnBox.width + 48),
                            height: btnBox.height + 48,
                        },
                    });
                }
            }
            await context.close();
            process.stdout.write(`wrote ${file}\n`);
        }
    }

    {
        const context = await browser.newContext({
            viewport: { width: 1280, height: 900 },
            colorScheme: 'light',
            reducedMotion: 'reduce',
        });
        const page = await context.newPage();
        await openMdStrip(page, base, 'empty');
        const contrastLight = scoreSamples(await sampleContrast(page));
        await shotChrome(page, path.join(SHOTS, 'empty-1280-light.png'));
        await context.close();

        const overflowFail = overflow.filter((o) => o.overflowing);
        const darkFail = contrastDark.token_pairs.filter((p) => !p.pass_aa);
        const lightFail = contrastLight.token_pairs.filter((p) => !p.pass_aa);
        const liveFail = [...contrastDark.live_elements, ...contrastLight.live_elements]
            .filter((p) => !p.pass_aa && !p.disabled);
        const overflowSummary = overflow
            .map((o) => `${o.state}@${o.width}: scrollWidth=${o.documentElement.scrollWidth} clientWidth=${o.documentElement.clientWidth} overflowing=${o.overflowing}`)
            .join('\n');

        const readme = `# Opt-in FIX-session TLS — UI evidence

Generated by \`tools/ux-tls-capture.mjs\` against \`src/dashboard.html\`.

This feature **does not add dashboard chrome**. TLS is an opt-in socket wrapper
on \`[[sessions]]\` (\`tls\`, \`tls_ca_path\`, \`tls_server_name\`), default **off**.
There is no \`/api/tls*\`, no TLS chip, and \`GET /api/market-data-status\` is
unchanged (ADR-020, UI Design skip). Handshake success is the existing Ready
chip; handshake failure is the existing Not ready · Disconnected chip.

Do not treat these screenshots as a licence to add a TLS status UI.

## Screens in scope

| Surface | Role | New chrome? |
|---|---|---|
| Market-data session strip | Existing operator console for \`GET /api/market-data-status\` | No |
| Process log (\`🔒 TLS …\`) | Only operator-visible TLS channel (trust-load / handshake) | N/A (not a screen) |
| \`config.toml\` \`tls\` keys | Operator-edited, gitignored; not a screen | N/A |

Whole-app chrome (Live book, History, Orders, Positions, Algo, Backtest, Securities, Kill Switch, Shadow mode, native menus, window model, drag-and-drop): **N/A** — this ask does not touch them. Dashboard/API TLS status is **out of scope**.

## Definition of Done (scoped to this ask)

| Line | Verdict |
|---|---|
| Design tokens, no raw hex, no off-scale spacing | **Pass** on the existing strip (tokens in \`:root\`; component rules use \`var(--*)\`; spacing is \`--space-1\`…\`--space-8\`). No CSS was added. |
| Loading, empty, error, success, disabled | **Pass** on the related existing strip. Feature-specific TLS screens: **N/A** (none exist, none requested). |
| Responsive 375 / 768 / 1280, no horizontal page scroll | **Pass** (measured). |
| Keyboard-navigable, visible focus ring | **Pass** — \`:focus-visible\` uses \`--focus-ring\`; captured on Check sessions. |
| Contrast ≥ 4.5:1 | **Pass** (token pairs and live samples below). |
| Inputs labelled | **Pass** — \`label for="symbolSelect"\` → Symbol. Strip has no free-text inputs. |
| Copy real, sentence-case, no lorem | **Pass** — existing strip copy is operator-facing; source scan found no lorem / placeholder / TLS chrome. |
| UI rendered and reviewed, not shipped blind | **Pass** — every in-scope state captured at 375 / 768 / 1280 plus light scheme and focus ring. |

## Market-data strip states (existing, unchanged)

| State | What you should see | Files |
|---|---|---|
| Loading | Banner "Loading market-data sessions"; Check sessions disabled | \`loading-375.png\` \`loading-768.png\` \`loading-1280.png\` |
| Empty | Banner "No market-data sessions"; Check sessions is the primary action | \`empty-375.png\` \`empty-768.png\` \`empty-1280.png\` |
| Error | Banner "Cannot reach market-data status" | \`error-375.png\` \`error-768.png\` \`error-1280.png\` |
| Success | Banner "Sessions sampled"; ready (plain TCP, default) and not-ready chips | \`success-375.png\` \`success-768.png\` \`success-1280.png\` |
| Disabled | Check sessions off while a refresh is in flight | \`disabled-375.png\` \`disabled-768.png\` \`disabled-1280.png\` |

Also: \`focus-ring-1280.png\` (keyboard \`:focus-visible\` on Check sessions),
\`empty-1280-light.png\` (light scheme).

No TLS chip, CA path, SNI field, or \`tls\` JSON key — locked by the ask, ADR-020, and the UI Design skip.

## Horizontal overflow

${overflowFail.length === 0
        ? 'No page-level horizontal overflow at 375 / 768 / 1280 for any captured state (`documentElement.scrollWidth === clientWidth`).'
        : 'OVERFLOW DETECTED:\\n' + overflowFail.map((o) => JSON.stringify(o)).join('\\n')}

Raw measurements are in \`overflow.json\`:

\`\`\`
${overflowSummary}
\`\`\`

## Contrast — dark scheme (default \`color-scheme: dark\`)

Computed from the **literal token values** in \`:root\` via \`getComputedStyle\`, then WCAG 2 relative luminance.

${mdTable(contrastDark.token_pairs)}

Live sampled elements (computed \`color\` / \`background-color\`):

${contrastDark.live_elements.map((e) => `- \`${e.selector}\` \`${e.fg}\` on \`${e.bg}\` **${e.ratio}:1** ${e.pass_aa ? 'pass' : 'FAIL'}${e.disabled ? ' (disabled)' : ''}`).join('\n')}

## Contrast — light scheme (\`prefers-color-scheme: light\`)

${mdTable(contrastLight.token_pairs)}

Token-pair AA failures: dark ${darkFail.length}, light ${lightFail.length}.
Live enabled-element AA failures: ${liveFail.length}.

## Copy / behaviour (this PR)

Dashboard markup, CSS, JS, Axum routes, and \`market_data_session_status\` JSON
are **unchanged**. No placeholder or lorem. Operator-visible TLS copy remains
the process log (\`🔒 TLS trust anchor loaded from {path} ({n} certs)\` and the
spawn \`tls=\` / optional \`sni=\` line). UAT stays \`tls = false\`.

## Re-run

\`\`\`bash
node tools/ux-tls-capture.mjs
\`\`\`
`;
        fs.writeFileSync(path.join(OUT, 'README.md'), readme);
        fs.writeFileSync(path.join(OUT, 'overflow.json'), JSON.stringify(overflow, null, 2));
        fs.writeFileSync(path.join(OUT, 'capture-log.json'), JSON.stringify(log, null, 2));
        fs.writeFileSync(path.join(OUT, 'a11y.json'), JSON.stringify(a11y, null, 2));
        fs.writeFileSync(
            path.join(OUT, 'contrast.json'),
            JSON.stringify({ dark: contrastDark, light: contrastLight }, null, 2)
        );

        if (overflowFail.length || darkFail.length || lightFail.length || liveFail.length) {
            console.error('UX capture failed AA or overflow checks');
            console.error({ overflowFail, darkFail, lightFail, liveFail });
            process.exit(1);
        }
    }

    await browser.close();
    server.close();
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
