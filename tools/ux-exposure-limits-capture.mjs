#!/usr/bin/env node
/**
 * Headless capture of the operator surfaces this feature reuses:
 *   - Algo-tab Kill Switch (drawdown halt is the same {halted} JSON)
 *   - Positions tab (inventory the exposure/drawdown gates read)
 *
 * Renders loading / empty / error / success / disabled at 375, 768 and 1280,
 * measures page-level horizontal overflow, and computes WCAG contrast from
 * live getComputedStyle token values.
 *
 *   node tools/ux-exposure-limits-capture.mjs
 *
 * Output: docs/ux-exposure-limits/
 */
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const OUT = path.join(ROOT, 'docs', 'ux-exposure-limits');
const SHOTS = path.join(OUT, 'screenshots');

function loadPlaywright() {
    const candidates = [
        path.join(ROOT, 'node_modules', 'playwright'),
        '/tmp/wozinga-fix/node_modules/playwright',
        '/tmp/repo/node_modules/playwright',
        '/tmp/pw/node_modules/playwright',
        '/tmp/ppm-grok-b1-ale6Ts/pw/node_modules/playwright',
        '/tmp/shot/node_modules/playwright',
        '/tmp/ux-preview/node_modules/playwright',
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

const ALGO_STATUS = {
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

const SHADOW_STATUS = {
    spread_capture: {
        paper_mode: true,
        paper_fills: 0,
        orders_placed: 0,
        position: 0,
        unrealised_pnl: 0,
        realised_pnl: 0,
    },
    market_imbalance: {
        paper_mode: true,
        paper_fills: 0,
        orders_placed: 0,
        position: 0,
        unrealised_pnl: 0,
        realised_pnl: 0,
    },
};

const POSITIONS_SAMPLE = {
    positions: [
        {
            symbol: 'BTC_USDT',
            net_qty: 0.01,
            avg_entry_px: 100.0,
            last_mid_px: 90.0,
            unrealized_pnl: -0.1,
            realized_pnl: -10.0,
            trade_count: 2,
        },
    ],
};

const VIEWPORTS = [
    { name: '375', width: 375, height: 812 },
    { name: '768', width: 768, height: 1024 },
    { name: '1280', width: 1280, height: 800 },
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

async function installApiMocks(page, scenario) {
    await page.route('**/*', async (route) => {
        const req = route.request();
        const u = new URL(req.url());
        const method = req.method();
        const p = u.pathname;

        if (p === '/api/algo/status') {
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify(ALGO_STATUS),
            });
        }
        if (p === '/api/shadow/status') {
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify(SHADOW_STATUS),
            });
        }
        if (p === '/api/market-data-status') {
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify({ sessions: [] }),
            });
        }
        if (p === '/api/positions') {
            if (scenario === 'pos-loading' || scenario === 'pos-disabled') {
                return; // hang — loading + refresh disabled
            }
            if (scenario === 'pos-error') {
                return route.fulfill({
                    status: 500,
                    contentType: 'application/json',
                    body: JSON.stringify({ error: 'upstream' }),
                });
            }
            if (scenario === 'pos-success') {
                return route.fulfill({
                    status: 200,
                    contentType: 'application/json',
                    body: JSON.stringify(POSITIONS_SAMPLE),
                });
            }
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify({ positions: [] }),
            });
        }
        if (p === '/api/kill-switch/status' && method === 'GET') {
            if (scenario === 'halt-loading') {
                return;
            }
            if (scenario === 'halt-error') {
                return route.fulfill({
                    status: 500,
                    contentType: 'application/json',
                    body: JSON.stringify({ error: 'upstream' }),
                });
            }
            if (scenario === 'halt-success-setup' || scenario === 'halt-halted') {
                return route.fulfill({
                    status: 200,
                    contentType: 'application/json',
                    body: JSON.stringify({ halted: true }),
                });
            }
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify({ halted: false }),
            });
        }
        if (p === '/api/kill-switch' && method === 'POST') {
            if (scenario === 'halt-disabled') {
                return;
            }
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify({
                    ok: true,
                    halted: true,
                    cancel_attempted: 2,
                    cancel_ok: 2,
                    cancel_failed: 0,
                }),
            });
        }
        if (p === '/api/kill-switch/reset' && method === 'POST') {
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify({ ok: true, halted: false }),
            });
        }
        return route.continue();
    });
}

async function openHalt(page, base, scenario) {
    await installApiMocks(page, scenario);
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.goto(base + '/', { waitUntil: 'domcontentloaded' });
    await page.locator('#tabbtn-algo').click();
    if (scenario === 'halt-loading') {
        await page.locator('#killSwitchStatus').waitFor({ state: 'visible' });
        await page.waitForTimeout(250);
        return;
    }
    if (scenario === 'halt-error') {
        await page.waitForFunction(
            () => document.getElementById('killSwitchStatus')?.textContent === 'Halt status unknown'
        );
        return;
    }
    if (scenario === 'halt-empty' || scenario === 'halt-disabled') {
        await page.waitForFunction(
            () => document.getElementById('killSwitchStatus')?.textContent === 'Not halted'
        );
        if (scenario === 'halt-disabled') {
            await page.locator('#killSwitchBtn').click();
            await page.waitForFunction(
                () => document.getElementById('killSwitchBtn')?.disabled === true
                    && document.getElementById('killSwitchResetBtn')?.disabled === true
            );
        }
        return;
    }
    if (scenario === 'halt-success-setup') {
        await page.waitForFunction(
            () => document.getElementById('killSwitchStatus')?.textContent === 'Halted'
        );
        await page.locator('#killSwitchResetBtn').click();
        await page.waitForFunction(() => {
            const t = document.querySelector('#killSwitchBanner .house-banner-title');
            return t && t.textContent === 'Kill Switch reset';
        });
        return;
    }
    if (scenario === 'halt-halted') {
        await page.waitForFunction(
            () => document.getElementById('killSwitchStatus')?.textContent === 'Halted'
        );
    }
}

async function openPositions(page, base, scenario) {
    await installApiMocks(page, scenario);
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.goto(base + '/', { waitUntil: 'domcontentloaded' });
    await page.locator('#tabbtn-positions').click();
    if (scenario === 'pos-loading' || scenario === 'pos-disabled') {
        await page.waitForFunction(() => {
            const t = document.querySelector('#positionsBanner .house-banner-title');
            return t && t.textContent === 'Loading positions';
        });
        await page.waitForFunction(
            () => document.getElementById('positionsRefreshBtn')?.disabled === true
        );
        return;
    }
    if (scenario === 'pos-error') {
        await page.waitForFunction(() => {
            const t = document.querySelector('#positionsBanner .house-banner-title');
            return t && t.textContent === 'Cannot load positions';
        });
        return;
    }
    if (scenario === 'pos-empty') {
        await page.waitForFunction(() => {
            const t = document.querySelector('#positionsBanner .house-banner-title');
            return t && t.textContent === 'No positions';
        });
        return;
    }
    if (scenario === 'pos-success') {
        await page.waitForFunction(() => {
            const t = document.querySelector('#positionsBanner .house-banner-title');
            return t && t.textContent === 'Positions loaded';
        });
    }
}

async function shotClip(page, dest, startSel) {
    await page.locator(startSel).scrollIntoViewIfNeeded();
    await page.evaluate((sel) => {
        const el = document.querySelector(sel);
        if (el) el.scrollIntoView({ block: 'start', inline: 'nearest' });
    }, startSel);
    await page.waitForTimeout(80);
    const clip = await page.evaluate((sel) => {
        const el = document.querySelector(sel);
        const vpW = window.innerWidth;
        const vpH = window.innerHeight;
        const top = el ? Math.max(0, Math.floor(el.getBoundingClientRect().top) - 8) : 0;
        return {
            x: 0,
            y: top,
            width: vpW,
            height: Math.max(80, vpH - top),
        };
    }, startSel);
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
                sample('#tab-algo .lead'),
                sample('#killSwitchStatus'),
                sample('#killSwitchBtn'),
                sample('#killSwitchResetBtn'),
                sample('#algoEnableBtn'),
                sample('#killSwitchBanner'),
                sample('#killSwitchBanner .house-banner-title'),
                sample('#algoStatusBanner'),
                sample('#positionsBanner'),
                sample('#positionsRefreshBtn'),
                sample('.house-field > span'),
                sample('#algoCfgSymbol'),
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
        ['--danger', '--danger-bg', 'error chip / halt banner'],
        ['--danger', '--bg', 'Kill Switch outlined button'],
        ['--success', '--success-bg', 'success chip / reset banner'],
        ['--success', '--bg', 'Not halted chip on page'],
        ['--info', '--info-bg', 'loading banner'],
        ['--warning', '--warning-bg', 'Spread capture is off banner'],
        ['--accent-contrast', '--accent', 'Enable algorithm primary button'],
        ['--text', '--surface-2', 'secondary control text'],
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

async function main() {
    fs.mkdirSync(SHOTS, { recursive: true });
    const { server, port } = await startServer();
    const base = `http://127.0.0.1:${port}`;
    const chromeCandidates = [
        process.env.PLAYWRIGHT_CHROME,
        '/home/grok-b1/.cache/ms-playwright/chromium-1187/chrome-linux/chrome',
        '/home/grok-b1/.cache/ms-playwright/chromium_headless_shell-1187/chrome-linux/headless_shell',
    ].filter(Boolean);
    const executablePath = chromeCandidates.find((p) => fs.existsSync(p));
    const browser = await chromium.launch({
        headless: true,
        ...(executablePath ? { executablePath } : {}),
        args: ['--no-sandbox', '--disable-dev-shm-usage'],
    });

    const haltScenarios = [
        { id: 'halt-loading', file: 'halt-loading', label: 'Halt loading — Checking halt, both halt buttons disabled' },
        { id: 'halt-empty', file: 'halt-empty', label: 'Halt empty — Not halted, Reset disabled' },
        { id: 'halt-error', file: 'halt-error', label: 'Halt error — Halt status unknown' },
        { id: 'halt-success-setup', file: 'halt-success', label: 'Halt success — Kill Switch reset (resume after drawdown)' },
        { id: 'halt-disabled', file: 'halt-disabled', label: 'Halt disabled — in-flight trigger' },
        { id: 'halt-halted', file: 'halt-halted', label: 'Halt halted — drawdown trip looks identical to operator halt' },
    ];
    const posScenarios = [
        { id: 'pos-loading', file: 'pos-loading', label: 'Positions loading — banner + table loading, refresh disabled' },
        { id: 'pos-empty', file: 'pos-empty', label: 'Positions empty — no fills yet' },
        { id: 'pos-error', file: 'pos-error', label: 'Positions error — cannot load' },
        { id: 'pos-success', file: 'pos-success', label: 'Positions success — net qty and realized P&L' },
        { id: 'pos-disabled', file: 'pos-disabled', label: 'Positions disabled — refresh disabled while loading' },
    ];

    const overflow = [];
    const log = [];
    let contrastDark = null;

    const haltPanel = '#tab-algo .panel-header';
    const posPanel = '#tab-positions .panel-header';

    for (const vp of VIEWPORTS) {
        for (const sc of haltScenarios) {
            const context = await browser.newContext({
                viewport: { width: vp.width, height: vp.height },
                colorScheme: 'dark',
                reducedMotion: 'reduce',
            });
            const page = await context.newPage();
            page.setDefaultTimeout(15000);
            await openHalt(page, base, sc.id);
            const file = `${sc.file}-${vp.name}.png`;
            const dest = path.join(SHOTS, file);
            await shotClip(page, dest, haltPanel);
            const ov = await measureOverflow(page);
            overflow.push({ width: vp.name, state: sc.file, ...ov, file });
            log.push({ file, label: sc.label, width: vp.name, overflowing: ov.overflowing });
            if (vp.name === '1280' && sc.id === 'halt-empty') {
                contrastDark = scoreSamples(await sampleContrast(page));
                await page.locator('#algoRefreshBtn').press('Tab');
                await page.waitForTimeout(80);
                const btnBox = await page.locator('#killSwitchBtn').boundingBox();
                if (btnBox) {
                    await page.screenshot({
                        path: path.join(SHOTS, 'halt-focus-ring-1280.png'),
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
        for (const sc of posScenarios) {
            const context = await browser.newContext({
                viewport: { width: vp.width, height: vp.height },
                colorScheme: 'dark',
                reducedMotion: 'reduce',
            });
            const page = await context.newPage();
            page.setDefaultTimeout(15000);
            await openPositions(page, base, sc.id);
            const file = `${sc.file}-${vp.name}.png`;
            const dest = path.join(SHOTS, file);
            await shotClip(page, dest, posPanel);
            const ov = await measureOverflow(page);
            overflow.push({ width: vp.name, state: sc.file, ...ov, file });
            log.push({ file, label: sc.label, width: vp.name, overflowing: ov.overflowing });
            await context.close();
            process.stdout.write(`wrote ${file}\n`);
        }
    }

    {
        const context = await browser.newContext({
            viewport: { width: 1280, height: 800 },
            colorScheme: 'light',
            reducedMotion: 'reduce',
        });
        const page = await context.newPage();
        await openHalt(page, base, 'halt-empty');
        const contrastLight = scoreSamples(await sampleContrast(page));
        await shotClip(page, path.join(SHOTS, 'halt-empty-1280-light.png'), haltPanel);
        await context.close();

        const overflowFail = overflow.filter((o) => o.overflowing);
        const darkFail = contrastDark.token_pairs.filter((p) => !p.pass_aa);
        const lightFail = contrastLight.token_pairs.filter((p) => !p.pass_aa);
        const liveFail = [...contrastDark.live_elements, ...contrastLight.live_elements]
            .filter((p) => !p.pass_aa && !p.disabled);
        const overflowSummary = overflow
            .map((o) => `${o.state}@${o.width}: scrollWidth=${o.documentElement.scrollWidth} clientWidth=${o.documentElement.clientWidth} overflowing=${o.overflowing}`)
            .join('\n');

        const readme = `# Exposure limits / drawdown guard — UI evidence

Generated by \`tools/ux-exposure-limits-capture.mjs\` against \`src/dashboard.html\`.
This feature **does not edit the dashboard**. Drawdown halt reuses the Algo-tab
Kill Switch (\`GET /api/kill-switch/status\` \`{halted}\`). Positions is inspection
only (net qty / realized P&L). Operator copy for the gates lives in
\`config.example.toml\`.

Do not treat these screenshots as a licence to restyle the Halt controls.

## Screens in scope

| Surface | Role | New chrome? |
|---|---|---|
| Algo-tab Kill Switch | Drawdown trip + reset (AC4–AC6) | No |
| Positions tab | Inventory the gates read | No |
| \`config.example.toml\` comments | Arming the gates | Copy polish only |

Whole-app chrome (Live, History, Backtest, Securities, header, native menus): **N/A**.

## Halt states (drawdown-triggered halt is identical)

| State | What you should see | Files |
|---|---|---|
| Loading | Chip "Checking halt", both halt buttons disabled | \`halt-loading-375.png\` \`halt-loading-768.png\` \`halt-loading-1280.png\` |
| Empty | Chip "Not halted", Kill Switch enabled, Reset disabled | \`halt-empty-375.png\` \`halt-empty-768.png\` \`halt-empty-1280.png\` |
| Error | Chip "Halt status unknown", error banner | \`halt-error-375.png\` \`halt-error-768.png\` \`halt-error-1280.png\` |
| Success | Banner "Kill Switch reset" after Reset (peak rebase is invisible) | \`halt-success-375.png\` \`halt-success-768.png\` \`halt-success-1280.png\` |
| Disabled | In-flight trigger: both halt buttons disabled | \`halt-disabled-375.png\` \`halt-disabled-768.png\` \`halt-disabled-1280.png\` |
| Halted | Chip "Halted", Reset enabled, "Kill Switch is on" | \`halt-halted-375.png\` \`halt-halted-768.png\` \`halt-halted-1280.png\` |

Also: \`halt-focus-ring-1280.png\` (keyboard \`:focus-visible\` on Kill Switch),
\`halt-empty-1280-light.png\` (light scheme).

## Positions states

| State | What you should see | Files |
|---|---|---|
| Loading | Banner "Loading positions", refresh disabled | \`pos-loading-375.png\` \`pos-loading-768.png\` \`pos-loading-1280.png\` |
| Empty | "No positions yet. Fills from executed orders appear here." | \`pos-empty-375.png\` \`pos-empty-768.png\` \`pos-empty-1280.png\` |
| Error | Banner "Cannot load positions" | \`pos-error-375.png\` \`pos-error-768.png\` \`pos-error-1280.png\` |
| Success | Row with net qty and Real P&L (drawdown input) | \`pos-success-375.png\` \`pos-success-768.png\` \`pos-success-1280.png\` |
| Disabled | Refresh disabled while the request is in flight (same paint as loading) | \`pos-disabled-375.png\` \`pos-disabled-768.png\` \`pos-disabled-1280.png\` |

No Positions columns for caps, peak, or remaining drawdown — locked.

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

## Copy polish (this PR)

\`config.example.toml\` comments rewritten to sentence-case, actionable operator
copy. No "placeholder" / lorem. Dashboard markup, CSS, and JS are unchanged.

## Re-run

\`\`\`bash
node tools/ux-exposure-limits-capture.mjs
\`\`\`
`;
        fs.writeFileSync(path.join(OUT, 'README.md'), readme);
        fs.writeFileSync(path.join(OUT, 'overflow.json'), JSON.stringify(overflow, null, 2));
        fs.writeFileSync(path.join(OUT, 'capture-log.json'), JSON.stringify(log, null, 2));
        fs.writeFileSync(
            path.join(OUT, 'contrast.json'),
            JSON.stringify({ dark: contrastDark, light: contrastLight }, null, 2)
        );

        if (overflowFail.length || darkFail.length || lightFail.length || liveFail.length) {
            console.error('UX capture failed AA or overflow checks');
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
