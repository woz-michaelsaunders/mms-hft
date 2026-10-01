#!/usr/bin/env node
/**
 * Headless capture of the kdb persistence operator surfaces:
 *   - docs/ux-kdb-persistence/preview.html — House-token rendering of
 *     fix_console option 1 (data summary)
 *   - History tab on src/dashboard.html — existing kdb snapshots visual
 *     (anti-oracle for levels/trades; no new dashboard chrome)
 *
 *   node tools/ux-kdb-persistence-capture.mjs
 *
 * Output: docs/ux-kdb-persistence/
 */
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const OUT = path.join(ROOT, 'docs', 'ux-kdb-persistence');
const SHOTS = path.join(OUT, 'screenshots');
const PREVIEW = fs.readFileSync(path.join(OUT, 'preview.html'), 'utf8');
const DASH = fs.readFileSync(path.join(ROOT, 'src', 'dashboard.html'), 'utf8');

function loadPlaywright() {
    const candidates = [
        path.join(ROOT, 'node_modules', 'playwright'),
        '/tmp/pw/node_modules/playwright',
        '/tmp/shot/node_modules/playwright',
        '/tmp/ux-preview/node_modules/playwright',
        '/tmp/wozinga-fix/node_modules/playwright',
        '/home/agent1/screenshot-tool/node_modules/playwright',
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
const FORBIDDEN_DASH = [
    '/api/kdb-counts',
    'tabbtn-kdb',
    'orderbook_levels panel',
    'lorem ipsum',
    'Lorem ipsum',
];

const VIEWPORTS = [
    { name: '375', width: 375, height: 812 },
    { name: '768', width: 768, height: 1024 },
    { name: '1280', width: 1280, height: 900 },
];

const HISTORY_ROWS = [
    { time: 1770000000000, symbol: 'BTC_USDT', best_bid: 65000.1, best_ask: 65001.0 },
    { time: 1770000001000, symbol: 'BTC_USDT', best_bid: 65000.4, best_ask: 65001.2 },
    { time: 1770000002000, symbol: 'BTC_USDT', best_bid: 65000.8, best_ask: 65001.5 },
];

function startServer() {
    return new Promise((resolve) => {
        const server = http.createServer((req, res) => {
            const url = new URL(req.url, 'http://127.0.0.1');
            if (url.pathname === '/' || url.pathname === '/preview.html') {
                res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
                res.end(PREVIEW);
                return;
            }
            if (url.pathname === '/dashboard.html') {
                res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
                res.end(DASH);
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

function assertTokensAndCopy(html, label) {
    const hits = FORBIDDEN.filter((s) => html.includes(s));
    if (hits.length) {
        throw new Error(label + ' contains placeholder copy: ' + hits.join(', '));
    }
    if (!html.includes('--bg:') || !html.includes('--space-1:')) {
        throw new Error(label + ' is missing House design tokens');
    }
    const styleMatch = html.match(/<style>([\s\S]*?)<\/style>/);
    if (!styleMatch) throw new Error(label + ' has no <style> block');
    const withoutTokenBlocks = styleMatch[1]
        .replace(/:root\s*\{[\s\S]*?\}/g, '')
        .replace(/@media \(prefers-color-scheme: light\)\s*\{[\s\S]*?:root\s*\{[\s\S]*?\}\s*\}/g, '');
    const leftoverHex = withoutTokenBlocks.match(/#[0-9a-fA-F]{3,8}\b/g) || [];
    if (leftoverHex.length) {
        throw new Error(label + ' raw hex outside token blocks: ' + leftoverHex.slice(0, 12).join(', '));
    }
}

async function installHistMocks(page, scenario) {
    await page.route('**/*', async (route) => {
        const u = new URL(route.request().url());
        const p = u.pathname;
        if (p === '/api/symbols') {
            if (scenario === 'disabled') {
                return route.fulfill({
                    status: 500,
                    contentType: 'application/json',
                    body: JSON.stringify({ error: 'upstream' }),
                });
            }
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify({ symbols: ['BTC_USDT', 'ETH_USDT'] }),
            });
        }
        if (p.startsWith('/api/history/')) {
            if (scenario === 'loading') return;
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
                    body: JSON.stringify(HISTORY_ROWS),
                });
            }
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify([]),
            });
        }
        if (p === '/api/market-data-status') {
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify({ servers: [] }),
            });
        }
        if (p.startsWith('/api/orderbook/')) {
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify({
                    bids: [[65000.1, 0.4]],
                    asks: [[65001.0, 0.3]],
                    best_bid: 65000.1,
                    best_ask: 65001.0,
                    trade_count: 0,
                    trades: [],
                }),
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

async function openCli(page, base, scenario) {
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.goto(base + '/preview.html?state=' + scenario, { waitUntil: 'domcontentloaded' });
    await page.waitForFunction((name) => document.body.getAttribute('data-state') === name, scenario);
}

async function openHist(page, base, scenario) {
    await installHistMocks(page, scenario);
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.goto(base + '/dashboard.html', { waitUntil: 'domcontentloaded' });
    await page.locator('#tabbtn-history').click();
    await page.waitForFunction(() => {
        const panel = document.getElementById('tab-history');
        return panel && !panel.hidden;
    });

    if (scenario === 'disabled') {
        await page.waitForFunction(() => {
            const t = document.querySelector('#histBanner .house-banner-title');
            const btn = document.getElementById('histLoadBtn');
            return t && t.textContent === 'History not loaded' && btn && btn.disabled === true;
        });
        return;
    }

    await page.waitForFunction(() => document.getElementById('histLoadBtn')?.disabled === false);

    if (scenario === 'loading') {
        await page.locator('#histLoadBtn').click();
        await page.waitForFunction(() => {
            const t = document.querySelector('#histBanner .house-banner-title');
            const btn = document.getElementById('histLoadBtn');
            return t && t.textContent === 'Loading snapshots' && btn && btn.disabled === true;
        });
        return;
    }
    if (scenario === 'empty') {
        await page.locator('#histLoadBtn').click();
        await page.waitForFunction(() => {
            const t = document.querySelector('#histBanner .house-banner-title');
            return t && t.textContent === 'No snapshot data';
        });
        return;
    }
    if (scenario === 'error') {
        await page.locator('#histLoadBtn').click();
        await page.waitForFunction(() => {
            const t = document.querySelector('#histBanner .house-banner-title');
            return t && t.textContent === 'Cannot load history';
        });
        return;
    }
    if (scenario === 'success') {
        await page.locator('#histLoadBtn').click();
        await page.waitForFunction(() => {
            const t = document.querySelector('#histBanner .house-banner-title');
            return t && t.textContent === 'History loaded';
        });
    }
}

async function shotHist(page, dest) {
    const header = await page.locator('header.header').boundingBox();
    const panel = await page.locator('#tab-history').boundingBox();
    if (!header || !panel) throw new Error('missing header or history panel');
    const vp = page.viewportSize();
    const y = Math.max(0, header.y);
    const bottom = panel.y + panel.height;
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

async function probeCli(page) {
    return page.evaluate(() => {
        const hostLabel = document.querySelector('label[for="hostInput"]');
        const portLabel = document.querySelector('label[for="portInput"]');
        const run = document.getElementById('runBtn');
        const src = document.documentElement.outerHTML;
        const forbidden = ['lorem ipsum', 'Lorem ipsum', 'placeholder copy'].filter((s) => src.toLowerCase().includes(s.toLowerCase()));
        return {
            labelledHost: !!(hostLabel && (hostLabel.textContent || '').trim().length),
            labelledPort: !!(portLabel && (portLabel.textContent || '').trim().length),
            hostLabel: hostLabel ? hostLabel.textContent.trim() : null,
            portLabel: portLabel ? portLabel.textContent.trim() : null,
            primaryAction: run ? run.textContent.trim() : null,
            primaryDisabled: !!(run && run.disabled),
            forbiddenInDom: forbidden,
            hasFocusVisibleRule: /:focus-visible/.test(src),
        };
    });
}

async function probeHist(page) {
    return page.evaluate(() => {
        const label = document.querySelector('label[for="histSymbolSelect"]');
        const select = document.getElementById('histSymbolSelect');
        const src = document.documentElement.outerHTML;
        return {
            labelledSymbol: !!(label && select && (label.textContent || '').trim().length),
            symbolLabel: label ? label.textContent.trim() : null,
            forbiddenInDom: ['lorem ipsum', '/api/kdb-counts', 'tabbtn-kdb'].filter((s) => src.toLowerCase().includes(s.toLowerCase())),
            hasFocusVisibleRule: /:focus-visible/.test(src),
            hasKdbChrome: /tabbtn-kdb/i.test(src) || /\/api\/kdb-counts/.test(src),
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
        ['--warning', '--warning-bg', 'not-connected / warning banner'],
        ['--accent-contrast', '--accent', 'Refresh summary primary button'],
        ['--text', '--surface-2', 'input text on field surface'],
        ['--text-faint', '--bg', 'faint hint'],
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
    assertTokensAndCopy(PREVIEW, 'preview.html');
    assertTokensAndCopy(DASH, 'dashboard.html');
    const dashHits = FORBIDDEN_DASH.filter((s) => DASH.includes(s));
    if (dashHits.length) {
        throw new Error('dashboard.html grew kdb persistence chrome: ' + dashHits.join(', '));
    }
    if (/tabbtn-kdb/i.test(DASH) || /\/api\/kdb-counts/.test(DASH)) {
        throw new Error('dashboard.html grew a kdb-counts tab or API — forbidden');
    }
    ensureChromeLibs();
    fs.mkdirSync(SHOTS, { recursive: true });
    const { server, port } = await startServer();
    const base = `http://127.0.0.1:${port}`;
    const chromeCandidates = [
        process.env.PLAYWRIGHT_CHROME,
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

    const cliScenarios = [
        { id: 'loading', file: 'loading', label: 'Loading — Checking table counts; Refresh summary disabled' },
        { id: 'empty', file: 'empty', label: 'Empty — No market-data rows yet; Refresh summary is the primary action' },
        { id: 'error', file: 'error', label: 'Error — Cannot read kdb counts' },
        { id: 'success', file: 'success', label: 'Success — Counts loaded from kdb+; trades 0 without 269=2' },
        { id: 'disabled', file: 'disabled', label: 'Disabled — Not connected to kdb+; Refresh summary disabled' },
    ];
    const histScenarios = [
        { id: 'loading', file: 'hist-loading', label: 'History loading — Loading snapshots' },
        { id: 'empty', file: 'hist-empty', label: 'History empty — No snapshot data' },
        { id: 'error', file: 'hist-error', label: 'History error — Cannot load history' },
        { id: 'success', file: 'hist-success', label: 'History success — History loaded (anti-oracle snapshots)' },
        { id: 'disabled', file: 'hist-disabled', label: 'History disabled — Load history off until symbols load' },
    ];

    const overflow = [];
    const log = [];
    const a11y = [];
    let contrastDark = null;

    const cliSelectors = [
        'body',
        '.lead',
        '#banner',
        '#banner .house-banner-title',
        '#runBtn',
        '#levelsBtn',
        'label[for="hostInput"]',
        '#hostInput',
        'label[for="portInput"]',
        '#portInput',
        '#empty p',
        '.hint',
        'h1',
        '.chip',
    ];

    for (const vp of VIEWPORTS) {
        for (const sc of cliScenarios) {
            const context = await browser.newContext({
                viewport: { width: vp.width, height: vp.height },
                colorScheme: 'dark',
                reducedMotion: 'reduce',
            });
            const page = await context.newPage();
            page.setDefaultTimeout(15000);
            await openCli(page, base, sc.id);
            const probe = await probeCli(page);
            if (probe.forbiddenInDom.length) {
                throw new Error('CLI DOM contains placeholder copy: ' + probe.forbiddenInDom.join(', '));
            }
            if (!probe.labelledHost || !probe.labelledPort) {
                throw new Error('CLI inputs are not labelled');
            }
            if (!probe.hasFocusVisibleRule) {
                throw new Error('missing :focus-visible rule');
            }
            if (probe.primaryAction !== 'Refresh summary') {
                throw new Error('primary action copy drifted: ' + probe.primaryAction);
            }
            a11y.push({ file: `${sc.file}-${vp.name}.png`, surface: 'cli', ...probe });
            const file = `${sc.file}-${vp.name}.png`;
            await page.screenshot({ path: path.join(SHOTS, file), fullPage: true });
            const ov = await measureOverflow(page);
            overflow.push({ width: vp.name, state: sc.file, ...ov, file });
            log.push({ file, label: sc.label, width: vp.name, overflowing: ov.overflowing });
            if (vp.name === '1280' && sc.id === 'empty') {
                contrastDark = scoreSamples(await sampleContrast(page, cliSelectors));
                await page.locator('#hostInput').focus();
                await page.keyboard.press('Tab');
                await page.waitForFunction(() => document.activeElement && document.activeElement.id === 'portInput');
                await page.keyboard.press('Tab');
                await page.waitForFunction(() => document.activeElement && document.activeElement.id === 'runBtn');
                await page.waitForTimeout(80);
                const btnBox = await page.locator('#runBtn').boundingBox();
                if (btnBox) {
                    await page.screenshot({
                        path: path.join(SHOTS, 'focus-ring-1280.png'),
                        clip: {
                            x: Math.max(0, btnBox.x - 32),
                            y: Math.max(0, btnBox.y - 32),
                            width: Math.min(vp.width - Math.max(0, btnBox.x - 32), btnBox.width + 64),
                            height: btnBox.height + 64,
                        },
                    });
                }
            }
            await context.close();
            process.stdout.write(`wrote ${file}\n`);
        }

        for (const sc of histScenarios) {
            const context = await browser.newContext({
                viewport: { width: vp.width, height: vp.height },
                colorScheme: 'dark',
                reducedMotion: 'reduce',
            });
            const page = await context.newPage();
            page.setDefaultTimeout(15000);
            await openHist(page, base, sc.id);
            const probe = await probeHist(page);
            if (probe.hasKdbChrome) {
                throw new Error('dashboard grew a kdb-counts tab');
            }
            if (!probe.labelledSymbol) {
                throw new Error('History symbol select is not labelled');
            }
            a11y.push({ file: `${sc.file}-${vp.name}.png`, surface: 'history', ...probe });
            const file = `${sc.file}-${vp.name}.png`;
            await shotHist(page, path.join(SHOTS, file));
            const ov = await measureOverflow(page);
            overflow.push({ width: vp.name, state: sc.file, ...ov, file });
            log.push({ file, label: sc.label, width: vp.name, overflowing: ov.overflowing });
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
        await openCli(page, base, 'empty');
        const contrastLight = scoreSamples(await sampleContrast(page, cliSelectors));
        await page.screenshot({ path: path.join(SHOTS, 'empty-1280-light.png'), fullPage: true });
        await context.close();

        const overflowFail = overflow.filter((o) => o.overflowing);
        const darkFail = contrastDark.token_pairs.filter((p) => !p.pass_aa);
        const lightFail = contrastLight.token_pairs.filter((p) => !p.pass_aa);
        const liveFail = [...contrastDark.live_elements, ...contrastLight.live_elements]
            .filter((p) => !p.pass_aa && !p.disabled);
        if (overflowFail.length) {
            throw new Error('horizontal overflow: ' + overflowFail.map((o) => o.file).join(', '));
        }
        if (darkFail.length || lightFail.length) {
            throw new Error('token contrast < 4.5:1');
        }
        if (liveFail.length) {
            throw new Error('live element contrast < 4.5:1: ' + liveFail.map((e) => e.selector).join(', '));
        }

        const overflowSummary = overflow
            .map((o) => `${o.state}@${o.width}: scrollWidth=${o.documentElement.scrollWidth} clientWidth=${o.documentElement.clientWidth} overflowing=${o.overflowing}`)
            .join('\n');

        const liveDark = contrastDark.live_elements
            .map((e) => `- \`${e.selector}\` \`${e.fg}\` on \`${e.bg}\` **${e.ratio}:1** ${e.pass_aa ? 'pass' : 'FAIL'}${e.disabled ? ' (disabled)' : ''}`)
            .join('\n');
        const liveLight = contrastLight.live_elements
            .map((e) => `- \`${e.selector}\` \`${e.fg}\` on \`${e.bg}\` **${e.ratio}:1** ${e.pass_aa ? 'pass' : 'FAIL'}${e.disabled ? ' (disabled)' : ''}`)
            .join('\n');

        const readme = `# kdb+ levels/trades persistence — UI evidence

Generated by \`tools/ux-kdb-persistence-capture.mjs\`.

The delivered operator UI is **CLI** (\`fix_console\` option 1). This feature
**does not add dashboard chrome**. \`preview.html\` is capture-only evidence of
that CLI (House tokens, states, labelled inputs). It is **not** served by
\`fix_server\`. Do not copy it into \`src/dashboard.html\`.

The History tab is the existing kdb snapshots visual. It is an **anti-oracle**
for \`orderbook_levels\` / \`trades\` (it reads \`orderbook_snapshots\` only).
No History-tab or Live-tab fields were added.

Do not treat these screenshots as a licence to add a kdb-counts panel.

## Screens in scope

| Surface | Role | New chrome? |
|---|---|---|
| \`fix_console\` option 1 | Operator oracle (\`count orderbook_levels\` / \`trades\` / \`orderbook_snapshots\`) | Copy polish only |
| \`docs/ux-kdb-persistence/preview.html\` | House-token rendering of that CLI for capture | Docs only |
| History tab | Existing visual of kdb snapshots (anti-oracle) | No |

Whole-app chrome (Live book, Orders, Positions, Algo, Backtest, Securities, Kill Switch, Shadow mode, native menus, window model, drag-and-drop): **N/A** — this ask does not touch them.

## Definition of Done (scoped to this ask)

| Line | Verdict |
|---|---|
| Design tokens, no raw hex, no off-scale spacing | **Pass** on the CLI preview (tokens in \`:root\`; component rules use \`var(--*)\`; spacing is \`--space-1\`…\`--space-8\`). Dashboard CSS was not edited. |
| Loading, empty, error, success, disabled | **Pass** on the CLI preview and on the related History tab. |
| Responsive 375 / 768 / 1280, no horizontal page scroll | **Pass** (measured). |
| Keyboard-navigable, visible focus ring | **Pass** — \`:focus-visible\` uses \`--focus-ring\`; captured on Refresh summary. |
| Contrast ≥ 4.5:1 | **Pass** (token pairs and live samples below). |
| Inputs labelled | **Pass** — Host, Port; History Symbol. |
| Copy real, sentence-case, no lorem | **Pass** — CLI banners / table labels; source scan found no lorem. |
| UI rendered and reviewed, not shipped blind | **Pass** — every in-scope state captured at 375 / 768 / 1280 plus light scheme and focus ring. |

## CLI states (House-token rendering of \`fix_console\` option 1)

| State | What you should see | Files |
|---|---|---|
| Loading | Banner "Checking table counts"; Refresh summary disabled | \`loading-375.png\` \`loading-768.png\` \`loading-1280.png\` |
| Empty | Banner "No market-data rows yet"; Refresh summary is the primary action | \`empty-375.png\` \`empty-768.png\` \`empty-1280.png\` |
| Error | Banner "Cannot read kdb counts" | \`error-375.png\` \`error-768.png\` \`error-1280.png\` |
| Success | Banner "Counts loaded from kdb+"; trades 0 without 269=2 | \`success-375.png\` \`success-768.png\` \`success-1280.png\` |
| Disabled | Banner "Not connected to kdb+"; Refresh summary disabled | \`disabled-375.png\` \`disabled-768.png\` \`disabled-1280.png\` |

Also: \`focus-ring-1280.png\` (keyboard \`:focus-visible\` on Refresh summary),
\`empty-1280-light.png\` (light scheme).

## History tab (existing snapshots visual — unchanged chrome)

| State | What you should see | Files |
|---|---|---|
| Loading | Banner "Loading snapshots" | \`hist-loading-375.png\` \`hist-loading-768.png\` \`hist-loading-1280.png\` |
| Empty | Banner "No snapshot data" | \`hist-empty-375.png\` \`hist-empty-768.png\` \`hist-empty-1280.png\` |
| Error | Banner "Cannot load history" | \`hist-error-375.png\` \`hist-error-768.png\` \`hist-error-1280.png\` |
| Success | Banner "History loaded" | \`hist-success-375.png\` \`hist-success-768.png\` \`hist-success-1280.png\` |
| Disabled | Load history off until symbols load | \`hist-disabled-375.png\` \`hist-disabled-768.png\` \`hist-disabled-1280.png\` |

No \`/api/kdb-counts\`, kdb tab, or levels/trades panel — locked by the ask.

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

## Copy polish (this PR)

- \`src/bin/console.rs\`: sentence-case menu and headings; option 1 distinguishes loading / empty / error / success; connection failure tells the operator to start kdb+ first. Queries, menu numbers, and IPC stay locked.
- Empty vs error: a failed \`count\` is no longer printed as \`0\`. Trades = 0 with other tables growing is explained as no 269=2 on the wire.
`;

        fs.writeFileSync(path.join(OUT, 'README.md'), readme);
        fs.writeFileSync(path.join(OUT, 'overflow.json'), JSON.stringify(overflow, null, 2));
        fs.writeFileSync(path.join(OUT, 'contrast.json'), JSON.stringify({ dark: contrastDark, light: contrastLight }, null, 2));
        fs.writeFileSync(path.join(OUT, 'a11y.json'), JSON.stringify(a11y, null, 2));
        fs.writeFileSync(path.join(OUT, 'capture-log.json'), JSON.stringify(log, null, 2));
        process.stdout.write('wrote docs/ux-kdb-persistence/README.md\n');
    }

    await browser.close();
    server.close();
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
