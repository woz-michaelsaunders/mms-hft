#!/usr/bin/env node
/**
 * Headless capture of the rollback-drill operator surfaces:
 *   - docs/ux-rollback-drill/preview.html — House-token rendering of the CLI
 *     (help / dry-run / loading / empty / error / success / disabled)
 *   - Live order book on src/dashboard.html — the GET /api/orderbook/BTC_USDT
 *     serving probe the drill already uses (no new dashboard chrome)
 *
 *   node tools/ux-rollback-drill-capture.mjs
 *
 * Output: docs/ux-rollback-drill/
 */
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const OUT = path.join(ROOT, 'docs', 'ux-rollback-drill');
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

const FORBIDDEN = ['lorem ipsum', 'Lorem ipsum', 'placeholder copy', 'TODO', 'FIXME'];

const VIEWPORTS = [
    { name: '375', width: 375, height: 812 },
    { name: '768', width: 768, height: 1024 },
    { name: '1280', width: 1280, height: 900 },
];

const ORDERBOOK_LIVE = {
    symbol: 'BTC_USDT',
    best_bid: 65000.1,
    best_ask: 65001.0,
    spread: 0.9,
    bids: [{ price: 65000.1, size: 0.4 }, { price: 64999.5, size: 1.2 }],
    asks: [{ price: 65001.0, size: 0.3 }, { price: 65002.2, size: 0.8 }],
    trades: [{ price: 65000.4, size: 0.1, timestamp: Date.now() }],
    trade_count: 12,
    timestamp: Date.now(),
};

const ORDERBOOK_EMPTY = {
    symbol: 'BTC_USDT',
    best_bid: 0,
    best_ask: 0,
    spread: 0,
    bids: [],
    asks: [],
    trades: [],
    trade_count: 0,
    timestamp: Date.now(),
};

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

async function installLiveMocks(page, scenario) {
    let bookHits = 0;
    await page.route('**/*', async (route) => {
        const u = new URL(route.request().url());
        const p = u.pathname;
        if (p.startsWith('/api/orderbook/')) {
            bookHits += 1;
            if (scenario === 'loading') return;
            if (scenario === 'disabled') {
                if (bookHits === 1) {
                    return route.fulfill({
                        status: 200,
                        contentType: 'application/json',
                        body: JSON.stringify(ORDERBOOK_EMPTY),
                    });
                }
                return;
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
                    body: JSON.stringify(ORDERBOOK_LIVE),
                });
            }
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify(ORDERBOOK_EMPTY),
            });
        }
        if (p === '/api/symbols') {
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify({ symbols: ['BTC_USDT', 'ETH_USDT'] }),
            });
        }
        if (p === '/api/market-data-status') {
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify({ servers: [] }),
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

async function openLive(page, base, scenario) {
    await installLiveMocks(page, scenario);
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.goto(base + '/dashboard.html', { waitUntil: 'domcontentloaded' });
    await page.locator('#tabbtn-live').click();

    if (scenario === 'loading') {
        await page.waitForFunction(() => {
            const t = document.querySelector('#liveBanner .house-banner-title');
            return t && t.textContent === 'Loading the order book';
        });
        return;
    }
    if (scenario === 'empty') {
        await page.waitForFunction(() => {
            const t = document.querySelector('#liveBanner .house-banner-title');
            return t && t.textContent === 'Empty book';
        });
        return;
    }
    if (scenario === 'error') {
        await page.waitForFunction(() => {
            const t = document.querySelector('#liveBanner .house-banner-title');
            return t && t.textContent === 'Cannot load the order book';
        });
        return;
    }
    if (scenario === 'success') {
        await page.waitForFunction(() => {
            const t = document.querySelector('#liveBanner .house-banner-title');
            return t && t.textContent === 'Order book updated';
        });
        return;
    }
    if (scenario === 'disabled') {
        await page.waitForFunction(() => {
            const t = document.querySelector('#liveBanner .house-banner-title');
            return t && t.textContent === 'Empty book';
        });
        await page.evaluate(() => {
            if (typeof updateOrderBook === 'function') updateOrderBook({ userInitiated: true });
        });
        await page.waitForFunction(() => document.getElementById('symbolSelect')?.disabled === true);
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

async function probeCli(page) {
    return page.evaluate(() => {
        const caseLabel = document.querySelector('label[for="caseSelect"]');
        const timeoutLabel = document.querySelector('label[for="timeoutInput"]');
        const run = document.getElementById('runBtn');
        const src = document.documentElement.outerHTML;
        const forbidden = ['lorem ipsum', 'Lorem ipsum', 'placeholder copy'].filter((s) => src.toLowerCase().includes(s.toLowerCase()));
        return {
            labelledCase: !!(caseLabel && (caseLabel.textContent || '').trim().length),
            labelledTimeout: !!(timeoutLabel && (timeoutLabel.textContent || '').trim().length),
            caseLabel: caseLabel ? caseLabel.textContent.trim() : null,
            timeoutLabel: timeoutLabel ? timeoutLabel.textContent.trim() : null,
            primaryAction: run ? run.textContent.trim() : null,
            primaryDisabled: !!(run && run.disabled),
            forbiddenInDom: forbidden,
            hasFocusVisibleRule: /:focus-visible/.test(src),
        };
    });
}

async function probeLive(page) {
    return page.evaluate(() => {
        const label = document.querySelector('label[for="symbolSelect"]');
        const select = document.getElementById('symbolSelect');
        const src = document.documentElement.outerHTML;
        return {
            labelledSymbol: !!(label && select && (label.textContent || '').trim().length),
            symbolLabel: label ? label.textContent.trim() : null,
            forbiddenInDom: ['lorem ipsum', 'rollback drill tab', '/api/drill'].filter((s) => src.toLowerCase().includes(s.toLowerCase())),
            hasFocusVisibleRule: /:focus-visible/.test(src),
            hasDrillChrome: /rollback drill/i.test(src) && /tabbtn-/.test(src) && /tabbtn-drill/.test(src),
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
        ['--warning', '--warning-bg', 'plan-only / skip banner'],
        ['--accent-contrast', '--accent', 'Run cases 1–4 primary button'],
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
    if (/tabbtn-drill/i.test(DASH) || /\/api\/drill/.test(DASH)) {
        throw new Error('dashboard.html grew rollback-drill chrome — forbidden');
    }
    ensureChromeLibs();
    fs.mkdirSync(SHOTS, { recursive: true });
    const { server, port } = await startServer();
    const base = `http://127.0.0.1:${port}`;
    const chromeCandidates = [
        process.env.PLAYWRIGHT_CHROME,
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
        { id: 'loading', file: 'loading', label: 'Loading — Checking UAT identity; Run cases 1–4 disabled' },
        { id: 'empty', file: 'empty', label: 'Empty — No results yet; Run cases 1–4 is the primary action' },
        { id: 'error', file: 'error', label: 'Error — Cannot run on this host' },
        { id: 'success', file: 'success', label: 'Success — Serving restored; per-case downtime' },
        { id: 'disabled', file: 'disabled', label: 'Disabled — Plan only; mutating run disabled' },
    ];
    const liveScenarios = [
        { id: 'loading', file: 'live-loading', label: 'Live loading — Loading the order book' },
        { id: 'empty', file: 'live-empty', label: 'Live empty — Empty book (bid/ask 0; drill serving-before would fail)' },
        { id: 'error', file: 'live-error', label: 'Live error — Cannot load the order book' },
        { id: 'success', file: 'live-success', label: 'Live success — Order book updated (the drill HTTP probe)' },
        { id: 'disabled', file: 'live-disabled', label: 'Live disabled — Symbol select off while a refresh is in flight' },
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
        '#dryRunBtn',
        'label[for="caseSelect"]',
        '#caseSelect',
        'label[for="timeoutInput"]',
        '#timeoutInput',
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
            if (!probe.labelledCase || !probe.labelledTimeout) {
                throw new Error('CLI inputs are not labelled');
            }
            if (!probe.hasFocusVisibleRule) {
                throw new Error('missing :focus-visible rule');
            }
            if (probe.primaryAction !== 'Run cases 1–4') {
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
                await page.locator('#caseSelect').focus();
                await page.keyboard.press('Tab');
                await page.waitForFunction(() => document.activeElement && document.activeElement.id === 'timeoutInput');
                await page.keyboard.press('Tab');
                await page.waitForFunction(() => document.activeElement && document.activeElement.id === 'runBtn');
                await page.waitForTimeout(80);
                const btnBox = await page.locator('#runBtn').boundingBox();
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

        for (const sc of liveScenarios) {
            const context = await browser.newContext({
                viewport: { width: vp.width, height: vp.height },
                colorScheme: 'dark',
                reducedMotion: 'reduce',
            });
            const page = await context.newPage();
            page.setDefaultTimeout(15000);
            await openLive(page, base, sc.id);
            const probe = await probeLive(page);
            if (probe.hasDrillChrome) {
                throw new Error('dashboard grew a drill tab');
            }
            if (!probe.labelledSymbol) {
                throw new Error('Live symbol select is not labelled');
            }
            a11y.push({ file: `${sc.file}-${vp.name}.png`, surface: 'live', ...probe });
            const file = `${sc.file}-${vp.name}.png`;
            if (sc.id === 'disabled') {
                await page.locator('#symbolSelect').scrollIntoViewIfNeeded();
            } else {
                await page.locator('#liveBanner').scrollIntoViewIfNeeded();
            }
            await page.waitForTimeout(40);
            await page.screenshot({ path: path.join(SHOTS, file) });
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

        const readme = `# Rollback drill — UI evidence

Generated by \`tools/ux-rollback-drill-capture.mjs\`.

The delivered operator UI is **CLI** (\`scripts/rollback-drill.sh\`). This feature
**does not add dashboard chrome**. \`preview.html\` is capture-only evidence of
that CLI (House tokens, states, labelled inputs). It is **not** served by
\`fix_server\`. Do not copy it into \`src/dashboard.html\`.

The Live order book is the existing operator surface for the drill's HTTP
serving probe (\`GET /api/orderbook/BTC_USDT\`). No Live-tab fields were added.

Do not treat these screenshots as a licence to add a drill tab.

## Screens in scope

| Surface | Role | New chrome? |
|---|---|---|
| \`rollback-drill.sh\` CLI | Operator entry (no-args = cases 1–4) | Copy polish only |
| \`docs/ux-rollback-drill/preview.html\` | House-token rendering of that CLI for capture | Docs only |
| Live order book | Existing visual of the serving HTTP probe | No |

Whole-app chrome (History, Orders, Positions, Algo, Backtest, Securities, Kill Switch, Shadow mode, native menus, window model, drag-and-drop): **N/A** — this ask does not touch them.

## Definition of Done (scoped to this ask)

| Line | Verdict |
|---|---|
| Design tokens, no raw hex, no off-scale spacing | **Pass** on the CLI preview (tokens in \`:root\`; component rules use \`var(--*)\`; spacing is \`--space-1\`…\`--space-8\`). Dashboard CSS was not edited. |
| Loading, empty, error, success, disabled | **Pass** on the CLI preview and on the related Live book. |
| Responsive 375 / 768 / 1280, no horizontal page scroll | **Pass** (measured). |
| Keyboard-navigable, visible focus ring | **Pass** — \`:focus-visible\` uses \`--focus-ring\`; captured on Run cases 1–4. |
| Contrast ≥ 4.5:1 | **Pass** (token pairs and live samples below). |
| Inputs labelled | **Pass** — Cases, Swap timeout (seconds); Live Symbol. |
| Copy real, sentence-case, no lorem | **Pass** — CLI help / dry-run / banners; source scan found no lorem. |
| UI rendered and reviewed, not shipped blind | **Pass** — every in-scope state captured at 375 / 768 / 1280 plus light scheme and focus ring. |

## CLI states (House-token rendering of rollback-drill.sh)

| State | What you should see | Files |
|---|---|---|
| Loading | Banner "Checking UAT identity"; Run cases 1–4 disabled | \`loading-375.png\` \`loading-768.png\` \`loading-1280.png\` |
| Empty | Banner "No results yet"; Run cases 1–4 is the primary action | \`empty-375.png\` \`empty-768.png\` \`empty-1280.png\` |
| Error | Banner "Cannot run on this host" | \`error-375.png\` \`error-768.png\` \`error-1280.png\` |
| Success | Banner "Serving restored"; per-case downtime | \`success-375.png\` \`success-768.png\` \`success-1280.png\` |
| Disabled | Banner "Plan only"; mutating run disabled | \`disabled-375.png\` \`disabled-768.png\` \`disabled-1280.png\` |

Also: \`focus-ring-1280.png\` (keyboard \`:focus-visible\` on Run cases 1–4),
\`empty-1280-light.png\` (light scheme).

## Live order book (existing serving probe — unchanged chrome)

| State | What you should see | Files |
|---|---|---|
| Loading | Banner "Loading the order book" | \`live-loading-375.png\` \`live-loading-768.png\` \`live-loading-1280.png\` |
| Empty | Banner "Empty book" (bid/ask 0 — serving-before would fail) | \`live-empty-375.png\` \`live-empty-768.png\` \`live-empty-1280.png\` |
| Error | Banner "Cannot load the order book" | \`live-error-375.png\` \`live-error-768.png\` \`live-error-1280.png\` |
| Success | Banner "Order book updated" (drill HTTP probe) | \`live-success-375.png\` \`live-success-768.png\` \`live-success-1280.png\` |
| Disabled | Symbol select off while a refresh is in flight | \`live-disabled-375.png\` \`live-disabled-768.png\` \`live-disabled-1280.png\` |

No drill tab, JSONL panel, or \`/api/drill\` — locked by the ask.

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

- \`scripts/rollback-drill.sh\` help, dry-run, preflight, pass/fail/skip lines: sentence-case, one primary action (no-args run cases 1–4), explicit loading / plan-only / skip copy.
- Behaviour unchanged: exit codes, JSONL fields, case logic, child-only dual env, no auto-rollback on missing Logon.

## Related coder must-fix (verified)

- PR #40 is **closed** (superseded by #41) as of 2026-09-22T17:59:36Z.
- On-box \`deploy.sh\` / \`deploy-poll.sh\` remain a reconstruction pending operator re-diff — documented on #41; this polish does not claim byte-identity.
`;

        fs.writeFileSync(path.join(OUT, 'README.md'), readme);
        fs.writeFileSync(path.join(OUT, 'overflow.json'), JSON.stringify(overflow, null, 2));
        fs.writeFileSync(path.join(OUT, 'contrast.json'), JSON.stringify({ dark: contrastDark, light: contrastLight }, null, 2));
        fs.writeFileSync(path.join(OUT, 'a11y.json'), JSON.stringify(a11y, null, 2));
        fs.writeFileSync(path.join(OUT, 'capture-log.json'), JSON.stringify(log, null, 2));
        process.stdout.write('wrote docs/ux-rollback-drill/README.md\n');
    }

    await browser.close();
    server.close();
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
