#!/usr/bin/env node
/**
 * Headless capture of the Algo-tab Kill Switch UI.
 *
 * Renders loading / empty / error / success / disabled at 375, 768 and 1280,
 * measures page-level horizontal overflow, and computes WCAG contrast for
 * the actual House token pairs taken from getComputedStyle (not guessed).
 *
 * Requires Playwright Chromium. From a machine with the browser deps:
 *
 *   npm install playwright
 *   npx playwright install chromium
 *   node tools/ux-kill-switch-capture.mjs
 *
 * Output: docs/ux-kill-switch/
 */
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const OUT = path.join(ROOT, 'docs', 'ux-kill-switch');
const SHOTS = path.join(OUT, 'screenshots');

function loadPlaywright() {
    const candidates = [
        path.join(ROOT, 'node_modules', 'playwright'),
        '/tmp/ppm-grok-b1-ale6Ts/pw/node_modules/playwright',
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

const VIEWPORTS = [
    { name: '375', width: 375, height: 812 },
    { name: '768', width: 768, height: 1024 },
    { name: '1280', width: 1280, height: 800 },
];

function json(res, status, body) {
    const payload = JSON.stringify(body);
    res.writeHead(status, {
        'content-type': 'application/json',
        'access-control-allow-origin': '*',
        'content-length': Buffer.byteLength(payload),
    });
    res.end(payload);
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
        if (p === '/api/kill-switch/status' && method === 'GET') {
            if (scenario === 'loading') {
                return; // hang — loading chip stays
            }
            if (scenario === 'error') {
                return route.fulfill({
                    status: 500,
                    contentType: 'application/json',
                    body: JSON.stringify({ error: 'upstream' }),
                });
            }
            if (scenario === 'success-setup' || scenario === 'halted') {
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
            if (scenario === 'disabled') {
                return; // hang — in-flight disables both buttons
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

async function openAlgo(page, base, scenario) {
    await installApiMocks(page, scenario);
    await page.emulateMedia({ reducedMotion: 'reduce' });
    await page.goto(base + '/', { waitUntil: 'domcontentloaded' });
    await page.locator('#tabbtn-algo').click();
    if (scenario === 'loading') {
        await page.locator('#killSwitchStatus').waitFor({ state: 'visible' });
        await page.waitForTimeout(250);
        return;
    }
    if (scenario === 'error') {
        await page.locator('#killSwitchStatus').waitFor();
        await page.waitForFunction(
            () => document.getElementById('killSwitchStatus')?.textContent === 'Halt status unknown'
        );
        return;
    }
    if (scenario === 'empty' || scenario === 'disabled') {
        await page.waitForFunction(
            () => document.getElementById('killSwitchStatus')?.textContent === 'Not halted'
        );
        if (scenario === 'disabled') {
            await page.locator('#killSwitchBtn').click();
            await page.waitForFunction(
                () => document.getElementById('killSwitchBtn')?.disabled === true
                    && document.getElementById('killSwitchResetBtn')?.disabled === true
            );
        }
        return;
    }
    if (scenario === 'success-setup') {
        await page.waitForFunction(
            () => document.getElementById('killSwitchStatus')?.textContent === 'Halted'
        );
        await page.locator('#killSwitchResetBtn').click();
        await page.waitForFunction(
            () => {
                const t = document.querySelector('#killSwitchBanner .house-banner-title');
                return t && t.textContent === 'Kill Switch reset';
            }
        );
        return;
    }
    if (scenario === 'halted') {
        await page.waitForFunction(
            () => document.getElementById('killSwitchStatus')?.textContent === 'Halted'
        );
    }
}

async function shotClip(page, dest) {
    await page.locator('#tab-algo .panel-header').scrollIntoViewIfNeeded();
    await page.evaluate(() => {
        const el = document.querySelector('#tab-algo .panel-header');
        if (el) el.scrollIntoView({ block: 'start', inline: 'nearest' });
    });
    await page.waitForTimeout(80);
    const clip = await page.evaluate(() => {
        const nodes = [
            document.querySelector('#tab-algo .panel-header'),
            document.querySelector('#killSwitchBanner:not([hidden])'),
            document.querySelector('#algoStatusBanner:not([hidden])'),
        ].filter((n) => n && n.getClientRects().length);
        const rects = nodes.map((n) => n.getBoundingClientRect());
        const vpW = window.innerWidth;
        const vpH = window.innerHeight;
        const left = 0;
        const top = Math.max(0, Math.min(...rects.map((r) => r.top)) - 8);
        const bottom = Math.min(vpH, Math.max(...rects.map((r) => r.bottom)) + 8);
        return {
            x: left,
            y: Math.floor(top),
            width: vpW,
            height: Math.max(80, Math.ceil(bottom - top)),
        };
    });
    if (!clip || clip.height < 10) {
        await page.screenshot({ path: dest, fullPage: false });
        return;
    }
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
        function sample(sel, extra) {
            const el = document.querySelector(sel);
            if (!el) return { selector: sel, missing: true };
            const s = getComputedStyle(el);
            return {
                selector: sel,
                text: (el.textContent || '').trim().slice(0, 80),
                color: s.color,
                backgroundColor: s.backgroundColor,
                borderColor: s.borderColor,
                opacity: s.opacity,
                disabled: !!el.disabled,
                extra: extra || null,
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
                sample('.house-field > span'),
                sample('#algoCfgSymbol'),
            ],
        };
    });
}

function scoreSamples(raw) {
    const bodyBg = parseCssColor(raw.samples.find((s) => s.selector === 'body')?.backgroundColor) || { r: 22, g: 28, b: 34, a: 1 };
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
        ['--success', '--bg', 'Not halted chip on page (if transparent)'],
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
    const browser = await chromium.launch({
        headless: true,
        args: ['--no-sandbox', '--disable-dev-shm-usage'],
    });

    const scenarios = [
        { id: 'loading', label: 'Loading — Checking halt, both halt buttons disabled' },
        { id: 'empty', label: 'Empty — Not halted, Reset disabled' },
        { id: 'error', label: 'Error — Halt status unknown' },
        { id: 'success-setup', file: 'success', label: 'Success — Kill Switch reset banner' },
        { id: 'disabled', label: 'Disabled — in-flight trigger, both halt buttons disabled' },
        { id: 'halted', label: 'Halted — Kill Switch is on, Reset enabled (post-trigger)' },
    ];

    const overflow = [];
    const log = [];
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
            await openAlgo(page, base, sc.id);
            const file = `${sc.file || sc.id}-${vp.name}.png`;
            const dest = path.join(SHOTS, file);
            await shotClip(page, dest);
            const ov = await measureOverflow(page);
            overflow.push({ width: vp.name, state: sc.file || sc.id, ...ov, file });
            log.push({ file, label: sc.label, width: vp.name, overflowing: ov.overflowing });
            if (vp.name === '1280' && sc.id === 'empty') {
                contrastDark = scoreSamples(await sampleContrast(page));
                await page.locator('#algoRefreshBtn').press('Tab');
                await page.waitForTimeout(80);
                const btnBox = await page.locator('#killSwitchBtn').boundingBox();
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

    // Light-scheme token check (prefers-color-scheme: light)
    {
        const context = await browser.newContext({
            viewport: { width: 1280, height: 800 },
            colorScheme: 'light',
            reducedMotion: 'reduce',
        });
        const page = await context.newPage();
        await openAlgo(page, base, 'empty');
        const contrastLight = scoreSamples(await sampleContrast(page));
        await shotClip(page, path.join(SHOTS, 'empty-1280-light.png'));
        await context.close();
        fs.writeFileSync(
            path.join(OUT, 'contrast.json'),
            JSON.stringify({ dark: contrastDark, light: contrastLight }, null, 2)
        );

        const overflowFail = overflow.filter((o) => o.overflowing);
        const darkFail = contrastDark.token_pairs.filter((p) => !p.pass_aa);
        const lightFail = contrastLight.token_pairs.filter((p) => !p.pass_aa);
        const liveFail = [...contrastDark.live_elements, ...contrastLight.live_elements].filter((p) => !p.pass_aa && !p.disabled);
        const overflowSummary = overflow.map((o) => `${o.state}@${o.width}: scrollWidth=${o.documentElement.scrollWidth} clientWidth=${o.documentElement.clientWidth} overflowing=${o.overflowing}`).join('\n');

        const readme = `# Kill Switch UI evidence

Generated by \`tools/ux-kill-switch-capture.mjs\` against \`src/dashboard.html\`.
Do not ship this tab without re-running the capture after visual edits.

## States rendered

| State | What you should see | Files |
|---|---|---|
| Loading | Chip "Checking halt", both halt buttons disabled | \`loading-375.png\` \`loading-768.png\` \`loading-1280.png\` |
| Empty | Chip "Not halted", Kill Switch enabled, Reset disabled | \`empty-375.png\` \`empty-768.png\` \`empty-1280.png\` |
| Error | Chip "Halt status unknown", error banner | \`error-375.png\` \`error-768.png\` \`error-1280.png\` |
| Success | Banner "Kill Switch reset", chip "Not halted" | \`success-375.png\` \`success-768.png\` \`success-1280.png\` |
| Disabled | In-flight trigger: both halt buttons disabled, "Triggering Kill Switch" | \`disabled-375.png\` \`disabled-768.png\` \`disabled-1280.png\` |
| Halted | Chip "Halted", Reset enabled, "Kill Switch is on" | \`halted-375.png\` \`halted-768.png\` \`halted-1280.png\` |

Also: \`focus-ring-1280.png\` (keyboard \`:focus-visible\` on Kill Switch), \`empty-1280-light.png\` (light scheme).

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

## PR #30 follow-up comments

Queried Forgejo as the agent token on 2026-09-21:

| Endpoint | Result |
|---|---|
| \`GET /api/v1/repos/wozinga-group/HFT-Fix-API/pulls/30\` | HTTP 200, \`comments: 0\`, \`review_comments: 0\`, \`state: open\`, head \`31e4427\` |
| \`GET /api/v1/repos/wozinga-group/HFT-Fix-API/pulls/30/reviews\` | HTTP 200, \`[]\` |
| \`GET /api/v1/repos/wozinga-group/HFT-Fix-API/pulls/30/comments\` | HTTP 404 (Forgejo has no inline review comments on this PR) |
| \`GET /api/v1/repos/wozinga-group/HFT-Fix-API/issues/30/comments\` | HTTP 403 (token lacks \`read:issue\`); PR payload already reports \`comments: 0\` |

No reviewer comments exist to address. Head of PR #30 is the previous polish commit; this branch continues from that commit.

## Re-run

\`\`\`bash
npm install playwright
npx playwright install chromium
node tools/ux-kill-switch-capture.mjs
\`\`\`
`;
        fs.writeFileSync(path.join(OUT, 'README.md'), readme);
        fs.writeFileSync(path.join(OUT, 'overflow.json'), JSON.stringify(overflow, null, 2));
        fs.writeFileSync(path.join(OUT, 'capture-log.json'), JSON.stringify(log, null, 2));
    }

    await browser.close();
    server.close();
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
