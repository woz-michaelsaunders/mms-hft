#!/usr/bin/env node
/**
 * Headless capture of the operator surfaces this SMP wiring reuses:
 *   - Algo-tab configuration (Save must round-trip REST-set SMP keys)
 *   - GET/POST /api/algo/* is the write surface; no new dashboard fields
 *
 * Renders loading / empty / error / success / disabled at 375, 768 and 1280,
 * measures page-level horizontal overflow, and computes WCAG contrast from
 * live getComputedStyle token values.
 *
 *   node tools/ux-smp-capture.mjs
 *
 * Output: docs/ux-smp/
 */
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { createRequire } from 'node:module';

const require = createRequire(import.meta.url);
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, '..');
const OUT = path.join(ROOT, 'docs', 'ux-smp');
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

const ALGO_STATUS_OFF = {
    config: {
        enabled: false,
        symbol: 'BTC_USDT',
        order_qty: 0.001,
        min_spread_bps: 5,
        max_position: 0.01,
        poll_interval_ms: 1000,
        time_in_force: '3',
        strategy: 'spread_capture',
        self_match_prevention_scope: null,
        self_match_prevention_instruction: null,
        self_match_prevention_id: null,
    },
    signals_fired: 0,
    orders_placed: 0,
    paper_mode: true,
    recent_signals: [],
};

const ALGO_STATUS_ON = {
    ...ALGO_STATUS_OFF,
    config: {
        ...ALGO_STATUS_OFF.config,
        enabled: true,
        self_match_prevention_scope: 'S',
        self_match_prevention_instruction: '1',
        self_match_prevention_id: '42',
    },
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

async function installApiMocks(page, scenario) {
    await page.route('**/*', async (route) => {
        const req = route.request();
        const u = new URL(req.url());
        const method = req.method();
        const p = u.pathname;

        if (p === '/api/algo/status') {
            if (scenario === 'loading' || scenario === 'disabled') {
                return; // hang — loading banner, form + Save disabled
            }
            if (scenario === 'error') {
                return route.fulfill({
                    status: 500,
                    contentType: 'application/json',
                    body: JSON.stringify({ error: 'upstream' }),
                });
            }
            if (scenario === 'success' || scenario === 'save-success' || scenario === 'save-error') {
                return route.fulfill({
                    status: 200,
                    contentType: 'application/json',
                    body: JSON.stringify(ALGO_STATUS_ON),
                });
            }
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify(ALGO_STATUS_OFF),
            });
        }
        if (p === '/api/algo/config' && method === 'POST') {
            if (scenario === 'save-error') {
                return route.fulfill({
                    status: 400,
                    contentType: 'application/json',
                    body: JSON.stringify({ error: 'malformed' }),
                });
            }
            if (scenario === 'save-success') {
                return route.fulfill({
                    status: 200,
                    contentType: 'application/json',
                    body: JSON.stringify({ ok: true }),
                });
            }
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify({ ok: true }),
            });
        }
        if (p === '/api/algo/enable' && method === 'POST') {
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify({ ok: true, enabled: true }),
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
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify({ halted: false }),
            });
        }
        if (p === '/api/positions') {
            return route.fulfill({
                status: 200,
                contentType: 'application/json',
                body: JSON.stringify({ positions: [] }),
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

    if (scenario === 'loading' || scenario === 'disabled') {
        await page.waitForFunction(() => {
            const t = document.querySelector('#algoStatusBanner .house-banner-title');
            return t && t.textContent === 'Loading algorithm status';
        });
        await page.waitForFunction(
            () => document.getElementById('algoSaveBtn')?.disabled === true
                && document.getElementById('algoEnableBtn')?.disabled === true
        );
        return;
    }
    if (scenario === 'error') {
        await page.waitForFunction(() => {
            const t = document.querySelector('#algoStatusBanner .house-banner-title');
            return t && t.textContent === 'Cannot load algorithm status';
        });
        return;
    }
    if (scenario === 'empty') {
        await page.waitForFunction(() => {
            const t = document.querySelector('#algoStatusBanner .house-banner-title');
            return t && t.textContent === 'Spread capture is off';
        });
        await page.waitForFunction(
            () => document.getElementById('algoEnableBtn')?.disabled === false
                && document.getElementById('algoSaveBtn')?.disabled === false
        );
        return;
    }
    if (scenario === 'success') {
        await page.waitForFunction(() => {
            const t = document.querySelector('#algoStatusBanner .house-banner-title');
            return t && t.textContent === 'Spread capture is on';
        });
        return;
    }
    if (scenario === 'save-success') {
        await page.waitForFunction(() => {
            const t = document.querySelector('#algoStatusBanner .house-banner-title');
            return t && t.textContent === 'Spread capture is on';
        });
        await page.locator('#algoSaveBtn').click();
        await page.waitForFunction(() => {
            const t = document.querySelector('#algoStatusBanner .house-banner-title');
            const inline = document.getElementById('algoCfgMsg');
            return t && t.textContent === 'Configuration saved'
                && inline && inline.textContent === 'Configuration saved.';
        });
        return;
    }
    if (scenario === 'save-error') {
        await page.waitForFunction(() => {
            const t = document.querySelector('#algoStatusBanner .house-banner-title');
            return t && t.textContent === 'Spread capture is on';
        });
        await page.locator('#algoSaveBtn').click();
        await page.waitForFunction(() => {
            const t = document.querySelector('#algoStatusBanner .house-banner-title');
            const inline = document.getElementById('algoCfgMsg');
            return t && t.textContent === 'Cannot save configuration'
                && inline && inline.textContent.startsWith('Cannot save configuration:');
        });
    }
}

async function shotClip(page, dest, startSel) {
    const el = page.locator(startSel);
    await el.scrollIntoViewIfNeeded();
    await page.waitForTimeout(80);
    // Full tab, not a viewport slice — the configuration form is the SMP surface.
    await el.screenshot({ path: dest });
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
                sample('#algoEnableBtn'),
                sample('#algoDisableBtn'),
                sample('#algoSaveBtn'),
                sample('#algoStatusBanner'),
                sample('#algoStatusBanner .house-banner-title'),
                sample('.house-field > span'),
                sample('#algoCfgSymbol'),
                sample('#algoCfgTIF'),
                sample('#algoCfgMsg'),
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
        ['--success', '--success-bg', 'success banner (on / saved)'],
        ['--info', '--info-bg', 'loading banner'],
        ['--warning', '--warning-bg', 'Spread capture is off banner'],
        ['--accent-contrast', '--accent', 'Enable algorithm primary button'],
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

async function main() {
    fs.mkdirSync(SHOTS, { recursive: true });
    const { server, port } = await startServer();
    const base = `http://127.0.0.1:${port}`;
    const chromeCandidates = [
        process.env.PLAYWRIGHT_CHROME,
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
        { id: 'loading', file: 'loading', label: 'Loading — Loading algorithm status; form and Save disabled' },
        { id: 'empty', file: 'empty', label: 'Empty / off — Spread capture is off; Enable is the primary action' },
        { id: 'error', file: 'error', label: 'Error — Cannot load algorithm status; Save blocked so cache cannot clobber SMP' },
        { id: 'success', file: 'success', label: 'Success / on — Spread capture is on; SMP keys cached, not shown as fields' },
        { id: 'disabled', file: 'disabled', label: 'Disabled — Save and Enable off while status is in flight' },
        { id: 'save-success', file: 'save-success', label: 'Save success — Configuration saved; Object.assign round-trips SMP keys' },
        { id: 'save-error', file: 'save-error', label: 'Save error — Cannot save configuration' },
    ];

    const overflow = [];
    const log = [];
    let contrastDark = null;
    const panel = '#tab-algo';

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
            const file = `${sc.file}-${vp.name}.png`;
            const dest = path.join(SHOTS, file);
            await shotClip(page, dest, panel);
            const ov = await measureOverflow(page);
            overflow.push({ width: vp.name, state: sc.file, ...ov, file });
            log.push({ file, label: sc.label, width: vp.name, overflowing: ov.overflowing });
            if (vp.name === '1280' && sc.id === 'empty') {
                contrastDark = scoreSamples(await sampleContrast(page));
                await page.locator('#algoEnableBtn').focus();
                await page.waitForTimeout(80);
                const btnBox = await page.locator('#algoEnableBtn').boundingBox();
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
        await openAlgo(page, base, 'empty');
        const contrastLight = scoreSamples(await sampleContrast(page));
        await shotClip(page, path.join(SHOTS, 'empty-1280-light.png'), panel);
        await context.close();

        const overflowFail = overflow.filter((o) => o.overflowing);
        const darkFail = contrastDark.token_pairs.filter((p) => !p.pass_aa);
        const lightFail = contrastLight.token_pairs.filter((p) => !p.pass_aa);
        const liveFail = [...contrastDark.live_elements, ...contrastLight.live_elements]
            .filter((p) => !p.pass_aa && !p.disabled);
        const overflowSummary = overflow
            .map((o) => `${o.state}@${o.width}: scrollWidth=${o.documentElement.scrollWidth} clientWidth=${o.documentElement.clientWidth} overflowing=${o.overflowing}`)
            .join('\n');

        const readme = `# Self-match prevention — UI evidence

Generated by \`tools/ux-smp-capture.mjs\` against \`src/dashboard.html\`.
This feature **does not edit the dashboard**. Operators set and inspect SMP
through \`POST /api/algo/config\` and \`GET /api/algo/status\`. The Algo-tab
Save path must keep \`Object.assign({}, _algoCfgCache, { form fields })\` so
REST-armed tags 12362 / 2964 / 2362 are not wiped. Operator copy for the
REST home lives in \`config.example.toml\`.

Do not treat these screenshots as a licence to add SMP inputs.

## Screens in scope

| Surface | Role | New chrome? |
|---|---|---|
| Algo tab — configuration | Existing Spread-capture form. Save round-trips SMP via \`_algoCfgCache\` | No |
| \`GET /api/algo/status\` / \`POST /api/algo/config\` | Set and inspect the three optional strings | No (JSON) |
| \`config.example.toml\` comments | REST home + inactive-gate copy | Copy polish only |

Whole-app chrome (Live, History, Orders, Positions, Backtest, Securities, header, Kill Switch, Shadow mode, native menus): **N/A**.

## Algo-tab states

| State | What you should see | Files |
|---|---|---|
| Loading | Banner "Loading algorithm status"; form and Save disabled | \`loading-375.png\` \`loading-768.png\` \`loading-1280.png\` |
| Empty / off | Banner "Spread capture is off"; Enable is the primary action | \`empty-375.png\` \`empty-768.png\` \`empty-1280.png\` |
| Error | Banner "Cannot load algorithm status"; Save blocked (cannot clobber SMP) | \`error-375.png\` \`error-768.png\` \`error-1280.png\` |
| Success / on | Banner "Spread capture is on"; no SMP fieldset | \`success-375.png\` \`success-768.png\` \`success-1280.png\` |
| Disabled | Save and Enable off while status is in flight | \`disabled-375.png\` \`disabled-768.png\` \`disabled-1280.png\` |
| Save success | Inline "Configuration saved." plus banner "The strategy will use these values on the next quote." | \`save-success-375.png\` \`save-success-768.png\` \`save-success-1280.png\` |
| Save error | Inline and banner "Cannot save configuration" | \`save-error-375.png\` \`save-error-768.png\` \`save-error-1280.png\` |

Also: \`focus-ring-1280.png\` (keyboard \`:focus-visible\` on Enable algorithm),
\`empty-1280-light.png\` (light scheme).

No Algo-tab inputs for scope / instruction / id — locked by UI Design and ADR-009.

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
copy for the REST SMP home. No placeholder or lorem. Dashboard markup, CSS,
and JS are unchanged — including \`algoSaveConfig\`'s full-config merge.

## Re-run

\`\`\`bash
node tools/ux-smp-capture.mjs
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
