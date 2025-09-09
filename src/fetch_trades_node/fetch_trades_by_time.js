// src/fetch_trades_node/fetch_trades_by_time.js
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const https = require('https');
const cliProgress = require('cli-progress');

/**
 * Usage:
 *   node src/fetch_trades_node/fetch_trades_by_time.js <PAIR> <START_ISO> <END_ISO> [MAX_PAGES]
 * Example:
 *   node src/fetch_trades_node/fetch_trades_by_time.js BTC-USD 2025-09-07T00:00:00Z 2025-09-08T00:00:00Z
 */

const PAIR = process.argv[2] || 'BTC-USD';
const START_ISO = process.argv[3];
const END_ISO = process.argv[4];
const MAX_PAGES = Number(process.argv[5] || 0); // 0 = no limit

if (!START_ISO || !END_ISO) {
  console.error('Usage: node src/fetch_trades_node/fetch_trades_by_time.js <PAIR> <START_ISO> <END_ISO> [MAX_PAGES]');
  process.exit(1);
}

const startTs = Date.parse(START_ISO);
const endTs = Date.parse(END_ISO);
if (!Number.isFinite(startTs) || !Number.isFinite(endTs) || !(startTs < endTs)) {
  console.error('Invalid START_ISO/END_ISO range');
  process.exit(1);
}

const OUTDIR = path.join(__dirname, '../../data/raw');
fs.mkdirSync(OUTDIR, { recursive: true });
const outName = `${PAIR}_${START_ISO.slice(0,10)}_to_${END_ISO.slice(0,10)}.jsonl`;
const outPath = path.join(OUTDIR, outName);
const out = fs.createWriteStream(outPath, { flags: 'w' });

// Force IPv4 & keep-alive (can help in some environments)
const httpsAgent = new https.Agent({ keepAlive: true, family: 4 });

const api = axios.create({
  baseURL: 'https://api.exchange.coinbase.com',
  timeout: 15000,
  httpsAgent,
  headers: {
    Accept: 'application/json',
    'User-Agent': 'whale-signal/1.0',
  },
});

const PAGE_LIMIT = 1000; // Exchange REST allows up to 1000
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function getTrades(params, attempt = 1) {
  try {
    const resp = await api.get(`/products/${PAIR}/trades`, { params, validateStatus: () => true });
    if (resp.status !== 200) {
      if ((resp.status === 429 || (resp.status >= 500 && resp.status < 600)) && attempt <= 5) {
        const backoff = 300 * attempt;
        console.warn(`HTTP ${resp.status} — retrying in ${backoff}ms (attempt ${attempt})`);
        await sleep(backoff);
        return getTrades(params, attempt + 1);
      }
      throw new Error(resp.data?.message || `HTTP ${resp.status}`);
    }
    return {
      data: resp.data,
      after: resp.headers['cb-after'] || null,   // older-page cursor
      before: resp.headers['cb-before'] || null, // newer-page cursor
    };
  } catch (err) {
    if (attempt <= 5) {
      const backoff = 300 * attempt;
      console.warn(`Request error "${err.message}" — retrying in ${backoff}ms (attempt ${attempt})`);
      await sleep(backoff);
      return getTrades(params, attempt + 1);
    }
    throw err;
  }
}

(async () => {
  console.log(`Fetching ${PAIR} trades between ${START_ISO} and ${END_ISO} ...`);

  let page = 0;
  let saved = 0;
  let afterCursor = null; // walk OLDER → OLDER using CB-AFTER

  // Progress bar across the entire walk from first oldestTs → START_ISO
  const bar = new cliProgress.SingleBar(
    {
      format: `Fetch ${PAIR} |{bar}| {percentage}% | page {page} | saved {saved} | oldest {oldest} | ETA {eta_formatted}`,
      hideCursor: true,
      stopOnComplete: true,
      clearOnComplete: true,
    },
    cliProgress.Presets.shades_classic
  );
  bar.start(100, 0, { page: 0, saved: 0, oldest: '-' });

  let baselineOldestTs = null; // first page's oldest timestamp
  const clamp01 = (x) => Math.max(0, Math.min(1, x));

  while (true) {
    page += 1;
    if (MAX_PAGES && page > MAX_PAGES) {
      console.log(`Reached MAX_PAGES=${MAX_PAGES}, stopping.`);
      break;
    }

    const params = { limit: PAGE_LIMIT };
    if (afterCursor) params.after = afterCursor;

    const t0 = Date.now();
    const { data: trades, after, before } = await getTrades(params);
    const dt = Date.now() - t0;

    if (!Array.isArray(trades) || trades.length === 0) {
      console.log('No trades returned; stopping.');
      break;
    }

    // establish baseline (first page's oldest)
    const oldest = trades[trades.length - 1];
    const oldestTs = Date.parse(oldest.time);
    if (baselineOldestTs === null) baselineOldestTs = oldestTs;

    // Save only rows in [start, end)
    let savedThisPage = 0;
    for (const t of trades) {
      const ts = Date.parse(t.time);
      if (ts >= startTs && ts < endTs) {
        out.write(
          JSON.stringify({
            pair: PAIR,
            time: t.time,
            trade_id: Number(t.trade_id),
            price: Number(t.price),
            size: Number(t.size),
            side: t.side,
          }) + '\n'
        );
        saved++;
        savedThisPage++;
      }
    }

    // Progress toward START_ISO across the whole walk:
    // percentage = (baselineOldestTs - oldestTs) / (baselineOldestTs - startTs)
    const denom = Math.max(1, baselineOldestTs - startTs);
    const pct = clamp01((baselineOldestTs - oldestTs) / denom) * 100;

    bar.update(Math.round(pct), {
      page,
      saved,
      oldest: new Date(oldestTs).toISOString(),
    });

    // Verbose line for debugging (optional):
    // console.log(`page ${page} (${dt}ms) | fetched ${trades.length} | saved ${savedThisPage} (total ${saved}) | CB-AFTER=${after ?? '-'} CB-BEFORE=${before ?? '-'} | oldest=${oldest.time}`);

    afterCursor = after; // go older

    // Stop when we've paged older than START
    if (oldestTs < startTs) {
      break;
    }

    await sleep(120); // be gentle
  }

  bar.update(100, { page, saved, oldest: '-' });
  bar.stop();

  out.end();
  console.log(`Done. Wrote ${saved} trades → ${outPath}`);
})().catch((err) => {
  console.error('Fatal error:', err.message);
  try { out.end(); } catch {}
  process.exit(1);
});
