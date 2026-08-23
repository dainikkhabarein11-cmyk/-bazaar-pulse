/**
 * Bazaar Pulse — RSS Fetcher
 * ---------------------------------
 * Pulls live headlines from Indian financial news RSS feeds,
 * normalizes them into a common shape, dedupes near-identical
 * stories, and writes a news.json the dashboard can read.
 *
 * Run on your own machine / server (needs open internet access —
 * this will NOT work inside a sandboxed AI code environment).
 *
 * Setup:
 *   npm init -y
 *   npm install rss-parser express cors node-cron
 *   node rss-fetcher.js
 */

const Parser = require('rss-parser');
const fs = require('fs');
const express = require('express');
const cors = require('cors');
const cron = require('node-cron');

const parser = new Parser({
  timeout: 10000,
  headers: {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
    'Accept': 'application/rss+xml, application/xml, text/xml, */*',
    'Accept-Language': 'en-IN,en;q=0.9',
    'Referer': 'https://www.google.com/',
  }
});

// Some feeds contain a raw, unescaped "&" (should be "&amp;") which breaks
// strict XML parsing. This fixes stray "&" that aren't already part of a
// valid entity like &amp; &lt; &gt; &quot; &apos; &#123;
function sanitizeXml(xml) {
  return xml.replace(/&(?!amp;|lt;|gt;|quot;|apos;|#\d+;|#x[0-9a-fA-F]+;)/g, '&amp;');
}

// Fetch raw text ourselves (so we can sanitize), trying each candidate URL
// for a source in order until one succeeds.
async function fetchFeed(source) {
  let lastErr;
  for (const url of source.urls) {
    try {
      const res = await fetch(url, {
        headers: parser.options.headers,
        signal: AbortSignal.timeout(10000),
        cache: 'no-store',
      });
      if (!res.ok) throw new Error(`Status code ${res.status}`);
      const raw = await res.text();
      const clean = sanitizeXml(raw);
      return await parser.parseString(clean);
    } catch (err) {
      lastErr = err;
      console.error(`  ...tried ${url} → ${err.message}`);
    }
  }
  throw lastErr;
}

// ---- Add / remove sources here. Each source lists one or more candidate
// ---- feed URLs — some publishers run multiple feed paths, or move them
// ---- over time, so we try each in order until one works.
//
// Moneycontrol in particular runs separate feeds per section — that's how
// you get their deeper analysis/opinion pieces and earnings coverage
// instead of just top-line headlines, so it's listed multiple times below
// with a different category each time.
const SOURCES = [
  { name: 'Economic Times', category: 'Markets', urls: [
      'https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms',
  ]},
  { name: 'Economic Times', category: 'Stocks', urls: [
      'https://economictimes.indiatimes.com/markets/stocks/rssfeeds/2146842.cms',
  ]},
  { name: 'Economic Times', category: 'Policy', urls: [
      'https://economictimes.indiatimes.com/news/economy/rssfeeds/1373380680.cms',
  ]},

  { name: 'Moneycontrol', category: 'Markets', urls: [
      'https://www.moneycontrol.com/rss/marketreports.xml',
      'https://www.moneycontrol.com/rss/latestnews.xml',
  ]},
  { name: 'Moneycontrol', category: 'Stocks', urls: [
      'https://www.moneycontrol.com/rss/buzzingstocks.xml',
  ]},
  { name: 'Moneycontrol', category: 'Earnings', urls: [
      'https://www.moneycontrol.com/rss/results.xml',
  ]},
  { name: 'Moneycontrol', category: 'Policy', urls: [
      'https://www.moneycontrol.com/rss/economy.xml',
  ]},

  { name: 'Business Standard', category: 'Markets', urls: [
      'https://www.business-standard.com/rss/markets-106.rss',
      'https://www.business-standard.com/rss/latest.rss',
  ]},
  { name: 'Business Standard', category: 'IPO', urls: [
      'https://www.business-standard.com/rss/markets/ipo-10618.rss',
  ]},
  { name: 'Business Standard', category: 'Policy', urls: [
      'https://www.business-standard.com/rss/economy-policy-102.rss',
  ]},

  { name: 'Mint', category: 'Markets', urls: [
      'https://www.livemint.com/rss/markets',
  ]},
  { name: 'Mint', category: 'Analysis', urls: [
      'https://www.livemint.com/rss/opinion',
  ]},
  { name: 'Mint', category: 'Policy', urls: [
      'https://www.livemint.com/rss/economy',
  ]},

  { name: 'Financial Express', category: 'Markets', urls: [
      'https://www.financialexpress.com/market/feed/',
  ]},
  { name: 'Financial Express', category: 'IPO', urls: [
      'https://www.financialexpress.com/market/ipo-news/feed/',
  ]},

  { name: 'CNBC-TV18', category: 'Markets', urls: [
      'https://www.cnbctv18.com/commonfeeds/v1/cne/rss/market.xml',
  ]},
  { name: 'CNBC-TV18', category: 'Earnings', urls: [
      'https://www.cnbctv18.com/commonfeeds/v1/cne/rss/earnings.xml',
  ]},

  { name: 'NDTV Profit', category: 'Markets', urls: [
      'https://www.ndtvprofit.com/stories.rss',
  ]},

  { name: 'Business Today', category: 'Markets', urls: [
      'https://www.businesstoday.in/rssfeeds/?id=225346',
  ]},

  { name: 'The Hindu BusinessLine', category: 'Markets', urls: [
      'https://www.thehindubusinessline.com/markets/feeder/default.rss',
  ]},
];

let cache = [];

// Simple similarity check to dedupe the same story covered by multiple outlets
function isSimilar(a, b) {
  const na = a.toLowerCase().replace(/[^a-z0-9 ]/g, '');
  const nb = b.toLowerCase().replace(/[^a-z0-9 ]/g, '');
  const wa = new Set(na.split(' ').filter(w => w.length > 3));
  const wb = new Set(nb.split(' ').filter(w => w.length > 3));
  const overlap = [...wa].filter(w => wb.has(w)).length;
  return overlap / Math.max(wa.size, wb.size, 1) > 0.75;
}

// ============================================================
// Live market data (indices + top movers) — via Yahoo Finance's
// free public quote endpoint. No API key needed. This is an
// unofficial feed (Yahoo doesn't publish it as a stable public
// API), so if it ever stops working, treat the symbols/URL below
// as the first thing to check or swap out.
// ============================================================

const INDEX_SYMBOLS = [
  { symbol: '^BSESN',   name: 'SENSEX' },
  { symbol: '^NSEI',    name: 'NIFTY 50' },
  { symbol: '^NSEBANK', name: 'BANK NIFTY' },
  { symbol: '^CNXIT',   name: 'NIFTY IT' },
  { symbol: 'INR=X',    name: 'USD/INR' },
];

// A basket of liquid large-cap NSE stocks to pull for the "Top Movers" panel.
const WATCHLIST_SYMBOLS = [
  'RELIANCE.NS', 'TCS.NS', 'HDFCBANK.NS', 'INFY.NS', 'ICICIBANK.NS',
  'TATAMOTORS.NS', 'ADANIPORTS.NS', 'ITC.NS', 'LT.NS', 'SBIN.NS',
  'HINDUNILVR.NS', 'KOTAKBANK.NS', 'BHARTIARTL.NS', 'AXISBANK.NS', 'MARUTI.NS',
];

async function fetchQuote(symbol) {
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}`;
  const res = await fetch(url, {
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
      'Accept': 'application/json',
    },
    signal: AbortSignal.timeout(8000),
    cache: 'no-store',
  });
  if (!res.ok) throw new Error(`Status code ${res.status}`);
  const data = await res.json();
  const meta = data?.chart?.result?.[0]?.meta;
  if (!meta || meta.regularMarketPrice == null) throw new Error('No price data');
  const price = meta.regularMarketPrice;
  const prevClose = meta.previousClose ?? meta.chartPreviousClose;
  const change = prevClose ? price - prevClose : 0;
  const pct = prevClose ? (change / prevClose) * 100 : 0;
  return { price, change, pct, up: change >= 0 };
}

let marketCache = { indices: [], movers: [], lastUpdated: null };

async function fetchMarketData() {
  const indices = [];
  for (const { symbol, name } of INDEX_SYMBOLS) {
    try {
      const q = await fetchQuote(symbol);
      indices.push({
        name,
        val: name === 'USD/INR'
          ? q.price.toFixed(2)
          : q.price.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 }),
        chg: (q.up ? '+' : '') + q.change.toFixed(2),
        pct: (q.up ? '+' : '') + q.pct.toFixed(2) + '%',
        up: q.up,
      });
    } catch (err) {
      console.error(`✗ Index ${name} (${symbol}) failed: ${err.message}`);
    }
  }

  const movers = [];
  for (const symbol of WATCHLIST_SYMBOLS) {
    try {
      const q = await fetchQuote(symbol);
      movers.push({
        name: symbol.replace('.NS', ''),
        pct: q.pct,
        val: (q.up ? '+' : '') + q.pct.toFixed(2) + '%',
        up: q.up,
      });
    } catch (err) {
      console.error(`✗ Stock ${symbol} failed: ${err.message}`);
    }
  }
  // Sort by absolute move, biggest movers first
  movers.sort((a, b) => Math.abs(b.pct) - Math.abs(a.pct));

  marketCache = { indices, movers: movers.slice(0, 8), lastUpdated: new Date().toISOString() };
  console.log(`[${marketCache.lastUpdated}] Market data: ${indices.length} indices, ${movers.length} stocks`);
}

// ============================================================
// News fetching (RSS)
// ============================================================

async function fetchAll() {
  const results = [];

  for (const source of SOURCES) {
    try {
      const feed = await fetchFeed(source);
      for (const item of feed.items.slice(0, 20)) {
        results.push({
          source: source.name,
          category: source.category,
          headline: item.title,
          snippet: (item.contentSnippet || item.content || '').slice(0, 400),
          url: item.link,
          publishedAt: item.isoDate || item.pubDate || new Date().toISOString(),
        });
      }
      console.log(`✓ ${source.name}: ${feed.items.length} items`);
    } catch (err) {
      console.error(`✗ ${source.name} failed: ${err.message}`);
    }
  }

  // Drop anything older than 5 days (or with a broken/unparseable date) —
  // some section feeds return evergreen or stale-dated content, so this
  // keeps the dashboard genuinely current regardless of source quirks.
  const FIVE_DAYS_MS = 5 * 24 * 60 * 60 * 1000;
  const now = Date.now();
  const fresh = results.filter(item => {
    const t = new Date(item.publishedAt).getTime();
    return !isNaN(t) && (now - t) <= FIVE_DAYS_MS && t <= now + 60000; // also drop bogus future dates
  });

  console.log(`Fetched ${results.length} total, ${results.length - fresh.length} dropped as stale/invalid (>5 days old)`);

  // Dedupe near-identical headlines across sources
  const deduped = [];
  for (const item of fresh) {
    const dup = deduped.find(d => isSimilar(d.headline, item.headline));
    if (!dup) deduped.push(item);
  }

  // Sort newest first
  deduped.sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt));

  cache = deduped;
  lastUpdated = new Date().toISOString();
  fs.writeFileSync('news.json', JSON.stringify(deduped, null, 2));
  console.log(`[${lastUpdated}] Saved ${deduped.length} deduped stories → news.json`);
}

// ---- Serve as an API the dashboard can fetch from ----
const app = express();
app.use(cors());
app.use((req, res, next) => {
  res.set('Cache-Control', 'no-store, no-cache, must-revalidate');
  next();
});

let lastUpdated = null;

app.get('/api/news', (req, res) => res.json({ lastUpdated, items: cache }));
app.get('/api/news/:category', (req, res) => {
  const cat = req.params.category;
  res.json({ lastUpdated, items: cache.filter(n => n.category.toLowerCase() === cat.toLowerCase()) });
});
app.get('/api/markets', (req, res) => res.json(marketCache));
// Hit this in your browser any time to force an immediate re-fetch (handy while testing)
app.get('/api/refresh', async (req, res) => {
  await fetchAll();
  res.json({ lastUpdated, count: cache.length });
});

const PORT = process.env.PORT || 4000;
app.listen(PORT, () => console.log(`API running at http://localhost:${PORT}/api/news`));

// ---- Fetch news every 5 minutes, market data every 2 minutes (prices move faster than headlines) ----
fetchAll();
fetchMarketData();
cron.schedule('*/5 * * * *', fetchAll);
cron.schedule('*/2 * * * *', fetchMarketData);
