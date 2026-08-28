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

// Gold/silver: Yahoo only gives international spot price (USD/troy oz).
// We convert to INR per 10g (gold) and per kg (silver) using the live
// USD/INR rate, which approximates — but won't exactly match — MCX prices,
// since MCX also bakes in import duty, GST, and local premium.
const TROY_OUNCE_GRAMS = 31.1035;
const METAL_SYMBOLS = [
  { symbol: 'GC=F', name: 'GOLD (10g)',  unitGrams: 10 },
  { symbol: 'SI=F', name: 'SILVER (1kg)', unitGrams: 1000 },
];

// A broad basket of NSE stocks spanning large/mid/small cap, each tagged
// with an approximate market cap (in ₹ crore). These figures are static
// snapshots, not live — good enough for the >5,000cr filter and for
// labeling a stock Large/Mid/Small cap, but they will drift out of date
// over time and should be refreshed periodically for accuracy.
const WATCHLIST = [
  // Large cap (~₹2,00,000cr+)
  { symbol:'RELIANCE.NS', mcapCr:1900000 }, { symbol:'TCS.NS', mcapCr:1400000 },
  { symbol:'HDFCBANK.NS', mcapCr:1200000 }, { symbol:'ICICIBANK.NS', mcapCr:800000 },
  { symbol:'INFY.NS', mcapCr:600000 }, { symbol:'BHARTIARTL.NS', mcapCr:900000 },
  { symbol:'SBIN.NS', mcapCr:700000 }, { symbol:'ITC.NS', mcapCr:550000 },
  { symbol:'HINDUNILVR.NS', mcapCr:550000 }, { symbol:'LT.NS', mcapCr:500000 },
  { symbol:'BAJFINANCE.NS', mcapCr:450000 }, { symbol:'KOTAKBANK.NS', mcapCr:350000 },
  { symbol:'MARUTI.NS', mcapCr:350000 }, { symbol:'AXISBANK.NS', mcapCr:350000 },
  { symbol:'ASIANPAINT.NS', mcapCr:280000 }, { symbol:'ADANIENT.NS', mcapCr:300000 },
  { symbol:'ADANIPORTS.NS', mcapCr:280000 }, { symbol:'ULTRACEMCO.NS', mcapCr:270000 },
  { symbol:'SUNPHARMA.NS', mcapCr:370000 }, { symbol:'TITAN.NS', mcapCr:300000 },
  { symbol:'NTPC.NS', mcapCr:330000 }, { symbol:'POWERGRID.NS', mcapCr:270000 },
  { symbol:'NESTLEIND.NS', mcapCr:230000 }, { symbol:'WIPRO.NS', mcapCr:250000 },
  { symbol:'HCLTECH.NS', mcapCr:430000 }, { symbol:'M&M.NS', mcapCr:300000 },
  { symbol:'TATASTEEL.NS', mcapCr:200000 }, { symbol:'JSWSTEEL.NS', mcapCr:220000 },
  { symbol:'ONGC.NS', mcapCr:270000 }, { symbol:'COALINDIA.NS', mcapCr:250000 },
  { symbol:'TATAMOTORS.NS', mcapCr:300000 },

  // Mid cap (~₹20,000cr – ₹2,00,000cr)
  { symbol:'FEDERALBNK.NS', mcapCr:45000 }, { symbol:'IDFCFIRSTB.NS', mcapCr:65000 },
  { symbol:'AUBANK.NS', mcapCr:48000 }, { symbol:'PERSISTENT.NS', mcapCr:75000 },
  { symbol:'COFORGE.NS', mcapCr:50000 }, { symbol:'MUTHOOTFIN.NS', mcapCr:85000 },
  { symbol:'LUPIN.NS', mcapCr:90000 }, { symbol:'BIOCON.NS', mcapCr:35000 },
  { symbol:'TORNTPHARM.NS', mcapCr:115000 }, { symbol:'TVSMOTOR.NS', mcapCr:110000 },
  { symbol:'ESCORTS.NS', mcapCr:40000 }, { symbol:'TRENT.NS', mcapCr:150000 },
  { symbol:'DIXON.NS', mcapCr:55000 }, { symbol:'POLYCAB.NS', mcapCr:90000 },
  { symbol:'CUMMINSIND.NS', mcapCr:95000 }, { symbol:'PAGEIND.NS', mcapCr:55000 },
  { symbol:'INDHOTEL.NS', mcapCr:85000 }, { symbol:'GODREJPROP.NS', mcapCr:65000 },
  { symbol:'OBEROIRLTY.NS', mcapCr:65000 }, { symbol:'CDSL.NS', mcapCr:22000 },

  // Small cap (~₹5,000cr – ₹20,000cr)
  { symbol:'GRANULES.NS', mcapCr:9000 }, { symbol:'JBCHEPHARM.NS', mcapCr:15000 },
  { symbol:'RATNAMANI.NS', mcapCr:18000 }, { symbol:'FINEORG.NS', mcapCr:14000 },
  { symbol:'CENTURYPLY.NS', mcapCr:7500 }, { symbol:'GESHIP.NS', mcapCr:9000 },
  { symbol:'TRIVENI.NS', mcapCr:5500 },
];

function capLabel(mcapCr){
  if (mcapCr >= 200000) return 'Large Cap';
  if (mcapCr >= 20000) return 'Mid Cap';
  return 'Small Cap';
}

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
  return { price, change, pct, prevClose, up: change >= 0 };
}

let marketCache = { indices: [], movers: { gainers: [], losers: [] }, lastUpdated: null };

async function fetchMarketData() {
  const indices = [];
  let usdInrQuote = null;
  for (const { symbol, name } of INDEX_SYMBOLS) {
    try {
      const q = await fetchQuote(symbol);
      if (name === 'USD/INR') usdInrQuote = q;
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

  // Gold & silver, converted to INR per 10g / per kg. To match how a real
  // MCX-style INR price moves, we combine BOTH the commodity's USD price
  // change AND the rupee's change against the dollar that day — using
  // today's and yesterday's price for each and computing the INR value at
  // both points, rather than just re-stating the USD % move.
  if (usdInrQuote) {
    const rateNow = usdInrQuote.price;
    const ratePrev = usdInrQuote.prevClose;
    for (const { symbol, name, unitGrams } of METAL_SYMBOLS) {
      try {
        const q = await fetchQuote(symbol); // USD per troy ounce
        const priceInrNow = (q.price / TROY_OUNCE_GRAMS) * unitGrams * rateNow;
        const priceInrPrev = (q.prevClose / TROY_OUNCE_GRAMS) * unitGrams * ratePrev;
        const changeInr = priceInrNow - priceInrPrev;
        const pctInr = (changeInr / priceInrPrev) * 100;
        const up = changeInr >= 0;
        indices.push({
          name,
          val: '₹' + priceInrNow.toLocaleString('en-IN', { maximumFractionDigits: 0 }),
          chg: (up ? '+' : '') + Math.round(changeInr).toLocaleString('en-IN'),
          pct: (up ? '+' : '') + pctInr.toFixed(2) + '%',
          up,
        });
      } catch (err) {
        console.error(`✗ Metal ${name} (${symbol}) failed: ${err.message}`);
      }
    }
  }

  const candidates = [];
  for (const { symbol, mcapCr } of WATCHLIST) {
    try {
      const q = await fetchQuote(symbol);
      // Filter: price > ₹100 and market cap > ₹5,000cr
      if (q.price <= 100 || mcapCr <= 5000) continue;
      candidates.push({
        name: symbol.replace('.NS', ''),
        price: q.price,
        pct: q.pct,
        val: (q.up ? '+' : '') + q.pct.toFixed(2) + '%',
        up: q.up,
        cap: capLabel(mcapCr),
      });
    } catch (err) {
      console.error(`✗ Stock ${symbol} failed: ${err.message}`);
    }
  }

  const gainers = [...candidates].sort((a, b) => b.pct - a.pct).slice(0, 8);
  const losers = [...candidates].sort((a, b) => a.pct - b.pct).slice(0, 8);

  marketCache = { indices, movers: { gainers, losers }, lastUpdated: new Date().toISOString() };
  console.log(`[${marketCache.lastUpdated}] Market data: ${indices.length} indices, ${candidates.length} eligible stocks (${gainers.length} gainers, ${losers.length} losers)`);
}

// ============================================================
// AI Summarizer — one-line summary + market impact read, via
// OpenRouter (OpenAI-compatible endpoint, works with many models
// including free ones). Set OPENROUTER_API_KEY as an environment
// variable on Render. Results are cached by article URL so the same
// story isn't re-summarized (and re-billed) every fetch cycle.
//
// Note: this cache lives in memory only, so it resets whenever Render
// restarts your service (e.g. after the free tier sleeps). That means
// occasional re-summarization bursts on cold starts — fine for a
// prototype, but worth knowing if API cost ever matters to you.
// ============================================================

const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY;
const OPENROUTER_MODEL = process.env.OPENROUTER_MODEL || 'google/gemma-4-31b-it:free';
const MAX_NEW_SUMMARIES_PER_CYCLE = 15; // keeps API cost/time bounded each run

const summaryCache = new Map(); // url -> { summary, impact, affected, note }

async function summarizeArticle(item) {
  const systemPrompt = `You are a financial news analyst. Given a headline and short snippet from Indian stock market news, respond with ONLY a raw JSON object — no markdown, no code fences, no explanation — in exactly this shape:
{"summary":"one plain sentence under 22 words summarizing what happened","impact":"Bullish"|"Bearish"|"Neutral"|"Mixed","affected":["up to 3 short stock tickers or sector names most relevant, or empty array"],"note":"one short, measured sentence under 20 words on the likely read — this is an interpretation, not investment advice, so avoid definitive predictions"}`;

  const userPrompt = `Headline: ${item.headline}\nSnippet: ${item.snippet}`;

  const res = await fetch('https://openrouter.ai/api/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${OPENROUTER_API_KEY}`,
      'Content-Type': 'application/json',
      'HTTP-Referer': 'https://bazaar-pulse.onrender.com',
      'X-Title': 'BazaarPulse',
    },
    body: JSON.stringify({
      model: OPENROUTER_MODEL,
      max_tokens: 200,
      temperature: 0.3,
      messages: [
        { role: 'system', content: systemPrompt },
        { role: 'user', content: userPrompt },
      ],
    }),
    signal: AbortSignal.timeout(20000),
  });

  if (!res.ok) throw new Error(`OpenRouter status ${res.status}`);
  const data = await res.json();
  const raw = data?.choices?.[0]?.message?.content?.trim();
  if (!raw) throw new Error('Empty response');

  // Models occasionally wrap JSON in code fences despite instructions — strip if present
  const cleaned = raw.replace(/^```json\s*|^```\s*|```$/g, '').trim();
  const parsed = JSON.parse(cleaned);
  if (!parsed.summary || !parsed.impact) throw new Error('Malformed JSON shape');
  return parsed;
}

async function enrichWithSummaries(items) {
  if (!OPENROUTER_API_KEY) {
    console.log('⚠ OPENROUTER_API_KEY not set — skipping AI summaries');
    return items;
  }

  // Only summarize items we haven't already cached, newest first, capped per cycle.
  // Kept smaller now that requests run one at a time to respect free-tier rate limits.
  const toSummarize = items
    .filter(item => !summaryCache.has(item.url))
    .slice(0, MAX_NEW_SUMMARIES_PER_CYCLE);

  const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
  const REQUEST_GAP_MS = 4500; // ~13 requests/min — safely under typical free-tier caps

  let summarized = 0, failed = 0;
  for (const item of toSummarize) {
    try {
      const result = await summarizeArticle(item);
      summaryCache.set(item.url, result);
      summarized++;
    } catch (err) {
      if (err.message.includes('429')) {
        // Rate limited — wait longer, then try this one item once more
        await sleep(15000);
        try {
          const retryResult = await summarizeArticle(item);
          summaryCache.set(item.url, retryResult);
          summarized++;
        } catch (retryErr) {
          console.error(`✗ Summarize failed (after retry) for "${item.headline.slice(0,50)}...": ${retryErr.message}`);
          failed++;
        }
      } else {
        console.error(`✗ Summarize failed for "${item.headline.slice(0,50)}...": ${err.message}`);
        failed++;
      }
    }
    await sleep(REQUEST_GAP_MS);
  }
  if (toSummarize.length > 0) {
    console.log(`AI summaries: ${summarized} generated, ${failed} failed, ${summaryCache.size} total cached`);
  }

  // Attach cached summary (if any) to every item in the list
  return items.map(item => {
    const ai = summaryCache.get(item.url);
    return ai ? { ...item, aiSummary: ai.summary, aiImpact: ai.impact, aiAffected: ai.affected, aiNote: ai.note } : item;
  });
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

  // Attach AI summary + impact read to as many recent stories as the per-cycle cap allows
  const enriched = await enrichWithSummaries(deduped);

  cache = enriched;
  lastUpdated = new Date().toISOString();
  fs.writeFileSync('news.json', JSON.stringify(enriched, null, 2));
  console.log(`[${lastUpdated}] Saved ${enriched.length} deduped stories → news.json`);
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
