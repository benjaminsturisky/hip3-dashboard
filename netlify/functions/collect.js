const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
const COLLECT_SECRET = process.env.COLLECT_SECRET;

const API_URL = 'https://api.hyperliquid.xyz/info';
const DEPLOYERS = ['xyz', 'hyna', 'cash', 'km', 'flx', 'vntl'];

const COLLATERAL_TOKENS = {
  0: 'USDC',
  235: 'USDE',
  268: 'USDT0',
  360: 'USDH'
};

function normalizeTicker(name) {
  let ticker = name;
  if (ticker.includes(':')) ticker = ticker.split(':').pop();
  ticker = ticker.replace(/-PERP|-USD|-USDT|-USDC|\/USD|\/USDT|@\d+/gi, '').toUpperCase().trim();
  const normalizations = {
    'XAU': 'GOLD', 'XAUUSD': 'GOLD',
    'XAG': 'SILVER', 'XAGUSD': 'SILVER',
    'WTI': 'OIL', 'CRUDE': 'OIL', 'USOIL': 'OIL',
    'GOOG': 'GOOGL', 'BRK.B': 'BRK', 'BRK.A': 'BRK'
  };
  return normalizations[ticker] || ticker;
}

async function fetchAPI(type, params = {}) {
  const res = await fetch(API_URL, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type, ...params })
  });
  if (!res.ok) throw new Error(`API error: ${res.status}`);
  return res.json();
}

exports.handler = async (event) => {
  // Only allow POST
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, body: 'Method not allowed' };
  }

  // Auth check
  const authHeader = event.headers.authorization || event.headers.Authorization;
  if (!COLLECT_SECRET || authHeader !== `Bearer ${COLLECT_SECRET}`) {
    return { statusCode: 401, body: 'Unauthorized' };
  }

  if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
    return { statusCode: 500, body: JSON.stringify({ error: 'Missing Supabase config' }) };
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);
  const timestamp = new Date().toISOString();

  const deployerRows = [];
  const tokenRows = [];

  for (const dexKey of DEPLOYERS) {
    try {
      const dexData = await fetchAPI('metaAndAssetCtxs', { dex: dexKey });
      if (!dexData || !Array.isArray(dexData) || dexData.length < 2) continue;

      const [meta, ctxs] = dexData;
      if (!meta?.universe || !ctxs) continue;

      const collateralIdx = meta.collateralToken ?? 0;
      const baseAsset = COLLATERAL_TOKENS[collateralIdx] || `TOKEN-${collateralIdx}`;

      let totalVol = 0, totalOI = 0, marketCount = 0;

      for (let i = 0; i < meta.universe.length; i++) {
        const asset = meta.universe[i];
        const ctx = ctxs[i];
        if (!ctx) continue;

        const vol = parseFloat(ctx.dayNtlVlm || 0);
        const oi = parseFloat(ctx.openInterest || 0) * parseFloat(ctx.markPx || 0);
        const price = parseFloat(ctx.markPx || 0);
        const ticker = normalizeTicker(asset.name);

        totalVol += vol;
        totalOI += oi;
        marketCount++;

        tokenRows.push({
          timestamp,
          ticker,
          deployer: dexKey,
          base_asset: baseAsset,
          volume: vol,
          open_interest: oi,
          price
        });
      }

      deployerRows.push({
        timestamp,
        deployer: dexKey,
        volume_24h: totalVol,
        open_interest: totalOI,
        market_count: marketCount
      });

      // Rate limit
      await new Promise(r => setTimeout(r, 200));
    } catch (e) {
      console.error(`Error fetching ${dexKey}:`, e.message);
    }
  }

  // Insert deployer snapshots
  if (deployerRows.length > 0) {
    const { error: dErr } = await supabase.from('deployer_snapshots').insert(deployerRows);
    if (dErr) console.error('Deployer insert error:', dErr.message);
  }

  // Insert token snapshots in batches of 500
  for (let i = 0; i < tokenRows.length; i += 500) {
    const batch = tokenRows.slice(i, i + 500);
    const { error: tErr } = await supabase.from('token_snapshots').insert(batch);
    if (tErr) console.error('Token insert error:', tErr.message);
  }

  const totalVol = deployerRows.reduce((s, d) => s + d.volume_24h, 0);
  const totalOI = deployerRows.reduce((s, d) => s + d.open_interest, 0);

  return {
    statusCode: 200,
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      success: true,
      timestamp,
      deployers: deployerRows.length,
      tokens: tokenRows.length,
      totalVolume: totalVol,
      totalOI: totalOI
    })
  };
};
