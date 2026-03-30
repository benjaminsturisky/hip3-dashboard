const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY || process.env.SUPABASE_ANON_KEY;

exports.handler = async (event) => {
  if (event.httpMethod !== 'GET') {
    return { statusCode: 405, body: 'Method not allowed' };
  }

  if (!SUPABASE_URL || !SUPABASE_KEY) {
    return { statusCode: 500, body: JSON.stringify({ error: 'Missing Supabase config' }) };
  }

  const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
  const params = event.queryStringParameters || {};
  const type = params.type || 'totals';
  const days = Math.min(parseInt(params.days) || 30, 90);
  const since = new Date(Date.now() - days * 24 * 60 * 60 * 1000).toISOString();

  const headers = {
    'Content-Type': 'application/json',
    'Cache-Control': 'public, max-age=900' // 15 min cache
  };

  try {
    // Total volume & OI over time (aggregated across all deployers per timestamp)
    if (type === 'totals') {
      const { data, error } = await supabase
        .from('deployer_snapshots')
        .select('timestamp, volume_24h, open_interest')
        .gte('timestamp', since)
        .order('timestamp', { ascending: true });

      if (error) throw error;

      // Group by timestamp and sum
      const grouped = {};
      for (const row of data) {
        const ts = row.timestamp;
        if (!grouped[ts]) grouped[ts] = { timestamp: ts, volume: 0, oi: 0 };
        grouped[ts].volume += row.volume_24h;
        grouped[ts].oi += row.open_interest;
      }

      return {
        statusCode: 200, headers,
        body: JSON.stringify(Object.values(grouped))
      };
    }

    // Single deployer history
    if (type === 'deployer') {
      const deployer = params.deployer;
      if (!deployer) return { statusCode: 400, body: JSON.stringify({ error: 'Missing deployer param' }) };

      const { data, error } = await supabase
        .from('deployer_snapshots')
        .select('timestamp, volume_24h, open_interest, market_count')
        .eq('deployer', deployer)
        .gte('timestamp', since)
        .order('timestamp', { ascending: true });

      if (error) throw error;
      return { statusCode: 200, headers, body: JSON.stringify(data) };
    }

    // Single token history
    if (type === 'token') {
      const ticker = params.ticker;
      if (!ticker) return { statusCode: 400, body: JSON.stringify({ error: 'Missing ticker param' }) };

      // Reverse normalization map: find all raw tickers that map to this normalized ticker
      const reverseMap = {
        'GOLD': ['GOLD', 'XAU', 'XAUUSD'],
        'SILVER': ['SILVER', 'XAG', 'XAGUSD'],
        'OIL': ['OIL', 'WTI', 'CRUDE', 'USOIL', 'CL'],
        'BRENT': ['BRENT', 'BRENTOIL'],
        'GOOGL': ['GOOGL', 'GOOG'],
        'BRK': ['BRK', 'BRK.B', 'BRK.A'],
        'SP500': ['SP500', 'US500', 'USA500', 'SPX'],
        'NDX': ['NDX', 'US100', 'USA100', 'NAS100', 'NASDAQ'],
        'DJI': ['DJI', 'US30', 'USA30', 'DOW30']
      };

      const tickerUpper = ticker.toUpperCase();
      const variants = reverseMap[tickerUpper] || [tickerUpper];

      const { data, error } = await supabase
        .from('token_snapshots')
        .select('timestamp, deployer, volume, open_interest, price')
        .in('ticker', variants)
        .gte('timestamp', since)
        .order('timestamp', { ascending: true });

      if (error) throw error;

      // Aggregate across deployers AND ticker variants per timestamp
      const grouped = {};
      for (const row of data) {
        const ts = row.timestamp;
        if (!grouped[ts]) grouped[ts] = { timestamp: ts, volume: 0, oi: 0, price: 0 };
        grouped[ts].volume += row.volume;
        grouped[ts].oi += row.open_interest;
        if (row.price > 0) grouped[ts].price = row.price;
      }

      return {
        statusCode: 200, headers,
        body: JSON.stringify(Object.values(grouped))
      };
    }

    // Base asset history (aggregate all tokens with a given base_asset per timestamp)
    if (type === 'base') {
      const base = params.base;
      if (!base) return { statusCode: 400, body: JSON.stringify({ error: 'Missing base param' }) };

      const { data, error } = await supabase
        .from('token_snapshots')
        .select('timestamp, volume, open_interest, base_asset')
        .eq('base_asset', base.toUpperCase())
        .gte('timestamp', since)
        .order('timestamp', { ascending: true });

      if (error) throw error;

      const grouped = {};
      for (const row of data) {
        const ts = row.timestamp;
        if (!grouped[ts]) grouped[ts] = { timestamp: ts, volume: 0, oi: 0 };
        grouped[ts].volume += row.volume;
        grouped[ts].oi += row.open_interest;
      }

      return {
        statusCode: 200, headers,
        body: JSON.stringify(Object.values(grouped))
      };
    }

    // Deployer breakdown over time (all deployers, for stacked chart)
    if (type === 'deployers') {
      const { data, error } = await supabase
        .from('deployer_snapshots')
        .select('timestamp, deployer, volume_24h, open_interest')
        .gte('timestamp', since)
        .order('timestamp', { ascending: true });

      if (error) throw error;
      return { statusCode: 200, headers, body: JSON.stringify(data) };
    }

    return { statusCode: 400, body: JSON.stringify({ error: 'Invalid type. Use: totals, deployer, token, deployers' }) };

  } catch (e) {
    console.error('History query error:', e);
    return { statusCode: 500, headers, body: JSON.stringify({ error: e.message, cause: e.cause?.message, code: e.code }) };
  }
};
