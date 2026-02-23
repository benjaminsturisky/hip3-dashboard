const DATABENTO_API_KEY = process.env.DATABENTO_API_KEY;

// In-memory cache with 20 minute TTL
const cache = {};
const CACHE_TTL = 20 * 60 * 1000; // 20 minutes

exports.handler = async (event) => {
  const symbol = event.queryStringParameters?.symbol;

  if (!symbol) {
    return {
      statusCode: 400,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ error: 'Missing symbol parameter' })
    };
  }

  // Check cache first
  const cached = cache[symbol];
  if (cached && Date.now() - cached.timestamp < CACHE_TTL) {
    return {
      statusCode: 200,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(cached.data)
    };
  }

  try {
    // Calculate yesterday's date, skip weekends
    const yesterday = new Date();
    yesterday.setDate(yesterday.getDate() - 1);
    const day = yesterday.getDay();
    if (day === 0) yesterday.setDate(yesterday.getDate() - 2);
    if (day === 6) yesterday.setDate(yesterday.getDate() - 1);

    const dateStr = yesterday.toISOString().split('T')[0];
    const startTime = `${dateStr}T19:30:00`;
    const endTime = `${dateStr}T19:30:05`;

    const url = `https://hist.databento.com/v0/timeseries.get_range?dataset=XNAS.ITCH&symbols=${symbol}&schema=mbp-10&start=${startTime}&end=${endTime}&encoding=json&limit=1`;

    const response = await fetch(url, {
      headers: {
        'Authorization': 'Basic ' + Buffer.from(DATABENTO_API_KEY + ':').toString('base64')
      }
    });

    if (!response.ok) {
      return {
        statusCode: response.status,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ error: `Databento error: ${response.status}` })
      };
    }

    const text = await response.text();
    const lines = text.trim().split('\n');

    let data = null;
    if (lines.length > 0 && lines[0]) {
      data = JSON.parse(lines[0]);
    }

    // Cache the result
    cache[symbol] = { data, timestamp: Date.now() };

    return {
      statusCode: 200,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(data)
    };
  } catch (e) {
    console.error(`Databento proxy error for ${symbol}:`, e.message);
    return {
      statusCode: 500,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ error: e.message })
    };
  }
};
