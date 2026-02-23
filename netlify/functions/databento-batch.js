const DATABENTO_API_KEY = process.env.DATABENTO_API_KEY;

// All supported TradFi symbols
const SYMBOLS = ['TSLA', 'NVDA', 'AAPL', 'AMZN', 'GOOGL', 'META', 'MSFT', 'AMD', 'NFLX', 'COIN', 'MSTR', 'PLTR', 'SPY', 'QQQ'];

exports.handler = async (event) => {
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

    // Fetch all symbols in ONE request (comma-separated)
    const symbolsParam = SYMBOLS.join(',');
    const url = `https://hist.databento.com/v0/timeseries.get_range?dataset=XNAS.ITCH&symbols=${symbolsParam}&schema=mbp-10&start=${startTime}&end=${endTime}&encoding=json`;

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
    const lines = text.trim().split('\n').filter(l => l);

    // Parse each line and group by symbol
    const results = {};
    for (const line of lines) {
      try {
        const data = JSON.parse(line);
        // Use the first record for each symbol (most recent)
        if (data.symbol && !results[data.symbol]) {
          results[data.symbol] = data;
        }
      } catch (e) {
        console.warn('Failed to parse line:', line);
      }
    }

    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'public, max-age=1200' // Cache for 20 minutes
      },
      body: JSON.stringify({
        date: dateStr,
        symbols: results
      })
    };
  } catch (e) {
    console.error('Databento batch error:', e.message);
    return {
      statusCode: 500,
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ error: e.message })
    };
  }
};
