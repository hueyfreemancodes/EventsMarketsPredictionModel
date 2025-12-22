# Configuration Guide

## API Keys Setup

### Single File Configuration

**All API keys go in: `config/api_keys.py`**

This is the ONLY file you need to edit to add your API keys. Simply open `config/api_keys.py` and replace the placeholder values with your actual credentials.

### Required APIs (for Phase 1)

1. **Polymarket** (Required for prediction market data)
   - Get API key: https://polymarket.com/settings/api
   - Add to: `POLYMARKET_API_KEY` and `POLYMARKET_API_SECRET`

2. **Kalshi** (Required for prediction market data)
   - Register at: https://trading-api.kalshi.com/
   - Add to: `KALSHI_EMAIL` and `KALSHI_PASSWORD`

### Optional APIs

3. **The Odds API** (Optional - for betting lines)
   - Get API key: https://the-odds-api.com/
   - Add to: `THE_ODDS_API_KEY`

4. **ESPN API** (Optional - if using ESPN data)
   - Add to: `ESPN_API_KEY`

5. **RapidAPI** (Optional - for various sports endpoints)
   - Get key: https://rapidapi.com/
   - Add to: `RAPIDAPI_KEY`

6. **SportsDataIO** (Optional - premium sports data)
   - Get key: https://sportsdata.io/
   - Add to: `SPORTSDATAIO_API_KEY`

### Testing Without API Keys

The code is designed to work without API keys for testing. You can:
- Test database connections
- Test data structures
- Run validation logic
- Test with mock data

When API keys are missing, the system will:
- Log warnings instead of errors
- Skip API-dependent operations
- Continue with available data sources

### Checking Configuration

Run this to see which APIs are configured:

```python
from config import get_all_configured_apis
print("Configured APIs:", get_all_configured_apis())
```

