# Trading AI Platform API Documentation

## Overview
This document provides comprehensive documentation for all API endpoints and WebSocket connections in the Trading AI Platform.

## Base URL
```
http://localhost:8000
```

---

## REST API Endpoints

### 1. Market Summary
**GET** `/api/market/summary`

Returns trending assets and quick forecast summary.

**Query Parameters:**
- `class_filter` (optional): `crypto`, `stocks`, `macro`, `all` (default: `crypto`)
- `limit` (optional): Number of assets to return (default: `10`)

**Response:**
```json
{
  "assets": [
    {
      "symbol": "BTC",
      "name": "Bitcoin",
      "current_price": 45000.50,
      "change_24h": 2.5,
      "forecast_direction": "UP",
      "confidence": 86,
      "predicted_price": 46125.25,
      "predicted_range": "$44,100.49–$47,250.51"
    }
  ]
}
```

### 2. Asset Forecast
**GET** `/api/asset/{symbol}/forecast`

Returns detailed forecast, confidence score, and chart data for a specific asset.

**Path Parameters:**
- `symbol`: Asset symbol (e.g., BTC, ETH, NVDA, AAPL)

**Query Parameters:**
- `timeframe` (optional): `1D`, `7D`, `1M`, `1Y`, `5Y` (default: `1D`)

**Response:**
```json
{
  "symbol": "BTC",
  "name": "Bitcoin",
  "current_price": 45000.50,
  "predicted_price": 46125.25,
  "forecast_direction": "UP",
  "confidence": 86,
  "predicted_range": "$44,100.49–$47,250.51",
  "change_24h": 2.5,
  "chart": {
    "actual": [44000, 44500, 45000],
    "predicted": [45500, 46000, 46500],
    "timestamps": ["2024-01-01T00:00:00", "2024-01-01T04:00:00", "2024-01-01T08:00:00"]
  },
  "last_updated": "2024-01-01T12:45:00Z"
}
```

### 3. Historical Trends & Accuracy
**GET** `/api/asset/{symbol}/trends`

Returns accuracy and trend history for a given asset.

**Path Parameters:**
- `symbol`: Asset symbol

**Query Parameters:**
- `timeframe` (optional): `1W`, `7D`, `1M`, `1Y`, `5Y` (default: `7D`)
- `view` (optional): `chart` or `table` (default: `chart`)

**Response (Chart Mode):**
```json
{
  "symbol": "BTC",
  "accuracy": 86,
  "chart": {
    "forecast": [54000, 54320, 54100, 53800],
    "actual": [54200, 54500, 53750, 53480],
    "timestamps": ["00:00", "04:00", "08:00", "12:00"]
  }
}
```

**Response (Table Mode):**
```json
{
  "symbol": "BTC",
  "accuracy": 86,
  "history": [
    {
      "date": "2024-01-01",
      "forecast": "UP",
      "actual": "UP",
      "result": "Hit"
    }
  ]
}
```

### 4. Asset Search
**GET** `/api/assets/search`

Search available assets by name or symbol.

**Query Parameters:**
- `q`: Search query

**Response:**
```json
{
  "results": [
    { "symbol": "BTC", "name": "Bitcoin", "class": "crypto" },
    { "symbol": "AAPL", "name": "Apple", "class": "stocks" }
  ]
}
```

### 5. Export Historical Data
**GET** `/api/asset/{symbol}/export`

Exports historical forecast vs. actual data as CSV.

**Path Parameters:**
- `symbol`: Asset symbol

**Query Parameters:**
- `timeframe` (optional): `1W`, `1M`, `1Y`, `5Y` (default: `1M`)

**Response:** CSV file download

### 6. Export Trends Data
**GET** `/api/asset/{symbol}/trends/export`

Exports trends historical data as CSV with accuracy metrics.

**Path Parameters:**
- `symbol`: Asset symbol

**Query Parameters:**
- `timeframe` (optional): `1W`, `1M`, `1Y`, `5Y` (default: `1M`)

**Response:** CSV file download

### 7. Favorites Management

#### Add Favorite
**POST** `/api/favorites/{symbol}`

**Response:**
```json
{
  "success": true,
  "symbol": "BTC"
}
```

#### Remove Favorite
**DELETE** `/api/favorites/{symbol}`

**Response:**
```json
{
  "success": true,
  "symbol": "BTC"
}
```

#### Get Favorites
**GET** `/api/favorites`

**Response:**
```json
{
  "favorites": ["BTC", "ETH", "NVDA"]
}
```

### 8. Accuracy Validation
**POST** `/api/asset/{symbol}/validate`

Validate forecast accuracy for symbol.

**Response:**
```json
{
  "symbol": "BTC",
  "validation": {
    "accuracy": 86,
    "total_predictions": 100,
    "correct_predictions": 86
  },
  "timestamp": "2024-01-01T12:45:00Z"
}
```

### 9. System Health
**GET** `/api/health`

Comprehensive system health check.

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2024-01-01T12:45:00Z",
  "services": {
    "database": "connected",
    "redis": "connected (api_cache, ml_cache)",
    "ml_model": "operational",
    "external_apis": "operational"
  },
  "total_check_time": "0.85s",
  "db_test_time": "0.12s",
  "ml_test_time": "0.45s",
  "api_test_time": "0.28s"
}
```

### 10. System Statistics

#### WebSocket Stats
**GET** `/api/websocket/stats`

**Response:**
```json
{
  "total_connections": 15,
  "connections_by_symbol": {
    "BTC": 5,
    "ETH": 3,
    "NVDA": 2
  },
  "timestamp": "2024-01-01T12:45:00Z"
}
```

#### System Stats
**GET** `/api/system/stats`

**Response:**
```json
{
  "task_manager": {
    "total_tasks": 150,
    "completed_tasks": 145,
    "failed_tasks": 2,
    "active_tasks": 3
  },
  "websocket_connections": 15,
  "timestamp": "2024-01-01T12:45:00Z"
}
```

---

## WebSocket Connections

### 1. Asset Forecast WebSocket
**WS** `/ws/asset/{symbol}/forecast`

Real-time forecast updates for a specific asset.

**Connection Messages:**

#### Incoming Messages (Client → Server)

**Set Timeframe:**
```json
{
  "type": "set_timeframe",
  "timeframe": "1D"
}
```

**Set Symbol:**
```json
{
  "type": "set_symbol",
  "symbol": "ETH"
}
```

#### Outgoing Messages (Server → Client)

**Crypto Forecast Update:**
```json
{
  "type": "timeframe_forecast",
  "symbol": "BTC",
  "timeframe": "4h",
  "current_price": 45000.50,
  "predicted_price": 46125.25,
  "forecast_direction": "UP",
  "confidence": 86,
  "change_24h": 2.5,
  "chart": {
    "past": [44000, 44500, 45000],
    "future": [45500, 46000, 46500],
    "timestamps": ["00:00", "04:00", "08:00"]
  },
  "timestamp": "2024-01-01T12:45:00Z"
}
```

**Stock Forecast Update:**
```json
{
  "type": "stock_forecast",
  "symbol": "NVDA",
  "timeframe": "1D",
  "current_price": 875.50,
  "predicted_price": 890.25,
  "forecast_direction": "UP",
  "confidence": 82,
  "change_24h": 1.8,
  "chart": {
    "past": [860, 870, 875],
    "future": [880, 885, 890],
    "timestamps": ["09:30", "12:00", "15:30"]
  },
  "timestamp": "2024-01-01T12:45:00Z"
}
```

**Timeframe Changed:**
```json
{
  "type": "timeframe_changed",
  "symbol": "BTC",
  "timeframe": "1D",
  "timestamp": "2024-01-01T12:45:00Z"
}
```

**Symbol Changed:**
```json
{
  "type": "symbol_changed",
  "symbol": "ETH",
  "timeframe": "4h",
  "timestamp": "2024-01-01T12:45:00Z"
}
```

**Ping:**
```json
{
  "type": "ping",
  "symbol": "BTC",
  "timeframe": "4h",
  "asset_type": "crypto",
  "timestamp": "2024-01-01T12:45:00Z"
}
```

### 2. Asset Trends WebSocket
**WS** `/ws/asset/{symbol}/trends`

Real-time trends and accuracy updates for a specific asset.

**Connection Messages:**

#### Incoming Messages (Client → Server)

**Set Symbol:**
```json
{
  "type": "set_symbol",
  "symbol": "ETH"
}
```

#### Outgoing Messages (Server → Client)

**Trends Update:**
```json
{
  "type": "trends_update",
  "symbol": "BTC",
  "accuracy": 86,
  "chart": {
    "forecast": [54000, 54320, 54100, 53800],
    "actual": [54200, 54500, 53750, 53480],
    "timestamps": ["00:00", "04:00", "08:00", "12:00"]
  },
  "last_updated": "2024-01-01T12:45:00Z"
}
```

### 3. Market Summary WebSocket
**WS** `/ws/market/summary`

Real-time market summary updates for all assets.

**Connection Messages:**

#### Incoming Messages (Client → Server)

**Set Filter:**
```json
{
  "type": "set_filter",
  "class_filter": "crypto"
}
```

#### Outgoing Messages (Server → Client)

**Market Summary Update:**
```json
{
  "type": "market_summary_update",
  "assets": [
    {
      "symbol": "BTC",
      "name": "Bitcoin",
      "current_price": 45000.50,
      "change_24h": 2.5,
      "volume": 28500000000,
      "forecast_direction": "UP",
      "confidence": 86,
      "predicted_price": 46125.25,
      "data_source": "Binance Stream",
      "asset_class": "crypto"
    }
  ],
  "timestamp": "2024-01-01T12:45:00Z",
  "update_count": 10,
  "crypto_count": 6,
  "stock_count": 4,
  "interval": 2
}
```

### 4. Deprecated Mobile WebSocket
**WS** `/ws/mobile`

**Note:** This endpoint is deprecated. Use `/ws/asset/{symbol}/forecast` or `/ws/asset/{symbol}/trends` instead.

**Connection Response:**
- WebSocket closes immediately with code 1000 and reason message.

---

## Supported Assets

### Cryptocurrencies (10)
- BTC (Bitcoin)
- ETH (Ethereum)
- USDT (Tether)
- XRP (Ripple)
- BNB (Binance Coin)
- SOL (Solana)
- USDC (USD Coin)
- DOGE (Dogecoin)
- ADA (Cardano)
- TRX (Tron)

### Stocks (10)
- NVDA (NVIDIA)
- MSFT (Microsoft)
- AAPL (Apple)
- GOOGL (Alphabet)
- AMZN (Amazon)
- META (Meta)
- AVGO (Broadcom)
- TSLA (Tesla)
- BRK-B (Berkshire Hathaway)
- JPM (JPMorgan Chase)

### Macro Indicators (5)
- GDP (Gross Domestic Product)
- CPI (Consumer Price Index)
- UNEMPLOYMENT (Unemployment Rate)
- FED_RATE (Federal Interest Rate)
- CONSUMER_CONFIDENCE (Consumer Confidence Index)

---

## Timeframes

### Asset Timeframes
- `1m` - 1 Minute
- `5m` - 5 Minutes
- `15m` - 15 Minutes
- `30m` - 30 Minutes
- `1h` - 1 Hour
- `4h` / `4H` - 4 Hours
- `1D` - 1 Day
- `1W` - 1 Week

### Export/Trends Timeframes
- `1W` - 1 Week
- `7D` - 7 Days
- `1M` - 1 Month
- `1Y` - 1 Year
- `5Y` - 5 Years

---

## Error Handling

### HTTP Status Codes
- `200` - Success
- `400` - Bad Request
- `404` - Not Found
- `429` - Rate Limited
- `500` - Internal Server Error

### Error Response Format
```json
{
  "error": "Error description",
  "timestamp": "2024-01-01T12:45:00Z"
}
```

### WebSocket Error Handling
- Connection closes with appropriate close codes
- Automatic reconnection with exponential backoff
- Graceful degradation when services unavailable

---

## Rate Limiting

- Default rate limits apply to all endpoints
- Export endpoints have stricter limits
- Rate limit headers included in responses
- 429 status code returned when limits exceeded

---

## Caching

### Redis Caching
- Prediction cache with configurable TTL
- Distributed caching across instances
- Automatic fallback to memory cache

### Memory Caching
- Local prediction cache
- WebSocket connection pooling
- Background cleanup tasks

---

## Real-time Features

### WebSocket Connection Management
- Connection pooling and reuse
- Graceful reconnection handling
- Automatic cleanup of stale connections
- Health monitoring with ping/pong

### Live Data Sources
- Binance WebSocket for crypto prices
- Yahoo Finance API for stock prices
- Real-time ML predictions
- Streaming market data

---

## Security

### CORS Configuration
- Configurable allowed origins
- Credential support
- Method and header restrictions

### Rate Limiting
- IP-based rate limiting
- Endpoint-specific limits
- Configurable time windows

### Input Validation
- Parameter validation
- SQL injection prevention
- XSS protection