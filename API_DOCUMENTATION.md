# Trading AI Platform - API Documentation

Complete API reference for the Trading AI Platform with REST endpoints and WebSocket connections.

## Base URL
```
http://localhost:8000
```

## Authentication
Currently, no authentication is required for API access.

---

## 📊 REST API Endpoints

### 1. Market Summary

**Endpoint:** `GET /api/market/summary`

**Description:** Returns market overview with forecasts for multiple assets

**Query Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `class` | string | No | `crypto` | Asset class filter (`crypto`, `stocks`, `macro`, `all`) |
| `limit` | integer | No | `10` | Maximum number of assets to return |

**Example Request:**
```bash
GET /api/market/summary?class=crypto&limit=5
```

**Response:**
```json
{
  "assets": [
    {
      "symbol": "BTC",
      "name": "Bitcoin",
      "current_price": 43250.50,
      "change_24h": 2.45,
      "volume": 28500000000,
      "forecast_direction": "UP",
      "confidence": 86,
      "predicted_range": "$43,800–$44,200",
      "data_source": "Binance Stream + ML Analysis"
    },
    {
      "symbol": "ETH",
      "name": "Ethereum",
      "current_price": 2650.75,
      "change_24h": -1.23,
      "volume": 15200000000,
      "forecast_direction": "DOWN",
      "confidence": 78,
      "predicted_range": "$2,580–$2,620",
      "data_source": "Binance Stream + ML Analysis"
    }
  ]
}
```

---

### 2. Asset Forecast

**Endpoint:** `GET /api/asset/{symbol}/forecast`

**Description:** Returns detailed forecast with historical and predicted chart data

**Path Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `symbol` | string | Yes | Asset symbol (BTC, ETH, NVDA, etc.) |

**Query Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `timeframe` | string | No | `1D` | Time period (`1D`, `7D`, `1M`, `1Y`, `5Y`) |

**Example Request:**
```bash
GET /api/asset/BTC/forecast?timeframe=7D
```

**Response:**
```json
{
  "symbol": "BTC",
  "name": "Bitcoin",
  "current_price": 43250.50,
  "predicted_price": 44100.25,
  "forecast_direction": "UP",
  "confidence": 86,
  "change_24h": 2.45,
  "predicted_range": "$43,800–$44,200",
  "data_source": "Binance/YFinance API + ML Analysis",
  "chart": {
    "actual": [42800, 42950, 43100, 43250],
    "predicted": [43400, 43600, 43800, 44100],
    "timestamps": ["2024-01-15T10:00:00Z", "2024-01-15T14:00:00Z", "2024-01-15T18:00:00Z", "2024-01-15T22:00:00Z"]
  },
  "last_updated": "2024-01-15T22:30:00Z"
}
```

**Error Response:**
```json
{
  "detail": "Failed to generate forecast for BTC 7D: No database data available"
}
```

---

### 3. Asset Trends & Accuracy

**Endpoint:** `GET /api/asset/{symbol}/trends`

**Description:** Returns historical accuracy and trend analysis

**Path Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `symbol` | string | Yes | Asset symbol |

**Query Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `timeframe` | string | No | `7D` | Time period (`1W`, `7D`, `1M`, `1Y`, `5Y`) |
| `view` | string | No | `chart` | Response format (`chart`, `table`) |

**Example Request (Chart View):**
```bash
GET /api/asset/BTC/trends?timeframe=7D&view=chart
```

**Response (Chart View):**
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

**Example Request (Table View):**
```bash
GET /api/asset/BTC/trends?timeframe=1M&view=table
```

**Response (Table View):**
```json
{
  "symbol": "BTC",
  "accuracy": 86,
  "history": [
    {
      "date": "2024-01-15",
      "forecast": "UP",
      "actual": "UP",
      "result": "Hit"
    },
    {
      "date": "2024-01-14",
      "forecast": "UP",
      "actual": "DOWN",
      "result": "Miss"
    }
  ]
}
```

---

### 4. Search Assets

**Endpoint:** `GET /api/assets/search`

**Description:** Search available assets by name or symbol

**Query Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `q` | string | Yes | Search query (symbol or name) |

**Example Request:**
```bash
GET /api/assets/search?q=bit
```

**Response:**
```json
{
  "results": [
    {
      "symbol": "BTC",
      "name": "Bitcoin",
      "class": "crypto"
    }
  ]
}
```

---

### 5. Export Historical Data

**Endpoint:** `GET /api/asset/{symbol}/export`

**Description:** Export historical forecast vs actual data as CSV

**Path Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `symbol` | string | Yes | Asset symbol |

**Query Parameters:**
| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `timeframe` | string | No | `1M` | Time period (`1W`, `1M`, `1Y`, `5Y`) |

**Example Request:**
```bash
GET /api/asset/BTC/export?timeframe=1M
```

**Response:**
- Content-Type: `text/csv`
- File download with filename: `{symbol}_export.csv`

**CSV Format:**
```csv
Date,Forecast,Actual,Result
2024-01-15,UP,UP,Hit
2024-01-14,DOWN,UP,Miss
2024-01-13,UP,UP,Hit
```

---

### 6. Export Trends Data

**Endpoint:** `GET /api/asset/{symbol}/trends/export`

**Description:** Export trends historical data as CSV with accuracy metrics

**Parameters:** Same as regular export endpoint

**Response CSV Format:**
```csv
Date,Forecast,Actual,Result,Accuracy
2024-01-15,UP,UP,Hit,Hit
2024-01-14,DOWN,UP,Miss,Miss
```

---

### 7. Favorites Management

#### Add Favorite
**Endpoint:** `POST /api/favorites/{symbol}`

**Example Request:**
```bash
POST /api/favorites/BTC
```

**Response:**
```json
{
  "success": true,
  "symbol": "BTC"
}
```

#### Remove Favorite
**Endpoint:** `DELETE /api/favorites/{symbol}`

**Response:**
```json
{
  "success": true,
  "symbol": "BTC"
}
```

#### Get Favorites
**Endpoint:** `GET /api/favorites`

**Response:**
```json
{
  "favorites": ["BTC", "ETH", "NVDA"]
}
```

---

### 8. System Health

**Endpoint:** `GET /api/health`

**Description:** Comprehensive system health check

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T22:30:00Z",
  "services": {
    "database": "connected",
    "redis": "connected (api_cache, ml_cache)",
    "ml_model": "operational",
    "external_apis": "operational"
  },
  "total_check_time": "0.45s",
  "db_test_time": "0.12s",
  "ml_test_time": "0.18s",
  "api_test_time": "0.15s",
  "db_pool_stats": {
    "size": 8,
    "min_size": 5,
    "max_size": 20,
    "idle_size": 3,
    "status": "active"
  }
}
```

---

### 9. System Statistics

**Endpoint:** `GET /api/system/stats`

**Response:**
```json
{
  "task_manager": {
    "total_tasks": 1250,
    "completed_tasks": 1248,
    "failed_tasks": 2,
    "active_tasks": 0,
    "timestamp": "2024-01-15T22:30:00Z"
  },
  "websocket_connections": 15,
  "timestamp": "2024-01-15T22:30:00Z"
}
```

**Endpoint:** `GET /api/websocket/stats`

**Response:**
```json
{
  "total_connections": 15,
  "unique_users": 8,
  "connections_by_symbol": {
    "BTC": 5,
    "ETH": 3,
    "NVDA": 2
  },
  "connections_by_timeframe": {
    "4h": 8,
    "1D": 5,
    "1W": 2
  },
  "avg_connections_per_user": 1.875,
  "timestamp": "2024-01-15T22:30:00Z"
}
```

---

## 🔌 WebSocket Connections

### 1. Asset Forecast WebSocket

**Endpoint:** `ws://localhost:8000/ws/asset/{symbol}/forecast`

**Description:** Real-time price updates and forecasts for specific asset

#### Connection
```javascript
const ws = new WebSocket('ws://localhost:8000/ws/asset/BTC/forecast');
```

#### Incoming Messages

**Historical Data (on connection):**
```json
{
  "type": "historical_data",
  "symbol": "BTC",
  "timeframe": "4h",
  "name": "Bitcoin",
  "chart": {
    "actual": [42800, 42950, 43100, 43250],
    "predicted": [43400, 43600, 43800, 44100],
    "timestamps": ["2024-01-15T10:00:00Z", "2024-01-15T14:00:00Z"]
  },
  "last_updated": "2024-01-15T22:30:00Z"
}
```

**Real-time Updates:**
```json
{
  "type": "realtime_update",
  "symbol": "BTC",
  "timeframe": "4h",
  "current_price": 43275.50,
  "predicted_price": 44125.25,
  "change_24h": 2.48,
  "volume": 28600000000,
  "timestamp": "22:35",
  "forecast_direction": "UP",
  "confidence": 87,
  "predicted_range": "$43,850–$44,250",
  "last_updated": "2024-01-15T22:35:00Z"
}
```

**Ping/Heartbeat:**
```json
{
  "type": "ping",
  "symbol": "BTC",
  "timeframe": "4h",
  "asset_type": "crypto",
  "timestamp": "2024-01-15T22:35:00Z"
}
```

#### Outgoing Messages

**Change Timeframe:**
```json
{
  "type": "set_timeframe",
  "timeframe": "1D"
}
```

**Change Asset:**
```json
{
  "type": "set_symbol",
  "symbol": "ETH"
}
```

#### Response to Timeframe Change:
```json
{
  "type": "timeframe_changed",
  "symbol": "BTC",
  "timeframe": "1D",
  "timestamp": "2024-01-15T22:35:00Z"
}
```

#### Response to Asset Change:
```json
{
  "type": "symbol_changed",
  "symbol": "ETH",
  "timeframe": "4h",
  "timestamp": "2024-01-15T22:35:00Z"
}
```

---

### 2. Asset Trends WebSocket

**Endpoint:** `ws://localhost:8000/ws/asset/{symbol}/trends`

**Description:** Real-time accuracy and trend updates

#### Connection
```javascript
const ws = new WebSocket('ws://localhost:8000/ws/asset/BTC/trends');
```

#### Incoming Messages

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
  "history": [
    {
      "date": "2024-01-15",
      "forecast": "UP",
      "actual": "UP",
      "result": "Hit"
    },
    {
      "date": "2024-01-14",
      "forecast": "DOWN",
      "actual": "UP",
      "result": "Miss"
    }
  ],
  "last_updated": "2024-01-15T22:35:00Z"
}
```

#### Outgoing Messages

**Change Symbol:**
```json
{
  "type": "set_symbol",
  "symbol": "ETH"
}
```

---

### 3. Market Summary WebSocket

**Endpoint:** `ws://localhost:8000/ws/market/summary`

**Description:** Real-time market-wide updates for all assets

#### Connection
```javascript
const ws = new WebSocket('ws://localhost:8000/ws/market/summary');
```

#### Incoming Messages

**Market Summary Update:**
```json
{
  "type": "market_summary_update",
  "assets": [
    {
      "symbol": "BTC",
      "name": "Bitcoin",
      "current_price": 43275.50,
      "change_24h": 2.48,
      "volume": 28600000000,
      "forecast_direction": "UP",
      "confidence": 87,
      "predicted_range": "$43,850–$44,250",
      "data_source": "Binance Stream",
      "asset_class": "crypto"
    },
    {
      "symbol": "NVDA",
      "name": "NVIDIA",
      "current_price": 875.25,
      "change_24h": 1.85,
      "volume": 45000000,
      "forecast_direction": "UP",
      "confidence": 82,
      "predicted_range": "$880.50–$890.75",
      "data_source": "Stock API",
      "asset_class": "stocks"
    }
  ],
  "timestamp": "2024-01-15T22:35:00Z",
  "update_count": 15,
  "crypto_count": 8,
  "stock_count": 7,
  "interval": 2
}
```

#### Outgoing Messages

**Set Filter (Future Enhancement):**
```json
{
  "type": "set_filter",
  "class_filter": "crypto"
}
```

---

## 🚨 Error Handling

### HTTP Status Codes
- `200` - Success
- `400` - Bad Request (invalid parameters)
- `404` - Not Found (invalid symbol)
- `429` - Too Many Requests (rate limited)
- `500` - Internal Server Error

### Error Response Format
```json
{
  "detail": "Error message describing what went wrong"
}
```

### Common Error Messages
- `"Unsupported symbol: XYZ"`
- `"Failed to generate forecast for BTC 7D: No database data available"`
- `"Rate limit exceeded. Try again later."`
- `"Database connection failed"`

---

## 📝 Rate Limiting

- **Default Limit:** 100 requests per 60 seconds per IP
- **Export Endpoints:** Special rate limiting applied
- **WebSocket:** No rate limiting on connections, but message frequency is controlled

---

## 🔧 WebSocket Connection Management

### Connection Lifecycle
1. **Connect** - Establish WebSocket connection
2. **Authentication** - None required currently
3. **Historical Data** - Receive initial data dump
4. **Real-time Updates** - Receive periodic updates
5. **Heartbeat** - Ping/pong every 30 seconds
6. **Disconnect** - Graceful cleanup

### Connection Pooling
- Multiple users can connect to same symbol/timeframe
- Efficient message broadcasting
- Automatic cleanup of dead connections
- Reconnection support

### Message Frequency
- **Forecast Updates:** Every 15 seconds (crypto), 5 minutes (stocks)
- **Trends Updates:** Every 5 seconds
- **Market Summary:** Every 2 seconds
- **Heartbeat:** Every 30 seconds

---

## 📊 Supported Symbols

### Cryptocurrencies
`BTC`, `ETH`, `USDT`, `XRP`, `BNB`, `SOL`, `USDC`, `DOGE`, `ADA`, `TRX`

### Stocks
`NVDA`, `MSFT`, `AAPL`, `GOOGL`, `AMZN`, `META`, `AVGO`, `TSLA`, `BRK-B`, `JPM`

### Macro Indicators
`GDP`, `CPI`, `UNEMPLOYMENT`, `FED_RATE`, `CONSUMER_CONFIDENCE`

---

## 🕐 Timeframes

### Supported Timeframes
- `1m`, `5m`, `15m`, `30m` - Minute intervals
- `1h`, `4h` - Hour intervals  
- `1D` - Daily
- `1W` - Weekly
- `1M`, `1Y`, `5Y` - Month/Year periods

### Default Timeframes
- **Crypto WebSocket:** `4h`
- **Stock WebSocket:** `1D`
- **API Endpoints:** `1D` or `7D`