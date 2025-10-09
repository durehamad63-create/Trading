# Trading AI Platform

A real-time financial forecasting system that provides AI-powered predictions for cryptocurrencies, stocks, and macro indicators through REST APIs and WebSocket connections.

## 🚀 Features

- **Real-time ML Predictions** for 25+ assets (crypto, stocks, macro indicators)
- **WebSocket Streaming** for live price updates and forecasts
- **Historical Accuracy Tracking** with trend analysis
- **Multi-level Caching** (Redis + Memory) for optimal performance
- **PostgreSQL Database** with connection pooling
- **Rate Limiting** and comprehensive error handling
- **CSV Export** functionality for historical data

## 📋 Supported Assets

### Cryptocurrencies (10)
BTC, ETH, USDT, XRP, BNB, SOL, USDC, DOGE, ADA, TRX

### Stocks (10) 
NVDA, MSFT, AAPL, GOOGL, AMZN, META, AVGO, TSLA, BRK-B, JPM

### Macro Indicators (5)
GDP, CPI, UNEMPLOYMENT, FED_RATE, CONSUMER_CONFIDENCE

## 🛠️ Prerequisites

- **Python 3.8+**
- **PostgreSQL 12+**
- **Redis 6+**
- **Git**

## 📦 Installation

### 1. Clone Repository
```bash
git clone <your-repo-url>
cd TradingApp
```

### 2. Create Virtual Environment
```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# Linux/Mac
source .venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

## ⚙️ Environment Configuration

### 1. Create .env File
Copy the example environment file and configure it:

```bash
cp .env.example .env
```

### 2. Configure .env Variables

Open `.env` file and set the following variables:

```env
# Database Configuration
DATABASE_URL=postgresql://postgres:your_password@localhost:5432/trading_db

# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_PREDICTION_DB=1
REDIS_ML_DB=2
REDIS_CHART_DB=3
REDIS_PASSWORD=

# API Keys for Stock Data (Optional but recommended)
ALPHA_VANTAGE_API_KEY=YOUR_API_KEY_HERE
IEX_CLOUD_TOKEN=YOUR_TOKEN_HERE

# Cache Settings
CACHE_TTL=60
PREDICTION_CACHE_TTL=300

# Rate Limiting
RATE_LIMIT_REQUESTS=100
RATE_LIMIT_WINDOW=60

# WebSocket Settings
WS_HEARTBEAT_INTERVAL=30
WS_FORECAST_INTERVAL=15
WS_TRENDS_INTERVAL=60
```

### 3. Environment Variables Explained

| Variable | Description | Required | Default |
|----------|-------------|----------|---------|
| `DATABASE_URL` | PostgreSQL connection string | ✅ | - |
| `REDIS_HOST` | Redis server hostname | ✅ | localhost |
| `REDIS_PORT` | Redis server port | ✅ | 6379 |
| `REDIS_DB` | Redis database for general cache | ✅ | 0 |
| `REDIS_PREDICTION_DB` | Redis database for ML predictions | ✅ | 1 |
| `REDIS_ML_DB` | Redis database for ML model cache | ✅ | 2 |
| `REDIS_CHART_DB` | Redis database for chart data | ✅ | 3 |
| `REDIS_PASSWORD` | Redis password (if required) | ❌ | - |
| `ALPHA_VANTAGE_API_KEY` | Alpha Vantage API key for stocks | ❌ | - |
| `IEX_CLOUD_TOKEN` | IEX Cloud token for stocks | ❌ | - |
| `CACHE_TTL` | General cache TTL in seconds | ❌ | 60 |
| `PREDICTION_CACHE_TTL` | ML prediction cache TTL | ❌ | 300 |

## 🗄️ Database Setup

### 1. Install PostgreSQL
- **Windows**: Download from [postgresql.org](https://www.postgresql.org/download/)
- **Ubuntu**: `sudo apt install postgresql postgresql-contrib`
- **macOS**: `brew install postgresql`

### 2. Create Database
```sql
-- Connect to PostgreSQL as superuser
psql -U postgres

-- Create database
CREATE DATABASE trading_db;

-- Create user (optional)
CREATE USER trading_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE trading_db TO trading_user;

-- Exit
\q
```

### 3. Update DATABASE_URL
```env
# If using default postgres user
DATABASE_URL=postgresql://postgres:your_password@localhost:5432/trading_db

# If using custom user
DATABASE_URL=postgresql://trading_user:your_password@localhost:5432/trading_db
```

## 🔴 Redis Setup

### 1. Install Redis
- **Windows**: Download from [redis.io](https://redis.io/download) or use WSL
- **Ubuntu**: `sudo apt install redis-server`
- **macOS**: `brew install redis`

### 2. Start Redis Server
```bash
# Windows/Linux
redis-server

# macOS (if installed via brew)
brew services start redis
```

### 3. Test Redis Connection
```bash
redis-cli ping
# Should return: PONG
```

## 📊 Data Collection Setup

### ⚠️ CRITICAL: Initial Data Collection

Before running the main application, you **MUST** collect historical data to populate the database:

### 1. Run Data Collector
```bash
# Navigate to data collector directory
cd data_collecter

# Run the 7-day data collector
python btc_trend_analysis.py
```

### 2. Data Collector Explanation

The `data_collecter/btc_trend_analysis.py` file:

- **Fetches 7 days** of historical OHLC data for all supported assets
- **Stores data** in PostgreSQL with proper timeframe formatting
- **Creates baseline** for ML model predictions
- **Populates** actual_prices table with historical data
- **Generates** initial forecast entries

### 3. What the Data Collector Does

```python
# The data collector performs these operations:
1. Connects to Binance API for crypto data (BTC, ETH, etc.)
2. Fetches Yahoo Finance data for stocks (NVDA, AAPL, etc.)
3. Generates macro indicator historical data
4. Stores OHLC data for multiple timeframes (1m, 5m, 15m, 1h, 4h, 1D, 1W)
5. Creates database entries with proper symbol_timeframe format
6. Validates data integrity and fills gaps
```

### 4. Expected Data Collection Output
```
✅ Collecting BTC data for 7 days...
✅ Stored 168 data points for BTC_1H
✅ Stored 1008 data points for BTC_15m
✅ Collecting ETH data for 7 days...
✅ Stored 168 data points for ETH_1H
...
✅ Data collection completed for all 25 assets
```

### 5. Verify Data Collection
```bash
# Check if data was stored correctly
python -c "
import asyncio
from database import TradingDatabase
import os
from dotenv import load_dotenv

async def check_data():
    load_dotenv()
    db = TradingDatabase()
    await db.connect()
    
    async with db.pool.acquire() as conn:
        count = await conn.fetchval('SELECT COUNT(*) FROM actual_prices')
        symbols = await conn.fetch('SELECT DISTINCT symbol FROM actual_prices LIMIT 10')
        print(f'Total data points: {count}')
        print(f'Sample symbols: {[row[0] for row in symbols]}')

asyncio.run(check_data())
"
```

## 🚀 Running the Application

### 1. Start the Server
```bash
# Make sure you're in the project root directory
python main.py

# Or using uvicorn directly
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### 2. Verify Server is Running
```bash
# Check health endpoint
curl http://localhost:8000/api/health

# Expected response:
{
  "status": "healthy",
  "services": {
    "database": "connected",
    "redis": "connected",
    "ml_model": "operational"
  }
}
```

## 🔗 API Endpoints

### REST API
- `GET /api/market/summary` - Market overview
- `GET /api/asset/{symbol}/forecast` - Asset predictions
- `GET /api/asset/{symbol}/trends` - Historical accuracy
- `GET /api/assets/search` - Search assets
- `GET /api/health` - System health

### WebSocket Endpoints
- `ws://localhost:8000/ws/asset/{symbol}/forecast` - Real-time forecasts
- `ws://localhost:8000/ws/asset/{symbol}/trends` - Live trends
- `ws://localhost:8000/ws/market/summary` - Market updates

## 🧪 Testing the Setup

### 1. Test Market Summary
```bash
curl "http://localhost:8000/api/market/summary?class=crypto&limit=5"
```

### 2. Test Asset Forecast
```bash
curl "http://localhost:8000/api/asset/BTC/forecast?timeframe=1D"
```

### 3. Test WebSocket (using wscat)
```bash
# Install wscat
npm install -g wscat

# Test WebSocket connection
wscat -c ws://localhost:8000/ws/asset/BTC/forecast
```

## 🔧 Troubleshooting

### Common Issues

#### 1. Database Connection Failed
```
❌ Database connection failed: connection refused
```
**Solution**: 
- Ensure PostgreSQL is running
- Check DATABASE_URL in .env
- Verify database exists

#### 2. Redis Connection Failed
```
⚠️ Redis not available, using memory cache
```
**Solution**:
- Start Redis server: `redis-server`
- Check REDIS_HOST and REDIS_PORT in .env

#### 3. ML Model Not Found
```
❌ CRITICAL: Cannot start: [Errno 2] No such file or directory: 'models/specialized_trading_model.pkl'
```
**Solution**:
- Ensure the model file exists in `models/` directory
- Check if the model was included in the repository

#### 4. No Historical Data
```
❌ No database data available for BTC 1D
```
**Solution**:
- Run the data collector: `python data_collecter/btc_trend_analysis.py`
- Wait for data collection to complete
- Restart the application

#### 5. API Rate Limiting
```
❌ Rate limited for NVDA, will retry later
```
**Solution**:
- Add API keys to .env file
- Reduce request frequency
- Use multiple API providers

### Debug Mode

Enable detailed logging by setting:
```python
# In main.py, change logging level
logging.basicConfig(level=logging.DEBUG, format='%(message)s')
```

## 📁 Project Structure

```
TradingApp/
├── main.py                 # FastAPI application entry point
├── database.py             # Database operations
├── requirements.txt        # Python dependencies
├── .env                   # Environment variables
├── .env.example           # Environment template
├── config/
│   └── settings.py        # Configuration management
├── modules/
│   ├── api_routes.py      # API endpoints
│   ├── ml_predictor.py    # ML prediction engine
│   ├── rate_limiter.py    # Rate limiting
│   └── accuracy_validator.py
├── models/
│   └── specialized_trading_model.pkl  # Pre-trained ML model
├── data_collecter/
│   └── btc_trend_analysis.py  # Historical data collector
├── realtime_websocket_service.py  # Crypto WebSocket streams
├── stock_realtime_service.py     # Stock real-time service
├── multi_asset_support.py        # Multi-asset data fetching
├── async_task_manager.py         # Task management
└── gap_filling_service.py        # Data gap filling
```

## 🔒 Security Notes

- Never commit `.env` file to version control
- Use strong passwords for database
- Consider using environment-specific configurations
- Enable Redis authentication in production
- Use HTTPS in production environments

## 📈 Performance Optimization

- **Database**: Connection pooling enabled (5-20 connections)
- **Redis**: Multi-database setup for different cache types
- **WebSocket**: Connection pooling and efficient broadcasting
- **API**: Rate limiting and response caching
- **ML**: Prediction caching with TTL

## ⚡ Quick Start Checklist

- [ ] Install Python 3.8+, PostgreSQL, Redis
- [ ] Clone repository and create virtual environment
- [ ] Install dependencies: `pip install -r requirements.txt`
- [ ] Copy `.env.example` to `.env` and configure variables
- [ ] Create PostgreSQL database
- [ ] Start Redis server
- [ ] **Run data collector**: `python data_collecter/btc_trend_analysis.py`
- [ ] Start application: `python main.py`
- [ ] Test health endpoint: `curl http://localhost:8000/api/health`
- [ ] Test API endpoints and WebSocket connections

**🎉 Your Trading AI Platform is now ready!**