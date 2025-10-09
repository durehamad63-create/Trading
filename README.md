# Trading AI Platform

A real-time financial forecasting system with ML-powered predictions for cryptocurrencies, stocks, and macro indicators through REST APIs and WebSocket connections.

## 🚀 Features

- **Real-time ML Predictions** using XGBoost model for 25+ assets
- **WebSocket Streaming** for live price updates and forecasts
- **Historical Accuracy Tracking** with trend analysis
- **Multi-level Caching** (Redis + Memory) for optimal performance
- **PostgreSQL Database** with connection pooling
- **Rate Limiting** and comprehensive error handling
- **CSV Export** functionality for historical data
- **Docker Support** for containerized deployment

## 📋 Supported Assets

### Cryptocurrencies (10)
BTC, ETH, USDT, XRP, BNB, SOL, USDC, DOGE, ADA, TRX

### Stocks (10) 
NVDA, MSFT, AAPL, GOOGL, AMZN, META, AVGO, TSLA, BRK-B, JPM

### Macro Indicators (5)
GDP, CPI, UNEMPLOYMENT, FED_RATE, CONSUMER_CONFIDENCE

## 🛠️ Prerequisites

- **Python 3.11+**
- **PostgreSQL 12+**
- **Redis 6+**
- **Git**

## 📦 Installation

### 1. Clone Repository
```bash
git clone https://github.com/ds1183406-create/Trading_app.git
cd Trading_app
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
```bash
cp .env.example .env
```

### 2. Configure .env Variables

```env
# Database Configuration
DATABASE_URL=postgresql://postgres:your_password@localhost:5432/trading_db

# Redis Configuration
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_PASSWORD=

# API Keys (Optional)
FRED_API_KEY=YOUR_FRED_API_KEY

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

# ML Model
MODEL_PATH=models/specialized_trading_model.pkl
```

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

## 🚀 Running the Application

### 1. Start the Server
```bash
python main.py
```

### 2. Verify Server is Running
```bash
# Check health endpoint
curl http://localhost:8000/api/health
```

## 🔗 API Endpoints

### REST API
- `GET /api/market/summary` - Market overview with class filtering
- `GET /api/asset/{symbol}/forecast` - Asset predictions with chart data
- `GET /api/asset/{symbol}/trends` - Historical accuracy analysis
- `GET /api/assets/search` - Search available assets
- `GET /api/health` - System health check

### WebSocket Endpoints
- `ws://localhost:8000/ws/asset/{symbol}/forecast` - Real-time forecasts
- `ws://localhost:8000/ws/asset/{symbol}/trends` - Live trends
- `ws://localhost:8000/ws/market/summary` - Market updates
- `ws://localhost:8000/ws/chart/{symbol}` - Enhanced chart data

## 🧪 Testing

### 1. API Testing
Open `test_api.html` in your browser for interactive API testing.

### 2. Test Market Summary
```bash
curl "http://localhost:8000/api/market/summary?class=crypto&limit=5"
```

### 3. Test Asset Forecast
```bash
curl "http://localhost:8000/api/asset/BTC/forecast?timeframe=1D"
```

## 🐳 Docker Deployment

### 1. Build Image
```bash
docker build -t trading-ai .
```

### 2. Run Container
```bash
docker run -p 8000:8000 --env-file .env trading-ai
```

## 📁 Project Structure

```
Trading_app/
├── main.py                    # FastAPI application entry point
├── database.py                # Database operations
├── requirements.txt           # Python dependencies
├── Dockerfile                 # Docker configuration
├── .env                      # Environment variables
├── config/
│   ├── settings.py           # Configuration management
│   └── symbols.py            # Symbol definitions
├── modules/
│   ├── api_routes.py         # API endpoints
│   ├── ml_predictor.py       # ML prediction engine
│   ├── rate_limiter.py       # Rate limiting
│   └── accuracy_validator.py # Accuracy validation
├── models/
│   └── specialized_trading_model.pkl  # Pre-trained XGBoost model
├── utils/
│   ├── api_client.py         # External API client
│   ├── cache_manager.py      # Redis cache management
│   ├── database_manager.py   # Database utilities
│   ├── error_handler.py      # Error handling
│   └── timestamp_utils.py    # Time utilities
├── realtime_websocket_service.py     # Crypto WebSocket streams
├── stock_realtime_service.py         # Stock real-time service
├── macro_realtime_service.py         # Macro indicators service
├── multi_asset_support.py            # Multi-asset data fetching
├── async_task_manager.py             # Task management
├── gap_filling_service.py            # Data gap filling
├── test_api.html                     # API testing interface
└── test_trends_local.html            # Trends testing interface
```

## 🔧 Key Components

### ML Model
- **XGBoost-based** specialized trading model
- **Real-time predictions** with deterministic seeds
- **Multi-asset support** for crypto, stocks, and macro indicators
- **Confidence scoring** based on market volatility

### Caching System
- **Redis** for distributed caching
- **Memory fallback** for high availability
- **Hot symbol prioritization** (BTC, ETH, NVDA, AAPL)
- **TTL-based expiration** for fresh data

### WebSocket Services
- **Real-time price streams** from Binance and Yahoo Finance
- **Connection pooling** for efficient resource usage
- **Automatic reconnection** and error handling
- **Multi-timeframe support** (1h, 4H, 1D, 7D, 1W, 1M)

## 🔒 Security Features

- **Rate limiting** with configurable thresholds
- **CORS middleware** for cross-origin requests
- **Trusted host middleware** for security
- **Error handling** without sensitive data exposure

## 📈 Performance Optimization

- **Connection pooling** for database (5-20 connections)
- **Multi-database Redis** setup for different cache types
- **Async/await** throughout for non-blocking operations
- **Background task management** for real-time streams
- **Deterministic caching** for consistent responses

## 🔧 Troubleshooting

### Common Issues

#### 1. Model Download Failed
```
❌ Cannot start: Model file not found
```
**Solution**: The model will auto-download from Google Drive on first run.

#### 2. Database Connection Failed
```
❌ Database connection failed: connection refused
```
**Solution**: 
- Ensure PostgreSQL is running
- Check DATABASE_URL in .env
- Verify database exists

#### 3. Redis Connection Failed
```
⚠️ Redis not available, using memory cache
```
**Solution**:
- Start Redis server: `redis-server`
- Check REDIS_HOST and REDIS_PORT in .env

## ⚡ Quick Start Checklist

- [ ] Install Python 3.11+, PostgreSQL, Redis
- [ ] Clone repository and create virtual environment
- [ ] Install dependencies: `pip install -r requirements.txt`
- [ ] Configure `.env` file with database and Redis settings
- [ ] Create PostgreSQL database
- [ ] Start Redis server
- [ ] Run application: `python main.py`
- [ ] Test health endpoint: `curl http://localhost:8000/api/health`
- [ ] Open `test_api.html` for interactive testing

**🎉 Your Trading AI Platform is now ready!**

## 📊 API Examples

### Market Summary
```bash
# Get crypto market summary
curl "http://localhost:8000/api/market/summary?class=crypto&limit=5"

# Get stock market summary  
curl "http://localhost:8000/api/market/summary?class=stocks&limit=5"

# Get macro indicators
curl "http://localhost:8000/api/market/summary?class=macro&limit=5"
```

### Asset Forecasts
```bash
# Get BTC forecast with chart data
curl "http://localhost:8000/api/asset/BTC/forecast?timeframe=1D"

# Get NVDA stock forecast
curl "http://localhost:8000/api/asset/NVDA/forecast?timeframe=1D"
```

### Historical Trends
```bash
# Get BTC accuracy trends
curl "http://localhost:8000/api/asset/BTC/trends?timeframe=7D"
```