"""
Enhanced Configuration Settings
"""
import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    # Database Configuration
    DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:admin123@localhost:5432/trading_db')
    
    # Redis Configuration
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', '6379'))
    REDIS_DB = int(os.getenv('REDIS_DB', '0'))
    
    # API Configuration
    BINANCE_API_URL = os.getenv('BINANCE_API_URL', 'https://api.binance.com/api/v3')
    FRED_API_KEY = os.getenv('FRED_API_KEY', '')
    
    # Rate Limiting
    RATE_LIMIT_REQUESTS = int(os.getenv('RATE_LIMIT_REQUESTS', '100'))
    RATE_LIMIT_WINDOW = int(os.getenv('RATE_LIMIT_WINDOW', '60'))
    
    # ML Model Configuration
    MODEL_PATH = os.getenv('MODEL_PATH', 'models/trading_model_20250912_064124.pkl')
    PREDICTION_CACHE_TTL = int(os.getenv('PREDICTION_CACHE_TTL', '300'))
    
    # Asset Configuration
    CRYPTO_SYMBOLS = ['BTC', 'ETH', 'BNB', 'USDT', 'XRP', 'SOL', 'USDC', 'DOGE', 'ADA', 'TRX']
    STOCK_SYMBOLS = ['NVDA', 'MSFT', 'AAPL', 'GOOGL', 'AMZN', 'META', 'AVGO', 'TSLA', 'BRK-B', 'JPM']
    MACRO_SYMBOLS = ['GDP', 'CPI', 'UNEMPLOYMENT', 'FED_RATE', 'CONSUMER_CONFIDENCE']
    
    # WebSocket Configuration
    WS_HEARTBEAT_INTERVAL = int(os.getenv('WS_HEARTBEAT_INTERVAL', '30'))
    WS_FORECAST_INTERVAL = int(os.getenv('WS_FORECAST_INTERVAL', '15'))
    WS_TRENDS_INTERVAL = int(os.getenv('WS_TRENDS_INTERVAL', '60'))

settings = Settings()