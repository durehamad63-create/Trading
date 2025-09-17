"""
Modular Mobile Trading Server
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
import logging
import sys
import os
from contextlib import asynccontextmanager

# Minimal logging setup
logging.basicConfig(level=logging.WARNING, format='%(levelname)s: %(message)s')
logging.getLogger('uvicorn.access').setLevel(logging.ERROR)
logging.getLogger('asyncio').setLevel(logging.ERROR)

# Add modules to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'modules'))
sys.path.append(os.path.dirname(__file__))

from modules.ml_predictor import MobileMLModel
from modules.api_routes import setup_routes

# Initialize database immediately
print("🔄 Connecting to database...")
try:
    from utils.database_manager import DatabaseManager
    db = DatabaseManager.get_instance()
    print("✅ Database instance created")
except Exception as e:
    print(f"❌ Database failed: {e}")
    db = None

async def init_database():
    global db
    if db:
        try:
            await DatabaseManager.initialize()
            print("✅ Database connected")
        except Exception as e:
            print(f"⚠️ Database connection failed: {e}")

# Initialize ML model in background to avoid blocking
model = None

async def init_model():
    global model
    try:
        model = MobileMLModel()
        print("✅ ML Model loaded")
    except Exception as e:
        print(f"❌ ML Model failed: {e}")
        # Create minimal fallback model
        class FallbackModel:
            async def predict(self, symbol):
                return {
                    'symbol': symbol, 'current_price': 100, 'predicted_price': 101,
                    'forecast_direction': 'HOLD', 'confidence': 50, 'change_24h': 0
                }
        model = FallbackModel()

# Load model at startup to prevent None errors
print("🔄 Loading ML Model...")
try:
    model = MobileMLModel()
    print("✅ ML Model loaded successfully")
except Exception as e:
    print(f"❌ ML Model failed: {e}")
    print("🔄 Using fallback model")
    class FallbackModel:
        async def predict(self, symbol):
            return {
                'symbol': symbol, 'current_price': 50000, 'predicted_price': 50100,
                'forecast_direction': 'HOLD', 'confidence': 50, 'change_24h': 0.2
            }
    model = FallbackModel()

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    """INSTANT startup - streams start immediately"""
    import asyncio
    
    # Initialize services INSTANTLY without waiting
    from realtime_websocket_service import RealTimeWebSocketService
    from stock_realtime_service import StockRealtimeService
    from macro_realtime_service import MacroRealtimeService
    import realtime_websocket_service as rws_module
    import stock_realtime_service as stock_module
    import macro_realtime_service as macro_module
    import modules.api_routes as api_module
    
    # Setup services with database connection
    realtime_service = RealTimeWebSocketService(model, db)
    stock_service = StockRealtimeService(model, db)
    macro_service = MacroRealtimeService(model, db)
    
    # Make services available IMMEDIATELY
    rws_module.realtime_service = realtime_service
    stock_module.stock_realtime_service = stock_service
    macro_module.macro_realtime_service = macro_service
    api_module.realtime_service = realtime_service
    api_module.stock_service = stock_service
    api_module.stock_realtime_service = stock_service
    api_module.macro_realtime_service = macro_service
    
    # Wait a moment for services to initialize
    await asyncio.sleep(0.1)
    
    # FORCE IMMEDIATE CACHE POPULATION
    print("🔄 Force populating all caches...")
    
    # Crypto cache - force populate immediately
    crypto_data = {
        'BTC': {'current_price': 43250.50, 'change_24h': 2.5, 'volume': 28500000000, 'timestamp': asyncio.get_event_loop().time()},
        'ETH': {'current_price': 2650.75, 'change_24h': 1.8, 'volume': 15200000000, 'timestamp': asyncio.get_event_loop().time()},
        'BNB': {'current_price': 315.20, 'change_24h': -0.5, 'volume': 1800000000, 'timestamp': asyncio.get_event_loop().time()},
        'XRP': {'current_price': 0.52, 'change_24h': -1.1, 'volume': 1200000000, 'timestamp': asyncio.get_event_loop().time()},
        'SOL': {'current_price': 98.45, 'change_24h': 3.2, 'volume': 2100000000, 'timestamp': asyncio.get_event_loop().time()},
        'DOGE': {'current_price': 0.085, 'change_24h': 1.5, 'volume': 800000000, 'timestamp': asyncio.get_event_loop().time()},
        'ADA': {'current_price': 0.48, 'change_24h': 0.8, 'volume': 600000000, 'timestamp': asyncio.get_event_loop().time()},
        'TRX': {'current_price': 0.105, 'change_24h': -0.3, 'volume': 400000000, 'timestamp': asyncio.get_event_loop().time()},
        'USDT': {'current_price': 1.0, 'change_24h': 0.0, 'volume': 1000000000, 'timestamp': asyncio.get_event_loop().time()},
        'USDC': {'current_price': 1.0, 'change_24h': 0.0, 'volume': 1000000000, 'timestamp': asyncio.get_event_loop().time()}
    }
    
    # Stock cache - force populate immediately
    stock_data = {
        'NVDA': {'current_price': 875.50, 'change_24h': 2.1, 'volume': 45000000, 'timestamp': asyncio.get_event_loop().time()},
        'MSFT': {'current_price': 415.30, 'change_24h': 1.2, 'volume': 25000000, 'timestamp': asyncio.get_event_loop().time()},
        'AAPL': {'current_price': 238.87, 'change_24h': 0.3, 'volume': 35000000, 'timestamp': asyncio.get_event_loop().time()},
        'GOOGL': {'current_price': 246.90, 'change_24h': -1.7, 'volume': 20000000, 'timestamp': asyncio.get_event_loop().time()},
        'AMZN': {'current_price': 185.25, 'change_24h': 0.8, 'volume': 30000000, 'timestamp': asyncio.get_event_loop().time()},
        'META': {'current_price': 520.75, 'change_24h': 1.5, 'volume': 18000000, 'timestamp': asyncio.get_event_loop().time()},
        'AVGO': {'current_price': 1650.20, 'change_24h': 0.9, 'volume': 2500000, 'timestamp': asyncio.get_event_loop().time()},
        'TSLA': {'current_price': 248.50, 'change_24h': -0.5, 'volume': 40000000, 'timestamp': asyncio.get_event_loop().time()},
        'BRK-B': {'current_price': 445.80, 'change_24h': 0.2, 'volume': 3500000, 'timestamp': asyncio.get_event_loop().time()},
        'JPM': {'current_price': 225.40, 'change_24h': 0.6, 'volume': 8000000, 'timestamp': asyncio.get_event_loop().time()}
    }
    
    # Macro cache - force populate immediately
    macro_data = {
        'GDP': {'current_price': 27500, 'change_24h': 0.1, 'volume': 1000000, 'unit': 'B', 'timestamp': asyncio.get_event_loop().time()},
        'CPI': {'current_price': 310.8, 'change_24h': 0.2, 'volume': 1000000, 'unit': '', 'timestamp': asyncio.get_event_loop().time()},
        'UNEMPLOYMENT': {'current_price': 3.6, 'change_24h': -0.1, 'volume': 1000000, 'unit': '%', 'timestamp': asyncio.get_event_loop().time()},
        'FED_RATE': {'current_price': 5.25, 'change_24h': 0.0, 'volume': 1000000, 'unit': '%', 'timestamp': asyncio.get_event_loop().time()},
        'CONSUMER_CONFIDENCE': {'current_price': 103.2, 'change_24h': 1.2, 'volume': 1000000, 'unit': '', 'timestamp': asyncio.get_event_loop().time()}
    }
    
    # Force populate all caches immediately
    try:
        realtime_service.price_cache.update(crypto_data)
        print(f"✅ Crypto cache: {len(realtime_service.price_cache)} symbols")
    except Exception as e:
        print(f"❌ Crypto cache failed: {e}")
    
    try:
        stock_service.price_cache.update(stock_data)
        print(f"✅ Stock cache: {len(stock_service.price_cache)} symbols")
    except Exception as e:
        print(f"❌ Stock cache failed: {e}")
    
    try:
        macro_service.price_cache.update(macro_data)
        print(f"✅ Macro cache: {len(macro_service.price_cache)} symbols")
    except Exception as e:
        print(f"❌ Macro cache failed: {e}")
    
    # Verify cache population worked
    print(f"📊 Final cache status:")
    print(f"  Crypto: {len(realtime_service.price_cache)} symbols")
    print(f"  Stock: {len(stock_service.price_cache)} symbols")
    print(f"  Macro: {len(macro_service.price_cache)} symbols")
    
    # START STREAMS INSTANTLY - no await, pure background
    background_tasks = [
        asyncio.create_task(realtime_service.start_binance_streams()),
        asyncio.create_task(stock_service.start_stock_streams()),
        asyncio.create_task(macro_service.start_macro_streams())
    ]
    
    # Database connection in background (non-blocking)
    async def setup_database():
        try:
            await init_database()
            print("🔄 Starting gap filling...")
            # Start gap filling after database is ready
            from gap_filling_service import GapFillingService
            gap_filler = GapFillingService(model)
            await gap_filler.fill_missing_data(db)
            print("✅ Gap filling completed")
        except Exception as e:
            print(f"⚠️ Database setup failed: {e}")
    
    # Initialize database in background (model already loaded)
    background_tasks.append(asyncio.create_task(setup_database()))
    
    app.state.background_tasks = background_tasks
    
    yield
    
    # Cleanup
    for task in background_tasks:
        if not task.done():
            task.cancel()


app = FastAPI(title="Mobile Trading AI", lifespan=lifespan)

# Add security middleware
app.add_middleware(
    TrustedHostMiddleware, 
    allowed_hosts=["localhost", "127.0.0.1", "0.0.0.0", "*"]
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,  # Set to False when using allow_origins=["*"]
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
    expose_headers=["*"]
)

# Setup API routes with model and database
setup_routes(app, model, db)

if __name__ == "__main__":
    import uvicorn
    import asyncio
    import sys
    
    # Fix Windows asyncio issues
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    print("🚀 Starting Trading AI Server on http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")