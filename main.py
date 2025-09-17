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