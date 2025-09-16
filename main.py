"""
Modular Mobile Trading Server
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
import logging
import sys
import os
import numpy as np
from contextlib import asynccontextmanager

# Set up detailed logging for predictions
logging.basicConfig(level=logging.INFO, format='%(message)s')
logging.getLogger('uvicorn.access').setLevel(logging.WARNING)

# Add modules to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'modules'))
sys.path.append(os.path.dirname(__file__))



from modules.ml_predictor import MobileMLModel
from modules.api_routes import setup_routes
try:
    from utils.database_manager import DatabaseManager
    db = DatabaseManager.get_instance()
except Exception as e:
    logging.error(f"❌ Database import failed: {e}")
    db = None

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logging.getLogger('uvicorn.access').setLevel(logging.WARNING)  # Reduce uvicorn noise



# Initialize ML model with validation
try:
    model = MobileMLModel()

except Exception as e:
    logging.error(f"❌ ML Model initialization failed: {e}")
    raise

from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize database and background tasks on startup"""
    # Database connection with connection pooling
    if db:
        try:
            await DatabaseManager.initialize()
            if db.pool:
                pass
            else:
                logging.error("❌ Database pool not created")
        except Exception as e:
            logging.error(f"❌ Database connection failed: {e}")
    
    # Start background services with task manager
    import asyncio
    background_tasks = []
    
    try:
        from async_task_manager import task_manager
        use_task_manager = True

    except ImportError:
        use_task_manager = False
        logging.warning("⚠️ Task manager not available, using direct async")
    
    # Initialize real-time services with shared model
    from realtime_websocket_service import RealTimeWebSocketService
    from stock_realtime_service import StockRealtimeService
    import realtime_websocket_service as rws_module
    import stock_realtime_service as stock_module
    import modules.api_routes as api_module
    
    # Crypto real-time service with database
    realtime_service = RealTimeWebSocketService(model, db)
    rws_module.realtime_service = realtime_service
    
    # Stock real-time service with database
    stock_service = StockRealtimeService(model, db)
    stock_module.stock_realtime_service = stock_service
    
    # Make both services available to API routes and globally
    api_module.realtime_service = realtime_service
    api_module.stock_service = stock_service
    api_module.stock_realtime_service = stock_service
    
    # Make services available globally
    import stock_realtime_service as stock_module
    stock_module.stock_realtime_service = stock_service
    rws_module.stock_realtime_service = stock_service
    
    # Start crypto streams
    if use_task_manager:
        task1 = await task_manager.run_background_task(
            "crypto_streams", 
            realtime_service.start_binance_streams
        )
        background_tasks.append(task1)
    else:
        task1 = asyncio.create_task(realtime_service.start_binance_streams())
        background_tasks.append(task1)
    
    # Start stock streams
    if use_task_manager:
        task2 = await task_manager.run_background_task(
            "stock_streams", 
            stock_service.start_stock_streams
        )
        background_tasks.append(task2)
    else:
        task2 = asyncio.create_task(stock_service.start_stock_streams())
        background_tasks.append(task2)

    
    # Gap filling service (low priority, background)
    if db and db.pool:
        from gap_filling_service import GapFillingService
        gap_filler = GapFillingService(model)  # Pass shared model
        if use_task_manager:
            task2 = await task_manager.run_background_task(
                "gap_filling", 
                gap_filler.fill_missing_data, 
                db
            )
            background_tasks.append(task2)

        else:
            task2 = asyncio.create_task(gap_filler.fill_missing_data(db))
            background_tasks.append(task2)

    
    # Store background tasks for cleanup
    app.state.background_tasks = background_tasks
    if use_task_manager:
        app.state.task_manager = task_manager
    
    yield
    
    # Cleanup on shutdown
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

# Setup all API routes with database
try:
    setup_routes(app, model, db)

except Exception as e:
    logging.error(f"❌ API routes setup failed: {e}")
    raise

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)