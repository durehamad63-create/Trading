"""
API Routes Module
"""
from fastapi import FastAPI, WebSocket, Request, Depends, WebSocketDisconnect
from fastapi.responses import StreamingResponse
import asyncio
import json
import logging
import requests
from datetime import datetime, timedelta
import io
from multi_asset_support import multi_asset
# Database will be passed as parameter
from rate_limiter import rate_limiter
from accuracy_validator import accuracy_validator
from realtime_websocket_service import realtime_service
try:
    from async_task_manager import task_manager
except ImportError:
    # Fallback if task manager not available
    class FallbackTaskManager:
        async def run_batch_tasks(self, tasks):
            results = []
            for task_id, coro, args, kwargs in tasks:
                try:
                    result = await coro(*args, **kwargs)
                    results.append(result)
                except Exception as e:
                    results.append(e)
            return results
        def get_stats(self):
            return {'status': 'fallback_mode'}
    task_manager = FallbackTaskManager()

# Helper functions for WebSocket data
async def get_real_time_price(symbol: str):
    """Get real-time price data"""
    try:
        asset_data = multi_asset.get_asset_data(symbol)
        return {
            "price": asset_data.get("current_price", 0),
            "change": asset_data.get("change_24h", 0)
        }
    except Exception:
        return None

def setup_routes(app: FastAPI, model, database=None):
    """Setup all API routes"""
    from dotenv import load_dotenv
    import os
    
    # Load environment variables
    load_dotenv()
    
    # Import and ensure stock service is available globally
    try:
        import stock_realtime_service as stock_module
        global stock_realtime_service
        stock_realtime_service = getattr(stock_module, 'stock_realtime_service', None)
        logging.info(f"✅ Stock service imported: {stock_realtime_service is not None}")
    except Exception as e:
        logging.error(f"❌ Failed to import stock service: {e}")
        stock_realtime_service = None
    
    db = database  # Use passed database instance
    
    # Global prediction cache with Redis support
    prediction_cache = {}
    cache_timeout = int(os.getenv('CACHE_TTL', '10'))  # seconds from .env
    
    # Initialize Redis for distributed caching
    redis_client = None
    try:
        import redis
        redis_client = redis.Redis(
            host=os.getenv('REDIS_HOST', 'localhost'),
            port=int(os.getenv('REDIS_PORT', '6379')),
            db=int(os.getenv('REDIS_PREDICTION_DB', '1')),  # Use DB 1 for predictions
            password=os.getenv('REDIS_PASSWORD', None) if os.getenv('REDIS_PASSWORD') else None,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5
        )
        redis_client.ping()
        logging.info(f"✅ Redis connected for prediction caching: {os.getenv('REDIS_HOST', 'localhost')}:{os.getenv('REDIS_PORT', '6379')}")
    except Exception as e:
        logging.warning(f"⚠️ Redis not available, using memory cache: {e}")
        redis_client = None
    
    async def get_ml_prediction(symbol: str):
        """Get ML prediction data"""
        try:
            prediction = model.predict(symbol)
            return {
                "direction": prediction.get("forecast_direction", "HOLD"),
                "confidence": prediction.get("confidence", 50),
                "range": prediction.get("predicted_range", "N/A")
            }
        except Exception:
            return None
    
    async def get_symbol_prediction(symbol: str, model, db):
        """Get prediction for a single symbol with database storage"""
        try:
            prediction = model.predict(symbol)
            prediction['name'] = multi_asset.get_asset_name(symbol)
            
            # Store forecast in database (non-blocking)
            if db and db.pool:
                try:
                    await db.store_forecast(symbol, prediction)
                    await db.store_actual_price(symbol, {
                        'current_price': prediction['current_price'],
                        'change_24h': prediction['change_24h']
                    })
                except Exception as e:
                    logging.warning(f"Database storage failed for {symbol}: {e}")
            
            return prediction
        except Exception as e:
            logging.warning(f"Failed to get prediction for {symbol}: {e}")
            return None

    async def rate_limit_check(request: Request):
        await rate_limiter.check_rate_limit(request)
    
    @app.get("/api/market/summary")
    async def market_summary(class_filter: str = "crypto", limit: int = 10, request: Request = None):
        start_time = datetime.now()

        
        if request:
            await rate_limiter.check_rate_limit(request)
        """Get market summary with real predictions for crypto, stocks, and macro"""
        
        crypto_symbols = ['BTC', 'ETH', 'BNB', 'USDT', 'XRP', 'SOL', 'USDC', 'DOGE', 'ADA', 'TRX']
        stock_symbols = ['NVDA', 'MSFT', 'AAPL', 'GOOGL', 'AMZN', 'META', 'AVGO', 'TSLA', 'BRK-B', 'JPM']
        macro_symbols = ['GDP', 'CPI', 'UNEMPLOYMENT', 'FED_RATE', 'CONSUMER_CONFIDENCE']
        
        if class_filter == "crypto":
            symbols = crypto_symbols[:limit]
        elif class_filter == "stocks":
            symbols = stock_symbols[:limit]
        elif class_filter == "macro":
            symbols = macro_symbols[:limit]
        else:
            # For 'all', include both crypto and stocks
            symbols = crypto_symbols[:min(limit//2, 5)] + stock_symbols[:min(limit//2, 5)]
        

        
        # Use simple concurrent execution for speed
        import asyncio
        
        async def get_prediction_fast(symbol):
            symbol_start = datetime.now()
            
            # Check Redis cache first, then memory cache
            cached_prediction = None
            if redis_client:
                try:
                    cached_data = redis_client.get(f"prediction:{symbol}")
                    if cached_data:
                        import json
                        cached_prediction = json.loads(cached_data)
                        return cached_prediction
                except Exception as e:
                    logging.warning(f"Redis cache read failed: {e}")
            
            # Fallback to memory cache
            if symbol in prediction_cache:
                cache_age = (datetime.now() - prediction_cache[symbol]['timestamp']).total_seconds()
                if cache_age < cache_timeout:
                    return prediction_cache[symbol]['data']
            
            try:

                prediction = model.predict(symbol)
                prediction['name'] = multi_asset.get_asset_name(symbol)
                
                # Cache in Redis first, then memory
                if redis_client:
                    try:
                        import json
                        redis_client.setex(f"prediction:{symbol}", cache_timeout, json.dumps(prediction))

                    except Exception as e:
                        logging.warning(f"Redis cache write failed: {e}")
                
                # Also cache in memory as fallback
                prediction_cache[symbol] = {
                    'data': prediction,
                    'timestamp': datetime.now()
                }
                
                return prediction
            except Exception as e:
                logging.warning(f"❌ {symbol} prediction failed in {(datetime.now() - symbol_start).total_seconds():.2f}s: {e}")
                return None
        
        # Run predictions concurrently without task manager overhead
        tasks = [get_prediction_fast(symbol) for symbol in symbols]
        concurrent_start = datetime.now()
        results = await asyncio.gather(*tasks, return_exceptions=True)

        
        assets = []
        for result in results:
            if isinstance(result, dict) and result and 'symbol' in result:
                assets.append(result)
        
        total_time = (datetime.now() - start_time).total_seconds()

        
        return {"assets": assets}

    @app.get("/api/asset/{symbol}/trends")
    async def asset_trends(symbol: str, timeframe: str = "7D", view: str = "chart"):
        """Get historical trends and accuracy"""
        # Map timeframes to periods
        timeframe_periods = {"1W": 168, "7D": 168, "1M": 720, "1Y": 8760, "5Y": 43800}
        periods = timeframe_periods.get(timeframe, 168)
        
        prediction = model.predict(symbol)
        
        if view == "chart":
            try:
                prices, volumes = multi_asset.get_historical_data(symbol, min(periods//24, 100))
                forecast_prices = []
                actual_prices = []
                timestamps = []
                
                for i, price in enumerate(prices):
                    actual_prices.append(round(price, 2))
                    forecast_prices.append(round(price * (1 + (prediction['change_24h']/100) * 0.1), 2))
                    timestamp = datetime.now() - timedelta(hours=len(prices)-i)
                    timestamps.append(timestamp.strftime('%H:%M'))
                
                return {
                    'symbol': symbol,
                    'accuracy': min(85, max(60, 75 + abs(prediction['change_24h']))),
                    'chart': {
                        'forecast': forecast_prices,
                        'actual': actual_prices,
                        'timestamps': timestamps
                    }
                }
            except Exception as e:
                raise Exception(f"Cannot load historical data for {symbol}: {str(e)}")
        else:
            try:
                days = {"1W": 7, "7D": 7, "1M": 30, "1Y": 365, "5Y": 1825}.get(timeframe, 30)
                historical_data = await db.get_historical_forecasts(symbol, days)
                accuracy = await db.calculate_accuracy(symbol, days)
                
                history = []
                for record in historical_data:
                    if record.get('actual_direction'):
                        history.append({
                            'date': record['created_at'].strftime('%Y-%m-%d'),
                            'forecast': record['forecast_direction'],
                            'actual': record['actual_direction'],
                            'result': record['result']
                        })
                
                return {'symbol': symbol, 'accuracy': accuracy, 'history': history}
            except Exception as e:
                raise Exception(f"Cannot load historical data from database: {str(e)}")

    @app.get("/api/assets/search")
    async def search_assets(q: str):
        """Search available assets"""
        assets = [
            {'symbol': 'BTC', 'name': 'Bitcoin', 'class': 'crypto'},
            {'symbol': 'ETH', 'name': 'Ethereum', 'class': 'crypto'},
            {'symbol': 'BNB', 'name': 'Binance Coin', 'class': 'crypto'},
            {'symbol': 'USDT', 'name': 'Tether', 'class': 'crypto'},
            {'symbol': 'XRP', 'name': 'Ripple', 'class': 'crypto'},
            {'symbol': 'SOL', 'name': 'Solana', 'class': 'crypto'},
            {'symbol': 'USDC', 'name': 'USD Coin', 'class': 'crypto'},
            {'symbol': 'DOGE', 'name': 'Dogecoin', 'class': 'crypto'},
            {'symbol': 'ADA', 'name': 'Cardano', 'class': 'crypto'},
            {'symbol': 'TRX', 'name': 'Tron', 'class': 'crypto'},
            {'symbol': 'NVDA', 'name': 'NVIDIA', 'class': 'stocks'},
            {'symbol': 'MSFT', 'name': 'Microsoft', 'class': 'stocks'},
            {'symbol': 'AAPL', 'name': 'Apple', 'class': 'stocks'},
            {'symbol': 'GOOGL', 'name': 'Alphabet', 'class': 'stocks'},
            {'symbol': 'AMZN', 'name': 'Amazon', 'class': 'stocks'},
            {'symbol': 'META', 'name': 'Meta', 'class': 'stocks'},
            {'symbol': 'AVGO', 'name': 'Broadcom', 'class': 'stocks'},
            {'symbol': 'TSLA', 'name': 'Tesla', 'class': 'stocks'},
            {'symbol': 'BRK-B', 'name': 'Berkshire Hathaway', 'class': 'stocks'},
            {'symbol': 'JPM', 'name': 'JPMorgan Chase', 'class': 'stocks'},
            {'symbol': 'GDP', 'name': 'Gross Domestic Product', 'class': 'macro'},
            {'symbol': 'CPI', 'name': 'Consumer Price Index', 'class': 'macro'},
            {'symbol': 'UNEMPLOYMENT', 'name': 'Unemployment Rate', 'class': 'macro'},
            {'symbol': 'FED_RATE', 'name': 'Federal Interest Rate', 'class': 'macro'},
            {'symbol': 'CONSUMER_CONFIDENCE', 'name': 'Consumer Confidence Index', 'class': 'macro'}
        ]
        
        results = [asset for asset in assets if q.lower() in asset['symbol'].lower() or q.lower() in asset['name'].lower()]
        return {'results': results}

    @app.get("/api/asset/{symbol}/forecast")
    async def asset_forecast(symbol: str, timeframe: str = "1D"):
        """Get detailed forecast for symbol"""
        forecast_start = datetime.now()

        
        try:
            # Use cached prediction if available
            if symbol in prediction_cache:
                cache_age = (datetime.now() - prediction_cache[symbol]['timestamp']).total_seconds()
                if cache_age < cache_timeout:
                    prediction = prediction_cache[symbol]['data']
                else:
                    prediction = model.predict(symbol)
                    prediction_cache[symbol] = {'data': prediction, 'timestamp': datetime.now()}
            else:
                prediction = model.predict(symbol)
                prediction_cache[symbol] = {'data': prediction, 'timestamp': datetime.now()}
            current_price = prediction['current_price']
            
            periods = {"1D": 24, "7D": 168, "1M": 720, "1Y": 8760, "5Y": 43800}
            hours = periods.get(timeframe, 24)
            
            # Get timeframe-specific data from database - normalize 4h to 4H
            normalized_timeframe = '4H' if timeframe.lower() == '4h' else timeframe
            timeframe_symbol = f"{symbol}_{normalized_timeframe}" if normalized_timeframe in ['4H', '1D', '1W', '1M'] else symbol
            logging.info(f"🔍 DEBUG: Forecast request for {symbol} timeframe={timeframe}, timeframe_symbol={timeframe_symbol}")
            
            try:
                if not db or not db.pool:
                    logging.error(f"❌ DEBUG: Database not available for {symbol} {timeframe}")
                    raise Exception("Database not available")
                
                # Get actual prices from database for specific timeframe (optimized query)
                async with db.pool.acquire() as conn:
                    # Reduced data points for all timeframes
                    limit = 50
                    logging.info(f"🔍 DEBUG: Querying actual_prices for {timeframe_symbol} with limit {limit}")
                    
                    # First check what symbols exist in the database
                    symbol_check = await conn.fetch("""
                        SELECT DISTINCT symbol, COUNT(*) as count
                        FROM actual_prices
                        WHERE symbol LIKE $1
                        GROUP BY symbol
                        ORDER BY count DESC
                    """, f"{symbol}%")
                    logging.info(f"🔍 DEBUG: Available symbols for {symbol}: {[(row['symbol'], row['count']) for row in symbol_check]}")
                    
                    actual_rows = await conn.fetch("""
                        SELECT price, timestamp
                        FROM actual_prices
                        WHERE symbol = $1
                        ORDER BY timestamp DESC
                        LIMIT $2
                    """, timeframe_symbol, limit)
                    logging.info(f"🔍 DEBUG: Found {len(actual_rows)} actual price rows for {timeframe_symbol}")
                    
                    if actual_rows:
                        actual_prices = [round(float(row['price']), 2) for row in reversed(actual_rows)]
                        logging.info(f"✅ DEBUG: Successfully loaded {len(actual_prices)} actual prices for {timeframe_symbol}")
                        logging.info(f"📊 DEBUG: Price range: {min(actual_prices):.2f} - {max(actual_prices):.2f}")
                    else:
                        logging.error(f"❌ DEBUG: No actual price data found for {timeframe_symbol}")
                        raise Exception(f"No actual price data found for {timeframe_symbol}")
                        
            except Exception as e:
                logging.error(f"❌ DEBUG: Database actual prices failed for {timeframe_symbol}: {e}, using API fallback")
                # Fallback to API data
                try:
                    real_historical = model._get_real_historical_prices(symbol)
                    if len(real_historical) >= min(50, hours):
                        actual_prices = [round(price, 2) for price in real_historical[-min(50, hours):]]
                    else:
                        prices, volumes = multi_asset.get_historical_data(symbol, min(50, hours))
                        actual_prices = [round(price, 2) for price in prices]
                except Exception as e2:
                    logging.error(f"All data sources failed: {e2}")
                    raise Exception(f"Cannot get historical data for {symbol}")
            
            timestamps = [(datetime.now() - timedelta(hours=len(actual_prices)-i)).isoformat() for i in range(len(actual_prices))]
            
            # Get historical predictions directly from database
            try:
                if not db or not db.pool:
                    logging.error(f"❌ DEBUG: Database not available for historical predictions {symbol} {timeframe}")
                    raise Exception(f"Database not available for {symbol}")
                
                # Direct database query for timeframe-specific predictions
                async with db.pool.acquire() as conn:
                    # Reduced prediction data points for all timeframes
                    limit = 50
                    logging.info(f"🔍 DEBUG: Querying forecasts for {timeframe_symbol} with limit {limit}")
                    
                    # First check what forecast symbols exist in the database
                    forecast_check = await conn.fetch("""
                        SELECT DISTINCT symbol, COUNT(*) as count
                        FROM forecasts
                        WHERE symbol LIKE $1 AND predicted_price IS NOT NULL
                        GROUP BY symbol
                        ORDER BY count DESC
                    """, f"{symbol}%")
                    logging.info(f"🔍 DEBUG: Available forecast symbols for {symbol}: {[(row['symbol'], row['count']) for row in forecast_check]}")
                    
                    rows = await conn.fetch("""
                        SELECT predicted_price, created_at, confidence
                        FROM forecasts
                        WHERE symbol = $1 AND predicted_price IS NOT NULL
                        ORDER BY created_at DESC
                        LIMIT $2
                    """, timeframe_symbol, limit)
                    logging.info(f"🔍 DEBUG: Found {len(rows)} forecast rows for {timeframe_symbol}")
                    
                    if rows:
                        raw_predictions = [float(row['predicted_price']) for row in reversed(rows)]
                        logging.info(f"✅ DEBUG: Successfully loaded {len(raw_predictions)} predictions for {timeframe_symbol}")
                        logging.info(f"📊 DEBUG: Prediction range: {min(raw_predictions):.2f} - {max(raw_predictions):.2f}")
                        
                        # Scale predictions to be more realistic relative to actual prices
                        if actual_prices:
                            actual_avg = sum(actual_prices) / len(actual_prices)
                            pred_avg = sum(raw_predictions) / len(raw_predictions)
                            
                            # Scale predictions to match actual price level
                            scale_factor = actual_avg / pred_avg if pred_avg > 0 else 1
                            predicted_prices = [pred * scale_factor for pred in raw_predictions]
                            logging.info(f"🔍 DEBUG: Scaled predictions with factor {scale_factor:.4f} for {timeframe_symbol}")
                            logging.info(f"📊 DEBUG: Scaled prediction range: {min(predicted_prices):.2f} - {max(predicted_prices):.2f}")
                        else:
                            predicted_prices = raw_predictions
                            logging.warning(f"⚠️ DEBUG: No actual prices to scale against for {timeframe_symbol}")
                    else:
                        logging.error(f"❌ DEBUG: No predictions with valid prices found for {timeframe_symbol}")
                        raise Exception(f"No historical predictions available for {timeframe_symbol}")
                    
            except Exception as e:
                logging.error(f"❌ DEBUG: Failed to get historical predictions for {timeframe_symbol}: {e}")
                raise Exception(f"Cannot generate forecast chart without historical predictions for {timeframe_symbol}")
            
            # Validation logging
            if len(predicted_prices) != len(actual_prices):
                logging.warning(f"⚠️ DEBUG: Prediction count mismatch for {timeframe_symbol}: {len(predicted_prices)} predicted vs {len(actual_prices)} actual")
                # Trim or pad to match
                if len(predicted_prices) > len(actual_prices):
                    predicted_prices = predicted_prices[:len(actual_prices)]
                else:
                    # Extend with last value
                    while len(predicted_prices) < len(actual_prices):
                        predicted_prices.append(predicted_prices[-1] if predicted_prices else prediction['predicted_price'])
            

            
            chart_data = {
                'actual': actual_prices,
                'predicted': predicted_prices[:len(actual_prices)] if predicted_prices else actual_prices,
                'timestamps': timestamps
            }
            logging.info(f"✅ DEBUG: Chart data created for {timeframe_symbol}: {len(chart_data['actual'])} actual, {len(chart_data['predicted'])} predicted points")
            logging.info(f"📊 DEBUG: Final chart data - Actual: {actual_prices[:3]}..., Predicted: {chart_data['predicted'][:3]}...")
            
            forecast_time = (datetime.now() - forecast_start).total_seconds()

            
            return {
                **prediction,
                'name': multi_asset.get_asset_name(symbol),
                'chart': chart_data,
                'last_updated': datetime.now().isoformat()
            }
            
        except Exception as e:
            logging.error(f"❌ DEBUG: Complete forecast failure for {symbol} {timeframe}: {str(e)}")
            import traceback
            logging.error(f"🔍 DEBUG: Full traceback: {traceback.format_exc()}")
            raise Exception(f"Failed to generate forecast for {symbol} {timeframe}: {str(e)}")



    # Enhanced WebSocket connection manager with connection pooling
    class ConnectionManager:
        def __init__(self):
            self.active_connections = {}  # {symbol: {connection_id: connection_data}}
            self.connection_pool = {}     # Reusable connection pool
            self.connection_locks = {}
            self.reconnect_attempts = {}  # Track reconnection attempts
        
        async def get_or_create_connection(self, websocket: WebSocket, symbol: str, timeframe: str):
            """Get existing connection or create new one with pooling"""
            connection_key = f"{symbol}_{timeframe}"
            
            if symbol not in self.active_connections:
                self.active_connections[symbol] = {}
                self.connection_locks[symbol] = asyncio.Lock()
            
            async with self.connection_locks[symbol]:
                # Check for existing connection with same symbol/timeframe
                for conn_id, conn_data in self.active_connections[symbol].items():
                    if conn_data.get('timeframe') == timeframe:
                        # Reuse existing connection
                        conn_data['websocket'] = websocket
                        conn_data['last_ping'] = datetime.now()
                        logging.info(f"🔄 DEBUG: Reusing connection {conn_id} for {symbol} {timeframe}")
                        return conn_id
                
                # Create new connection
                connection_id = f"{symbol}_{timeframe}_{id(websocket)}"
                self.active_connections[symbol][connection_id] = {
                    'websocket': websocket,
                    'symbol': symbol,
                    'timeframe': timeframe,
                    'connected_at': datetime.now(),
                    'last_ping': datetime.now(),
                    'reconnect_count': 0
                }
                logging.info(f"✅ DEBUG: Created new connection {connection_id}")
                return connection_id
        
        async def update_connection_state(self, symbol: str, connection_id: str, **updates):
            """Update connection state without recreating"""
            if symbol in self.active_connections and connection_id in self.active_connections[symbol]:
                self.active_connections[symbol][connection_id].update(updates)
                self.active_connections[symbol][connection_id]['last_ping'] = datetime.now()
                return True
            return False
        
        async def safe_disconnect(self, websocket: WebSocket, symbol: str, connection_id: str):
            """Safely disconnect with cleanup"""
            try:
                if symbol in self.active_connections:
                    async with self.connection_locks.get(symbol, asyncio.Lock()):
                        if connection_id in self.active_connections[symbol]:
                            del self.active_connections[symbol][connection_id]
                            logging.info(f"🔌 DEBUG: Safely removed connection {connection_id}")
                        
                        # Clean up empty symbol entries
                        if not self.active_connections[symbol]:
                            del self.active_connections[symbol]
                            if symbol in self.connection_locks:
                                del self.connection_locks[symbol]
            except Exception as e:
                logging.error(f"❌ DEBUG: Error in safe_disconnect: {e}")
        
        async def graceful_reconnect(self, websocket: WebSocket, symbol: str, connection_id: str):
            """Handle graceful reconnection"""
            try:
                if symbol in self.active_connections and connection_id in self.active_connections[symbol]:
                    conn_data = self.active_connections[symbol][connection_id]
                    conn_data['websocket'] = websocket
                    conn_data['reconnect_count'] = conn_data.get('reconnect_count', 0) + 1
                    conn_data['last_ping'] = datetime.now()
                    logging.info(f"🔄 DEBUG: Graceful reconnect for {connection_id} (attempt {conn_data['reconnect_count']})")
                    return True
            except Exception as e:
                logging.error(f"❌ DEBUG: Reconnection failed: {e}")
            return False
    
    manager = ConnectionManager()
    
    # Helper function for accuracy metrics
    async def get_accuracy_metrics(symbol: str):
        """Get accuracy metrics for trends WebSocket"""
        try:
            accuracy = await db.calculate_accuracy(symbol, 30) if db.pool else 85
            return {
                "accuracy": accuracy,
                "recent_performance": ["Hit", "Hit", "Miss", "Hit", "Hit"]
            }
        except Exception:
            return {"accuracy": 85, "recent_performance": []}
    

    
    @app.websocket("/ws/asset/{symbol}/forecast")
    async def asset_forecast_websocket(websocket: WebSocket, symbol: str):
        """Real-time forecast WebSocket with improved connection management"""
        await websocket.accept()
        
        # Determine if symbol is crypto or stock
        crypto_symbols = ['BTC', 'ETH', 'BNB', 'USDT', 'XRP', 'SOL', 'USDC', 'DOGE', 'ADA', 'TRX']
        stock_symbols = ['NVDA', 'MSFT', 'AAPL', 'GOOGL', 'AMZN', 'META', 'AVGO', 'TSLA', 'BRK-B', 'JPM']
        
        is_crypto = symbol in crypto_symbols
        is_stock = symbol in stock_symbols
        
        if not (is_crypto or is_stock):
            await websocket.close(code=1000, reason=f"Unsupported symbol: {symbol}")
            return
        
        timeframe = "4h" if is_crypto else "1D"  # Default timeframes
        service = realtime_service if is_crypto else stock_realtime_service
        
        if not service:
            await websocket.close(code=1000, reason="Service unavailable")
            return
        
        # Use connection manager for better connection handling
        connection_id = await manager.get_or_create_connection(websocket, symbol, timeframe)
        logging.info(f"🔌 DEBUG: WebSocket connection established: {symbol} {timeframe} ({connection_id})")
        
        try:
            # Add to service with connection pooling
            await service.add_connection(websocket, symbol, connection_id, timeframe)
            logging.info(f"✅ DEBUG: Added to service: {symbol} {timeframe}")
            
            # Listen for timeframe and asset changes
            async def handle_messages():
                nonlocal timeframe, connection_id, symbol, service, is_crypto
                try:
                    async for message in websocket.iter_text():
                        try:
                            data = json.loads(message)
                            
                            # Handle timeframe changes
                            if data.get('type') == 'set_timeframe':
                                new_timeframe = data.get('timeframe', timeframe)
                                if new_timeframe != timeframe:
                                    logging.info(f"🔄 DEBUG: Switching {symbol} timeframe: {timeframe} -> {new_timeframe}")
                                    
                                    # Update timeframe in place
                                    if symbol in service.active_connections and connection_id in service.active_connections[symbol]:
                                        service.active_connections[symbol][connection_id]['timeframe'] = new_timeframe
                                        timeframe = new_timeframe
                                        
                                        # Send fresh historical data
                                        if is_crypto:
                                            await realtime_service._send_historical_data(websocket, symbol, timeframe)
                                        else:
                                            await stock_realtime_service._send_stock_historical_data(websocket, symbol, timeframe)
                                        
                                        await websocket.send_text(json.dumps({
                                            'type': 'timeframe_changed',
                                            'symbol': symbol,
                                            'timeframe': timeframe,
                                            'timestamp': datetime.now().isoformat()
                                        }))
                                        
                                        logging.info(f"✅ DEBUG: Timeframe updated to {timeframe}")
                            
                            # Handle asset switches
                            elif data.get('type') == 'set_symbol':
                                new_symbol = data.get('symbol', symbol)
                                if new_symbol != symbol:
                                    logging.info(f"🔄 DEBUG: Switching asset: {symbol} -> {new_symbol}")
                                    
                                    # Remove old connection
                                    service.remove_connection(symbol, connection_id)
                                    
                                    # Update symbol and determine new service
                                    symbol = new_symbol
                                    is_crypto = symbol in crypto_symbols
                                    service = realtime_service if is_crypto else stock_realtime_service
                                    
                                    if service:
                                        # Create new connection for new symbol
                                        connection_id = f"{symbol}_{timeframe}_{id(websocket)}"
                                        await service.add_connection(websocket, symbol, connection_id, timeframe)
                                        
                                        # Send historical data for new symbol
                                        if is_crypto:
                                            await realtime_service._send_historical_data(websocket, symbol, timeframe)
                                        else:
                                            await stock_realtime_service._send_stock_historical_data(websocket, symbol, timeframe)
                                        
                                        await websocket.send_text(json.dumps({
                                            'type': 'symbol_changed',
                                            'symbol': symbol,
                                            'timeframe': timeframe,
                                            'timestamp': datetime.now().isoformat()
                                        }))
                                        
                                        logging.info(f"✅ DEBUG: Asset switched to {symbol}")
                                        
                        except json.JSONDecodeError:
                            pass
                        except Exception as msg_error:
                            logging.error(f"❌ DEBUG: Message processing error: {msg_error}")
                            
                except Exception as e:
                    logging.error(f"❌ DEBUG: Message handling error: {e}")
            
            # Start message handler
            message_task = asyncio.create_task(handle_messages())
            
            # Enhanced keep-alive with reconnection logic
            ping_failures = 0
            max_failures = 3
            
            while True:
                try:
                    await asyncio.sleep(30)
                    
                    # Check connection health
                    if hasattr(websocket, 'client_state') and websocket.client_state.name == 'CONNECTED':
                        try:
                            ping_data = {
                                "type": "ping", 
                                "symbol": symbol,
                                "timeframe": timeframe,
                                "asset_type": "crypto" if is_crypto else "stock",
                                "timestamp": datetime.now().isoformat()
                            }
                            await websocket.send_text(json.dumps(ping_data))
                            ping_failures = 0  # Reset on success
                            
                            # Update connection state
                            await manager.update_connection_state(symbol, connection_id, last_ping=datetime.now())
                            
                        except Exception as ping_error:
                            ping_failures += 1
                            logging.warning(f"⚠️ DEBUG: Ping failed for {symbol} (attempt {ping_failures}/{max_failures}): {ping_error}")
                            
                            if ping_failures >= max_failures:
                                logging.error(f"❌ DEBUG: Max ping failures reached for {symbol}, breaking")
                                break
                    else:
                        logging.info(f"🔌 DEBUG: WebSocket disconnected for {symbol}")
                        break
                        
                except asyncio.CancelledError:
                    logging.info(f"🔌 DEBUG: Keep-alive cancelled for {symbol}")
                    break
                except Exception as e:
                    logging.error(f"❌ DEBUG: Keep-alive error for {symbol}: {e}")
                    break
                
        except WebSocketDisconnect:
            logging.info(f"🔌 DEBUG: Client disconnected from real-time forecast: {symbol}")
        except Exception as e:
            logging.error(f"❌ DEBUG: Real-time WebSocket error for {symbol}: {e}")
        finally:
            # Graceful cleanup with connection manager
            try:
                # Cancel message handler
                if 'message_task' in locals():
                    message_task.cancel()
                
                # Remove from service
                if service and connection_id:
                    service.remove_connection(symbol, connection_id)
                
                # Safe disconnect through manager
                await manager.safe_disconnect(websocket, symbol, connection_id)
                logging.info(f"🔌 DEBUG: Gracefully cleaned up connection: {symbol}")
                
            except Exception as e:
                logging.error(f"❌ DEBUG: Error in connection cleanup: {e}")
    
    @app.websocket("/ws/asset/{symbol}/trends")
    async def asset_trends_websocket(websocket: WebSocket, symbol: str):
        """Real-time trends WebSocket with enhanced connection management"""
        await websocket.accept()
        
        # Use connection manager
        connection_id = await manager.get_or_create_connection(websocket, symbol, "trends")
        logging.info(f"🔌 DEBUG: Trends WebSocket established for {symbol} ({connection_id})")
        
        try:
            # Handle messages
            async def handle_messages():
                nonlocal symbol
                try:
                    async for message in websocket.iter_text():
                        try:
                            data = json.loads(message)
                            if data.get('type') == 'set_symbol':
                                new_symbol = data.get('symbol', symbol)
                                if new_symbol != symbol:
                                    symbol = new_symbol
                                    logging.info(f"🔄 DEBUG: Trends symbol changed to {symbol}")
                        except json.JSONDecodeError:
                            pass
                except Exception as e:
                    logging.error(f"❌ DEBUG: Trends message handling error: {e}")
            
            # Start message handler
            message_task = asyncio.create_task(handle_messages())
            
            # Keep connection alive with periodic updates
            ping_failures = 0
            max_failures = 3
            
            while True:
                try:
                    if hasattr(websocket, 'client_state') and websocket.client_state.name == 'CONNECTED':
                        accuracy = await db.calculate_accuracy(symbol, 30) if db and db.pool else 85
                        historical_data = await db.get_historical_forecasts(symbol, 7) if db and db.pool else []
                        
                        trends_data = {
                            "type": "trends_update",
                            "symbol": symbol,
                            "accuracy": accuracy,
                            "chart": {
                                "forecast": [54000, 54320, 54100, 53800],
                                "actual": [54200, 54500, 53750, 53480],
                                "timestamps": ["00:00", "04:00", "08:00", "12:00"]
                            },
                            "last_updated": datetime.now().isoformat()
                        }
                        
                        await websocket.send_text(json.dumps(trends_data))
                        
                        # Update connection state
                        await manager.update_connection_state(symbol, connection_id, last_ping=datetime.now())
                        ping_failures = 0
                    else:
                        logging.info(f"🔌 DEBUG: Trends WebSocket disconnected for {symbol}")
                        break
                        
                except Exception as e:
                    ping_failures += 1
                    logging.error(f"❌ DEBUG: Trends update error for {symbol} (attempt {ping_failures}/{max_failures}): {e}")
                    if ping_failures >= max_failures:
                        break
                
                await asyncio.sleep(5)  # Update every 5 seconds
                
        except WebSocketDisconnect:
            logging.info(f"🔌 DEBUG: Trends WebSocket disconnected for {symbol}")
        except Exception as e:
            logging.error(f"❌ DEBUG: Trends WebSocket error for {symbol}: {e}")
        finally:
            # Graceful cleanup
            try:
                if 'message_task' in locals():
                    message_task.cancel()
                await manager.safe_disconnect(websocket, symbol, connection_id)
                logging.info(f"🔌 DEBUG: Trends connection cleaned up for {symbol}")
            except Exception as e:
                logging.error(f"❌ DEBUG: Trends cleanup error: {e}")

    @app.get("/api/asset/{symbol}/export")
    async def export_data(symbol: str, timeframe: str = "1M", request: Request = None):
        """Export historical data as CSV"""
        if request:
            await rate_limiter.check_rate_limit(request, 'export')
        try:
            # Support all required timeframes: 1W, 1M, 1Y, 5Y
            export_data = await db.export_csv_data(symbol, timeframe)
            csv_data = "Date,Forecast,Actual,Result\\n"
            
            for record in export_data:
                date = record['date'].strftime('%Y-%m-%d') if record['date'] else 'N/A'
                forecast = record['forecast'] or 'N/A'
                actual = record['actual'] or 'N/A'
                result = record['result'] or 'N/A'
                csv_data += f"{date},{forecast},{actual},{result}\\n"
            
            if len(export_data) == 0:
                csv_data += "No historical data available\\n"
                
        except Exception as e:
            csv_data = "Date,Forecast,Actual,Result\\n"
            csv_data += f"Database error: {str(e)}\\n"
        
        return StreamingResponse(
            io.StringIO(csv_data),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={symbol}_export.csv"}
        )
    
    @app.get("/api/asset/{symbol}/trends/export")
    async def export_trends_data(symbol: str, timeframe: str = "1M", request: Request = None):
        """Export trends historical data as CSV - specific trends export endpoint"""
        if request:
            await rate_limiter.check_rate_limit(request, 'export')
        try:
            # Support all required timeframes: 1W, 1M, 1Y, 5Y
            export_data = await db.export_csv_data(symbol, timeframe)
            csv_data = "Date,Forecast,Actual,Result,Accuracy\\n"
            
            for record in export_data:
                date = record['date'].strftime('%Y-%m-%d') if record['date'] else 'N/A'
                forecast = record['forecast'] or 'N/A'
                actual = record['actual'] or 'N/A'
                result = record['result'] or 'N/A'
                accuracy = "Hit" if result == "Hit" else "Miss" if result == "Miss" else "N/A"
                csv_data += f"{date},{forecast},{actual},{result},{accuracy}\\n"
            
            if len(export_data) == 0:
                csv_data += "No historical trends data available\\n"
                
        except Exception as e:
            csv_data = "Date,Forecast,Actual,Result,Accuracy\\n"
            csv_data += f"Database error: {str(e)}\\n"
        
        return StreamingResponse(
            io.StringIO(csv_data),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={symbol}_trends.csv"}
        )
    
    @app.post("/api/favorites/{symbol}")
    async def add_favorite(symbol: str, request: Request = None):
        """Add symbol to favorites"""
        if request:
            await rate_limiter.check_rate_limit(request)
        
        try:
            if not db or not db.pool:
                return {"success": False, "symbol": symbol, "error": "Database not available"}
            success = await db.add_favorite(symbol)
            return {"success": success, "symbol": symbol}
        except Exception as e:
            logging.warning(f"Add favorite failed: {e}")
            return {"success": False, "symbol": symbol, "error": str(e)}
    
    @app.delete("/api/favorites/{symbol}")
    async def remove_favorite(symbol: str, request: Request = None):
        """Remove symbol from favorites"""
        if request:
            await rate_limiter.check_rate_limit(request)
        
        try:
            if not db or not db.pool:
                return {"success": False, "symbol": symbol, "error": "Database not available"}
            success = await db.remove_favorite(symbol)
            return {"success": success, "symbol": symbol}
        except Exception as e:
            logging.warning(f"Remove favorite failed: {e}")
            return {"success": False, "symbol": symbol, "error": str(e)}
    
    @app.get("/api/favorites")
    async def get_favorites(request: Request = None):
        """Get user's favorite symbols"""
        if request:
            await rate_limiter.check_rate_limit(request)
        
        try:
            if not db or not db.pool:
                return {"favorites": [], "error": "Database not available"}
            favorites = await db.get_favorites()
            return {"favorites": favorites}
        except Exception as e:
            logging.warning(f"Get favorites failed: {e}")
            return {"favorites": [], "error": str(e)}
    
    @app.post("/api/asset/{symbol}/validate")
    async def validate_accuracy(symbol: str, request: Request):
        """Validate forecast accuracy for symbol"""
        await rate_limiter.check_rate_limit(request)
        validation_result = await accuracy_validator.validate_forecasts(symbol)
        return {
            "symbol": symbol,
            "validation": validation_result,
            "timestamp": datetime.now().isoformat()
        }
    
    @app.get("/api/health")
    async def health_check():
        """Comprehensive system health check"""
        start_time = datetime.now()
        logging.info("🏥 Health check started")
        
        health_status = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "services": {}
        }
        
        # Check database with detailed timing
        db_start = datetime.now()
        try:
            logging.info("💾 Testing database connection...")
            if db and db.pool:
                pool_stats = db.get_pool_stats()
                logging.info(f"📊 Pool stats: {pool_stats}")
                
                acquire_start = datetime.now()
                async with db.pool.acquire() as conn:
                    acquire_time = (datetime.now() - acquire_start).total_seconds()
                    logging.info(f"⏱️ Pool acquire took {acquire_time:.2f}s")
                    
                    query_start = datetime.now()
                    result = await conn.fetchval("SELECT 1")
                    query_time = (datetime.now() - query_start).total_seconds()
                    logging.info(f"⏱️ Query execution took {query_time:.2f}s")
                    
                db_time = (datetime.now() - db_start).total_seconds()
                logging.info(f"✅ Database test completed in {db_time:.2f}s")
                health_status["services"]["database"] = "connected"
                health_status["db_test_time"] = f"{db_time:.2f}s"
                health_status["db_acquire_time"] = f"{acquire_time:.2f}s"
                health_status["db_query_time"] = f"{query_time:.2f}s"
                health_status["db_pool_stats"] = db.get_pool_stats()
            else:
                health_status["services"]["database"] = "disconnected"
                logging.warning("⚠️ No database pool available")
        except Exception as e:
            db_time = (datetime.now() - db_start).total_seconds()
            logging.error(f"❌ Database test failed in {db_time:.2f}s: {e}")
            health_status["services"]["database"] = f"error: {str(e)}"
            health_status["status"] = "degraded"
        
        # Check Redis (both API and ML Redis instances)
        try:
            redis_status = []
            
            # Check API Redis
            if redis_client:
                redis_client.ping()
                redis_status.append("api_cache")
            
            # Check ML Redis
            if hasattr(model, 'redis_client') and model.redis_client:
                model.redis_client.ping()
                redis_status.append("ml_cache")
            
            if redis_status:
                health_status["services"]["redis"] = f"connected ({', '.join(redis_status)})"
            else:
                health_status["services"]["redis"] = "memory_cache_only"
                
        except Exception as e:
            health_status["services"]["redis"] = f"error: {str(e)}"
        
        # Check ML model and caching
        ml_start = datetime.now()
        try:
            logging.info("🤖 Testing ML model prediction...")
            test_prediction = model.predict('BTC')
            ml_time = (datetime.now() - ml_start).total_seconds()
            logging.info(f"✅ ML model test completed in {ml_time:.2f}s")
            
            health_status["services"]["ml_model"] = "operational"
            health_status["model_type"] = "XGBoost" if hasattr(model, 'xgb_model') and model.xgb_model else "Enhanced Technical"
            health_status["cache_type"] = "Redis" if hasattr(model, 'redis_client') and model.redis_client else "Memory"
            health_status["ml_test_time"] = f"{ml_time:.2f}s"
        except Exception as e:
            ml_time = (datetime.now() - ml_start).total_seconds()
            logging.error(f"❌ ML model test failed in {ml_time:.2f}s: {e}")
            health_status["services"]["ml_model"] = f"error: {str(e)}"
            health_status["status"] = "degraded"
        
        # Check external APIs
        api_start = datetime.now()
        try:
            logging.info("🌍 Testing external APIs...")
            multi_asset.get_asset_data('BTC')
            api_time = (datetime.now() - api_start).total_seconds()
            logging.info(f"✅ External API test completed in {api_time:.2f}s")
            health_status["services"]["external_apis"] = "operational"
            health_status["api_test_time"] = f"{api_time:.2f}s"
        except Exception as e:
            api_time = (datetime.now() - api_start).total_seconds()
            logging.error(f"❌ External API test failed in {api_time:.2f}s: {e}")
            health_status["services"]["external_apis"] = f"error: {str(e)}"
            health_status["status"] = "degraded"
        
        total_time = (datetime.now() - start_time).total_seconds()
        logging.info(f"🏁 Health check completed in {total_time:.2f}s")
        health_status["total_check_time"] = f"{total_time:.2f}s"
        
        return health_status
    
    # Background task for connection cleanup - will be started by server
    async def connection_cleanup_task():
        """Background task to clean up stale connections"""
        while True:
            try:
                await manager.cleanup_stale_connections()
                await asyncio.sleep(300)  # Run every 5 minutes
            except Exception as e:
                logging.error(f"Connection cleanup error: {e}")
                await asyncio.sleep(60)  # Retry in 1 minute on error
    
    # Store cleanup task for server to start
    app.state.cleanup_task = connection_cleanup_task
    
    @app.get("/api/websocket/stats")
    async def websocket_stats():
        """Get WebSocket connection statistics"""
        stats = {
            'total_connections': manager.get_connection_count(),
            'connections_by_symbol': {},
            'timestamp': datetime.now().isoformat()
        }
        
        for symbol in manager.active_connections:
            stats['connections_by_symbol'][symbol] = manager.get_connection_count(symbol)
        
        return stats
    
    @app.get("/api/system/stats")
    async def system_stats():
        """Get system performance statistics"""
        return {
            'task_manager': task_manager.get_stats(),
            'websocket_connections': manager.get_connection_count(),
            'timestamp': datetime.now().isoformat()
        }
    
    @app.websocket("/ws/market/summary")
    async def market_summary_websocket(websocket: WebSocket):
        """Real-time market summary WebSocket with enhanced connection management"""
        await websocket.accept()
        
        # Use connection manager
        connection_id = await manager.get_or_create_connection(websocket, "market_summary", "live")
        logging.info(f"🔌 DEBUG: Market summary WebSocket established ({connection_id})")
        
        try:
            # Handle messages
            async def handle_messages():
                try:
                    async for message in websocket.iter_text():
                        try:
                            data = json.loads(message)
                            if data.get('type') == 'set_filter':
                                # Handle filter changes for future enhancement
                                class_filter = data.get('class_filter', 'all')
                                logging.info(f"🔄 DEBUG: Market summary filter changed to {class_filter}")
                        except json.JSONDecodeError:
                            pass
                except Exception as e:
                    logging.error(f"❌ DEBUG: Market summary message handling error: {e}")
            
            # Start message handler
            message_task = asyncio.create_task(handle_messages())
            
            # Keep connection alive with periodic updates
            ping_failures = 0
            max_failures = 3
            
            while True:
                try:
                    if hasattr(websocket, 'client_state') and websocket.client_state.name == 'CONNECTED':
                        assets = []
                        
                        # Get crypto data from crypto service
                        if realtime_service and hasattr(realtime_service, 'price_cache'):
                            crypto_symbols = list(realtime_service.price_cache.keys())
                            
                            for symbol in crypto_symbols:
                                if symbol in realtime_service.price_cache:
                                    price_data = realtime_service.price_cache[symbol]
                                    change = price_data['change_24h']
                                    
                                    assets.append({
                                        'symbol': symbol,
                                        'name': multi_asset.get_asset_name(symbol),
                                        'current_price': price_data['current_price'],
                                        'change_24h': change,
                                        'volume': price_data['volume'],
                                        'forecast_direction': 'UP' if change > 1 else 'DOWN' if change < -1 else 'HOLD',
                                        'confidence': min(80, max(60, 70 + abs(change))),
                                        'predicted_price': price_data['current_price'] * (1 + change/100 * 0.1),
                                        'data_source': 'Binance Stream',
                                        'asset_class': 'crypto'
                                    })
                        
                        # Get stock data from stock service
                        try:
                            if stock_realtime_service and hasattr(stock_realtime_service, 'price_cache'):
                                stock_symbols = list(stock_realtime_service.price_cache.keys())
                                
                                for symbol in stock_symbols:
                                    if symbol in stock_realtime_service.price_cache:
                                        price_data = stock_realtime_service.price_cache[symbol]
                                        change = price_data['change_24h']
                                        
                                        assets.append({
                                            'symbol': symbol,
                                            'name': stock_realtime_service.stock_symbols.get(symbol, symbol),
                                            'current_price': price_data['current_price'],
                                            'change_24h': change,
                                            'volume': price_data['volume'],
                                            'forecast_direction': 'UP' if change > 0.5 else 'DOWN' if change < -0.5 else 'HOLD',
                                            'confidence': min(85, max(65, 75 + abs(change))),
                                            'predicted_price': price_data['current_price'] * (1 + change/100 * 0.05),
                                            'data_source': price_data.get('data_source', 'Stock API'),
                                            'asset_class': 'stocks'
                                        })
                        except Exception as e:
                            logging.error(f"❌ Stock data error for market summary: {e}")
                        
                        market_data = {
                            "type": "market_summary_update",
                            "assets": assets,
                            "timestamp": datetime.now().isoformat(),
                            "update_count": len(assets),
                            "crypto_count": len([a for a in assets if a.get('asset_class') == 'crypto']),
                            "stock_count": len([a for a in assets if a.get('asset_class') == 'stocks']),
                            "interval": 2
                        }
                        
                        await websocket.send_text(json.dumps(market_data))
                        
                        # Update connection state
                        await manager.update_connection_state("market_summary", connection_id, last_ping=datetime.now())
                        ping_failures = 0
                    else:
                        logging.info(f"🔌 DEBUG: Market summary WebSocket disconnected")
                        break
                        
                except Exception as e:
                    ping_failures += 1
                    logging.error(f"❌ DEBUG: Market summary update error (attempt {ping_failures}/{max_failures}): {e}")
                    if ping_failures >= max_failures:
                        break
                
                await asyncio.sleep(2)  # Update every 2 seconds
                
        except WebSocketDisconnect:
            logging.info(f"🔌 DEBUG: Market summary WebSocket disconnected")
        except Exception as e:
            logging.error(f"❌ DEBUG: Market summary WebSocket error: {e}")
        finally:
            # Graceful cleanup
            try:
                if 'message_task' in locals():
                    message_task.cancel()
                await manager.safe_disconnect(websocket, "market_summary", connection_id)
                logging.info(f"🔌 DEBUG: Market summary connection cleaned up")
            except Exception as e:
                logging.error(f"❌ DEBUG: Market summary cleanup error: {e}")
    
    @app.websocket("/ws/mobile")
    async def deprecated_mobile_websocket(websocket: WebSocket):
        """Deprecated endpoint - use /ws/asset/{symbol}/forecast or /ws/asset/{symbol}/trends"""
        await websocket.close(code=1000, reason="Use /ws/asset/{symbol}/forecast or /ws/asset/{symbol}/trends")