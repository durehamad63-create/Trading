#!/usr/bin/env python3
"""
Real-time WebSocket service with live Binance data streams
"""
import asyncio
import websockets
import json
import logging
from datetime import datetime
from modules.ml_predictor import MobileMLModel
from multi_asset_support import MultiAssetSupport, multi_asset
import aiohttp

class RealTimeWebSocketService:
    def __init__(self, model=None, database=None):
        self.model = model  # Use shared model instance
        self.database = database  # Store database reference
        self.multi_asset = MultiAssetSupport()
        self.active_connections = {}  # {symbol: {connection_id: {websocket, timeframe}}}
        self.binance_streams = {}
        self.price_cache = {}  # Real-time tick data
        self.candle_cache = {}  # Timeframe-specific candle data
        
        # Binance WebSocket streams for all crypto symbols
        self.binance_symbols = {
            'BTC': 'btcusdt', 'ETH': 'ethusdt', 'BNB': 'bnbusdt',
            'SOL': 'solusdt', 'ADA': 'adausdt', 'XRP': 'xrpusdt',
            'DOGE': 'dogeusdt', 'TRX': 'trxusdt'
            # Note: USDT and USDC are stablecoins, handled separately
        }
        
        # Timeframe intervals in minutes
        self.timeframe_intervals = {
            '1m': 1, '5m': 5, '15m': 15, '30m': 30,
            '1h': 60, '4h': 240, '1D': 1440, '1W': 10080
        }
    
    async def start_binance_streams(self):
        """Start Binance WebSocket streams for all symbols immediately"""
        # Start all symbols immediately
        for symbol, binance_symbol in self.binance_symbols.items():
            asyncio.create_task(self._binance_stream(symbol, binance_symbol))
            await asyncio.sleep(0.1)  # Small delay between connections
        
        # Handle stablecoins separately with fixed prices
        asyncio.create_task(self._handle_stablecoins())
        

    

    
    async def _handle_stablecoins(self):
        """Handle stablecoins with fixed prices"""
        stablecoins = ['USDT', 'USDC']
        
        while True:
            try:
                for symbol in stablecoins:
                    # Set fixed price for stablecoins
                    self.price_cache[symbol] = {
                        'current_price': 1.0,
                        'change_24h': 0.0,
                        'volume': 1000000000,  # High volume for stablecoins
                        'timestamp': datetime.now()
                    }
                
                await asyncio.sleep(30)  # Update every 30 seconds
                
            except Exception as e:
                logging.error(f"❌ Stablecoin handler error: {e}")
                await asyncio.sleep(60)
    
    async def _binance_stream(self, symbol, binance_symbol):
        """Individual Binance WebSocket stream for a symbol (non-blocking)"""
        uri = f"wss://stream.binance.com:9443/ws/{binance_symbol}@ticker"
        
        while True:
            try:
                async with websockets.connect(uri) as websocket:

                    
                    async for message in websocket:
                        try:
                            data = json.loads(message)
                            
                            # Extract real-time price data
                            current_price = float(data['c'])  # Current price
                            change_24h = float(data['P'])     # 24h change %
                            volume = float(data['v'])         # Volume
                            
                            # Update price cache
                            self.price_cache[symbol] = {
                                'current_price': current_price,
                                'change_24h': change_24h,
                                'volume': volume,
                                'timestamp': datetime.now()
                            }

                            
                            # Update candle data for all timeframes and generate forecasts
                            if symbol in self.active_connections and self.active_connections[symbol]:
                                asyncio.create_task(self._update_candles_and_forecast(symbol, current_price, volume, change_24h))
                            
                        except Exception as e:
                            logging.warning(f"Error processing message for {symbol}: {e}")
                            continue
                        
            except Exception as e:
                error_msg = str(e)
                if "400" in error_msg:
                    logging.error(f"❌ Binance API error for {symbol}: 400")
                    # Don't retry immediately for 400 errors
                    await asyncio.sleep(60)
                else:
                    logging.error(f"❌ Binance stream error for {symbol}: {e}")
                    await asyncio.sleep(10)
                
                # Remove from cache if connection fails
                if symbol in self.price_cache:
                    del self.price_cache[symbol]
    
    async def _update_candles_and_forecast(self, symbol, price, volume, change_24h):
        """Update candle data for all timeframes and generate forecasts"""
        try:
            current_time = datetime.now()
            
            # Get unique timeframes from active connections
            timeframes = set()
            if symbol in self.active_connections:
                for conn_data in self.active_connections[symbol].values():
                    timeframes.add(conn_data['timeframe'])
            
            # Update candles for each timeframe
            for timeframe in timeframes:
                await self._update_candle_data(symbol, timeframe, price, volume, current_time)
                
                # Rate limit predictions per timeframe - more frequent for 4h
                rate_key = f"{symbol}_{timeframe}"
                if not hasattr(self, 'last_prediction_time'):
                    self.last_prediction_time = {}
                
                update_interval = 5 if timeframe in ['4h', '4H'] else self._get_update_interval(timeframe)
                if (rate_key in self.last_prediction_time and 
                    (current_time - self.last_prediction_time[rate_key]).total_seconds() < update_interval):
                    continue
                
                # Generate timeframe-specific forecast
                await self._generate_timeframe_forecast(symbol, timeframe, current_time)
                self.last_prediction_time[rate_key] = current_time
                
        except Exception as e:
            logging.error(f"Error updating candles and forecast for {symbol}: {e}")
    
    def _get_update_interval(self, timeframe):
        """Get appropriate update interval for timeframe"""
        intervals = {
            '1m': 5, '5m': 15, '15m': 30, '30m': 60,
            '1h': 120, '4h': 300, '1D': 600, '1W': 1800
        }
        return intervals.get(timeframe, 60)
    
    async def _update_candle_data(self, symbol, timeframe, price, volume, timestamp):
        """Update candle data for specific timeframe"""
        try:
            if symbol not in self.candle_cache:
                self.candle_cache[symbol] = {}
            if timeframe not in self.candle_cache[symbol]:
                self.candle_cache[symbol][timeframe] = []
            
            interval_minutes = self.timeframe_intervals.get(timeframe, 60)
            candle_start = timestamp.replace(second=0, microsecond=0)
            
            # Round to timeframe boundary
            if interval_minutes >= 60:
                candle_start = candle_start.replace(minute=0)
                if interval_minutes >= 1440:  # Daily or higher
                    candle_start = candle_start.replace(hour=0)
            else:
                minute_boundary = (candle_start.minute // interval_minutes) * interval_minutes
                candle_start = candle_start.replace(minute=minute_boundary)
            
            candles = self.candle_cache[symbol][timeframe]
            
            # Update or create current candle
            if candles and candles[-1]['timestamp'] == candle_start:
                # Update existing candle
                candle = candles[-1]
                candle['high'] = max(candle['high'], price)
                candle['low'] = min(candle['low'], price)
                candle['close'] = price
                candle['volume'] += volume
            else:
                # Create new candle
                new_candle = {
                    'timestamp': candle_start,
                    'open': price,
                    'high': price,
                    'low': price,
                    'close': price,
                    'volume': volume
                }
                candles.append(new_candle)
                
                # Consistent candle storage
                max_candles = 100
                if len(candles) > max_candles:
                    candles.pop(0)
                    
        except Exception as e:
            logging.error(f"Error updating candle data: {e}")
    
    async def _generate_timeframe_forecast(self, symbol, timeframe, current_time):
        """Generate real-time price update for specific timeframe"""
        try:
            # Get candle data for this timeframe
            if (symbol not in self.candle_cache or 
                timeframe not in self.candle_cache[symbol] or 
                not self.candle_cache[symbol][timeframe]):
                return
            
            candles = self.candle_cache[symbol][timeframe]
            current_candle = candles[-1]
            current_price = float(current_candle['close'])
            
            # Generate ML prediction periodically (not every tick)
            prediction = self.model.predict(symbol)
            if not isinstance(prediction, dict):
                return
            
            # Generate real-time predicted price
            predicted_price = float(prediction.get('predicted_price', current_price))
            
            # Send real-time price update (for live chart updates)
            realtime_data = {
                "type": "realtime_update",
                "symbol": str(symbol),
                "timeframe": str(timeframe),
                "current_price": float(current_price),
                "predicted_price": float(predicted_price),
                "change_24h": float(self.price_cache.get(symbol, {}).get('change_24h', 0)),
                "volume": float(current_candle['volume']),
                "timestamp": current_time.strftime("%H:%M"),
                "forecast_direction": str(prediction.get('forecast_direction', 'HOLD')),
                "confidence": int(prediction.get('confidence', 50)),
                "predicted_range": str(prediction.get('predicted_range', f"${current_price*0.98:.2f}–${current_price*1.02:.2f}")),
                "last_updated": current_time.isoformat()
            }
            
            # Broadcast real-time update
            await self._broadcast_to_timeframe(symbol, timeframe, realtime_data)
            
        except Exception as e:
            logging.error(f"Error generating realtime update: {e}")
    
    async def _store_realtime_data(self, symbol, price_data, prediction):
        """Store real-time price and forecast data to database"""
        try:
            # Use same database access pattern
            db = self.database
            if not db or not db.pool:
                try:
                    from database import db as global_db
                    if global_db and global_db.pool:
                        db = global_db
                    else:
                        return
                except:
                    return
            
            # Store actual price (with rate limiting - every minute)
            current_time = datetime.now()
            if not hasattr(self, 'last_stored') or symbol not in self.last_stored:
                self.last_stored = {}
            
            # Store price data every 30 seconds to avoid database overload
            if (symbol not in self.last_stored or 
                (current_time - self.last_stored[symbol]).total_seconds() >= 30):
                
                await db.store_actual_price(symbol, price_data)
                self.last_stored[symbol] = current_time
                
                # Invalidate chart cache when new data is stored
                try:
                    import redis
                    import os
                    from dotenv import load_dotenv
                    load_dotenv()
                    
                    redis_client = redis.Redis(
                        host=os.getenv('REDIS_HOST', 'localhost'),
                        port=int(os.getenv('REDIS_PORT', '6379')),
                        db=int(os.getenv('REDIS_DB', '0'))
                    )
                    
                    # Clear cache for this symbol
                    redis_client.delete(f"chart_data:{symbol}:7D")
                    redis_client.delete(f"websocket_history:{symbol}")
                except:
                    pass
            
            # Store forecast (less frequently - every 2 minutes)
            if not hasattr(self, 'last_forecast_stored'):
                self.last_forecast_stored = {}
            
            if (symbol not in self.last_forecast_stored or 
                (current_time - self.last_forecast_stored[symbol]).total_seconds() >= 120):
                
                await db.store_forecast(symbol, prediction)
                self.last_forecast_stored[symbol] = current_time
                
        except Exception as e:
            logging.error(f"❌ Failed to store real-time data for {symbol}: {e}")
    
    async def _broadcast_to_timeframe(self, symbol, timeframe, data):
        """Efficient broadcast with connection pooling and batching"""
        if symbol not in self.active_connections:
            return
        
        # Batch message for efficiency
        message = json.dumps(data, default=str)
        
        # Get matching connections efficiently
        matching_connections = [
            (connection_id, conn_data['websocket']) 
            for connection_id, conn_data in self.active_connections[symbol].items() 
            if conn_data['timeframe'] == timeframe
        ]
        
        if not matching_connections:
            return
        
        # Batch send with limited concurrency
        semaphore = asyncio.Semaphore(10)  # Limit concurrent sends
        
        async def send_batch(conn_id, websocket):
            async with semaphore:
                try:
                    await websocket.send_text(message)
                except Exception as e:
                    if "connectionclosed" in str(e).lower():
                        # Remove dead connection
                        self.active_connections[symbol].pop(conn_id, None)
        
        # Execute batch sends
        await asyncio.gather(
            *[send_batch(conn_id, ws) for conn_id, ws in matching_connections],
            return_exceptions=True
        )
    

    
    async def add_connection(self, websocket, symbol, connection_id, timeframe='1D'):
        """Add connection with efficient pooling"""
        if symbol not in self.active_connections:
            self.active_connections[symbol] = {}
        
        # Check for existing connection with same timeframe (connection reuse)
        existing_conn = None
        for conn_id, conn_data in self.active_connections[symbol].items():
            if conn_data['timeframe'] == timeframe and conn_id != connection_id:
                existing_conn = conn_data
                break
        
        self.active_connections[symbol][connection_id] = {
            'websocket': websocket,
            'timeframe': timeframe,
            'connected_at': datetime.now(),
            'user_id': connection_id.split('_')[-1]  # Extract user ID for pooling
        }
        
        # Send historical data (cached if available from existing connection)
        if existing_conn and hasattr(existing_conn, 'cached_historical'):
            await websocket.send_text(existing_conn['cached_historical'])
        else:
            await self._send_historical_data(websocket, symbol, timeframe)
    
    async def _send_historical_data(self, websocket, symbol, timeframe):
        """Send cached historical data with improved caching"""
        try:
            # Multi-level cache: Redis -> Memory -> Database
            cache_key = f"websocket_history:{symbol}:{timeframe}"
            
            # Check memory cache first (fastest)
            if hasattr(self, 'memory_cache') and cache_key in self.memory_cache:
                cached_data = self.memory_cache[cache_key]
                if (datetime.now() - cached_data['timestamp']).total_seconds() < 300:  # 5 min TTL
                    await websocket.send_text(cached_data['message'])
                    return
            
            # Try Redis cache
            try:
                import redis
                import os
                from dotenv import load_dotenv
                load_dotenv()
                
                redis_client = redis.Redis(
                    host=os.getenv('REDIS_HOST', 'localhost'),
                    port=int(os.getenv('REDIS_PORT', '6379')),
                    db=int(os.getenv('REDIS_DB', '0')),
                    decode_responses=True
                )
                
                cached_message = redis_client.get(cache_key)
                if cached_message:
                    # Cache in memory for next request
                    if not hasattr(self, 'memory_cache'):
                        self.memory_cache = {}
                    self.memory_cache[cache_key] = {
                        'message': cached_message,
                        'timestamp': datetime.now()
                    }
                    await websocket.send_text(cached_message)
                    return
            except Exception as e:
                logging.warning(f"⚠️ Redis cache failed: {e}")
            
            # Use database from constructor or fallback to global
            db = self.database
            if not db or not db.pool:
                try:
                    from database import db as global_db
                    if global_db and global_db.pool:
                        db = global_db
                        logging.info(f"✅ DEBUG: Using global database for {symbol} {timeframe}")
                    else:
                        logging.error(f"❌ DEBUG: No database available for {symbol} {timeframe}")
                        return
                except Exception as e:
                    logging.error(f"❌ DEBUG: Database import failed for {symbol} {timeframe}: {e}")
                    return
            
            logging.info(f"✅ DEBUG: Using database for {symbol} {timeframe}")
            
            # Get historical chart data from database with normalized timeframe
            normalized_tf = '4H' if timeframe.lower() == '4h' else timeframe
            query_symbol = f"{symbol}_{normalized_tf}"
            logging.info(f"🔍 DEBUG: Querying database for {query_symbol} (original: {symbol} {timeframe})")
            chart_data = await db.get_chart_data(query_symbol, normalized_tf)
            logging.info(f"🔍 DEBUG: Database returned: actual={len(chart_data.get('actual', []))}, forecast={len(chart_data.get('forecast', []))}, timestamps={len(chart_data.get('timestamps', []))}")
            
            if chart_data['actual'] and chart_data['forecast']:
                # Use database data
                points = 50
                actual_data = [float(x) for x in chart_data['actual'][-points:]]
                forecast_data = [float(x) for x in chart_data['forecast'][-points:]]
                timestamps = [str(x) for x in chart_data['timestamps'][-points:]]
                logging.info(f"✅ DEBUG: Using database data for {symbol} {timeframe}: {len(actual_data)} points")
            else:
                # Force database query with timeframe-specific symbol
                logging.warning(f"⚠️ DEBUG: No data for {symbol} {timeframe}, trying timeframe-specific query")
                timeframe_symbol = f"{symbol}_{timeframe}" if timeframe in ['1m', '5m', '15m', '1h', '4h', '1D', '1W'] else symbol
                chart_data = await db.get_chart_data(timeframe_symbol, timeframe)
                
                if chart_data['actual'] and chart_data['forecast']:
                    points = 50
                    actual_data = [float(x) for x in chart_data['actual'][-points:]]
                    forecast_data = [float(x) for x in chart_data['forecast'][-points:]]
                    timestamps = [str(x) for x in chart_data['timestamps'][-points:]]
                    logging.info(f"✅ DEBUG: Using timeframe-specific database data for {timeframe_symbol}: {len(actual_data)} points")
                else:
                    logging.error(f"❌ DEBUG: No database data available for {symbol}/{timeframe_symbol}")
                    return  # Don't send synthetic data
            
            historical_message = {
                "type": "historical_data",
                "symbol": str(symbol),
                "timeframe": str(timeframe),
                "name": str(multi_asset.get_asset_name(symbol)),
                "chart": {
                    "actual": actual_data,
                    "predicted": forecast_data,
                    "timestamps": timestamps
                },
                "last_updated": datetime.now().isoformat()
            }
            
            message_json = json.dumps(historical_message)
            
            # Cache the message for future connections
            if not hasattr(self, 'memory_cache'):
                self.memory_cache = {}
            self.memory_cache[cache_key] = {
                'message': message_json,
                'timestamp': datetime.now()
            }
            
            # Also cache in Redis for 10 minutes
            try:
                redis_client.setex(cache_key, 600, message_json)
            except:
                pass
            
            await websocket.send_text(message_json)
            logging.info(f"✅ DEBUG: Sent historical data for {symbol} {timeframe} with {len(actual_data)} points")
            
        except Exception as e:
            logging.error(f"❌ DEBUG: Failed to send historical data for {symbol} {timeframe}: {e}")
            # Don't send anything if database fails
            return
    
    def remove_connection(self, symbol, connection_id):
        """Remove WebSocket connection"""
        if symbol in self.active_connections and connection_id in self.active_connections[symbol]:
            del self.active_connections[symbol][connection_id]


# Global real-time service - will be initialized with shared model
realtime_service = None