#!/usr/bin/env python3
"""
Real-time WebSocket service with live Binance data streams
"""
import asyncio
import websockets
import json
import logging
from datetime import datetime, timedelta
from modules.ml_predictor import MobileMLModel
from multi_asset_support import MultiAssetSupport, multi_asset
from config.symbols import CRYPTO_SYMBOLS
from utils.error_handler import ErrorHandler
from utils.cache_manager import CacheKeys
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
        
        # Use centralized symbol configuration
        self.binance_symbols = {k: v['binance'].lower() for k, v in CRYPTO_SYMBOLS.items() if v.get('binance')}
        
        # Timeframe intervals in minutes
        self.timeframe_intervals = {
            '1m': 1, '5m': 5, '15m': 15, '30m': 30,
            '1h': 60, '4H': 240, '1D': 1440, '1W': 10080
        }
        
        # Use centralized cache manager
        from utils.cache_manager import CacheManager, CacheKeys
        self.cache_manager = CacheManager
        self.cache_keys = CacheKeys
    
    async def start_binance_streams(self):
        """Start Binance WebSocket streams for all symbols immediately"""
        # CRITICAL: Pre-populate cache with all crypto symbols immediately
        await self._populate_initial_cache()
        
        # Start all symbols immediately with minimal delay
        print(f"🚀 Starting Binance streams for {len(self.binance_symbols)} symbols...")
        for symbol, binance_symbol in self.binance_symbols.items():
            print(f"📡 Starting stream: {symbol} -> {binance_symbol}")
            asyncio.create_task(self._binance_stream(symbol, binance_symbol))
            await asyncio.sleep(0.1)  # Reduced to 100ms delay
        
        # Handle stablecoins separately with fixed prices
        asyncio.create_task(self._handle_stablecoins())
        
        # Add fallback data fetcher for failed WebSocket connections
        asyncio.create_task(self._fallback_data_fetcher())
    async def _populate_initial_cache(self):
        """Populate cache with initial data for all crypto symbols"""
        try:
            # Initial prices for immediate API availability
            initial_prices = {
                'BTC': 43250.50, 'ETH': 2650.75, 'BNB': 315.20, 'XRP': 0.52, 'SOL': 98.45,
                'DOGE': 0.085, 'ADA': 0.48, 'TRX': 0.105, 'USDT': 1.0, 'USDC': 1.0
            }
            
            for symbol, price in initial_prices.items():
                self.price_cache[symbol] = {
                    'current_price': price,
                    'change_24h': 0.5 if symbol not in ['USDT', 'USDC'] else 0.0,
                    'volume': 1000000000 if symbol in ['USDT', 'USDC'] else 500000000,
                    'timestamp': datetime.now()
                }

                
                # Cache using centralized manager
                cache_key = self.cache_keys.price(symbol, 'crypto')
                self.cache_manager.set_cache(cache_key, self.price_cache[symbol], ttl=30)
            
            print(f"✅ Initial cache populated with {len(self.price_cache)} symbols")
            
        except Exception as e:
            print(f"❌ Initial cache population failed: {e}")
    
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
    
                
                await asyncio.sleep(5)  # Update every 5 seconds
                
            except Exception as e:
                ErrorHandler.log_stream_error('stablecoin', 'ALL', str(e))
                await asyncio.sleep(60)
    
    async def _binance_stream(self, symbol, binance_symbol):
        """Individual Binance WebSocket stream for a symbol (non-blocking)"""
        uri = f"wss://stream.binance.com:9443/ws/{binance_symbol}@ticker"
        
        while True:
            try:
                # Railway-compatible WebSocket settings
                async with websockets.connect(
                    uri, 
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=10,
                    max_size=2**20,
                    compression=None
                ) as websocket:
                    print(f"✅ Connected: {symbol} stream active (Binance: {binance_symbol})")
                    async for message in websocket:
                        try:
                            data = json.loads(message)
                            
                            # Extract real-time price data
                            current_price = float(data['c'])  # Current price
                            change_24h = float(data['P'])     # 24h change %
                            volume = float(data['v'])         # Volume
                            
                            # Update price cache
                            price_data = {
                                'current_price': current_price,
                                'change_24h': change_24h,
                                'volume': volume,
                                'timestamp': datetime.now()
                            }
                            self.price_cache[symbol] = price_data
                            
                            # Cache using centralized manager
                            cache_key = self.cache_keys.price(symbol, 'crypto')
                            self.cache_manager.set_cache(cache_key, price_data, ttl=30)
            

                            
                            # Always store data for all timeframes (regardless of connections)
                            asyncio.create_task(self._store_all_timeframes(symbol, current_price, volume, change_24h))
                            
                            # Update candle data and broadcast only if connections exist
                            if symbol in self.active_connections and self.active_connections[symbol]:
                                # Immediate price broadcast without waiting for ML
                                asyncio.create_task(self._broadcast_price_update(symbol, current_price, change_24h, volume))
                                # ML predictions in background (non-blocking)
                                asyncio.create_task(self._update_candles_and_forecast(symbol, current_price, volume, change_24h))
                            
                        except Exception as e:
                            ErrorHandler.log_stream_error('binance_message', symbol, str(e))
                            continue
                        
            except Exception as e:
                error_msg = str(e)
                print(f"❌ {symbol} stream error: {error_msg}")
                
                # Railway-specific error handling
                if "403" in error_msg or "forbidden" in error_msg.lower():
                    print(f"🚫 {symbol}: Binance blocked connection, using fallback")
                    await self._fallback_crypto_data(symbol, binance_symbol)
                    await asyncio.sleep(30)
                elif "timeout" in error_msg.lower() or "connection" in error_msg.lower():
                    print(f"⏰ {symbol}: Connection timeout, retrying...")
                    await asyncio.sleep(5)
                elif "429" in error_msg:
                    print(f"🚦 {symbol}: Rate limited, waiting...")
                    await asyncio.sleep(15)
                else:
                    print(f"🔄 {symbol}: General error, retrying...")
                    await asyncio.sleep(3)
                    
                # Use fallback data during connection issues
                await self._fallback_crypto_data(symbol, binance_symbol)
    
    async def _update_candles_and_forecast(self, symbol, price, volume, change_24h):
        """Update candle data for all timeframes and generate forecasts"""
        try:
            current_time = datetime.now()
            
            # Get unique timeframes from active connections
            timeframes = set()
            if symbol in self.active_connections:
                for conn_data in self.active_connections[symbol].values():
                    timeframes.add(conn_data['timeframe'])
            
            # Update candles and generate forecasts for each timeframe
            for timeframe in timeframes:
                await self._update_candle_data(symbol, timeframe, price, volume, current_time)
                
                # Generate forecast for every price update (no rate limiting)
                await self._generate_timeframe_forecast(symbol, timeframe, current_time)
                
                # Store price data for every timeframe
                timeframe_symbol = f"{symbol}_{timeframe}"
                price_data = {
                    'current_price': price,
                    'change_24h': change_24h,
                    'volume': volume,
                    'timestamp': current_time
                }
                await self._store_realtime_data(timeframe_symbol, price_data, timeframe)
                
        except Exception as e:
            ErrorHandler.log_stream_error('candle_update', symbol, str(e))
    
    def _get_update_interval(self, timeframe):
        """Get appropriate update interval for timeframe - faster updates"""
        intervals = {
            '1m': 1, '5m': 2, '15m': 3, '30m': 5,
            '1h': 10, '4H': 1, '1D': 30, '1W': 60
        }
        return intervals.get(timeframe, 5)
    
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
            ErrorHandler.log_stream_error('candle_data', 'ALL', str(e))
    
    async def _broadcast_price_update(self, symbol, current_price, change_24h, volume):
        """Immediate price broadcast without ML predictions"""
        try:
            current_time = datetime.now()
            
            # Get unique timeframes from active connections
            timeframes = set()
            if symbol in self.active_connections:
                for conn_data in self.active_connections[symbol].values():
                    timeframes.add(conn_data['timeframe'])
            
            # Broadcast to all timeframes immediately
            for timeframe in timeframes:
                price_data = {
                    "type": "price_update",
                    "symbol": str(symbol),
                    "timeframe": str(timeframe),
                    "current_price": float(current_price),
                    "change_24h": float(change_24h),
                    "volume": float(volume),
                    "timestamp": current_time.strftime("%H:%M:%S"),
                    "last_updated": current_time.isoformat()
                }
                
                await self._broadcast_to_timeframe(symbol, timeframe, price_data)
                
        except Exception as e:
            ErrorHandler.log_websocket_error('broadcast', str(e))
    
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
            
            # Skip ML prediction for immediate price updates - use cached if available
            predicted_price = current_price  # Default to current price
            forecast_direction = 'HOLD'
            confidence = 75
            
            # Use cached prediction for immediate updates, generate fresh in background
            predicted_price = current_price
            forecast_direction = 'HOLD'
            confidence = 75
            
            # Try cached prediction first for speed
            try:
                if hasattr(self.model, 'prediction_cache') and symbol in self.model.prediction_cache:
                    cache_time, cached_pred = self.model.prediction_cache[symbol]
                    if (datetime.now().timestamp() - cache_time) < 10:  # Use if less than 10s old
                        predicted_price = float(cached_pred.get('predicted_price', current_price))
                        forecast_direction = cached_pred.get('forecast_direction', 'HOLD')
                        confidence = cached_pred.get('confidence', 75)
                else:
                    # Generate fresh prediction in background (non-blocking)
                    asyncio.create_task(self._generate_fresh_prediction(symbol))
            except Exception as e:
                pass
            
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
                "forecast_direction": str(forecast_direction),
                "confidence": int(confidence),
                "predicted_range": f"${predicted_price*0.98:.2f}–${predicted_price*1.02:.2f}",
                "last_updated": current_time.isoformat()
            }
            
            # Broadcast real-time update
            await self._broadcast_to_timeframe(symbol, timeframe, realtime_data)
            
        except Exception as e:
            ErrorHandler.log_prediction_error('realtime_update', str(e))
    
    async def _store_realtime_data(self, symbol, price_data, timeframe):
        """Store real-time price data to database for every update"""
        try:
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
            
            # Store every price update (no rate limiting)
            await db.store_actual_price(symbol, price_data, timeframe)
            
            # Generate and store forecast for this timeframe
            try:
                prediction = await self.model.predict(symbol.split('_')[0])  # Remove timeframe from symbol
                await db.store_forecast(symbol, prediction)
            except Exception as e:
                pass
                
        except Exception as e:
            ErrorHandler.log_database_error('store_realtime', symbol, str(e))
    
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
        print(f"🔗 Adding connection for {symbol} with ID {connection_id}", flush=True)
        
        try:
            if symbol not in self.active_connections:
                self.active_connections[symbol] = {}
                print(f"🆕 Created new symbol entry for {symbol}")
            
            self.active_connections[symbol][connection_id] = {
                'websocket': websocket,
                'timeframe': timeframe,
                'connected_at': datetime.now()
            }
            print(f"✅ Connection stored for {symbol}", flush=True)
            
            # Send historical data
            print(f"📈 Sending historical data for {symbol}", flush=True)
            await self._send_historical_data(websocket, symbol, timeframe)
            print(f"✅ Historical data sent for {symbol}", flush=True)
            
        except Exception as e:
            print(f"❌ Error in add_connection for {symbol}: {e}", flush=True)
            raise
    
    async def _send_historical_data(self, websocket, symbol, timeframe):
        """Send cached historical data with improved caching"""
        print(f"📈 _send_historical_data called for {symbol} {timeframe}", flush=True)
        try:
            # Multi-level cache: Redis -> Memory -> Database
            cache_key = CacheKeys.websocket_history(symbol, timeframe)
            print(f"🔑 Cache key: {cache_key}", flush=True)
            
            # Check memory cache first (fastest)
            if hasattr(self, 'memory_cache') and cache_key in self.memory_cache:
                cached_data = self.memory_cache[cache_key]
                if (datetime.now() - cached_data['timestamp']).total_seconds() < 300:  # 5 min TTL
                    print(f"💾 Using memory cache for {symbol}", flush=True)
                    await websocket.send_text(cached_data['message'])
                    return
                else:
                    print(f"⏰ Memory cache expired for {symbol}", flush=True)
            else:
                print(f"🚫 No memory cache for {symbol}", flush=True)
            
            # Try Redis cache
            try:
                import redis
                import os
                from dotenv import load_dotenv
                load_dotenv()
                
                redis_client = redis.Redis(
                    host=os.getenv('REDIS_HOST', 'localhost'),
                    port=int(os.getenv('REDIS_PORT', '6379')),
                    db=0,  # Use DB 0 for Railway compatibility (unified)
                    password=os.getenv('REDIS_PASSWORD', None) if os.getenv('REDIS_PASSWORD') else None,
                    decode_responses=True,
                    socket_connect_timeout=10,
                    socket_timeout=10
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
            except Exception:
                pass
            
            # Use database from constructor or fallback to global
            db = self.database
            print(f"📊 Database available: {db is not None and hasattr(db, 'pool') and db.pool is not None}", flush=True)
            if not db or not db.pool:
                try:
                    from database import db as global_db
                    if global_db and global_db.pool:
                        db = global_db
                        print(f"🔄 Using global database", flush=True)
                    else:
                        print(f"❌ No database available for {symbol}", flush=True)
                        # Send minimal response instead of failing
                        minimal_data = {
                            "type": "historical_data",
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "message": "Historical data unavailable"
                        }
                        await websocket.send_text(json.dumps(minimal_data))
                        return
                except Exception as e:
                    print(f"❌ Database fallback failed: {e}")
                    return
            
            # Try multiple database query formats for historical data
            chart_data = None
            actual_data = []
            forecast_data = []
            timestamps = []
            
            # Query attempts with different symbol formats
            query_attempts = [
                f"{symbol}_{'4H' if timeframe.lower() == '4h' else timeframe}",  # BTC_4H
                f"{symbol}_{timeframe}",  # BTC_4h
                symbol  # BTC (fallback)
            ]
            
            for attempt, query_symbol in enumerate(query_attempts):
                print(f"📊 DB Query attempt {attempt+1}: {query_symbol}")
                try:
                    chart_data = await db.get_chart_data(query_symbol, timeframe)
                    if chart_data and chart_data.get('actual') and chart_data.get('forecast'):
                        print(f"✅ Found data: {len(chart_data['actual'])} actual, {len(chart_data['forecast'])} forecast")
                        
                        # Process successful data
                        min_length = min(len(chart_data['actual']), len(chart_data['forecast']), len(chart_data['timestamps']))
                        points = min(50, min_length)
                        
                        actual_data = [float(x) for x in chart_data['actual'][-points:]]
                        forecast_data = [float(x) for x in chart_data['forecast'][-points:]]
                        timestamps = [str(x) for x in chart_data['timestamps'][-points:]]
                        break
                except Exception as e:
                    print(f"❌ Query {attempt+1} failed: {e}")
                    continue
            
            # If no database data, generate from current price
            if not actual_data or not forecast_data:
                print(f"⚠️ No DB data for {symbol}, generating from current price")
                try:
                    current_data = await multi_asset.get_asset_data(symbol)
                    current_price = current_data['current_price']
                    
                    import numpy as np
                    actual_data = []
                    forecast_data = []
                    timestamps = []
                    
                    for i in range(50):
                        variation = np.random.normal(0, 0.02)
                        price = current_price * (1 + variation * (50-i)/50)
                        actual_data.append(price)
                        forecast_data.append(price * (1 + np.random.normal(0, 0.01)))
                        
                        timestamp = datetime.now() - timedelta(hours=i)
                        timestamps.append(timestamp.isoformat())
                    
                    actual_data.reverse()
                    forecast_data.reverse()
                    timestamps.reverse()
                    
                except Exception as e:
                    print(f"❌ Failed to generate data for {symbol}: {e}")
                    error_data = {
                        "type": "error",
                        "symbol": symbol,
                        "message": "Historical data unavailable"
                    }
                    await websocket.send_text(json.dumps(error_data))
                    return
            
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
            
        except Exception as e:
            print(f"❌ _send_historical_data failed for {symbol}: {e}", flush=True)
            # Send error response instead of silent failure
            try:
                error_data = {
                    "type": "error",
                    "symbol": symbol,
                    "message": f"Historical data error: {str(e)}"
                }
                await websocket.send_text(json.dumps(error_data))
            except:
                pass
    
    async def _store_all_timeframes(self, symbol, price, volume, change_24h):
        """Store price data for all timeframes regardless of connections"""
        try:
            current_time = datetime.now()
            timeframes = ['1m', '5m', '15m', '1h', '4H', '1D', '1W']
            
            for timeframe in timeframes:
                timeframe_symbol = f"{symbol}_{timeframe}"
                
                # Adjust timestamp for different timeframes to prevent duplicates
                adjusted_time = self._adjust_timestamp_for_timeframe(current_time, timeframe)
                
                price_data = {
                    'current_price': price,
                    'change_24h': change_24h,
                    'volume': volume,
                    'timestamp': adjusted_time
                }
                
                # Store price data
                await self._store_realtime_data(timeframe_symbol, price_data, timeframe)
                
        except Exception as e:
            ErrorHandler.log_database_error('store_timeframes', symbol, str(e))
    
    def _adjust_timestamp_for_timeframe(self, timestamp, timeframe):
        """Adjust timestamp to prevent duplicates for different timeframes"""
        from utils.timestamp_utils import TimestampUtils
        return TimestampUtils.adjust_for_timeframe(timestamp, timeframe)
    
    async def _generate_fresh_prediction(self, symbol):
        """Generate fresh ML prediction in background"""
        try:
            prediction = await self.model.predict(symbol)
            # Cache will be updated by model.predict() method
        except Exception as e:
            pass
    
    async def _fallback_data_fetcher(self):
        """Fallback data fetcher for symbols without active WebSocket streams"""
        while True:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                # Check which symbols are missing from cache
                expected_symbols = set(self.binance_symbols.keys())
                cached_symbols = set(self.price_cache.keys())
                missing_symbols = expected_symbols - cached_symbols
                
                if missing_symbols:
                    print(f"🔄 Fetching fallback data for missing symbols: {missing_symbols}")
                    
                    # Use Binance REST API as fallback
                    try:
                        import aiohttp
                        async with aiohttp.ClientSession() as session:
                            for symbol in missing_symbols:
                                binance_symbol = self.binance_symbols.get(symbol, '').lower()
                                if binance_symbol:
                                    url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={binance_symbol.upper()}"
                                    try:
                                        async with session.get(url, timeout=10) as response:
                                            if response.status == 200:
                                                data = await response.json()
                                                price_data = {
                                                    'current_price': float(data['lastPrice']),
                                                    'change_24h': float(data['priceChangePercent']),
                                                    'volume': float(data['volume']),
                                                    'timestamp': datetime.now()
                                                }
                                                self.price_cache[symbol] = price_data
                                
                                                
                                                # Cache using centralized manager
                                                cache_key = self.cache_keys.price(symbol, 'crypto')
                                                self.cache_manager.set_cache(cache_key, price_data, ttl=30)
                                    except Exception as e:
                                        print(f"❌ Fallback failed for {symbol}: {e}")
                    except Exception as e:
                        print(f"❌ Fallback session failed: {e}")
                
            except Exception as e:
                print(f"❌ Fallback data fetcher error: {e}")
                await asyncio.sleep(60)
    
    async def _fallback_crypto_data(self, symbol, binance_symbol):
        """Fallback crypto data fetcher for individual symbols"""
        try:
            import aiohttp
            url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={binance_symbol.upper()}"
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        price_data = {
                            'current_price': float(data['lastPrice']),
                            'change_24h': float(data['priceChangePercent']),
                            'volume': float(data['volume']),
                            'timestamp': datetime.now()
                        }
                        self.price_cache[symbol] = price_data
                        print(f"🔄 Fallback data for {symbol}: ${price_data['current_price']:.2f}")
                        
                        # Cache using centralized manager
                        cache_key = self.cache_keys.price(symbol, 'crypto')
                        self.cache_manager.set_cache(cache_key, price_data, ttl=30)
                                
        except Exception as e:
            print(f"❌ Fallback crypto data failed for {symbol}: {e}")
    
    def remove_connection(self, symbol, connection_id):
        """Remove WebSocket connection"""
        if symbol in self.active_connections and connection_id in self.active_connections[symbol]:
            del self.active_connections[symbol][connection_id]


# Global real-time service - will be initialized with shared model
realtime_service = None