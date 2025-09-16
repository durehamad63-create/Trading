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
            'DOGE': 'dogeusdt', 'TRX': 'trxusdt', 'USDT': 'usdtusdc',
            'USDC': 'usdcusdt'
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
                logging.error(f"❌ Binance stream error for {symbol}: {e}")
                # Remove from cache if connection fails
                if symbol in self.price_cache:
                    del self.price_cache[symbol]
                await asyncio.sleep(10)  # Longer delay on error
    
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
        """Generate forecast for specific timeframe"""
        try:
            # Get candle data for this timeframe
            if (symbol not in self.candle_cache or 
                timeframe not in self.candle_cache[symbol] or 
                not self.candle_cache[symbol][timeframe]):
                return
            
            candles = self.candle_cache[symbol][timeframe]
            current_candle = candles[-1]
            
            # Generate ML prediction using timeframe-specific data
            prediction = self.model.predict(symbol)
            if not isinstance(prediction, dict):
                return
            
            # ML prediction generated
            
            # Create timeframe-specific forecast data
            current_price = float(current_candle['close'])
            predicted_price = float(prediction.get('predicted_price', current_price))
            
            # Consistent chart points
            chart_points = 50
            past_prices = [float(c['close']) for c in candles[-chart_points:]]
            future_prices = [predicted_price]
            timestamps = [c['timestamp'].strftime("%H:%M" if timeframe in ['1m', '5m', '15m', '30m', '1h'] else "%m-%d") for c in candles[-chart_points:]]
            
            forecast_data = {
                "type": "timeframe_forecast",
                "symbol": str(symbol),
                "timeframe": str(timeframe),
                "name": str(prediction.get('name', multi_asset.get_asset_name(symbol))),
                "forecast_direction": str(prediction.get('forecast_direction', 'HOLD')),
                "confidence": int(prediction.get('confidence', 50)),
                "predicted_range": str(prediction.get('predicted_range', f"${current_price*0.98:.2f}–${current_price*1.02:.2f}")),
                "chart": {
                    "past": past_prices,
                    "future": future_prices,
                    "timestamps": timestamps
                },
                "last_updated": current_time.isoformat(),
                "current_price": float(current_price),
                "change_24h": float(self.price_cache.get(symbol, {}).get('change_24h', 0)),
                "volume": float(current_candle['volume'])
            }
            
            # Broadcast to connections with this timeframe
            await self._broadcast_to_timeframe(symbol, timeframe, forecast_data)
            
        except Exception as e:
            logging.error(f"Error generating timeframe forecast: {e}")
    
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
        """Broadcast data to WebSocket connections for specific symbol and timeframe"""
        if symbol not in self.active_connections:
            return
        
        # Send to connections with matching timeframe
        tasks = []
        try:
            connections_copy = dict(self.active_connections[symbol])
            
            for connection_id, conn_data in connections_copy.items():
                if conn_data['timeframe'] == timeframe:
                    task = asyncio.create_task(self._send_safe(conn_data['websocket'], data, symbol, connection_id))
                    tasks.append(task)
            
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
                
        except Exception as e:
            logging.error(f"Error in _broadcast_to_timeframe: {e}")
    
    async def _send_safe(self, websocket, data, symbol, connection_id):
        """Safely send data to WebSocket connection"""
        try:
            if not hasattr(websocket, 'send_text') or not isinstance(data, dict):
                return
            
            message = json.dumps(data, default=str)
            await websocket.send_text(message)
        except Exception as e:
            # Only remove connection on specific WebSocket closed errors
            error_str = str(e).lower()
            if "connectionclosed" in error_str or "connection is closed" in error_str:
                try:
                    if symbol in self.active_connections and connection_id in self.active_connections[symbol]:
                        del self.active_connections[symbol][connection_id]
                except Exception:
                    pass
    
    async def add_connection(self, websocket, symbol, connection_id, timeframe='1D'):
        """Add new WebSocket connection with timeframe"""
        logging.info(f"🔌 DEBUG: add_connection called for {symbol} {timeframe} with connection_id {connection_id}")
        
        if symbol not in self.active_connections:
            self.active_connections[symbol] = {}
        
        self.active_connections[symbol][connection_id] = {
            'websocket': websocket,
            'timeframe': timeframe,
            'connected_at': datetime.now()
        }
        
        # Send historical data first
        logging.info(f"📊 DEBUG: About to send historical data for {symbol} {timeframe}")
        await self._send_historical_data(websocket, symbol, timeframe)
        logging.info(f"✅ DEBUG: Historical data sent for {symbol} {timeframe}")
        
        # Skip immediate forecast - let real-time service handle all updates
        logging.info(f"✅ DEBUG: Connection added for {symbol} {timeframe}, waiting for real-time updates")
    
    async def _send_historical_data(self, websocket, symbol, timeframe):
        """Send cached historical data to new WebSocket connection"""
        try:
            logging.info(f"📊 DEBUG: _send_historical_data called for {symbol} {timeframe}")
            
            # Try Redis cache first for instant loading
            cache_key = f"websocket_history:{symbol}:{timeframe}"
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
                    logging.info(f"✅ DEBUG: Found Redis cache for {symbol} {timeframe}")
                    await websocket.send_text(cached_message)
                    return
                else:
                    logging.info(f"🔍 DEBUG: No Redis cache for {cache_key}, proceeding to database")
            except Exception as e:
                logging.warning(f"⚠️ DEBUG: Redis cache failed for {symbol} {timeframe}: {e}")
            
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
            else:
                logging.info(f"✅ DEBUG: Using constructor database for {symbol} {timeframe}")
            
            # Get historical chart data from database using correct timeframe
            logging.info(f"🔍 DEBUG: WebSocket getting chart data for {symbol} {timeframe}")
            chart_data = await db.get_chart_data(symbol, timeframe)
            logging.info(f"🔍 DEBUG: WebSocket chart data result: actual={len(chart_data.get('actual', []))}, forecast={len(chart_data.get('forecast', []))}, timestamps={len(chart_data.get('timestamps', []))}")
            
            if chart_data['actual'] and chart_data['forecast']:
                # Consistent historical data points
                points = 50
                actual_data = [float(x) for x in chart_data['actual'][-points:]]
                forecast_data = [float(x) for x in chart_data['forecast'][-points:]]
                timestamps = [str(x) for x in chart_data['timestamps'][-points:]]
                logging.info(f"✅ DEBUG: WebSocket using database data for {symbol} {timeframe}: {len(actual_data)} points")
                logging.info(f"📊 DEBUG: Sample actual data: {actual_data[:3]}...")
                logging.info(f"📊 DEBUG: Sample forecast data: {forecast_data[:3]}...")
            else:
                # Generate synthetic data if no database data
                logging.warning(f"⚠️ DEBUG: WebSocket no database data for {symbol} {timeframe}, generating synthetic")
                current_price = self.price_cache.get(symbol, {}).get('current_price', 115000)
                actual_data = [current_price + (i * 100) for i in range(-15, 15)]
                forecast_data = [price * 1.01 for price in actual_data]
                from datetime import timedelta
                timestamps = [(datetime.now() - timedelta(hours=15-i)).isoformat() for i in range(30)]
                logging.info(f"📊 DEBUG: Generated synthetic data with {len(actual_data)} points")
            
            historical_message = {
                "type": "historical_data",
                "symbol": str(symbol),
                "timeframe": str(timeframe),
                "name": str(multi_asset.get_asset_name(symbol)),
                "chart": {
                    "past": actual_data,
                    "future": forecast_data,
                    "timestamps": timestamps
                },
                "last_updated": datetime.now().isoformat()
            }
            
            message_json = json.dumps(historical_message)
            logging.info(f"✅ DEBUG: Sending historical message for {symbol} {timeframe} with {len(actual_data)} data points")
            logging.info(f"📊 DEBUG: Historical message type: {historical_message['type']}, chart keys: {list(historical_message['chart'].keys())}")
            await websocket.send_text(message_json)
            logging.info(f"✅ DEBUG: Historical message sent successfully for {symbol} {timeframe}")
            
        except Exception as e:
            logging.error(f"❌ DEBUG: Failed to send historical data for {symbol} {timeframe}: {e}")
            import traceback
            logging.error(f"🔍 DEBUG: Full traceback: {traceback.format_exc()}")
    
    def remove_connection(self, symbol, connection_id):
        """Remove WebSocket connection"""
        if symbol in self.active_connections and connection_id in self.active_connections[symbol]:
            del self.active_connections[symbol][connection_id]


# Global real-time service - will be initialized with shared model
realtime_service = None