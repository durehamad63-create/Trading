"""
Real-time Stock Data Stream Service
"""
import asyncio
import json
import logging
import aiohttp
from datetime import datetime, timedelta
from typing import Dict
# Removed yfinance import - using direct API calls
import os
from dotenv import load_dotenv

class StockRealtimeService:
    def __init__(self, model=None, database=None):
        self.model = model
        self.database = database
        self.active_connections = {}
        self.price_cache = {}
        self.candle_cache = {}
        
        # Stock symbols from requirements
        self.stock_symbols = {
            'NVDA': 'NVIDIA', 'MSFT': 'Microsoft', 'AAPL': 'Apple',
            'GOOGL': 'Alphabet', 'AMZN': 'Amazon', 'META': 'Meta',
            'AVGO': 'Broadcom', 'TSLA': 'Tesla', 'BRK-B': 'Berkshire Hathaway',
            'JPM': 'JPMorgan Chase'
        }
        
        # API keys from environment
        load_dotenv()
        self.alpha_vantage_key = os.getenv('ALPHA_VANTAGE_API_KEY', 'YOUR_API_KEY')
        self.iex_token = os.getenv('IEX_CLOUD_TOKEN', 'YOUR_TOKEN')
        
        # Update intervals per timeframe (seconds)
        self.update_intervals = {
            '1m': 60, '5m': 300, '15m': 900, '30m': 1800,
            '1h': 3600, '4h': 14400, '1D': 86400, '1W': 604800
        }
        
        self.last_update = {}
        self.session = None
        
        # Initialize Redis for stock caching
        self.redis_client = None
        try:
            import redis
            self.redis_client = redis.Redis(
                host=os.getenv('REDIS_HOST', 'localhost'),
                port=int(os.getenv('REDIS_PORT', '6379')),
                db=int(os.getenv('REDIS_PREDICTION_DB', '1')),
                password=os.getenv('REDIS_PASSWORD', None) if os.getenv('REDIS_PASSWORD') else None,
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            self.redis_client.ping()
            logging.info(f"✅ Stock Redis connected: {os.getenv('REDIS_HOST', 'localhost')}:{os.getenv('REDIS_PORT', '6379')}")
        except Exception as e:
            logging.warning(f"⚠️ Stock Redis not available: {e}")
            self.redis_client = None
    
    async def start_stock_streams(self):
        """Start real-time data collection for all stocks"""
        self.session = aiohttp.ClientSession()
        logging.info("🏢 Starting stock real-time streams...")
        
        # Start single rotating stream for all stocks
        asyncio.create_task(self._rotating_stock_stream())
        
        logging.info(f"✅ Rotating stock stream started for {len(self.stock_symbols)} symbols")
    
    async def _rotating_stock_stream(self):
        """Dual stream that processes two stocks simultaneously"""
        symbols = list(self.stock_symbols.keys())
        current_index = 0
        
        logging.info(f"🔄 Starting dual rotating stock stream for {len(symbols)} symbols")
        
        while True:
            try:
                # Get two symbols for parallel processing
                symbol1 = symbols[current_index]
                symbol2 = symbols[(current_index + 1) % len(symbols)]
                
                # Process two stocks simultaneously
                tasks = [
                    self._process_single_stock(symbol1),
                    self._process_single_stock(symbol2)
                ]
                
                await asyncio.gather(*tasks, return_exceptions=True)
                
                # Move to next pair of symbols
                current_index = (current_index + 2) % len(symbols)
                
                # Wait 1 second before next pair (completes full cycle in 5 seconds)
                await asyncio.sleep(1)
                
            except Exception as e:
                logging.error(f"❌ Dual rotating stock stream error: {e}")
                await asyncio.sleep(5)
    
    async def _process_single_stock(self, symbol):
        """Process a single stock update"""
        try:
            # Get real-time price data
            price_data = await self._get_realtime_stock_data(symbol)
            if price_data:
                # Update cache
                self.price_cache[symbol] = {
                    **price_data,
                    'timestamp': datetime.now()
                }
                
                # Log price updates
                # logging.info(f"📊 STOCK UPDATE: {symbol} = ${price_data['current_price']:.2f} ({price_data['change_24h']:+.2f}%) - {price_data['data_source']}")
                
                # Update candles and generate forecasts if connections exist
                if symbol in self.active_connections and self.active_connections[symbol]:
                    await self._update_stock_candles_and_forecast(symbol, price_data)
        except Exception as e:
            logging.error(f"❌ Error processing {symbol}: {e}")
    

    
    async def _get_realtime_stock_data(self, symbol) -> Dict:
        """Get real-time stock data with rate limiting and fallback"""
        
        # No delay needed in rotating pattern
        
        # 1. Try Yahoo Finance direct API with rate limit handling
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            async with self.session.get(url, timeout=15, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    if 'chart' in data and data['chart']['result']:
                        result = data['chart']['result'][0]
                        meta = result['meta']
                        current_price = meta['regularMarketPrice']
                        prev_close = meta['previousClose']
                        change_pct = ((current_price - prev_close) / prev_close) * 100
                        
                        # logging.info(f"✅ Yahoo API success for {symbol}: ${current_price}")
                        return {
                            'current_price': float(current_price),
                            'change_24h': float(change_pct),
                            'volume': float(meta.get('regularMarketVolume', 0)),
                            'high': float(meta.get('regularMarketDayHigh', current_price)),
                            'low': float(meta.get('regularMarketDayLow', current_price)),
                            'data_source': 'Yahoo Finance API'
                        }
                elif response.status == 429:
                    logging.warning(f"⚠️ Rate limited for {symbol}, will retry later")
                    return None
        except Exception as e:
            logging.warning(f"⚠️ Yahoo API error for {symbol}: {e}")
        
        # 2. Try Alpha Vantage if available
        try:
            if self.alpha_vantage_key != "YOUR_API_KEY":
                url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={self.alpha_vantage_key}"
                async with self.session.get(url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        if "Global Quote" in data:
                            quote = data["Global Quote"]
                            return {
                                'current_price': float(quote["05. price"]),
                                'change_24h': float(quote["10. change percent"].replace('%', '')),
                                'volume': float(quote["06. volume"]),
                                'high': float(quote["03. high"]),
                                'low': float(quote["04. low"]),
                                'data_source': 'Alpha Vantage'
                            }
        except Exception as e:
            logging.warning(f"Alpha Vantage failed for {symbol}: {e}")
        
        # 3. Try IEX Cloud if available
        try:
            if self.iex_token != "YOUR_TOKEN":
                url = f"https://cloud.iexapis.com/stable/stock/{symbol}/quote?token={self.iex_token}"
                async with self.session.get(url, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        return {
                            'current_price': float(data["latestPrice"]),
                            'change_24h': float(data["changePercent"]) * 100,
                            'volume': float(data["latestVolume"]),
                            'high': float(data["high"]),
                            'low': float(data["low"]),
                            'data_source': 'IEX Cloud'
                        }
        except Exception as e:
            logging.warning(f"IEX Cloud failed for {symbol}: {e}")
        
        return None
    
    async def _update_stock_candles_and_forecast(self, symbol, price_data):
        """Update candle data and generate forecasts for active timeframes"""
        try:
            current_time = datetime.now()
            
            # Get unique timeframes from active connections
            timeframes = set()
            if symbol in self.active_connections:
                for conn_data in self.active_connections[symbol].values():
                    timeframes.add(conn_data['timeframe'])
            
            # Update candles for each timeframe
            for timeframe in timeframes:
                await self._update_stock_candle_data(symbol, timeframe, price_data, current_time)
                
                # Rate limit predictions per timeframe
                rate_key = f"{symbol}_{timeframe}"
                if rate_key in self.last_update:
                    time_diff = (current_time - self.last_update[rate_key]).total_seconds()
                    if time_diff < self.update_intervals.get(timeframe, 300):
                        continue
                
                # Generate timeframe-specific forecast
                await self._generate_stock_forecast(symbol, timeframe, current_time)
                self.last_update[rate_key] = current_time
                logging.info(f"🤖 Generated ML forecast for {symbol} {timeframe}")
                
        except Exception as e:
            logging.error(f"❌ Error updating stock candles for {symbol}: {e}")
    
    async def _update_stock_candle_data(self, symbol, timeframe, price_data, timestamp):
        """Update candle data for specific timeframe"""
        try:
            if symbol not in self.candle_cache:
                self.candle_cache[symbol] = {}
            if timeframe not in self.candle_cache[symbol]:
                self.candle_cache[symbol][timeframe] = []
            
            # Get timeframe interval in minutes
            interval_map = {
                '1m': 1, '5m': 5, '15m': 15, '30m': 30,
                '1h': 60, '4h': 240, '1D': 1440, '1W': 10080
            }
            interval_minutes = interval_map.get(timeframe, 60)
            
            # Round timestamp to candle boundary
            candle_start = timestamp.replace(second=0, microsecond=0)
            if interval_minutes >= 60:
                candle_start = candle_start.replace(minute=0)
                if interval_minutes >= 1440:
                    candle_start = candle_start.replace(hour=0)
            else:
                minute_boundary = (candle_start.minute // interval_minutes) * interval_minutes
                candle_start = candle_start.replace(minute=minute_boundary)
            
            candles = self.candle_cache[symbol][timeframe]
            current_price = price_data['current_price']
            volume = price_data['volume']
            
            # Update or create current candle
            if candles and candles[-1]['timestamp'] == candle_start:
                candle = candles[-1]
                candle['high'] = max(candle['high'], current_price)
                candle['low'] = min(candle['low'], current_price)
                candle['close'] = current_price
                candle['volume'] += volume
            else:
                new_candle = {
                    'timestamp': candle_start,
                    'open': current_price,
                    'high': current_price,
                    'low': current_price,
                    'close': current_price,
                    'volume': volume
                }
                candles.append(new_candle)
                
                if len(candles) > 100:
                    candles.pop(0)
                    
        except Exception as e:
            logging.error(f"Error updating stock candle data: {e}")
    
    async def _generate_stock_forecast(self, symbol, timeframe, current_time):
        """Generate ML forecast for stock symbol and timeframe"""
        try:
            # Get candle data
            if (symbol not in self.candle_cache or 
                timeframe not in self.candle_cache[symbol] or 
                not self.candle_cache[symbol][timeframe]):
                return
            
            candles = self.candle_cache[symbol][timeframe]
            current_candle = candles[-1]
            
            # Generate ML prediction
            prediction = self.model.predict(symbol)
            if not isinstance(prediction, dict):
                return
            
            # Cache prediction in Redis
            if self.redis_client:
                try:
                    import json
                    cache_key = f"prediction:{symbol}"
                    self.redis_client.setex(cache_key, 30, json.dumps(prediction))
                    logging.info(f"💾 Cached stock prediction for {symbol} in Redis")
                except Exception as e:
                    logging.warning(f"Redis cache failed for {symbol}: {e}")
            
            # Create forecast data
            current_price = float(current_candle['close'])
            predicted_price = float(prediction.get('predicted_price', current_price))
            
            # Chart data
            chart_points = 50
            past_prices = [float(c['close']) for c in candles[-chart_points:]]
            future_prices = [predicted_price]
            timestamps = [c['timestamp'].strftime("%H:%M" if timeframe in ['1m', '5m', '15m', '30m', '1h'] else "%m-%d") for c in candles[-chart_points:]]
            
            forecast_data = {
                "type": "stock_forecast",
                "symbol": str(symbol),
                "timeframe": str(timeframe),
                "name": str(self.stock_symbols.get(symbol, symbol)),
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
            
            # Broadcast to connections
            connections_count = len(self.active_connections.get(symbol, {}))
            await self._broadcast_to_timeframe(symbol, timeframe, forecast_data)
            if connections_count > 0:
                logging.info(f"📡 Broadcasted {symbol} forecast to {connections_count} WebSocket connections")
            
        except Exception as e:
            logging.error(f"❌ Error generating stock forecast: {e}")
    
    async def _broadcast_to_timeframe(self, symbol, timeframe, data):
        """Broadcast data to WebSocket connections for specific symbol and timeframe"""
        if symbol not in self.active_connections:
            return
        
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
            logging.error(f"❌ Error in stock broadcast: {e}")
    
    async def _send_safe(self, websocket, data, symbol, connection_id):
        """Safely send data to WebSocket connection"""
        try:
            message = json.dumps(data, default=str)
            await websocket.send_text(message)
        except Exception as e:
            error_str = str(e).lower()
            if "connectionclosed" in error_str or "connection is closed" in error_str:
                try:
                    if symbol in self.active_connections and connection_id in self.active_connections[symbol]:
                        del self.active_connections[symbol][connection_id]
                except Exception:
                    pass
    
    async def add_connection(self, websocket, symbol, connection_id, timeframe='1D'):
        """Add new WebSocket connection for stock"""
        logging.info(f"🔌 DEBUG: add_connection called for stock {symbol} {timeframe} with connection_id {connection_id}")
        
        if symbol not in self.active_connections:
            self.active_connections[symbol] = {}
        
        self.active_connections[symbol][connection_id] = {
            'websocket': websocket,
            'timeframe': timeframe,
            'connected_at': datetime.now()
        }
        
        # Send historical data
        logging.info(f"📊 DEBUG: About to send stock historical data for {symbol} {timeframe}")
        await self._send_stock_historical_data(websocket, symbol, timeframe)
        logging.info(f"✅ DEBUG: Stock historical data sent for {symbol} {timeframe}")
        logging.info(f"🔌 Stock WebSocket connected: {symbol} {timeframe} (Total: {len(self.active_connections[symbol])})")
    
    async def _send_stock_historical_data(self, websocket, symbol, timeframe):
        """Send historical stock data to new connection"""
        try:
            logging.info(f"📊 DEBUG: _send_stock_historical_data called for {symbol} {timeframe}")
            
            # Use database from constructor or fallback to global
            db = self.database
            if not db or not db.pool:
                try:
                    from database import db as global_db
                    if global_db and global_db.pool:
                        db = global_db
                        logging.info(f"✅ DEBUG: Using global database for stock {symbol} {timeframe}")
                    else:
                        logging.error(f"❌ DEBUG: No database available for stock {symbol} {timeframe}")
                        return
                except Exception as e:
                    logging.error(f"❌ DEBUG: Database import failed for stock {symbol} {timeframe}: {e}")
                    return
            else:
                logging.info(f"✅ DEBUG: Using constructor database for stock {symbol} {timeframe}")
                
            logging.info(f"🔍 DEBUG: Getting stock chart data for {symbol} {timeframe}")
            chart_data = await db.get_chart_data(symbol, timeframe)
            logging.info(f"🔍 DEBUG: Stock chart data result: actual={len(chart_data.get('actual', []))}, forecast={len(chart_data.get('forecast', []))}, timestamps={len(chart_data.get('timestamps', []))}")
            
            if chart_data['actual'] and chart_data['forecast']:
                points = 50
                actual_data = [float(x) for x in chart_data['actual'][-points:]]
                forecast_data = [float(x) for x in chart_data['forecast'][-points:]]
                timestamps = [str(x) for x in chart_data['timestamps'][-points:]]
                logging.info(f"✅ DEBUG: Using database data for stock {symbol} {timeframe}: {len(actual_data)} points")
                logging.info(f"📊 DEBUG: Sample stock actual data: {actual_data[:3]}...")
            else:
                # Generate synthetic data
                logging.warning(f"⚠️ DEBUG: No database data for stock {symbol} {timeframe}, generating synthetic")
                current_price = self.price_cache.get(symbol, {}).get('current_price', 150)
                actual_data = [current_price + (i * 2) for i in range(-25, 25)]
                forecast_data = [price * 1.005 for price in actual_data]
                timestamps = [(datetime.now() - timedelta(hours=25-i)).isoformat() for i in range(50)]
                logging.info(f"📊 DEBUG: Generated synthetic stock data with {len(actual_data)} points")
            
            historical_message = {
                "type": "stock_historical_data",
                "symbol": str(symbol),
                "timeframe": str(timeframe),
                "name": str(self.stock_symbols.get(symbol, symbol)),
                "chart": {
                    "past": actual_data,
                    "future": forecast_data,
                    "timestamps": timestamps
                },
                "last_updated": datetime.now().isoformat()
            }
            
            logging.info(f"✅ DEBUG: Sending stock historical message for {symbol} {timeframe} with {len(actual_data)} data points")
            await websocket.send_text(json.dumps(historical_message))
            
        except Exception as e:
            logging.error(f"❌ DEBUG: Failed to send stock historical data for {symbol} {timeframe}: {e}")
            import traceback
            logging.error(f"🔍 DEBUG: Full traceback: {traceback.format_exc()}")
    
    def remove_connection(self, symbol, connection_id):
        """Remove WebSocket connection"""
        logging.info(f"🔌 DEBUG: remove_connection called for stock {symbol} connection_id {connection_id}")
        if symbol in self.active_connections and connection_id in self.active_connections[symbol]:
            del self.active_connections[symbol][connection_id]
            remaining = len(self.active_connections[symbol])
            logging.info(f"🔌 DEBUG: Stock WebSocket disconnected: {symbol} (Remaining: {remaining})")
        else:
            logging.warning(f"⚠️ DEBUG: Connection {connection_id} not found for stock {symbol}")
    
    async def close(self):
        """Close the service and cleanup"""
        if self.session:
            await self.session.close()

# Global stock service instance
stock_realtime_service = None