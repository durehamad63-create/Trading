"""
Real-time Macro Indicators Stream Service
"""
import asyncio
import json
import logging
import aiohttp
from datetime import datetime, timedelta
from typing import Dict
import os
import numpy as np
from dotenv import load_dotenv
from utils.error_handler import ErrorHandler

class MacroRealtimeService:
    def __init__(self, model=None, database=None):
        self.model = model
        self.database = database
        self.active_connections = {}
        self.price_cache = {}
        
        # Macro indicators with realistic base values
        self.macro_indicators = {
            'GDP': {'value': 27000, 'unit': 'B', 'change': 0.02, 'volatility': 0.001},
            'CPI': {'value': 310.5, 'unit': '', 'change': 0.003, 'volatility': 0.002},
            'UNEMPLOYMENT': {'value': 3.7, 'unit': '%', 'change': -0.001, 'volatility': 0.01},
            'FED_RATE': {'value': 5.25, 'unit': '%', 'change': 0.0, 'volatility': 0.005},
            'CONSUMER_CONFIDENCE': {'value': 102.3, 'unit': '', 'change': 0.001, 'volatility': 0.02}
        }
        
        load_dotenv()
        self.session = None
        self.fred_api_key = os.getenv('FRED_API_KEY')
        
        # FRED series IDs for real economic data
        self.fred_series = {
            'GDP': 'GDP',
            'CPI': 'CPIAUCSL',
            'UNEMPLOYMENT': 'UNRATE',
            'FED_RATE': 'FEDFUNDS',
            'CONSUMER_CONFIDENCE': 'UMCSENT'
        }
        
        # Use centralized cache manager
        from utils.cache_manager import CacheManager, CacheKeys
        self.cache_manager = CacheManager
        self.cache_keys = CacheKeys
        
    async def start_macro_streams(self):
        """Start macro indicators simulation"""
        self.session = aiohttp.ClientSession()
        
        # Start macro data simulation
        print(f"🚀 Starting macro streams for {len(self.macro_indicators)} indicators...")
        asyncio.create_task(self._macro_data_stream())
        
    async def _macro_data_stream(self):
        """Fetch real macro economic data from FRED API"""
        while True:
            try:
                for symbol, config in self.macro_indicators.items():
                    # Try to get real FRED data first
                    real_data = await self._fetch_fred_data(symbol)
                    
                    if real_data:
                        new_value = real_data['value']
                        change_pct = real_data['change_pct']
                        print(f"📊 FRED: {symbol} = {new_value} ({change_pct:+.2f}%)")
                    else:
                        # Fallback to simulation if FRED fails
                        base_value = config['value']
                        trend = config['change']
                        volatility = config['volatility']
                        
                        change = np.random.normal(trend, volatility)
                        new_value = base_value * (1 + change)
                        change_pct = change * 100
                        
                        # Update base value for next iteration
                        self.macro_indicators[symbol]['value'] = new_value
                    
                    # Update cache
                    cache_data = {
                        'current_price': new_value,
                        'change_24h': change_pct,
                        'volume': 1000000,
                        'timestamp': datetime.now(),
                        'unit': config['unit']
                    }
                    self.price_cache[symbol] = cache_data
                    
                    # Cache using centralized manager
                    cache_key = self.cache_keys.price(symbol, 'macro')
                    self.cache_manager.set_cache(cache_key, cache_data, ttl=300)  # 5 min cache for FRED data
                    
                    # Store data for all timeframes if connections exist
                    if symbol in self.active_connections and self.active_connections[symbol]:
                        asyncio.create_task(self._store_macro_data_all_timeframes(symbol, self.price_cache[symbol]))
                        asyncio.create_task(self._broadcast_macro_update(symbol, self.price_cache[symbol]))
                
                # Update every 5 minutes (FRED data updates infrequently)
                await asyncio.sleep(300)
                
            except Exception as e:
                ErrorHandler.log_stream_error('macro', 'ALL', str(e))
                await asyncio.sleep(60)
    
    async def _broadcast_macro_update(self, symbol, price_data):
        """Broadcast macro indicator updates"""
        try:
            current_time = datetime.now()
            
            # Get unique timeframes from active connections
            timeframes = set()
            if symbol in self.active_connections:
                for conn_data in self.active_connections[symbol].values():
                    timeframes.add(conn_data['timeframe'])
            
            # Broadcast to all timeframes
            for timeframe in timeframes:
                macro_data = {
                    "type": "macro_update",
                    "symbol": str(symbol),
                    "timeframe": str(timeframe),
                    "current_price": float(price_data['current_price']),
                    "change_24h": float(price_data['change_24h']),
                    "volume": float(price_data['volume']),
                    "unit": price_data['unit'],
                    "data_source": "Economic Simulation",
                    "timestamp": current_time.strftime("%H:%M:%S"),
                    "last_updated": current_time.isoformat()
                }
                
                await self._broadcast_to_timeframe(symbol, timeframe, macro_data)
                
        except Exception as e:
            ErrorHandler.log_websocket_error('macro_broadcast', str(e))
    
    async def _store_macro_data_all_timeframes(self, symbol, price_data):
        """Store macro data for all timeframes"""
        try:
            current_time = datetime.now()
            timeframes = ['1W', '1M']  # Macro indicators only update weekly/monthly
            
            for timeframe in timeframes:
                timeframe_symbol = f"{symbol}_{timeframe}"
                
                # Adjust timestamp for timeframe
                adjusted_time = self._adjust_timestamp_for_timeframe(current_time, timeframe)
                
                adjusted_price_data = {
                    **price_data,
                    'timestamp': adjusted_time
                }
                
                # Store data
                if self.database and self.database.pool:
                    await self.database.store_actual_price(timeframe_symbol, adjusted_price_data, timeframe)
                    
                    # Generate and store forecast
                    try:
                        prediction = await self.model.predict(symbol)
                        await self.database.store_forecast(timeframe_symbol, prediction)
                    except Exception as e:
                        pass
                        
        except Exception as e:
            ErrorHandler.log_database_error('macro_store_timeframes', 'ALL', str(e))
    
    def _adjust_timestamp_for_timeframe(self, timestamp, timeframe):
        """Adjust timestamp for different timeframes"""
        if timeframe == '1M':
            # Monthly: round to start of month
            return timestamp.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        elif timeframe == '1W':
            # Weekly: round to start of week (Monday)
            days_since_monday = timestamp.weekday()
            week_start = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
            return week_start - timedelta(days=days_since_monday)
        elif timeframe == '1D':
            # Daily: round to start of day
            return timestamp.replace(hour=0, minute=0, second=0, microsecond=0)
        elif timeframe == '4H':
            # 4-hour: round to 4-hour boundaries
            hour_boundary = (timestamp.hour // 4) * 4
            return timestamp.replace(hour=hour_boundary, minute=0, second=0, microsecond=0)
        else:
            # 1h and others: round to hour
            return timestamp.replace(minute=0, second=0, microsecond=0)
    
    async def _broadcast_to_timeframe(self, symbol, timeframe, data):
        """Broadcast to specific timeframe connections"""
        if symbol not in self.active_connections:
            return
        
        message = json.dumps(data, default=str)
        
        matching_connections = [
            (conn_id, conn_data['websocket']) 
            for conn_id, conn_data in self.active_connections[symbol].items() 
            if conn_data['timeframe'] == timeframe
        ]
        
        if not matching_connections:
            return
        
        semaphore = asyncio.Semaphore(10)
        
        async def send_to_connection(conn_id, websocket):
            async with semaphore:
                try:
                    await websocket.send_text(message)
                except Exception as e:
                    if "connectionclosed" in str(e).lower():
                        self.active_connections[symbol].pop(conn_id, None)
        
        await asyncio.gather(
            *[send_to_connection(conn_id, ws) for conn_id, ws in matching_connections],
            return_exceptions=True
        )
    
    async def add_connection(self, websocket, symbol, connection_id, timeframe='1D'):
        """Add macro indicator connection"""
        print(f"🏦 Macro service adding connection for {symbol} with ID {connection_id}")
        
        try:
            if symbol not in self.active_connections:
                self.active_connections[symbol] = {}
                print(f"🆕 Created new macro symbol entry for {symbol}")
            
            self.active_connections[symbol][connection_id] = {
                'websocket': websocket,
                'timeframe': timeframe,
                'connected_at': datetime.now()
            }
            print(f"✅ Macro connection stored for {symbol}")
            
            # Send historical data
            print(f"📊 Sending macro historical data for {symbol}")
            await self._send_macro_historical_data(websocket, symbol, timeframe)
            print(f"✅ Macro historical data sent for {symbol}")
            
        except Exception as e:
            print(f"❌ Error in macro add_connection for {symbol}: {e}")
            raise
    
    async def _send_macro_historical_data(self, websocket, symbol, timeframe):
        """Send historical macro data"""
        print(f"🏦 _send_macro_historical_data called for {symbol} {timeframe}")
        try:
            # Use database or generate synthetic data
            db = self.database
            print(f"📊 Macro database available: {db is not None and hasattr(db, 'pool') and db.pool is not None}")
            if not db or not db.pool:
                try:
                    from database import db as global_db
                    if global_db and global_db.pool:
                        db = global_db
                        print(f"🔄 Using global database for macro")
                    else:
                        print(f"❌ No database available for macro {symbol}")
                        # Send minimal response instead of failing
                        minimal_data = {
                            "type": "historical_data",
                            "symbol": symbol,
                            "timeframe": timeframe,
                            "message": "Macro historical data unavailable"
                        }
                        await websocket.send_text(json.dumps(minimal_data))
                        return
                except Exception as e:
                    print(f"❌ Macro database fallback failed: {e}")
                    return
            
            # Try multiple database query formats for macro historical data
            chart_data = None
            actual_data = []
            forecast_data = []
            timestamps = []
            
            # Query attempts with different symbol formats (macro uses 1W, 1M timeframes)
            query_attempts = [
                f"{symbol}_{timeframe}",  # GDP_1W
                f"{symbol}_1W",  # GDP_1W (default macro timeframe)
                f"{symbol}_1M",  # GDP_1M (monthly macro)
                symbol  # GDP (fallback)
            ]
            
            for attempt, query_symbol in enumerate(query_attempts):
                print(f"📊 Macro DB Query attempt {attempt+1}: {query_symbol}")
                try:
                    chart_data = await db.get_chart_data(query_symbol, timeframe)
                    if chart_data and chart_data.get('actual') and chart_data.get('forecast'):
                        print(f"✅ Found macro data: {len(chart_data['actual'])} actual, {len(chart_data['forecast'])} forecast")
                        
                        # Process successful data
                        min_length = min(len(chart_data['actual']), len(chart_data['forecast']), len(chart_data['timestamps']))
                        points = min(50, min_length)
                        
                        actual_data = [float(x) for x in chart_data['actual'][-points:]]
                        forecast_data = [float(x) for x in chart_data['forecast'][-points:]]
                        timestamps = [str(x) for x in chart_data['timestamps'][-points:]]
                        break
                except Exception as e:
                    print(f"❌ Macro query {attempt+1} failed: {e}")
                    continue
            
            # If no database data, generate from current value
            if not actual_data or not forecast_data:
                print(f"⚠️ No DB data for macro {symbol}, generating from current value")
                try:
                    current_data = multi_asset._get_macro_data(symbol)
                    current_value = current_data['current_price']
                    
                    import numpy as np
                    actual_data = []
                    forecast_data = []
                    timestamps = []
                    
                    for i in range(50):
                        variation = np.random.normal(0, 0.005)
                        value = current_value * (1 + variation * (50-i)/50)
                        actual_data.append(value)
                        forecast_data.append(value * (1 + np.random.normal(0, 0.003)))
                        
                        timestamp = datetime.now() - timedelta(weeks=i)
                        timestamps.append(timestamp.isoformat())
                    
                    actual_data.reverse()
                    forecast_data.reverse()
                    timestamps.reverse()
                    
                except Exception as e:
                    print(f"❌ Failed to generate macro data for {symbol}: {e}")
                    error_data = {
                        "type": "error",
                        "symbol": symbol,
                        "message": "Macro historical data unavailable"
                    }
                    await websocket.send_text(json.dumps(error_data))
                    return
            
            # Get indicator name
            indicator_names = {
                'GDP': 'Gross Domestic Product',
                'CPI': 'Consumer Price Index',
                'UNEMPLOYMENT': 'Unemployment Rate',
                'FED_RATE': 'Federal Interest Rate',
                'CONSUMER_CONFIDENCE': 'Consumer Confidence Index'
            }
            
            historical_message = {
                "type": "historical_data",
                "symbol": str(symbol),
                "timeframe": str(timeframe),
                "name": indicator_names.get(symbol, symbol),
                "chart": {
                    "actual": actual_data,
                    "predicted": forecast_data,
                    "timestamps": timestamps
                },
                "last_updated": datetime.now().isoformat()
            }
            
            await websocket.send_text(json.dumps(historical_message))
            
        except Exception as e:
            print(f"❌ _send_macro_historical_data failed for {symbol}: {e}")
            # Send error response instead of silent failure
            try:
                error_data = {
                    "type": "error",
                    "symbol": symbol,
                    "message": f"Macro historical data error: {str(e)}"
                }
                await websocket.send_text(json.dumps(error_data))
            except:
                pass
    
    def remove_connection(self, symbol, connection_id):
        """Remove macro indicator connection"""
        if symbol in self.active_connections and connection_id in self.active_connections[symbol]:
            del self.active_connections[symbol][connection_id]
    
    async def _fetch_fred_data(self, symbol):
        """Fetch real data from FRED API"""
        if not self.fred_api_key:
            return None
            
        try:
            series_id = self.fred_series.get(symbol)
            if not series_id:
                return None
                
            # Get latest 2 observations to calculate change
            url = f"https://api.stlouisfed.org/fred/series/observations"
            params = {
                'series_id': series_id,
                'api_key': self.fred_api_key,
                'file_type': 'json',
                'limit': 2,
                'sort_order': 'desc'
            }
            
            if not self.session:
                self.session = aiohttp.ClientSession()
                
            async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                if response.status == 200:
                    data = await response.json()
                    observations = data.get('observations', [])
                    
                    if len(observations) >= 1:
                        current_obs = observations[0]
                        current_value = float(current_obs['value'])
                        
                        # Calculate change if we have previous value
                        change_pct = 0
                        if len(observations) >= 2:
                            prev_value = float(observations[1]['value'])
                            if prev_value != 0:
                                change_pct = ((current_value - prev_value) / prev_value) * 100
                        
                        return {
                            'value': current_value,
                            'change_pct': change_pct,
                            'date': current_obs['date']
                        }
                        
        except Exception as e:
            print(f"❌ FRED API error for {symbol}: {e}")
            return None
    
    async def close(self):
        """Close the service"""
        if self.session:
            await self.session.close()

# Global macro service instance
macro_realtime_service = None