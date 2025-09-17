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
        """Simulate macro economic data updates"""
        while True:
            try:
                for symbol, config in self.macro_indicators.items():
                    # Simulate realistic economic data changes
                    base_value = config['value']
                    trend = config['change']
                    volatility = config['volatility']
                    
                    # Add realistic noise and trend
                    change = np.random.normal(trend, volatility)
                    new_value = base_value * (1 + change)
                    
                    # Update base value for next iteration
                    self.macro_indicators[symbol]['value'] = new_value
                    
                    # Calculate percentage change
                    change_pct = change * 100
                    
                    # Update cache
                    cache_data = {
                        'current_price': new_value,
                        'change_24h': change_pct,
                        'volume': 1000000,  # Simulated volume
                        'timestamp': datetime.now(),
                        'unit': config['unit']
                    }
                    self.price_cache[symbol] = cache_data
                    
                    # Cache using centralized manager
                    cache_key = self.cache_keys.price(symbol, 'macro')
                    self.cache_manager.set_cache(cache_key, cache_data, ttl=60)
    
                    
                    # Macro updates (logging removed)
                    
                    # Store data for all timeframes if connections exist
                    if symbol in self.active_connections and self.active_connections[symbol]:
                        asyncio.create_task(self._store_macro_data_all_timeframes(symbol, self.price_cache[symbol]))
                        asyncio.create_task(self._broadcast_macro_update(symbol, self.price_cache[symbol]))
                
                # Update every 30 seconds (macro data changes slowly)
                await asyncio.sleep(30)
                
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
            
            print(f"📊 Macro DB Query: {symbol} for timeframe {timeframe}")
            chart_data = await db.get_chart_data(symbol, timeframe)
            print(f"📊 Macro DB Result: {len(chart_data.get('actual', []))} actual, {len(chart_data.get('forecast', []))} forecast")
            
            if chart_data['actual'] and chart_data['forecast']:
                # Ensure proper data alignment
                min_length = min(len(chart_data['actual']), len(chart_data['forecast']), len(chart_data['timestamps']))
                points = min(50, min_length)
                
                actual_data = [float(x) for x in chart_data['actual'][-points:]]
                forecast_data = [float(x) for x in chart_data['forecast'][-points:]]
                timestamps = [str(x) for x in chart_data['timestamps'][-points:]]
                
                print(f"📊 Macro historical data for {symbol}: {len(actual_data)} actual, {len(forecast_data)} forecast")
            else:
                print(f"❌ No macro database data for {symbol} - not sending fake data")
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
    
    async def close(self):
        """Close the service"""
        if self.session:
            await self.session.close()

# Global macro service instance
macro_realtime_service = None