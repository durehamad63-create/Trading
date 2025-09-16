"""
Database models and operations for trading forecasts
"""
import asyncpg
import asyncio
from datetime import datetime
import json
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class TradingDatabase:
    def __init__(self, database_url=None):
        self.database_url = database_url or os.getenv('DATABASE_URL', 'postgresql://postgres:password@localhost/trading_db')
        self.database_url = database_url
        self.pool = None
    
    async def connect(self):
        """Initialize database connection pool"""
        try:
            self.pool = await asyncpg.create_pool(
                self.database_url, 
                min_size=5, 
                max_size=20,
                command_timeout=5,
                server_settings={
                    'application_name': 'trading_app',
                    'tcp_keepalives_idle': '600',
                    'tcp_keepalives_interval': '30',
                    'tcp_keepalives_count': '3'
                }
            )
            await self.create_tables()
            logging.info(f"✅ Database connected with pool: min={self.pool._minsize}, max={self.pool._maxsize}")
        except Exception as e:
            logging.error(f"❌ Database connection failed: {e}")
            self.pool = None
    
    def get_pool_stats(self):
        """Get connection pool statistics"""
        if not self.pool:
            return {'status': 'no_pool'}
        
        return {
            'size': self.pool.get_size(),
            'min_size': self.pool.get_min_size(),
            'max_size': self.pool.get_max_size(),
            'idle_size': self.pool.get_idle_size(),
            'status': 'active'
        }
    
    async def create_tables(self):
        """Create required tables"""
        async with self.pool.acquire() as conn:
            # Forecasts table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS forecasts (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    forecast_direction VARCHAR(10) NOT NULL,
                    confidence INTEGER NOT NULL,
                    predicted_price DECIMAL(15,2),
                    predicted_range VARCHAR(100),
                    trend_score INTEGER,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Actual prices table with OHLC data
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS actual_prices (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(20) NOT NULL,
                    timeframe VARCHAR(10) NOT NULL,
                    open_price DECIMAL(15,2),
                    high DECIMAL(15,2),
                    low DECIMAL(15,2),
                    close_price DECIMAL(15,2),
                    price DECIMAL(15,2) NOT NULL,
                    change_24h DECIMAL(8,4),
                    volume DECIMAL(20,2),
                    timestamp TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Add missing columns if they don't exist (for existing databases)
            try:
                await conn.execute("ALTER TABLE actual_prices ADD COLUMN IF NOT EXISTS timeframe VARCHAR(10) DEFAULT '1D'")
                await conn.execute("ALTER TABLE actual_prices ADD COLUMN IF NOT EXISTS open_price DECIMAL(15,2)")
                await conn.execute("ALTER TABLE actual_prices ADD COLUMN IF NOT EXISTS high DECIMAL(15,2)")
                await conn.execute("ALTER TABLE actual_prices ADD COLUMN IF NOT EXISTS low DECIMAL(15,2)")
                await conn.execute("ALTER TABLE actual_prices ADD COLUMN IF NOT EXISTS close_price DECIMAL(15,2)")
            except Exception as e:
                logging.warning(f"Column addition warning: {e}")
            
            # Accuracy tracking table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS forecast_accuracy (
                    id SERIAL PRIMARY KEY,
                    symbol VARCHAR(10) NOT NULL,
                    forecast_id INTEGER REFERENCES forecasts(id) UNIQUE,
                    actual_direction VARCHAR(10),
                    result VARCHAR(10), -- 'Hit' or 'Miss'
                    accuracy_score DECIMAL(5,2),
                    evaluated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            # Favorites table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS user_favorites (
                    id SERIAL PRIMARY KEY,
                    user_id VARCHAR(50) DEFAULT 'default_user',
                    symbol VARCHAR(20) NOT NULL,
                    added_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(user_id, symbol)
                )
            """)
            
            # Create indexes and unique constraints
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_forecasts_symbol_time ON forecasts(symbol, created_at)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_actual_symbol_time ON actual_prices(symbol, timestamp)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_favorites_user ON user_favorites(user_id)")
            
            # Add unique constraint to prevent duplicate price entries
            try:
                await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS unique_symbol_timestamp ON actual_prices (symbol, timestamp)")
            except:
                pass  # Index already exists
    
    async def store_forecast(self, symbol, forecast_data):
        """Store forecast prediction"""
        if not self.pool:
            return None
            
        async with self.pool.acquire() as conn:
            return await conn.fetchval("""
                INSERT INTO forecasts (symbol, forecast_direction, confidence, predicted_price, predicted_range, trend_score)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING id
            """, symbol, forecast_data['forecast_direction'], forecast_data['confidence'],
                forecast_data.get('predicted_price'), forecast_data.get('predicted_range'),
                forecast_data.get('trend_score'))
    
    async def store_actual_price(self, symbol, price_data, timeframe='1D'):
        """Store actual market price with OHLC data"""
        if not self.pool:
            return
            
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO actual_prices (symbol, timeframe, open_price, high, low, close_price, price, change_24h, volume, timestamp)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            """, symbol, timeframe, 
                price_data.get('open_price'), price_data.get('high'), 
                price_data.get('low'), price_data.get('close_price'),
                price_data['current_price'], price_data.get('change_24h'), 
                price_data.get('volume'), price_data.get('timestamp', datetime.now()))
    
    async def get_last_stored_time(self, symbol):
        """Get last stored timestamp for a symbol"""
        if not self.pool:
            return None
            
        async with self.pool.acquire() as conn:
            result = await conn.fetchval("""
                SELECT MAX(timestamp) FROM actual_prices WHERE symbol = $1
            """, symbol)
            return result
    
    async def store_historical_batch(self, symbol, historical_data, timeframe='1D'):
        """Store batch of historical data with OHLC"""
        if not self.pool or not historical_data:
            return
            
        async with self.pool.acquire() as conn:
            # Insert batch with conflict handling
            for data in historical_data:
                try:
                    await conn.execute("""
                        INSERT INTO actual_prices (symbol, timeframe, open_price, high, low, close_price, price, change_24h, volume, timestamp)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                        ON CONFLICT DO NOTHING
                    """, symbol, timeframe,
                        data.get('open', data.get('open_price')),
                        data.get('high'),
                        data.get('low'), 
                        data.get('close', data.get('close_price', data.get('price'))),
                        data.get('close', data.get('close_price', data.get('price'))),
                        data.get('change_24h', 0),
                        data.get('volume', 0),
                        data['timestamp'])
                except Exception as e:
                    logging.warning(f"Failed to insert data point: {e}")
                    pass  # Skip duplicates
    
    async def get_historical_forecasts(self, symbol, days=30):
        """Get historical forecasts for accuracy analysis"""
        if not self.pool:
            return []
            
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT f.*, fa.actual_direction, fa.result, fa.accuracy_score
                FROM forecasts f
                LEFT JOIN forecast_accuracy fa ON f.id = fa.forecast_id
                WHERE f.symbol = $1 AND f.created_at >= NOW() - INTERVAL '%s days'
                ORDER BY f.created_at DESC
            """ % days, symbol)
            
            return [dict(row) for row in rows]
    
    async def calculate_accuracy(self, symbol, days=30):
        """Calculate forecast accuracy percentage"""
        if not self.pool:
            return 0
            
        async with self.pool.acquire() as conn:
            result = await conn.fetchrow("""
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN fa.result = 'Hit' THEN 1 END) as hits
                FROM forecasts f
                JOIN forecast_accuracy fa ON f.id = fa.forecast_id
                WHERE f.symbol = $1 AND f.created_at >= NOW() - INTERVAL '%s days'
            """ % days, symbol)
            
            if result['total'] > 0:
                return round((result['hits'] / result['total']) * 100, 2)
            return 0
    
    async def get_chart_data(self, symbol, timeframe='7D'):
        """Get historical data for charts with Redis caching"""
        if not self.pool:
            return {'forecast': [], 'actual': [], 'timestamps': []}
        
        # Create timeframe-specific symbol for database queries
        timeframe_map = {'5m': '5m', '15m': '15m', '30m': '30m', '1h': '1h', '4h': '4H', '4H': '4H', '1D': '1D', '1W': '1W'}
        db_timeframe = timeframe_map.get(timeframe, timeframe)
        db_symbol = f"{symbol}_{db_timeframe}"  # All data stored with timeframe suffix
        
        # Try Redis cache first
        cache_key = f"chart_data:{symbol}:{timeframe}"
        try:
            import redis
            import json
            import os
            from dotenv import load_dotenv
            load_dotenv()
            
            redis_client = redis.Redis(
                host=os.getenv('REDIS_HOST', 'localhost'),
                port=int(os.getenv('REDIS_PORT', '6379')),
                db=int(os.getenv('REDIS_DB', '0')),
                decode_responses=True
            )
            
            cached_data = redis_client.get(cache_key)
            if cached_data:
                return json.loads(cached_data)
        except Exception as e:
            logging.warning(f"⚠️ DEBUG: Redis cache failed for {cache_key}: {e}")
            
        days = {'1D': 7, '7D': 7, '1M': 30, '1Y': 365, '4H': 7, '4h': 7, '5m': 7, '15m': 7, '30m': 7, '1h': 7, '1W': 30}.get(timeframe, 7)
        logging.info(f"🔍 DEBUG: get_chart_data for {symbol} {timeframe} using {days} days, db_symbol={db_symbol}")
        
        async with self.pool.acquire() as conn:
            # Get forecasts for timeframe-specific symbol
            logging.info(f"🔍 DEBUG: Querying forecasts table for {db_symbol} with {days} days interval")
            forecast_rows = await conn.fetch("""
                SELECT predicted_price, created_at
                FROM forecasts
                WHERE symbol = $1 AND created_at >= NOW() - INTERVAL '%s days'
                ORDER BY created_at
            """ % days, db_symbol)
            logging.info(f"🔍 DEBUG: Found {len(forecast_rows)} forecast rows for {db_symbol}")
            
            # Get actual prices for timeframe-specific symbol
            logging.info(f"🔍 DEBUG: Querying actual_prices table for {db_symbol} with {days} days interval")
            actual_rows = await conn.fetch("""
                SELECT price, timestamp
                FROM actual_prices
                WHERE symbol = $1 AND timestamp >= NOW() - INTERVAL '%s days'
                ORDER BY timestamp
            """ % days, db_symbol)
            logging.info(f"🔍 DEBUG: Found {len(actual_rows)} actual price rows for {db_symbol}")
            
            chart_data = {
                'forecast': [float(row['predicted_price']) for row in forecast_rows if row['predicted_price']],
                'actual': [float(row['price']) for row in actual_rows],
                'timestamps': [row['timestamp'].isoformat() for row in actual_rows]
            }
            logging.info(f"✅ DEBUG: Chart data created for {db_symbol}: forecast={len(chart_data['forecast'])}, actual={len(chart_data['actual'])}, timestamps={len(chart_data['timestamps'])}")
            
            # Cache for 60 seconds
            try:
                redis_client.setex(cache_key, 60, json.dumps(chart_data))
                logging.info(f"🔍 DEBUG: Cached chart data for {cache_key}")
            except Exception as e:
                logging.warning(f"⚠️ DEBUG: Failed to cache chart data: {e}")
            
            logging.info(f"✅ DEBUG: Returning chart data for {symbol} {timeframe} (db_symbol={db_symbol})")
            return chart_data
    
    async def export_csv_data(self, symbol, timeframe='1M'):
        """Export historical data for CSV"""
        if not self.pool:
            return []
            
        days = {'1W': 7, '1M': 30, '1Y': 365, '5Y': 1825}.get(timeframe, 30)
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT 
                    f.created_at::date as date,
                    f.forecast_direction as forecast,
                    fa.actual_direction as actual,
                    fa.result
                FROM forecasts f
                LEFT JOIN forecast_accuracy fa ON f.id = fa.forecast_id
                WHERE f.symbol = $1 AND f.created_at >= NOW() - INTERVAL '%s days'
                ORDER BY f.created_at DESC
            """ % days, symbol)
            
            return [dict(row) for row in rows]
    
    async def add_favorite(self, symbol, user_id='default_user'):
        """Add symbol to favorites"""
        if not self.pool:
            return False
        async with self.pool.acquire() as conn:
            try:
                await conn.execute(
                    "INSERT INTO user_favorites (user_id, symbol) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                    user_id, symbol
                )
                return True
            except:
                return False
    
    async def remove_favorite(self, symbol, user_id='default_user'):
        """Remove symbol from favorites"""
        if not self.pool:
            return False
        async with self.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM user_favorites WHERE user_id = $1 AND symbol = $2",
                user_id, symbol
            )
            return True
    
    async def get_favorites(self, user_id='default_user'):
        """Get user's favorite symbols"""
        if not self.pool:
            return []
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT symbol FROM user_favorites WHERE user_id = $1 ORDER BY added_at DESC",
                user_id
            )
            return [row['symbol'] for row in rows]

# Global database instance
db = TradingDatabase()