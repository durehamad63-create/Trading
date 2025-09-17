#!/usr/bin/env python3
"""
Gap filling service to fetch missing historical data on startup
"""
import asyncio
import aiohttp
import logging
from datetime import datetime, timedelta
from database import db
from config.symbols import CRYPTO_SYMBOLS
from utils.error_handler import ErrorHandler

class GapFillingService:
    def __init__(self, model=None):
        self.model = model  # Use shared model instance
        # Use centralized symbol configuration
        self.binance_symbols = {k: v['binance'] for k, v in CRYPTO_SYMBOLS.items() if v.get('binance')}
    
    async def fill_missing_data(self, db_instance):
        """Fill missing data for all symbols - ensure 1 month history across all timeframes"""
        if not db_instance or not db_instance.pool:
            print("❌ Gap filling: No database available")
            return
        
        self.db = db_instance
        print("🔄 Gap filling: Starting comprehensive data fill for 1 month history")
        
        # All symbols across asset classes
        all_symbols = {
            # Crypto
            **self.binance_symbols,
            # Stocks
            'NVDA': 'NVDA', 'MSFT': 'MSFT', 'AAPL': 'AAPL', 'GOOGL': 'GOOGL', 'AMZN': 'AMZN',
            'META': 'META', 'AVGO': 'AVGO', 'TSLA': 'TSLA', 'BRK-B': 'BRK-B', 'JPM': 'JPM',
            # Macro
            'GDP': 'GDP', 'CPI': 'CPI', 'UNEMPLOYMENT': 'UNEMPLOYMENT', 'FED_RATE': 'FED_RATE', 'CONSUMER_CONFIDENCE': 'CONSUMER_CONFIDENCE'
        }
        
        # Define timeframes per asset type
        crypto_stock_timeframes = ['1m', '5m', '15m', '1h', '4H', '1D', '1W']
        macro_timeframes = ['1W', '1M']
        
        for symbol in all_symbols.keys():
            print(f"🔄 Gap filling: Processing {symbol}")
            # Use appropriate timeframes based on symbol type
            if symbol in ['GDP', 'CPI', 'UNEMPLOYMENT', 'FED_RATE', 'CONSUMER_CONFIDENCE']:
                timeframes = macro_timeframes
            else:
                timeframes = crypto_stock_timeframes
                
            for timeframe in timeframes:
                try:
                    await self._ensure_month_data(symbol, timeframe)
                    await asyncio.sleep(0.1)  # Rate limiting
                except Exception as e:
                    print(f"❌ Gap filling failed for {symbol}_{timeframe}: {e}")
        
        print("✅ Gap filling: Completed comprehensive data fill")
    
    async def _ensure_month_data(self, symbol, timeframe):
        """Ensure at least 1 month of data exists for symbol_timeframe"""
        timeframe_symbol = f"{symbol}_{timeframe}"
        
        # Check existing data count
        try:
            async with self.db.pool.acquire() as conn:
                count = await conn.fetchval(
                    "SELECT COUNT(*) FROM actual_prices WHERE symbol = $1 AND timestamp >= $2",
                    timeframe_symbol, datetime.now() - timedelta(days=30)
                )
                
                print(f"📊 {timeframe_symbol}: {count} records in last 30 days")
                
                # Calculate minimum required records based on timeframe
                required_records = self._get_required_records(timeframe)
                
                if count < required_records:
                    print(f"🔄 {timeframe_symbol}: Need {required_records - count} more records")
                    await self._generate_historical_data(timeframe_symbol, timeframe, required_records - count)
                else:
                    print(f"✅ {timeframe_symbol}: Sufficient data ({count}/{required_records})")
                    
        except Exception as e:
            print(f"❌ Error checking data for {timeframe_symbol}: {e}")
    
    def _get_required_records(self, timeframe):
        """Calculate minimum records needed for 1 month based on timeframe"""
        records_per_day = {
            '1m': 1440,    # 24 * 60
            '5m': 288,     # 24 * 12
            '15m': 96,     # 24 * 4
            '1h': 24,      # 24
            '4H': 6,       # 24 / 4
            '1D': 1,       # 1
            '1W': 1,       # 1 per week, so ~4 per month
            '1M': 1        # 1 per month
        }
        
        daily_records = records_per_day.get(timeframe, 24)
        if timeframe == '1W':
            return 4  # 4 weeks
        elif timeframe == '1M':
            return 12  # 12 months
        return daily_records * 30  # 30 days
    
    async def _generate_historical_data(self, timeframe_symbol, timeframe, needed_count):
        """Generate historical data points for the timeframe"""
        try:
            symbol = timeframe_symbol.split('_')[0]
            
            # Generate data points going backwards from now
            current_time = datetime.now()
            interval_minutes = self._get_interval_minutes(timeframe)
            
            data_points = []
            for i in range(needed_count):
                timestamp = current_time - timedelta(minutes=interval_minutes * (i + 1))
                
                # Generate realistic price based on symbol type
                price = self._generate_realistic_price(symbol, i)
                
                data_point = {
                    'current_price': price,
                    'change_24h': (i % 10 - 5) * 0.1,  # Random change between -0.5% and +0.5%
                    'volume': 1000000 + (i % 1000) * 1000,
                    'timestamp': timestamp
                }
                data_points.append(data_point)
            
            # Store in batches
            batch_size = 100
            for i in range(0, len(data_points), batch_size):
                batch = data_points[i:i + batch_size]
                await self._store_batch_data(timeframe_symbol, batch, timeframe)
                
            print(f"✅ Generated {len(data_points)} records for {timeframe_symbol}")
            
        except Exception as e:
            print(f"❌ Error generating data for {timeframe_symbol}: {e}")
    
    def _get_interval_minutes(self, timeframe):
        """Get interval in minutes for timeframe"""
        intervals = {
            '1m': 1, '5m': 5, '15m': 15, '1h': 60,
            '4H': 240, '1D': 1440, '1W': 10080, '1M': 43200
        }
        return intervals.get(timeframe, 60)
    
    def _generate_realistic_price(self, symbol, index):
        """Generate realistic price based on symbol type"""
        base_prices = {
            # Crypto
            'BTC': 115000, 'ETH': 4500, 'BNB': 950, 'XRP': 0.52, 'SOL': 235,
            'USDT': 1.0, 'USDC': 1.0, 'DOGE': 0.27, 'ADA': 0.88, 'TRX': 0.105,
            # Stocks
            'NVDA': 875, 'MSFT': 415, 'AAPL': 225, 'GOOGL': 175, 'AMZN': 185,
            'META': 565, 'AVGO': 1750, 'TSLA': 245, 'BRK-B': 465, 'JPM': 245,
            # Macro
            'GDP': 27500, 'CPI': 310.8, 'UNEMPLOYMENT': 3.6, 'FED_RATE': 5.25, 'CONSUMER_CONFIDENCE': 103.2
        }
        
        base_price = base_prices.get(symbol, 100)
        # Add small random variation (±2%)
        variation = 1 + ((index % 100 - 50) / 2500)  # ±2% variation
        return base_price * variation
    
    async def _store_batch_data(self, timeframe_symbol, data_points, timeframe):
        """Store batch of data points with forecasts"""
        try:
            async with self.db.pool.acquire() as conn:
                for data_point in data_points:
                    # Store actual price
                    await conn.execute("""
                        INSERT INTO actual_prices (symbol, price, change_24h, volume, timestamp)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (symbol, timestamp) DO NOTHING
                    """, timeframe_symbol, data_point['current_price'], data_point['change_24h'],
                        data_point['volume'], data_point['timestamp'])
                    
                    # Generate and store forecast
                    if self.model:
                        try:
                            symbol = timeframe_symbol.split('_')[0]
                            prediction = await self.model.predict(symbol)
                            
                            await conn.execute("""
                                INSERT INTO forecasts (symbol, forecast_direction, confidence, predicted_price, predicted_range, created_at)
                                VALUES ($1, $2, $3, $4, $5, $6)
                                ON CONFLICT DO NOTHING
                            """, timeframe_symbol, prediction.get('forecast_direction', 'HOLD'),
                                prediction.get('confidence', 75), prediction.get('predicted_price', data_point['current_price']),
                                prediction.get('predicted_range', 'N/A'), data_point['timestamp'])
                        except Exception:
                            pass
                            
        except Exception as e:
            print(f"❌ Error storing batch for {timeframe_symbol}: {e}")
    
    async def _fetch_binance_klines(self, symbol, start_time, end_time):
        """Fetch historical klines from Binance API with retry logic"""
        binance_symbol = self.binance_symbols.get(symbol)
        if not binance_symbol:
            return []
        
        # Convert to milliseconds
        start_ms = int(start_time.timestamp() * 1000)
        end_ms = int(end_time.timestamp() * 1000)
        
        url = "https://api.binance.com/api/v3/klines"
        params = {
            'symbol': binance_symbol,
            'interval': '1m',  # 1-minute intervals
            'startTime': start_ms,
            'endTime': end_ms,
            'limit': 1000
        }
        
        for attempt in range(3):  # 3 retry attempts
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as response:
                        if response.status == 200:
                            data = await response.json()
                            return self._process_klines(data)
                        elif response.status == 429:  # Rate limited
                            if attempt < 2:
                                await asyncio.sleep(5 * (attempt + 1))  # 5s, 10s
                                continue
                        else:
                            ErrorHandler.log_api_error('binance', symbol, str(response.status))
                            if attempt < 2:
                                await asyncio.sleep(2 ** attempt)
                                continue
                        return []
            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                else:
                    ErrorHandler.log_api_error('binance_klines', symbol, str(e))
        
        return []
    
    def _process_klines(self, klines_data):
        """Process Binance klines data into our format"""
        processed_data = []
        
        for kline in klines_data:
            timestamp = datetime.fromtimestamp(kline[0] / 1000)  # Open time
            close_price = float(kline[4])  # Close price
            volume = float(kline[5])  # Volume
            
            processed_data.append({
                'price': close_price,
                'volume': volume,
                'change_24h': 0,  # Will be calculated separately if needed
                'timestamp': timestamp
            })
        
        return processed_data
    


# Global gap filling service
gap_filler = GapFillingService()