#!/usr/bin/env python3
"""
Collect 7 days historical data for all 20 assets across all timeframes
"""
import asyncio
import asyncpg
import requests
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

class DataCollector:
    def __init__(self):
        self.db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:admin123@localhost:5432/trading_db')
        self.pool = None
        
        # All 20 assets from requirements
        self.crypto_symbols = ['BTC', 'ETH', 'USDT', 'XRP', 'BNB', 'SOL', 'USDC', 'DOGE', 'ADA', 'TRX']
        self.stock_symbols = ['NVDA', 'MSFT', 'AAPL', 'GOOGL', 'AMZN', 'META', 'AVGO', 'TSLA', 'BRK-B', 'JPM']
        
        # Timeframes from requirements
        self.timeframes = ['1m', '5m', '15m', '1h', '4H', '1D', '1W']
        
        # Binance symbol mapping
        self.binance_map = {
            'BTC': 'BTCUSDT', 'ETH': 'ETHUSDT', 'BNB': 'BNBUSDT',
            'SOL': 'SOLUSDT', 'ADA': 'ADAUSDT', 'XRP': 'XRPUSDT',
            'DOGE': 'DOGEUSDT', 'TRX': 'TRXUSDT', 'USDT': 'BTCUSDT',
            'USDC': 'USDCUSDT'
        }
        
        # Binance interval mapping
        self.binance_intervals = {
            '1m': '1m', '5m': '5m', '15m': '15m', '1h': '1h',
            '4H': '4h', '1D': '1d', '1W': '1w'
        }
    
    async def connect_db(self):
        """Connect to database"""
        try:
            self.pool = await asyncpg.create_pool(self.db_url, min_size=5, max_size=20)
            logging.info("✅ Database connected")
        except Exception as e:
            logging.error(f"❌ Database connection failed: {e}")
            raise
    
    async def clean_database(self):
        """Clean database tables to avoid conflicts"""
        try:
            async with self.pool.acquire() as conn:
                # Clean all relevant tables
                await conn.execute("DELETE FROM forecasts")
                await conn.execute("DELETE FROM actual_prices")
                await conn.execute("DELETE FROM forecast_accuracy")
                
                logging.info("🧹 Database cleaned - removed all existing data")
        except Exception as e:
            logging.error(f"❌ Error cleaning database: {e}")
            raise
    
    async def close_db(self):
        """Close database connection"""
        if self.pool:
            await self.pool.close()
    
    def get_crypto_data(self, symbol, timeframe):
        """Get crypto data from Binance with multiple API calls"""
        try:
            binance_symbol = self.binance_map.get(symbol)
            if not binance_symbol:
                return []
            
            interval = self.binance_intervals[timeframe]
            
            # Calculate limit for 7 days
            limits = {
                '1m': 10080, '5m': 2016, '15m': 672, '1h': 168,
                '4H': 42, '1D': 7, '1W': 1
            }
            total_required = limits.get(timeframe, 168)
            
            # Multiple API calls for large datasets
            max_per_call = 1000  # Binance limit
            all_klines = []
            
            if total_required <= max_per_call:
                # Single API call
                url = f"https://api.binance.com/api/v3/klines?symbol={binance_symbol}&interval={interval}&limit={total_required}"
                response = requests.get(url, timeout=30)
                
                if response.status_code == 200:
                    all_klines = response.json()
                else:
                    logging.error(f"❌ Binance API error for {symbol}: {response.status_code}")
                    return []
            else:
                # Multiple API calls needed
                calls_needed = (total_required + max_per_call - 1) // max_per_call
                end_time = int(datetime.now().timestamp() * 1000)
                
                for call_num in range(calls_needed):
                    records_to_fetch = min(max_per_call, total_required - len(all_klines))
                    
                    url = "https://api.binance.com/api/v3/klines"
                    params = {
                        'symbol': binance_symbol,
                        'interval': interval,
                        'limit': records_to_fetch,
                        'endTime': end_time
                    }
                    
                    response = requests.get(url, params=params, timeout=30)
                    
                    if response.status_code == 200:
                        klines = response.json()
                        if not klines:
                            break
                        
                        # Add to collection (reverse order for chronological)
                        all_klines = klines + all_klines
                        
                        # Update end_time for next call
                        end_time = klines[0][0] - 1
                        
                        # Rate limiting
                        import time
                        time.sleep(0.5)
                        
                        if len(all_klines) >= total_required:
                            break
                    else:
                        logging.error(f"❌ Binance API error for {symbol}: {response.status_code}")
                        break
            
            # Convert to data format
            data = []
            for kline in all_klines:
                timestamp = datetime.fromtimestamp(kline[0] / 1000)
                data.append({
                    'timestamp': timestamp,
                    'open': float(kline[1]),
                    'high': float(kline[2]),
                    'low': float(kline[3]),
                    'close': float(kline[4]),
                    'volume': float(kline[5])
                })
            
            return data
            
        except Exception as e:
            logging.error(f"❌ Error getting crypto data for {symbol}: {e}")
            return []
    
    def get_stock_data(self, symbol, timeframe):
        """Get stock data from Yahoo Finance"""
        try:
            # Yahoo Finance intervals
            yahoo_intervals = {
                '1m': '1m', '5m': '5m', '15m': '15m', '1h': '1h',
                '4H': '1h', '1D': '1d', '1W': '1wk'
            }
            
            interval = yahoo_intervals.get(timeframe, '1d')
            
            # For 4H, we'll get hourly data and aggregate
            if timeframe == '4H':
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1h&range=7d"
            else:
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval={interval}&range=7d"
            
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            response = requests.get(url, timeout=30, headers=headers)
            
            if response.status_code == 200:
                data_json = response.json()
                if 'chart' in data_json and data_json['chart']['result']:
                    result = data_json['chart']['result'][0]
                    timestamps = result['timestamp']
                    indicators = result['indicators']['quote'][0]
                    
                    data = []
                    for i, ts in enumerate(timestamps):
                        if (indicators['close'][i] is not None and 
                            indicators['open'][i] is not None):
                            timestamp = datetime.fromtimestamp(ts)
                            data.append({
                                'timestamp': timestamp,
                                'open': float(indicators['open'][i]),
                                'high': float(indicators['high'][i]),
                                'low': float(indicators['low'][i]),
                                'close': float(indicators['close'][i]),
                                'volume': float(indicators['volume'][i]) if indicators['volume'][i] else 0
                            })
                    
                    # Aggregate to 4H if needed
                    if timeframe == '4H':
                        data = self.aggregate_to_4h(data)
                    
                    return data
            else:
                logging.error(f"❌ Yahoo API error for {symbol}: {response.status_code}")
                return []
        except Exception as e:
            logging.error(f"❌ Error getting stock data for {symbol}: {e}")
            return []
    
    def aggregate_to_4h(self, hourly_data):
        """Aggregate hourly data to 4H candles"""
        if not hourly_data:
            return []
        
        aggregated = []
        current_candle = None
        
        for data_point in hourly_data:
            hour = data_point['timestamp'].hour
            
            # Start new 4H candle at 0, 4, 8, 12, 16, 20
            if hour % 4 == 0:
                if current_candle:
                    aggregated.append(current_candle)
                
                current_candle = {
                    'timestamp': data_point['timestamp'].replace(minute=0, second=0, microsecond=0),
                    'open': data_point['open'],
                    'high': data_point['high'],
                    'low': data_point['low'],
                    'close': data_point['close'],
                    'volume': data_point['volume']
                }
            else:
                if current_candle:
                    current_candle['high'] = max(current_candle['high'], data_point['high'])
                    current_candle['low'] = min(current_candle['low'], data_point['low'])
                    current_candle['close'] = data_point['close']
                    current_candle['volume'] += data_point['volume']
        
        if current_candle:
            aggregated.append(current_candle)
        
        return aggregated
    
    async def store_data(self, symbol, timeframe, data):
        """Store data in database"""
        if not data:
            return 0
        
        try:
            symbol_tf = f"{symbol}_{timeframe}"
            
            async with self.pool.acquire() as conn:
                # Insert actual prices
                for item in data:
                    await conn.execute("""
                        INSERT INTO actual_prices (symbol, timeframe, open_price, high, low, close_price, price, change_24h, volume, timestamp)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                        ON CONFLICT DO NOTHING
                    """, f"{symbol}_{timeframe}", timeframe, item['open'], item['high'], item['low'], 
                         item['close'], item['close'], 0, item['volume'], item['timestamp'])
                
                return len(data)
        except Exception as e:
            logging.error(f"❌ Error storing data for {symbol}_{timeframe}: {e}")
            return 0
    
    async def generate_predictions(self, symbol, timeframe, data):
        """Generate trend-based realistic predictions"""
        if len(data) < 5:
            return 0
        
        try:
            symbol_tf = f"{symbol}_{timeframe}"
            predictions_stored = 0
            
            async with self.pool.acquire() as conn:
                # Generate predictions for each data point
                for i, item in enumerate(data):
                    try:
                        # Calculate price change from previous point
                        change = 0
                        if i > 0:
                            prev_close = data[i-1]['close']
                            change = ((item['close'] - prev_close) / prev_close) * 100
                        
                        # Calculate trend using moving averages
                        prices = [d['close'] for d in data[max(0, i-10):i+1]]
                        if len(prices) >= 5:
                            sma_5 = sum(prices[-5:]) / 5
                            sma_10 = sum(prices[-10:]) / 10 if len(prices) >= 10 else sma_5
                            trend = (sma_5 - sma_10) / sma_10 * 100 if sma_10 > 0 else 0
                        else:
                            trend = change
                        
                        # Generate timeframe-specific prediction
                        close_price = item['close']
                        
                        # Adjust prediction strength based on timeframe
                        if timeframe == '1m':
                            prediction_factor = 0.02
                            base_confidence = 60
                            threshold = 0.1
                        elif timeframe == '5m':
                            prediction_factor = 0.05
                            base_confidence = 65
                            threshold = 0.2
                        elif timeframe == '15m':
                            prediction_factor = 0.08
                            base_confidence = 70
                            threshold = 0.3
                        elif timeframe == '1h':
                            prediction_factor = 0.12
                            base_confidence = 75
                            threshold = 0.5
                        elif timeframe == '4H':
                            prediction_factor = 0.4
                            base_confidence = 80
                            threshold = 1.0
                        elif timeframe == '1D':
                            prediction_factor = 0.6
                            base_confidence = 85
                            threshold = 2.0
                        elif timeframe == '1W':
                            prediction_factor = 1.5
                            base_confidence = 90
                            threshold = 5.0
                        else:
                            prediction_factor = 0.6
                            base_confidence = 75
                            threshold = 1.0
                        
                        # Calculate predicted price with trend influence
                        predicted_price = close_price * (1 + trend/100 * prediction_factor)
                        
                        # Determine direction and confidence
                        if abs(change) > threshold:
                            direction = 'UP' if change > 0 else 'DOWN'
                            confidence = min(95, max(base_confidence, base_confidence + abs(change) * 5))
                        else:
                            direction = 'HOLD'
                            confidence = base_confidence
                        
                        # Create predicted range
                        range_factor = 0.02
                        predicted_range = f"${predicted_price*(1-range_factor):.2f}–${predicted_price*(1+range_factor):.2f}"
                        
                        # Store forecast
                        await conn.execute("""
                            INSERT INTO forecasts (symbol, predicted_price, confidence, 
                                                 forecast_direction, predicted_range, trend_score, created_at)
                            VALUES ($1, $2, $3, $4, $5, $6, $7)
                        """, symbol_tf, predicted_price, int(confidence), direction, 
                             predicted_range, int(trend), item['timestamp'])
                        
                        predictions_stored += 1
                        
                        if predictions_stored % 100 == 0:
                            logging.info(f"    Generated {predictions_stored} predictions for {symbol_tf}")
                            
                    except Exception as e:
                        logging.warning(f"Error generating prediction for {symbol_tf}: {e}")
                        continue
            
            return predictions_stored
        except Exception as e:
            logging.error(f"❌ Error generating predictions for {symbol}_{timeframe}: {e}")
            return 0
    
    async def collect_all_data(self):
        """Collect data for all assets and timeframes"""
        logging.info("🔄 Starting 7-day data collection for all 20 assets...")
        logging.info("🧹 Cleaning database first...")
        await self.clean_database()
        logging.info("=" * 60)
        
        total_records = 0
        
        # Process crypto assets
        for symbol in self.crypto_symbols:
            logging.info(f"📊 Processing crypto: {symbol}")
            
            for timeframe in self.timeframes:
                try:
                    data = self.get_crypto_data(symbol, timeframe)
                    if data:
                        count = await self.store_data(symbol, timeframe, data)
                        pred_count = await self.generate_predictions(symbol, timeframe, data)
                        total_records += count
                        logging.info(f"  ✅ {symbol}_{timeframe}: Stored {count} records, {pred_count} predictions")
                    else:
                        logging.info(f"  ⚠️ {symbol}_{timeframe}: No data available")
                    
                    # Small delay to avoid rate limits
                    await asyncio.sleep(0.1)
                except Exception as e:
                    logging.error(f"  ❌ {symbol}_{timeframe}: {e}")
        
        # Process stock assets
        for symbol in self.stock_symbols:
            logging.info(f"📈 Processing stock: {symbol}")
            
            for timeframe in self.timeframes:
                try:
                    data = self.get_stock_data(symbol, timeframe)
                    if data:
                        count = await self.store_data(symbol, timeframe, data)
                        pred_count = await self.generate_predictions(symbol, timeframe, data)
                        total_records += count
                        logging.info(f"  ✅ {symbol}_{timeframe}: Stored {count} records, {pred_count} predictions")
                    else:
                        logging.info(f"  ⚠️ {symbol}_{timeframe}: No data available")
                    
                    # Longer delay for stocks to avoid rate limits
                    await asyncio.sleep(1)
                except Exception as e:
                    logging.error(f"  ❌ {symbol}_{timeframe}: {e}")
        
        logging.info("=" * 60)
        logging.info(f"✅ Data collection completed!")
        logging.info(f"📊 Total records stored: {total_records:,}")
        logging.info(f"🎯 Assets processed: {len(self.crypto_symbols + self.stock_symbols)}")
        logging.info(f"⏱️ Timeframes: {len(self.timeframes)}")

async def main():
    collector = DataCollector()
    
    try:
        await collector.connect_db()
        await collector.collect_all_data()
    finally:
        await collector.close_db()

if __name__ == "__main__":
    asyncio.run(main())