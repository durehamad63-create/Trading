#!/usr/bin/env python3
"""
Monthly Data Collection & ML Prediction Service
Collects 1 month of historical data and generates ML predictions for all 25 assets
"""
import asyncio
import aiohttp
import logging
import numpy as np
import random
from datetime import datetime, timedelta
from config.symbols import CRYPTO_SYMBOLS, STOCK_SYMBOLS, MACRO_SYMBOLS
from utils.error_handler import ErrorHandler
from typing import List, Dict, Any

class GapFillingService:
    def __init__(self, model=None):
        self.model = model
        # All 25 assets from configuration
        self.crypto_symbols = list(CRYPTO_SYMBOLS.keys())  # 10 crypto
        self.stock_symbols = list(STOCK_SYMBOLS.keys())    # 10 stocks  
        self.macro_symbols = list(MACRO_SYMBOLS.keys())    # 5 macro
        self.all_symbols = self.crypto_symbols + self.stock_symbols + self.macro_symbols
        
        # Only store higher timeframes - no 1m, 5m, 15m
        self.crypto_stock_timeframes = ['1h', '4H', '1D', '7D', '1W', '1M']
        self.macro_timeframes = ['1D', '7D', '1W', '1M']
        
        # Maintain exactly 1000 records per timeframe
        self.max_records = 1000
        
        # Binance mapping for stablecoins
        self.binance_mapping = {'USDT': 'BTCUSDT', 'USDC': 'BTCUSDT'}
        self.binance_intervals = {
            '1h': '1h', '4H': '4h', '1D': '1d', '7D': '1d', '1W': '1w', '1M': '1M'
        }
        
        # Time intervals for proper data storage (avoid storing every second)
        self.storage_intervals = {
            '1h': timedelta(hours=1),
            '4H': timedelta(hours=4), 
            '1D': timedelta(days=1),
            '7D': timedelta(days=7),
            '1W': timedelta(weeks=1),
            '1M': timedelta(days=30)
        }
        
        print(f"🚀 Efficient Data Collector initialized for {len(self.all_symbols)} assets:")
        print(f"   📈 Crypto: {len(self.crypto_symbols)} symbols")
        print(f"   📊 Stocks: {len(self.stock_symbols)} symbols") 
        print(f"   🏛️ Macro: {len(self.macro_symbols)} symbols")
        print(f"   ⏱️ Timeframes: {self.crypto_stock_timeframes}")
        print(f"   📊 Max records per timeframe: {self.max_records}")
    
    async def fill_missing_data(self, db_instance):
        """Collect historical data efficiently with proper time intervals"""
        if not db_instance or not db_instance.pool:
            print("❌ Data collection: No database available")
            return
        
        self.db = db_instance
        print("🚀 Starting efficient data collection for all 25 assets")
        
        # Clear database at startup to avoid conflicts
        await self._clear_database()
        
        total_processed = 0
        total_accuracy = 0
        
        # Process all asset classes
        for asset_class, symbols, timeframes in [
            ("crypto", self.crypto_symbols, self.crypto_stock_timeframes),
            ("stocks", self.stock_symbols, self.crypto_stock_timeframes), 
            ("macro", self.macro_symbols, self.macro_timeframes)
        ]:
            print(f"📊 Processing {asset_class} assets: {len(symbols)} symbols")
            
            for i, symbol in enumerate(symbols):
                print(f"  🔄 {symbol} ({i+1}/{len(symbols)})...")
                
                for timeframe in timeframes:
                    try:
                        # Get historical data
                        data = await self._get_monthly_data(symbol, timeframe, asset_class)
                        
                        if not data or len(data) < 20:
                            print(f"    ❌ Insufficient data for {symbol} {timeframe}")
                            continue
                        
                        # Generate ML predictions
                        predictions = self._generate_ml_predictions(data, symbol, timeframe, asset_class)
                        
                        if not predictions:
                            print(f"    ❌ No predictions for {symbol} {timeframe}")
                            continue
                        
                        # Calculate accuracy
                        results = self._calculate_accuracy(predictions)
                        
                        # Store data with proper intervals and maintain record limit
                        await self._store_historical_data_efficient(symbol, timeframe, data)
                        await self._store_predictions(symbol, timeframe, predictions, results)
                        
                        # Maintain record limit
                        symbol_tf = f'{symbol}_{timeframe}'
                        await self._maintain_record_limit(symbol_tf, timeframe)
                        
                        # Calculate accuracy stats
                        if results:
                            hits = sum(1 for r in results if r['result'] == 'Hit')
                            accuracy = (hits / len(results)) * 100
                            total_accuracy += accuracy
                            print(f"    ✅ {symbol} {timeframe}: {accuracy:.1f}% accuracy ({hits}/{len(results)} hits)")
                        
                        total_processed += 1
                        await asyncio.sleep(0.2)  # Rate limiting
                        
                    except Exception as e:
                        print(f"    ❌ Error processing {symbol} {timeframe}: {e}")
        
        # Final statistics
        if total_processed > 0:
            avg_accuracy = total_accuracy / total_processed
            print(f"🎯 Data collection completed: {avg_accuracy:.1f}% average accuracy")
            print(f"📊 Total processed: {total_processed} symbol-timeframe combinations")
        
        print("✅ Efficient data collection with ML predictions completed!")
    
    async def _clear_database(self):
        """Clear database at startup to avoid conflicts"""
        print("🧹 Clearing database at startup...")
        try:
            async with self.db.pool.acquire() as conn:
                await conn.execute("TRUNCATE TABLE actual_prices CASCADE")
                await conn.execute("TRUNCATE TABLE forecasts CASCADE")
                await conn.execute("TRUNCATE TABLE forecast_accuracy CASCADE")
                print("✅ Database cleared successfully")
        except Exception as e:
            print(f"⚠️ Database clear warning: {e}")
    
    async def _maintain_record_limit(self, symbol_tf: str, timeframe: str):
        """Maintain exactly 1000 records per symbol-timeframe"""
        try:
            async with self.db.pool.acquire() as conn:
                # Keep only latest 1000 records
                await conn.execute("""
                    DELETE FROM actual_prices 
                    WHERE symbol = $1 AND id NOT IN (
                        SELECT id FROM actual_prices 
                        WHERE symbol = $1 
                        ORDER BY timestamp DESC 
                        LIMIT $2
                    )
                """, symbol_tf, self.max_records)
        except Exception as e:
            print(f"      ⚠️ Record limit maintenance failed for {symbol_tf}: {e}")
    
    async def _get_monthly_data(self, symbol: str, timeframe: str, asset_class: str) -> List[Dict]:
        """Get 1 month of data for symbol and timeframe"""
        if asset_class == "crypto":
            return await self._get_crypto_data(symbol, timeframe)
        elif asset_class == "macro":
            return await self._get_macro_data(symbol, timeframe)
        else:
            return await self._get_stock_data(symbol, timeframe)
    
    async def _get_crypto_data(self, symbol: str, timeframe: str) -> List[Dict]:
        """Get crypto data from Binance with proper intervals"""
        for retry in range(3):
            try:
                interval = self.binance_intervals[timeframe]
                total_needed = self.max_records
                binance_symbol = self.binance_mapping.get(symbol, f"{symbol}USDT")
                
                url = f"https://api.binance.com/api/v3/klines?symbol={binance_symbol}&interval={interval}&limit={min(1000, total_needed)}"
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as response:
                        if response.status == 200:
                            klines = await response.json()
                            data = []
                            
                            for kline in klines:
                                timestamp = datetime.fromtimestamp(kline[0] / 1000)
                                data.append({
                                    'timestamp': timestamp,
                                    'open': float(kline[1]),
                                    'high': float(kline[2]),
                                    'low': float(kline[3]),
                                    'close': float(kline[4]),
                                    'volume': float(kline[5])
                                })
                            
                            print(f"    ✅ {symbol} {timeframe}: Got {len(data)} records from Binance")
                            return data
                        else:
                            raise Exception(f"API error: {response.status}")
                            
            except Exception as e:
                if retry < 2:
                    print(f"    ⚠️ Retry {retry+1}/3 for {symbol} {timeframe}: {e}")
                    await asyncio.sleep(2 ** retry)
                else:
                    print(f"    ❌ Failed after 3 retries for {symbol} {timeframe}: {e}")
        return []
    
    async def _get_stock_data(self, symbol: str, timeframe: str) -> List[Dict]:
        """Get stock data from Yahoo Finance with proper intervals"""
        for retry in range(3):
            try:
                range_mapping = {
                    '1h': '730d', '4H': '730d', '1D': '2y', '7D': '2y', '1W': '5y', '1M': '10y'
                }
                
                interval_mapping = {
                    '1h': '1h', '4H': '1h', '1D': '1d', '7D': '1d', '1W': '1wk', '1M': '1mo'
                }
                
                yahoo_interval = interval_mapping[timeframe]
                yahoo_range = range_mapping[timeframe]
                
                url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval={yahoo_interval}&range={yahoo_range}"
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=60), headers=headers) as response:
                        if response.status == 200:
                            data_json = await response.json()
                            if 'chart' in data_json and data_json['chart']['result']:
                                result = data_json['chart']['result'][0]
                                timestamps = result['timestamp']
                                indicators = result['indicators']['quote'][0]
                                
                                data = []
                                for i in range(len(timestamps)):
                                    if (i < len(indicators['close']) and 
                                        indicators['close'][i] is not None):
                                        timestamp = datetime.fromtimestamp(timestamps[i])
                                        data.append({
                                            'timestamp': timestamp,
                                            'open': float(indicators['open'][i]),
                                            'high': float(indicators['high'][i]),
                                            'low': float(indicators['low'][i]),
                                            'close': float(indicators['close'][i]),
                                            'volume': float(indicators['volume'][i]) if indicators['volume'][i] else 0
                                        })
                                
                                # Take last 1000 records and aggregate if needed
                                data = data[-self.max_records:] if len(data) > self.max_records else data
                                
                                if timeframe == '4H':
                                    data = self._aggregate_to_4h(data)
                                elif timeframe == '7D':
                                    data = self._aggregate_to_7d(data)
                                elif timeframe == '1M':
                                    data = self._aggregate_to_1m(data)
                                
                                filtered_data = data
                                
                                print(f"    ✅ {symbol} {timeframe}: Got {len(filtered_data)} records from Yahoo")
                                return filtered_data
                        else:
                            raise Exception(f"API error: {response.status}")
                            
            except Exception as e:
                if retry < 2:
                    print(f"    ⚠️ Retry {retry+1}/3 for {symbol} {timeframe}: {e}")
                    await asyncio.sleep(2 ** retry)
                else:
                    print(f"    ❌ Failed after 3 retries for {symbol} {timeframe}: {e}")
        return []
    
    def _aggregate_to_4h(self, data: List[Dict]) -> List[Dict]:
        """Aggregate hourly data to 4-hour candles"""
        if not data:
            return []
        
        aggregated = []
        i = 0
        
        while i < len(data) - 3:
            chunk = data[i:i+4]
            if len(chunk) >= 2:
                aggregated.append({
                    'timestamp': chunk[-1]['timestamp'],
                    'open': chunk[0]['open'],
                    'high': max(d['high'] for d in chunk),
                    'low': min(d['low'] for d in chunk),
                    'close': chunk[-1]['close'],
                    'volume': sum(d['volume'] for d in chunk)
                })
            i += 4
        
        return aggregated
    
    def _aggregate_to_7d(self, data: List[Dict]) -> List[Dict]:
        """Aggregate daily data to 7-day periods"""
        if not data:
            return []
        
        aggregated = []
        i = 0
        
        while i < len(data) - 6:
            chunk = data[i:i+7]
            if len(chunk) >= 5:
                aggregated.append({
                    'timestamp': chunk[-1]['timestamp'],
                    'open': chunk[0]['open'],
                    'high': max(d['high'] for d in chunk),
                    'low': min(d['low'] for d in chunk),
                    'close': chunk[-1]['close'],
                    'volume': sum(d['volume'] for d in chunk)
                })
            i += 7
        
        return aggregated
    
    def _aggregate_to_1m(self, data: List[Dict]) -> List[Dict]:
        """Aggregate daily data to monthly periods"""
        if not data:
            return []
        
        aggregated = []
        i = 0
        
        while i < len(data) - 29:
            chunk = data[i:i+30]
            if len(chunk) >= 20:
                aggregated.append({
                    'timestamp': chunk[-1]['timestamp'],
                    'open': chunk[0]['open'],
                    'high': max(d['high'] for d in chunk),
                    'low': min(d['low'] for d in chunk),
                    'close': chunk[-1]['close'],
                    'volume': sum(d['volume'] for d in chunk)
                })
            i += 30
        
        return aggregated
    
    async def _get_macro_data(self, symbol: str, timeframe: str) -> List[Dict]:
        """Generate synthetic macro economic data with proper intervals"""
        try:
            base_values = {
                'GDP': 27000, 'CPI': 310.5, 'UNEMPLOYMENT': 3.7,
                'FED_RATE': 5.25, 'CONSUMER_CONFIDENCE': 102.3
            }
            
            total_needed = self.max_records
            data = []
            base_value = base_values.get(symbol, 100)
            
            # Generate data with proper time intervals
            interval = self.storage_intervals.get(timeframe, timedelta(days=1))
            
            for i in range(total_needed):
                timestamp = datetime.now() - (interval * (total_needed - i))
                
                if timeframe == '1D':
                    variation = random.uniform(-0.002, 0.002)
                elif timeframe == '7D':
                    variation = random.uniform(-0.008, 0.008)
                elif timeframe == '1W':
                    variation = random.uniform(-0.01, 0.01)
                elif timeframe == '1M':
                    variation = random.uniform(-0.02, 0.02)
                else:
                    variation = random.uniform(-0.005, 0.005)
                
                current_value = base_value * (1 + variation)
                
                data.append({
                    'timestamp': timestamp,
                    'open': current_value,
                    'high': current_value * 1.001,
                    'low': current_value * 0.999,
                    'close': current_value,
                    'volume': random.randint(800000, 1200000)
                })
            
            print(f"    ✅ {symbol} {timeframe}: Generated {len(data)} macro data points")
            return data
            
        except Exception as e:
            print(f"    ❌ Failed to generate macro data for {symbol} {timeframe}: {e}")
            return []
    
    def _generate_ml_predictions(self, data: List[Dict], symbol: str, timeframe: str, asset_class: str) -> List[Dict]:
        """Generate ML-based predictions using technical analysis"""
        predictions = []
        
        if len(data) < 20:
            return predictions
        
        # Convert to numpy arrays for calculations
        prices = np.array([d['close'] for d in data])
        volumes = np.array([d['volume'] for d in data])
        
        for i in range(10, len(data)):
            try:
                # Technical indicators
                sma_5 = np.mean(prices[i-5:i])
                sma_10 = np.mean(prices[i-10:i])
                sma_20 = np.mean(prices[i-20:i]) if i >= 20 else sma_10
                
                # RSI calculation
                price_changes = np.diff(prices[max(0, i-14):i])
                gains = np.where(price_changes > 0, price_changes, 0)
                losses = np.where(price_changes < 0, -price_changes, 0)
                avg_gain = np.mean(gains) if len(gains) > 0 else 0
                avg_loss = np.mean(losses) if len(losses) > 0 else 0.01
                
                if avg_loss == 0:
                    rsi = 100 if avg_gain > 0 else 50
                else:
                    rsi = 100 - (100 / (1 + avg_gain / avg_loss))
                
                # Volume analysis
                avg_volume = np.mean(volumes[i-10:i])
                volume_ratio = volumes[i-1] / avg_volume if avg_volume > 0 else 1
                
                # Volatility
                volatility = np.std(prices[i-10:i]) / np.mean(prices[i-10:i]) * 100
                
                current_price = prices[i-1]
                
                # ML-like prediction logic
                trend_strength = (sma_5 - sma_20) / sma_20 * 100 if sma_20 > 0 else 0
                momentum = (current_price - prices[i-5]) / prices[i-5] * 100 if i >= 5 else 0
                
                # Prediction factors based on timeframe
                timeframe_multipliers = {
                    '1m': 0.1, '5m': 0.2, '15m': 0.3, '1h': 0.5,
                    '4H': 0.8, '1D': 1.0, '1W': 1.5
                }
                
                multiplier = timeframe_multipliers.get(timeframe, 0.5)
                
                # Combine indicators for prediction
                prediction_signal = 0
                
                # Trend signals
                if sma_5 > sma_10 > sma_20:
                    prediction_signal += 2 * multiplier
                elif sma_5 < sma_10 < sma_20:
                    prediction_signal -= 2 * multiplier
                
                # RSI signals
                if rsi < 30:
                    prediction_signal += 1.5 * multiplier
                elif rsi > 70:
                    prediction_signal -= 1.5 * multiplier
                
                # Volume confirmation
                if volume_ratio > 1.5:
                    prediction_signal *= 1.2
                elif volume_ratio < 0.5:
                    prediction_signal *= 0.8
                
                # Add market-specific adjustments
                if asset_class == "crypto":
                    prediction_signal *= 1.3
                    noise_factor = random.uniform(-0.8, 0.8)
                elif asset_class == "stocks":
                    prediction_signal *= 0.9
                    noise_factor = random.uniform(-0.4, 0.4)
                else:  # macro
                    prediction_signal *= 0.3
                    noise_factor = random.uniform(-0.1, 0.1)
                
                prediction_signal += noise_factor
                
                # Calculate predicted price
                price_change_percent = prediction_signal / 100
                predicted_price = current_price * (1 + price_change_percent)
                
                # Determine direction and confidence
                if abs(price_change_percent) > 0.005:
                    direction = 'UP' if price_change_percent > 0 else 'DOWN'
                else:
                    direction = 'HOLD'
                
                signal_strength = abs(prediction_signal)
                base_confidence = min(95, max(55, 70 + signal_strength * 3))
                
                if volatility > 5:
                    confidence = base_confidence * 0.8
                elif volatility < 1:
                    confidence = base_confidence * 1.1
                else:
                    confidence = base_confidence
                
                confidence = int(min(95, max(55, confidence)))
                
                predictions.append({
                    'timestamp': data[i]['timestamp'],
                    'actual_price': current_price,
                    'predicted_price': predicted_price,
                    'forecast_direction': direction,
                    'confidence': confidence,
                    'trend_score': int(prediction_signal * 10),
                    'rsi': int(rsi),
                    'volatility': round(volatility, 2)
                })
                
            except Exception as e:
                print(f"      ⚠️ Prediction error at index {i}: {e}")
                continue
        
        return predictions
    
    def _calculate_accuracy(self, predictions: List[Dict]) -> List[Dict]:
        """Calculate accuracy by comparing predictions with next actual prices"""
        results = []
        
        for i in range(len(predictions) - 1):
            current = predictions[i]
            next_actual = predictions[i + 1]['actual_price']
            
            # Calculate actual direction
            price_change = (next_actual - current['actual_price']) / current['actual_price']
            
            if abs(price_change) > 0.005:
                actual_direction = 'UP' if price_change > 0 else 'DOWN'
            else:
                actual_direction = 'HOLD'
            
            # Compare with prediction
            predicted_direction = current['forecast_direction']
            result = 'Hit' if predicted_direction == actual_direction else 'Miss'
            
            results.append({
                'timestamp': predictions[i + 1]['timestamp'],
                'predicted_direction': predicted_direction,
                'actual_direction': actual_direction,
                'result': result,
                'actual_price': next_actual,
                'predicted_price': current['predicted_price'],
                'confidence': current['confidence']
            })
        
        return results
    
    async def _store_historical_data_efficient(self, symbol: str, timeframe: str, data: List[Dict]):
        """Store historical data with proper time intervals - no every-second storage"""
        try:
            symbol_tf = f'{symbol}_{timeframe}'
            interval = self.storage_intervals.get(timeframe, timedelta(days=1))
            
            # Filter data to store only at proper intervals
            filtered_data = []
            last_stored_time = None
            
            for item in sorted(data, key=lambda x: x['timestamp']):
                current_time = item['timestamp']
                
                # Only store if enough time has passed based on timeframe
                if last_stored_time is None or (current_time - last_stored_time) >= interval:
                    # Normalize timestamp based on timeframe
                    if timeframe == '1h':
                        normalized_timestamp = current_time.replace(minute=0, second=0, microsecond=0)
                    elif timeframe == '4H':
                        hour = (current_time.hour // 4) * 4
                        normalized_timestamp = current_time.replace(hour=hour, minute=0, second=0, microsecond=0)
                    elif timeframe == '1D':
                        normalized_timestamp = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
                    elif timeframe == '7D':
                        days_since_monday = current_time.weekday()
                        normalized_timestamp = current_time.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days_since_monday)
                    elif timeframe == '1W':
                        days_since_monday = current_time.weekday()
                        normalized_timestamp = current_time.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days_since_monday)
                    elif timeframe == '1M':
                        normalized_timestamp = current_time.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                    else:
                        normalized_timestamp = current_time.replace(second=0, microsecond=0)
                    
                    filtered_data.append({
                        **item,
                        'timestamp': normalized_timestamp
                    })
                    last_stored_time = current_time
            
            # Store filtered data
            async with self.db.pool.acquire() as conn:
                for item in filtered_data:
                    await conn.execute("""
                        INSERT INTO actual_prices (symbol, timeframe, open_price, high, low, 
                                                 close_price, price, volume, timestamp)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                        ON CONFLICT (symbol, timestamp) DO UPDATE SET
                            price = EXCLUDED.price,
                            close_price = EXCLUDED.close_price,
                            volume = EXCLUDED.volume
                    """, symbol_tf, timeframe, item['open'], item['high'], item['low'],
                         item['close'], item['close'], item['volume'], item['timestamp'])
            
            print(f"    ✅ {symbol_tf}: Stored {len(filtered_data)} records (filtered from {len(data)})")
                
        except Exception as e:
            print(f"      ❌ Error storing historical data for {symbol_tf}: {e}")
    
    async def _store_predictions(self, symbol: str, timeframe: str, predictions: List[Dict], results: List[Dict]):
        """Store ML predictions and accuracy results with conflict handling"""
        try:
            symbol_tf = f'{symbol}_{timeframe}'
            async with self.db.pool.acquire() as conn:
                # Store predictions with conflict handling
                for pred in predictions:
                    await conn.execute("""
                        INSERT INTO forecasts (symbol, predicted_price, confidence, 
                                             forecast_direction, trend_score, created_at)
                        VALUES ($1, $2, $3, $4, $5, $6)
                        ON CONFLICT DO NOTHING
                    """, symbol_tf, pred['predicted_price'], pred['confidence'],
                         pred['forecast_direction'], pred['trend_score'], pred['timestamp'])
                
                # Store accuracy results with conflict handling
                for result in results:
                    await conn.execute("""
                        INSERT INTO forecast_accuracy (symbol, actual_direction, result, evaluated_at)
                        VALUES ($1, $2, $3, $4)
                        ON CONFLICT DO NOTHING
                    """, symbol_tf, result['actual_direction'], result['result'], result['timestamp'])
                
        except Exception as e:
            print(f"      ❌ Error storing predictions for {symbol_tf}: {e}")

# Global monthly data collection service
gap_filler = GapFillingService()