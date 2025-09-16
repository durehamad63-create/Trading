#!/usr/bin/env python3
"""
Gap filling service to fetch missing historical data on startup
"""
import asyncio
import aiohttp
import logging
from datetime import datetime, timedelta
from database import db

class GapFillingService:
    def __init__(self, model=None):
        self.model = model  # Use shared model instance
        self.binance_symbols = {
            'BTC': 'BTCUSDT', 'ETH': 'ETHUSDT', 'BNB': 'BNBUSDT',
            'SOL': 'SOLUSDT', 'ADA': 'ADAUSDT', 'XRP': 'XRPUSDT',
            'DOGE': 'DOGEUSDT', 'TRX': 'TRXUSDT', 'USDT': 'USDTUSDC',
            'USDC': 'USDCUSDT'
        }
    
    async def fill_missing_data(self, db_instance):
        """Fill missing data for all symbols since last stored timestamp"""
        if not db_instance or not db_instance.pool:
            logging.warning("⚠️ Database not connected, skipping gap filling")
            return
        
        self.db = db_instance
            
        logging.info("🔄 Starting gap filling process...")
        
        for symbol in self.binance_symbols.keys():
            try:
                await self._fill_symbol_gaps(symbol)
                # Add small delay to prevent overwhelming the system
                await asyncio.sleep(0.1)
            except Exception as e:
                logging.error(f"❌ Gap filling failed for {symbol}: {e}")
        
        logging.info("✅ Gap filling completed")
    
    async def _fill_symbol_gaps(self, symbol):
        """Fill gaps for a specific symbol"""
        # Get last stored timestamp
        last_time = await self.db.get_last_stored_time(symbol)
        
        if not last_time:
            # No data exists, fetch last 24 hours
            start_time = datetime.now() - timedelta(hours=24)
        else:
            # Fetch from last stored time
            start_time = last_time
        
        end_time = datetime.now()
        
        # Skip if gap is less than 1 minute
        if (end_time - start_time).total_seconds() < 60:
            return
        

        
        # Fetch historical data from Binance
        historical_data = await self._fetch_binance_klines(symbol, start_time, end_time)
        
        if historical_data:
            await self.db.store_historical_batch(symbol, historical_data)

            
            # Generate and store missing forecasts for historical data
            await self._fill_missing_forecasts(symbol, historical_data)
    
    async def _fetch_binance_klines(self, symbol, start_time, end_time):
        """Fetch historical klines from Binance API"""
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
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        return self._process_klines(data)
                    else:
                        logging.error(f"Binance API error for {symbol}: {response.status}")
                        return []
        except Exception as e:
            logging.error(f"Failed to fetch {symbol} data: {e}")
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
    
    async def _fill_missing_forecasts(self, symbol, historical_data):
        """Generate and store forecasts for historical price data"""
        if not self.model:
            logging.warning(f"⚠️ No model available for forecasts, skipping {symbol}")
            return
            
        try:

            
            for data_point in historical_data:
                # Generate forecast for this historical price
                try:
                    prediction = self.model.predict(symbol)
                    
                    # Store forecast with historical timestamp
                    forecast_data = {
                        'forecast_direction': prediction['forecast_direction'],
                        'confidence': prediction['confidence'],
                        'predicted_price': prediction['predicted_price'],
                        'predicted_range': prediction.get('predicted_range', ''),
                        'trend_score': prediction.get('trend_score', 0)
                    }
                    
                    # Store with historical timestamp
                    async with self.db.pool.acquire() as conn:
                        await conn.execute("""
                            INSERT INTO forecasts (symbol, forecast_direction, confidence, predicted_price, predicted_range, trend_score, created_at)
                            VALUES ($1, $2, $3, $4, $5, $6, $7)
                        """, symbol, forecast_data['forecast_direction'], forecast_data['confidence'],
                            forecast_data['predicted_price'], forecast_data['predicted_range'],
                            forecast_data['trend_score'], data_point['timestamp'])
                    
                except Exception as e:
                    logging.warning(f"Failed to generate forecast for {symbol} at {data_point['timestamp']}: {e}")
                    continue
            

            
        except Exception as e:
            logging.error(f"❌ Failed to fill missing forecasts for {symbol}: {e}")

# Global gap filling service
gap_filler = GapFillingService()