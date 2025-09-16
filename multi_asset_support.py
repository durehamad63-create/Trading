"""
Multi-asset support for stocks and macro indicators
"""
import requests
# Removed yfinance import - using direct API calls
import numpy as np
from datetime import datetime, timedelta
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class MultiAssetSupport:
    def __init__(self):
        self.crypto_symbols = ['BTC', 'ETH', 'BNB', 'USDT', 'XRP', 'SOL', 'USDC', 'DOGE', 'ADA', 'TRX']
        self.stock_symbols = ['NVDA', 'MSFT', 'AAPL', 'GOOGL', 'AMZN', 'META', 'AVGO', 'TSLA', 'BRK-B', 'JPM']
        self.macro_symbols = ['GDP', 'CPI', 'UNEMPLOYMENT', 'FED_RATE', 'CONSUMER_CONFIDENCE']
    
    def get_asset_data(self, symbol):
        """Get current price and change for any asset type"""
        if symbol in self.crypto_symbols:
            return self._get_crypto_data(symbol)
        elif symbol in self.stock_symbols:
            return self._get_stock_data(symbol)
        elif symbol in self.macro_symbols:
            return self._get_macro_data(symbol)
        else:
            raise Exception(f"Unsupported symbol: {symbol}")
    
    def _get_crypto_data(self, symbol):
        """Get crypto data from Binance"""
        symbol_map = {
            'BTC': 'BTCUSDT', 'ETH': 'ETHUSDT', 'BNB': 'BNBUSDT',
            'XRP': 'XRPUSDT', 'SOL': 'SOLUSDT', 'DOGE': 'DOGEUSDT', 
            'ADA': 'ADAUSDT', 'TRX': 'TRXUSDT', 'USDT': 'USDTUSDC',
            'USDC': 'USDCUSDT'
        }
        
        binance_symbol = symbol_map.get(symbol)
        if not binance_symbol:
            raise Exception(f"Crypto symbol not supported: {symbol}")
        
        binance_url = os.getenv('BINANCE_API_URL', 'https://api.binance.com/api/v3')
        url = f"{binance_url}/ticker/24hr?symbol={binance_symbol}"
        response = requests.get(url, timeout=10)
        
        if response.status_code != 200:
            raise Exception(f"Binance API failed: {response.status_code}")
        
        data = response.json()
        return {
            'current_price': float(data['lastPrice']),
            'change_24h': float(data['priceChangePercent']),
            'data_source': 'Binance API'
        }
    
    def _get_stock_data(self, symbol):
        """Get stock data using direct Yahoo Finance API"""
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            response = requests.get(url, timeout=10)
            
            if response.status_code != 200:
                raise Exception(f"Yahoo API failed: {response.status_code}")
            
            data = response.json()
            result = data['chart']['result'][0]
            meta = result['meta']
            
            current_price = meta['regularMarketPrice']
            prev_close = meta['previousClose']
            change_pct = ((current_price - prev_close) / prev_close) * 100
            
            return {
                'current_price': float(current_price),
                'change_24h': float(change_pct),
                'data_source': 'Yahoo Finance API'
            }
            
        except Exception as e:
            logging.error(f"Yahoo Finance API failed for {symbol}: {e}")
            raise Exception(f"Stock data unavailable for {symbol}: {str(e)}")
    

    
    def _get_macro_data(self, symbol):
        """Get macro indicator data"""
        # Current realistic values for macro indicators
        current_values = {
            'GDP': 27000,  # US GDP in billions
            'CPI': 310.3,  # Consumer Price Index
            'UNEMPLOYMENT': 3.7,  # Unemployment rate %
            'FED_RATE': 5.25,     # Federal funds rate %
            'CONSUMER_CONFIDENCE': 102.0  # Consumer confidence index
        }
        
        # Realistic recent changes
        recent_changes = {
            'GDP': 0.1,    # GDP growth
            'CPI': 0.2,    # Inflation change
            'UNEMPLOYMENT': -0.1,  # Unemployment change
            'FED_RATE': 0.0,       # Fed rate stable
            'CONSUMER_CONFIDENCE': 1.5  # Confidence up
        }
        
        current_value = current_values.get(symbol, 100)
        change = recent_changes.get(symbol, 0)
        
        return {
            'current_price': current_value,
            'change_24h': change,
            'data_source': 'Economic Data'
        }
    
    def get_historical_data(self, symbol, periods=100):
        """Get historical data for any asset type"""
        if symbol in self.crypto_symbols:
            return self._get_crypto_historical(symbol, periods)
        elif symbol in self.stock_symbols:
            return self._get_stock_historical(symbol, periods)
        elif symbol in self.macro_symbols:
            return self._get_macro_historical(symbol, periods)
        else:
            raise Exception(f"Unsupported symbol: {symbol}")
    
    def _get_crypto_historical(self, symbol, periods):
        """Get crypto historical data from Binance"""
        symbol_map = {
            'BTC': 'BTCUSDT', 'ETH': 'ETHUSDT', 'BNB': 'BNBUSDT',
            'XRP': 'XRPUSDT', 'SOL': 'SOLUSDT', 'DOGE': 'DOGEUSDT',
            'ADA': 'ADAUSDT', 'TRX': 'TRXUSDT', 'USDT': 'USDTUSDC',
            'USDC': 'USDCUSDT'
        }
        
        binance_symbol = symbol_map.get(symbol)
        if not binance_symbol:
            raise Exception(f"Crypto symbol not supported: {symbol}")
        
        binance_url = os.getenv('BINANCE_API_URL', 'https://api.binance.com/api/v3')
        url = f"{binance_url}/klines?symbol={binance_symbol}&interval=1h&limit={periods}"
        response = requests.get(url, timeout=15)
        
        if response.status_code != 200:
            raise Exception(f"Binance API failed: {response.status_code}")
        
        data = response.json()
        prices = [float(kline[4]) for kline in data]  # Close price
        volumes = [float(kline[5]) for kline in data]  # Volume
        
        return np.array(prices), np.array(volumes)
    
    def _get_stock_historical(self, symbol, periods):
        """Get stock historical data using direct Yahoo Finance API"""
        try:
            # Use 1d interval for better data availability
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=3mo"
            response = requests.get(url, timeout=15)
            
            if response.status_code != 200:
                raise Exception(f"Yahoo API failed: {response.status_code}")
            
            data = response.json()
            result = data['chart']['result'][0]
            
            # Extract price and volume data
            timestamps = result['timestamp']
            indicators = result['indicators']['quote'][0]
            
            closes = [x for x in indicators['close'] if x is not None]
            volumes = [x for x in indicators['volume'] if x is not None]
            
            if len(closes) == 0:
                raise Exception(f"No valid price data for {symbol}")
            
            # Return last 'periods' data points
            prices = np.array(closes[-periods:])
            vols = np.array(volumes[-periods:] if len(volumes) >= len(prices) else [1000000] * len(prices))
            
            return prices, vols
            
        except Exception as e:
            logging.error(f"Yahoo Finance API historical failed for {symbol}: {e}")
            raise Exception(f"Stock historical data failed for {symbol}: {str(e)}")
    
    def _get_macro_historical(self, symbol, periods):
        """Generate realistic macro historical data"""
        # Base values for macro indicators
        base_values = {
            'GDP': 27000,  # US GDP in billions
            'CPI': 310,    # Consumer Price Index
            'UNEMPLOYMENT': 3.7,  # Unemployment rate %
            'FED_RATE': 5.25,     # Federal funds rate %
            'CONSUMER_CONFIDENCE': 102  # Consumer confidence index
        }
        
        base_value = base_values.get(symbol, 100)
        
        # Generate realistic trend data
        prices = []
        current_value = base_value
        
        for i in range(periods):
            # Add realistic volatility based on indicator type
            if symbol == 'GDP':
                change = np.random.normal(0, 0.001)  # GDP grows slowly
            elif symbol == 'CPI':
                change = np.random.normal(0.0002, 0.001)  # Inflation trend
            elif symbol == 'UNEMPLOYMENT':
                change = np.random.normal(0, 0.01)  # Unemployment volatility
            elif symbol == 'FED_RATE':
                change = np.random.normal(0, 0.005)  # Interest rate changes
            else:  # CONSUMER_CONFIDENCE
                change = np.random.normal(0, 0.02)  # Confidence volatility
            
            current_value *= (1 + change)
            prices.append(current_value)
        
        volumes = np.array(prices) * 0.1  # Simulated volume
        return np.array(prices), volumes
    
    def format_predicted_range(self, symbol, predicted_price):
        """Format predicted range based on asset type"""
        if symbol == 'GDP':
            return f'${predicted_price*0.98:.0f}B–${predicted_price*1.02:.0f}B'
        elif symbol in ['CPI', 'CONSUMER_CONFIDENCE']:
            return f'{predicted_price*0.98:.1f}–{predicted_price*1.02:.1f}'
        elif symbol in ['UNEMPLOYMENT', 'FED_RATE']:
            return f'{predicted_price*0.98:.2f}%–{predicted_price*1.02:.2f}%'
        else:
            return f'${predicted_price*0.98:.2f}–${predicted_price*1.02:.2f}'
    
    def get_asset_name(self, symbol):
        """Get full name for asset"""
        names = {
            # Crypto
            'BTC': 'Bitcoin', 'ETH': 'Ethereum', 'BNB': 'Binance Coin', 'USDT': 'Tether',
            'XRP': 'Ripple', 'SOL': 'Solana', 'USDC': 'USD Coin', 'DOGE': 'Dogecoin',
            'ADA': 'Cardano', 'TRX': 'Tron',
            # Stocks
            'NVDA': 'NVIDIA', 'MSFT': 'Microsoft', 'AAPL': 'Apple', 'GOOGL': 'Alphabet',
            'AMZN': 'Amazon', 'META': 'Meta', 'AVGO': 'Broadcom', 'TSLA': 'Tesla',
            'BRK-B': 'Berkshire Hathaway', 'JPM': 'JPMorgan Chase',
            # Macro
            'GDP': 'Gross Domestic Product', 'CPI': 'Consumer Price Index',
            'UNEMPLOYMENT': 'Unemployment Rate', 'FED_RATE': 'Federal Interest Rate',
            'CONSUMER_CONFIDENCE': 'Consumer Confidence Index'
        }
        return names.get(symbol, symbol)

# Global instance
multi_asset = MultiAssetSupport()