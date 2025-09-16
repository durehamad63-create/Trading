"""
Enhanced Stock Data APIs
"""
import requests
import yfinance as yf
import logging
from typing import Dict, Optional

class StockDataProvider:
    def __init__(self):
        self.alpha_vantage_key = "YOUR_API_KEY"  # Get free key from alphavantage.co
        self.iex_token = "YOUR_TOKEN"  # Get from iexcloud.io
        
    def get_real_time_price(self, symbol: str) -> Optional[float]:
        """Get real-time stock price with fallback chain"""
        
        # 1. Try Alpha Vantage (most reliable for real-time)
        try:
            url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={self.alpha_vantage_key}"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if "Global Quote" in data:
                    price = float(data["Global Quote"]["05. price"])
                    logging.info(f"✅ Alpha Vantage: {symbol} = ${price}")
                    return price
        except Exception as e:
            logging.warning(f"Alpha Vantage failed for {symbol}: {e}")
        
        # 2. Try IEX Cloud
        try:
            url = f"https://cloud.iexapis.com/stable/stock/{symbol}/quote?token={self.iex_token}"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                price = float(data["latestPrice"])
                logging.info(f"✅ IEX Cloud: {symbol} = ${price}")
                return price
        except Exception as e:
            logging.warning(f"IEX Cloud failed for {symbol}: {e}")
        
        # 3. Fallback to Yahoo Finance (your current method)
        try:
            ticker = yf.Ticker(symbol)
            data = ticker.history(period="1d", interval="1m")
            if not data.empty:
                price = float(data['Close'].iloc[-1])
                logging.info(f"✅ Yahoo Finance: {symbol} = ${price}")
                return price
        except Exception as e:
            logging.warning(f"Yahoo Finance failed for {symbol}: {e}")
        
        return None
    
    def get_market_data(self, symbol: str) -> Dict:
        """Get comprehensive market data"""
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            hist = ticker.history(period="2d")
            
            current_price = float(hist['Close'].iloc[-1])
            prev_close = float(hist['Close'].iloc[-2]) if len(hist) > 1 else current_price
            change_24h = ((current_price - prev_close) / prev_close) * 100
            
            return {
                'symbol': symbol,
                'current_price': current_price,
                'change_24h': change_24h,
                'volume': float(hist['Volume'].iloc[-1]),
                'market_cap': info.get('marketCap', 0),
                'pe_ratio': info.get('trailingPE', 0),
                'data_source': 'Yahoo Finance'
            }
        except Exception as e:
            logging.error(f"Market data failed for {symbol}: {e}")
            return {}

# Usage in your ml_predictor.py
stock_provider = StockDataProvider()