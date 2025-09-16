import requests
import logging

class APIClient:
    @staticmethod
    def get_binance_price(symbol):
        """Get price from Binance API"""
        try:
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                return float(response.json()['price'])
        except Exception as e:
            logging.error(f"❌ BINANCE API ERROR [{symbol}]: {e}")
        return None
    
    @staticmethod
    def get_binance_change(symbol):
        """Get 24h change from Binance API"""
        try:
            url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                return float(response.json()['priceChangePercent'])
        except Exception as e:
            logging.error(f"❌ BINANCE API ERROR [{symbol}]: {e}")
        return 0.0
    
    @staticmethod
    def get_yahoo_price(symbol):
        """Get price from Yahoo Finance API"""
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            response = requests.get(url, timeout=15, headers=headers)
            if response.status_code == 200:
                data = response.json()
                if 'chart' in data and data['chart']['result']:
                    return float(data['chart']['result'][0]['meta']['regularMarketPrice'])
        except Exception as e:
            logging.error(f"❌ YAHOO API ERROR [{symbol}]: {e}")
        return None
    
    @staticmethod
    def get_yahoo_change(symbol):
        """Get 24h change from Yahoo Finance API"""
        try:
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            response = requests.get(url, timeout=15, headers=headers)
            if response.status_code == 200:
                data = response.json()
                if 'chart' in data and data['chart']['result']:
                    meta = data['chart']['result'][0]['meta']
                    current = meta['regularMarketPrice']
                    prev = meta['previousClose']
                    return ((current - prev) / prev) * 100
        except Exception as e:
            logging.error(f"❌ YAHOO API ERROR [{symbol}]: {e}")
        return 0.0