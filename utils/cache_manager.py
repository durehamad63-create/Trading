"""Centralized cache key management"""

class CacheKeys:
    @staticmethod
    def prediction(symbol):
        return f"prediction:{symbol}"
    
    @staticmethod
    def websocket_history(symbol, timeframe):
        return f"websocket_history:{symbol}:{timeframe}"
    
    @staticmethod
    def chart_data(symbol, timeframe):
        return f"chart_data:{symbol}:{timeframe}"
    
    @staticmethod
    def market_summary():
        return "market_summary_data"
    
    @staticmethod
    def stock_history(symbol, timeframe):
        return f"stock_history:{symbol}:{timeframe}"