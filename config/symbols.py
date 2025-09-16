"""Centralized symbol configuration"""

CRYPTO_SYMBOLS = {
    'BTC': {'binance': 'BTCUSDT', 'yahoo': 'BTC-USD'},
    'ETH': {'binance': 'ETHUSDT', 'yahoo': 'ETH-USD'},
    'BNB': {'binance': 'BNBUSDT', 'yahoo': 'BNB-USD'},
    'SOL': {'binance': 'SOLUSDT', 'yahoo': 'SOL-USD'},
    'ADA': {'binance': 'ADAUSDT', 'yahoo': 'ADA-USD'},
    'XRP': {'binance': 'XRPUSDT', 'yahoo': 'XRP-USD'},
    'DOGE': {'binance': 'DOGEUSDT', 'yahoo': 'DOGE-USD'},
    'TRX': {'binance': 'TRXUSDT', 'yahoo': 'TRX-USD'},
    'USDT': {'binance': None, 'yahoo': 'USDT-USD', 'fixed_price': 1.0},
    'USDC': {'binance': None, 'yahoo': 'USDC-USD', 'fixed_price': 1.0}
}

STOCK_SYMBOLS = {
    'NVDA': {'yahoo': 'NVDA'},
    'MSFT': {'yahoo': 'MSFT'},
    'AAPL': {'yahoo': 'AAPL'},
    'GOOGL': {'yahoo': 'GOOGL'},
    'AMZN': {'yahoo': 'AMZN'},
    'META': {'yahoo': 'META'},
    'AVGO': {'yahoo': 'AVGO'},
    'TSLA': {'yahoo': 'TSLA'},
    'BRK-B': {'yahoo': 'BRK-B'},
    'JPM': {'yahoo': 'JPM'}
}

MACRO_SYMBOLS = {
    'GDP': {'value': 27000, 'change': 0.1},
    'CPI': {'value': 310.3, 'change': 0.2},
    'UNEMPLOYMENT': {'value': 3.7, 'change': -0.1},
    'FED_RATE': {'value': 5.25, 'change': 0.0},
    'CONSUMER_CONFIDENCE': {'value': 102.0, 'change': 1.5}
}

SYMBOL_NAMES = {
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