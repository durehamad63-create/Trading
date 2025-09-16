#!/usr/bin/env python3
"""
Fixed WebSocket Cache with Proper Asset Classification
"""

# Fixed caching logic with proper crypto/stock classification
fixed_cache_code = '''
        async def get_cached_market_data():
            """Get market data from Redis cache or generate if not cached"""
            cache_key = "market_summary_data"
            
            # Try Redis cache first
            if redis_client:
                try:
                    cached_data = redis_client.get(cache_key)
                    if cached_data:
                        return json.loads(cached_data)
                except Exception as e:
                    logging.warning(f"Redis cache read failed: {e}")
            
            # Define symbol lists for proper classification
            crypto_symbols = ['BTC', 'ETH', 'BNB', 'USDT', 'XRP', 'SOL', 'USDC', 'DOGE', 'ADA', 'TRX']
            stock_symbols = ['NVDA', 'MSFT', 'AAPL', 'GOOGL', 'AMZN', 'META', 'AVGO', 'TSLA', 'BRK-B', 'JPM']
            
            assets = []
            
            # Get crypto data - only from crypto symbols
            if realtime_service and hasattr(realtime_service, 'price_cache'):
                for symbol in realtime_service.price_cache.keys():
                    if symbol in crypto_symbols:
                        price_data = realtime_service.price_cache[symbol]
                        change = price_data['change_24h']
                        
                        assets.append({
                            'symbol': symbol,
                            'name': multi_asset.get_asset_name(symbol),
                            'current_price': price_data['current_price'],
                            'change_24h': change,
                            'volume': price_data['volume'],
                            'forecast_direction': 'UP' if change > 1 else 'DOWN' if change < -1 else 'HOLD',
                            'confidence': min(80, max(60, 70 + abs(change))),
                            'predicted_range': f"${price_data['current_price']*0.98:.2f}–${price_data['current_price']*1.02:.2f}",
                            'data_source': 'Binance Stream',
                            'asset_class': 'crypto'
                        })
            
            # Get stock data - only from stock symbols
            if stock_realtime_service and hasattr(stock_realtime_service, 'price_cache'):
                for symbol in stock_realtime_service.price_cache.keys():
                    if symbol in stock_symbols:
                        price_data = stock_realtime_service.price_cache[symbol]
                        change = price_data['change_24h']
                        
                        assets.append({
                            'symbol': symbol,
                            'name': stock_realtime_service.stock_symbols.get(symbol, symbol),
                            'current_price': price_data['current_price'],
                            'change_24h': change,
                            'volume': price_data['volume'],
                            'forecast_direction': 'UP' if change > 0.5 else 'DOWN' if change < -0.5 else 'HOLD',
                            'confidence': min(85, max(65, 75 + abs(change))),
                            'predicted_range': f"${price_data['current_price']*0.98:.2f}–${price_data['current_price']*1.02:.2f}",
                            'data_source': price_data.get('data_source', 'Stock API'),
                            'asset_class': 'stocks'
                        })
            
            # Cache the data for 5 seconds
            if redis_client and assets:
                try:
                    redis_client.setex(cache_key, 5, json.dumps(assets))
                except Exception as e:
                    logging.warning(f"Redis cache write failed: {e}")
            
            return assets
'''

print("✅ Fixed asset classification:")
print("- Crypto symbols: Only from crypto_symbols list")
print("- Stock symbols: Only from stock_symbols list") 
print("- Proper filtering: Checks symbol in list before adding")
print("- Correct asset_class: 'crypto' for crypto, 'stocks' for stocks")