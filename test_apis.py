#!/usr/bin/env python3
"""
Test API connectivity
"""
import requests
import sys
import os

# Add current directory to path
sys.path.append(os.path.dirname(__file__))

def test_binance_api():
    """Test Binance API"""
    try:
        url = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            data = response.json()
            print(f"✓ Binance API: BTC = ${float(data['price']):,.2f}")
            return True
        else:
            print(f"✗ Binance API failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Binance API error: {e}")
        return False

def test_yahoo_api():
    """Test Yahoo Finance API"""
    try:
        url = "https://query1.finance.yahoo.com/v8/finance/chart/AAPL"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        response = requests.get(url, timeout=10, headers=headers)
        if response.status_code == 200:
            data = response.json()
            if 'chart' in data and data['chart']['result']:
                price = data['chart']['result'][0]['meta']['regularMarketPrice']
                print(f"✓ Yahoo API: AAPL = ${float(price):,.2f}")
                return True
        print("✗ Yahoo API failed: No data")
        return False
    except Exception as e:
        print(f"✗ Yahoo API error: {e}")
        return False

def test_local_server():
    """Test local server"""
    try:
        url = "http://localhost:8000/api/health"
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            print("✓ Local server: Running")
            return True
        else:
            print(f"✗ Local server: {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Local server: Not running ({e})")
        return False

def main():
    """Test all APIs"""
    print("🧪 Testing API Connectivity...\n")
    
    results = []
    results.append(test_binance_api())
    results.append(test_yahoo_api())
    results.append(test_local_server())
    
    print(f"\n📊 Results: {sum(results)}/3 APIs working")
    
    if all(results):
        print("✅ All APIs are working!")
    elif any(results):
        print("⚠️ Some APIs are working")
    else:
        print("❌ No APIs are working - check internet connection")

if __name__ == "__main__":
    main()