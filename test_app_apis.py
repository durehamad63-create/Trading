#!/usr/bin/env python3
"""
Test Trading App APIs
"""
import requests
import json

BASE_URL = "http://localhost:8000"

def test_health():
    """Test health endpoint"""
    try:
        response = requests.get(f"{BASE_URL}/api/health", timeout=5)
        if response.status_code == 200:
            data = response.json()
            print(f"✓ Health: {data['status']}")
            return True
        else:
            print(f"✗ Health: {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Health: {e}")
        return False

def test_market_summary():
    """Test market summary endpoint"""
    try:
        response = requests.get(f"{BASE_URL}/api/market/summary?class=crypto&limit=3", timeout=10)
        if response.status_code == 200:
            data = response.json()
            print(f"✓ Market Summary: {len(data.get('assets', []))} assets")
            return True
        else:
            print(f"✗ Market Summary: {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Market Summary: {e}")
        return False

def test_asset_forecast():
    """Test asset forecast endpoint"""
    try:
        response = requests.get(f"{BASE_URL}/api/asset/BTC/forecast", timeout=10)
        if response.status_code == 200:
            data = response.json()
            print(f"✓ BTC Forecast: ${data.get('current_price', 'N/A')}")
            return True
        else:
            print(f"✗ BTC Forecast: {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ BTC Forecast: {e}")
        return False

def test_asset_trends():
    """Test asset trends endpoint"""
    try:
        response = requests.get(f"{BASE_URL}/api/asset/BTC/trends", timeout=10)
        if response.status_code == 200:
            data = response.json()
            print(f"✓ BTC Trends: {data.get('accuracy', 'N/A')}% accuracy")
            return True
        else:
            print(f"✗ BTC Trends: {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ BTC Trends: {e}")
        return False

def test_search():
    """Test search endpoint"""
    try:
        response = requests.get(f"{BASE_URL}/api/assets/search?q=BTC", timeout=5)
        if response.status_code == 200:
            data = response.json()
            print(f"✓ Search: {len(data.get('results', []))} results")
            return True
        else:
            print(f"✗ Search: {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ Search: {e}")
        return False

def main():
    """Test all app APIs"""
    print("🧪 Testing Trading App APIs...\n")
    
    tests = [
        test_health,
        test_market_summary,
        test_asset_forecast,
        test_asset_trends,
        test_search
    ]
    
    results = []
    for test in tests:
        results.append(test())
    
    print(f"\n📊 Results: {sum(results)}/{len(results)} APIs working")
    
    if all(results):
        print("✅ All app APIs are working!")
    elif any(results):
        print("⚠️ Some app APIs are working")
    else:
        print("❌ App APIs not working - start server with: python main.py")

if __name__ == "__main__":
    main()