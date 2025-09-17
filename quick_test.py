import requests
import json

BASE_URL = "http://localhost:8000"

def test_health():
    """Quick health check"""
    try:
        response = requests.get(f"{BASE_URL}/api/health", timeout=5)
        print(f"Health: {response.status_code} - {response.json()}")
        return response.status_code == 200
    except Exception as e:
        print(f"Health check failed: {e}")
        return False

def test_sample_assets():
    """Test a few sample assets quickly"""
    assets = ['BTC', 'NVDA', 'GDP']
    
    for asset in assets:
        print(f"\n--- Testing {asset} ---")
        
        # Test forecast
        try:
            response = requests.get(f"{BASE_URL}/api/asset/{asset}/forecast", timeout=5)
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Forecast: {data.get('prediction', 'N/A')}")
            else:
                print(f"❌ Forecast failed: {response.status_code}")
        except Exception as e:
            print(f"❌ Forecast error: {e}")
        
        # Test trends
        try:
            response = requests.get(f"{BASE_URL}/api/asset/{asset}/trends", timeout=5)
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Trends: {data.get('overall_accuracy', 'N/A')}%")
            else:
                print(f"❌ Trends failed: {response.status_code}")
        except Exception as e:
            print(f"❌ Trends error: {e}")

def test_market_summary():
    """Test market summary"""
    for asset_class in ['crypto', 'stock', 'macro']:
        try:
            response = requests.get(f"{BASE_URL}/api/market/summary?class={asset_class}&limit=3", timeout=5)
            if response.status_code == 200:
                data = response.json()
                count = len(data.get('assets', []))
                print(f"✅ Market {asset_class}: {count} assets")
            else:
                print(f"❌ Market {asset_class} failed: {response.status_code}")
        except Exception as e:
            print(f"❌ Market {asset_class} error: {e}")

if __name__ == "__main__":
    print("🚀 Quick API Test\n")
    
    if test_health():
        print("\n📊 Market Summary:")
        test_market_summary()
        
        print("\n📈 Sample Asset Tests:")
        test_sample_assets()
    else:
        print("❌ Server not responding")