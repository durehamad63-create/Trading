import urllib.request
import json

BASE_URL = "https://easygoing-beauty-production.up.railway.app"

def test_market_summary():
    """Test market summary endpoints"""
    
    print("🚀 Testing Railway Market Summary API\n")
    
    for asset_class in ['crypto', 'stocks', 'macro', 'all']:
        try:
            url = f"{BASE_URL}/api/market/summary?class={asset_class}&limit=10"
            print(f"📊 Testing: {asset_class}")
            print(f"URL: {url}")
            
            with urllib.request.urlopen(url, timeout=15) as response:
                if response.status == 200:
                    data = json.loads(response.read().decode())
                    assets = data.get('assets', [])
                    count = len(assets)
                    
                    print(f"✅ Status: {response.status}")
                    print(f"✅ Assets returned: {count}")
                    
                    if count > 0:
                        print("📋 Assets found:")
                        for i, asset in enumerate(assets[:5]):  # Show first 5
                            symbol = asset.get('symbol', 'N/A')
                            name = asset.get('name', 'N/A')
                            price = asset.get('current_price', 'N/A')
                            change = asset.get('change_24h', 'N/A')
                            source = asset.get('data_source', 'N/A')
                            asset_type = asset.get('asset_class', 'N/A')
                            
                            print(f"  {i+1}. {symbol} ({name})")
                            print(f"     Price: ${price}")
                            print(f"     Change: {change}%")
                            print(f"     Source: {source}")
                            print(f"     Type: {asset_type}")
                    else:
                        print("⚠️ No assets returned")
                        
                else:
                    print(f"❌ HTTP Status: {response.status}")
                    
        except Exception as e:
            print(f"❌ Error testing {asset_class}: {e}")
        
        print("-" * 50)

if __name__ == "__main__":
    test_market_summary()