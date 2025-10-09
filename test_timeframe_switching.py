#!/usr/bin/env python3
"""
Automated test script for timeframe switching functionality
"""
import asyncio
import websockets
import json
import time
from datetime import datetime

class TimeframeSwitchingTest:
    def __init__(self, base_url="wss://trading-production-85d8.up.railway.app"):
        self.base_url = base_url
        self.test_results = []
        
    async def test_timeframe_switching(self, symbol="BTC", initial_timeframe="1D"):
        """Test timeframe switching without reconnection"""
        print(f"🧪 Testing timeframe switching for {symbol}")
        
        ws_url = f"{self.base_url}/ws/chart/{symbol}?timeframe={initial_timeframe}"
        print(f"📡 Connecting to: {ws_url}")
        
        try:
            async with websockets.connect(ws_url) as websocket:
                print(f"✅ Connected successfully")
                
                # Test sequence of timeframe changes
                timeframes_to_test = ["4H", "1h", "1D", "7D", "1W", "1M"]
                
                for i, new_timeframe in enumerate(timeframes_to_test):
                    print(f"\n🔄 Test {i+1}: Switching to {new_timeframe}")
                    
                    # Send timeframe change message
                    message = {
                        "type": "change_timeframe",
                        "timeframe": new_timeframe
                    }
                    
                    await websocket.send(json.dumps(message))
                    print(f"📤 Sent timeframe change request: {new_timeframe}")
                    
                    # Wait for confirmation and chart data
                    confirmation_received = False
                    chart_data_received = False
                    timeout = 10  # 10 second timeout
                    start_time = time.time()
                    
                    while (not confirmation_received or not chart_data_received) and (time.time() - start_time) < timeout:
                        try:
                            response = await asyncio.wait_for(websocket.recv(), timeout=2.0)
                            data = json.loads(response)
                            
                            if data.get("type") == "timeframe_changed":
                                if data.get("timeframe") == new_timeframe:
                                    print(f"✅ Confirmation received: {data['timeframe']}")
                                    confirmation_received = True
                                else:
                                    print(f"❌ Wrong timeframe in confirmation: expected {new_timeframe}, got {data.get('timeframe')}")
                            
                            elif data.get("type") == "chart_update":
                                if data.get("timeframe") == new_timeframe:
                                    print(f"📊 Chart data received for {data['timeframe']}")
                                    print(f"   - Past points: {len(data.get('chart', {}).get('past', []))}")
                                    print(f"   - Future points: {len(data.get('chart', {}).get('future', []))}")
                                    chart_data_received = True
                                
                        except asyncio.TimeoutError:
                            continue
                        except Exception as e:
                            print(f"❌ Error receiving message: {e}")
                            break
                    
                    # Record test result
                    test_passed = confirmation_received and chart_data_received
                    self.test_results.append({
                        "timeframe": new_timeframe,
                        "confirmation": confirmation_received,
                        "chart_data": chart_data_received,
                        "passed": test_passed
                    })
                    
                    if test_passed:
                        print(f"✅ Test {i+1} PASSED")
                    else:
                        print(f"❌ Test {i+1} FAILED")
                    
                    # Wait a bit before next test
                    await asyncio.sleep(1)
                
                print(f"\n📊 Test Summary:")
                passed_tests = sum(1 for result in self.test_results if result["passed"])
                total_tests = len(self.test_results)
                print(f"   Passed: {passed_tests}/{total_tests}")
                
                for result in self.test_results:
                    status = "✅ PASS" if result["passed"] else "❌ FAIL"
                    print(f"   {result['timeframe']}: {status}")
                
        except Exception as e:
            print(f"❌ Connection failed: {e}")
            return False
        
        return passed_tests == total_tests

    async def test_multiple_symbols(self):
        """Test timeframe switching with different symbols"""
        symbols = ["BTC", "ETH", "NVDA", "AAPL"]
        
        print(f"\n🔄 Testing multiple symbols: {symbols}")
        
        for symbol in symbols:
            print(f"\n📈 Testing {symbol}...")
            success = await self.test_timeframe_switching(symbol, "1D")
            if success:
                print(f"✅ {symbol} tests passed")
            else:
                print(f"❌ {symbol} tests failed")
            
            await asyncio.sleep(2)  # Wait between symbol tests

    async def test_invalid_timeframes(self):
        """Test handling of invalid timeframes"""
        print(f"\n🚫 Testing invalid timeframes...")
        
        ws_url = f"{self.base_url}/ws/chart/BTC?timeframe=1D"
        
        try:
            async with websockets.connect(ws_url) as websocket:
                print(f"✅ Connected for invalid timeframe test")
                
                invalid_timeframes = ["2H", "30m", "invalid", ""]
                
                for invalid_tf in invalid_timeframes:
                    print(f"🧪 Testing invalid timeframe: '{invalid_tf}'")
                    
                    message = {
                        "type": "change_timeframe",
                        "timeframe": invalid_tf
                    }
                    
                    await websocket.send(json.dumps(message))
                    
                    # Should not receive confirmation for invalid timeframes
                    try:
                        response = await asyncio.wait_for(websocket.recv(), timeout=3.0)
                        data = json.loads(response)
                        
                        if data.get("type") == "timeframe_changed":
                            print(f"❌ Unexpected confirmation for invalid timeframe: {invalid_tf}")
                        else:
                            print(f"✅ No confirmation for invalid timeframe (correct behavior)")
                    
                    except asyncio.TimeoutError:
                        print(f"✅ No response for invalid timeframe '{invalid_tf}' (correct behavior)")
                    
                    await asyncio.sleep(1)
                    
        except Exception as e:
            print(f"❌ Invalid timeframe test failed: {e}")

    async def run_all_tests(self):
        """Run all timeframe switching tests"""
        print("🚀 Starting Timeframe Switching Tests")
        print("=" * 50)
        
        # Test 1: Basic timeframe switching
        await self.test_timeframe_switching("BTC", "1D")
        
        # Test 2: Multiple symbols
        await self.test_multiple_symbols()
        
        # Test 3: Invalid timeframes
        await self.test_invalid_timeframes()
        
        print("\n" + "=" * 50)
        print("🏁 All tests completed!")

async def main():
    """Main test runner"""
    tester = TimeframeSwitchingTest()
    
    try:
        await tester.run_all_tests()
    except KeyboardInterrupt:
        print("\n🛑 Tests interrupted by user")
    except Exception as e:
        print(f"❌ Test runner failed: {e}")

if __name__ == "__main__":
    print("🧪 Timeframe Switching Test Script")
    print("Testing against production server: trading-production-85d8.up.railway.app")
    print("Press Ctrl+C to stop tests\n")
    
    asyncio.run(main())