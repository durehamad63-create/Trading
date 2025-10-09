#!/usr/bin/env python3
"""
Test script for WebSocket connection opening/closing with timeframe switching
"""
import asyncio
import websockets
import json
import time
from datetime import datetime

class ConnectionSwitchingTest:
    def __init__(self, base_url="wss://trading-production-85d8.up.railway.app"):
        self.base_url = base_url
        self.connection_results = []
        
    async def test_single_connection(self, symbol, timeframe):
        """Test single connection for specific timeframe"""
        ws_url = f"{self.base_url}/ws/chart/{symbol}?timeframe={timeframe}"
        
        start_time = time.time()
        try:
            async with websockets.connect(ws_url) as websocket:
                connect_time = time.time() - start_time
                print(f"✅ Connected to {symbol} {timeframe} in {connect_time:.2f}s")
                
                # Wait for initial chart data
                data_received = False
                timeout = 10
                
                while not data_received and (time.time() - start_time) < timeout:
                    try:
                        response = await asyncio.wait_for(websocket.recv(), timeout=3.0)
                        data = json.loads(response)
                        
                        if data.get("type") == "chart_update":
                            data_received = True
                            print(f"📊 Chart data received for {symbol} {timeframe}")
                            print(f"   - Past: {len(data.get('chart', {}).get('past', []))}")
                            print(f"   - Future: {len(data.get('chart', {}).get('future', []))}")
                            
                    except asyncio.TimeoutError:
                        continue
                
                total_time = time.time() - start_time
                return {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "connected": True,
                    "data_received": data_received,
                    "connect_time": connect_time,
                    "total_time": total_time,
                    "success": data_received
                }
                
        except Exception as e:
            print(f"❌ Connection failed for {symbol} {timeframe}: {e}")
            return {
                "symbol": symbol,
                "timeframe": timeframe,
                "connected": False,
                "data_received": False,
                "connect_time": 0,
                "total_time": time.time() - start_time,
                "success": False,
                "error": str(e)
            }

    async def test_timeframe_connection_switching(self, symbol="BTC"):
        """Test opening/closing connections for different timeframes"""
        print(f"🔄 Testing connection switching for {symbol}")
        
        timeframes = ["1h", "4H", "1D", "7D", "1W", "1M"]
        results = []
        
        for i, timeframe in enumerate(timeframes):
            print(f"\n📡 Test {i+1}: Opening connection for {timeframe}")
            
            result = await self.test_single_connection(symbol, timeframe)
            results.append(result)
            
            if result["success"]:
                print(f"✅ {timeframe} connection test PASSED")
            else:
                print(f"❌ {timeframe} connection test FAILED")
            
            # Brief pause between connections
            await asyncio.sleep(1)
        
        return results

    async def test_rapid_connection_switching(self, symbol="BTC"):
        """Test rapid opening/closing of connections"""
        print(f"\n⚡ Testing rapid connection switching for {symbol}")
        
        timeframes = ["1D", "4H", "1D", "1h", "1D"]
        tasks = []
        
        # Start all connections simultaneously
        for timeframe in timeframes:
            task = asyncio.create_task(self.test_single_connection(symbol, timeframe))
            tasks.append(task)
        
        # Wait for all connections to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        successful = sum(1 for r in results if isinstance(r, dict) and r.get("success"))
        print(f"📊 Rapid switching: {successful}/{len(timeframes)} connections successful")
        
        return results

    async def test_connection_stability(self, symbol="BTC", timeframe="1D", duration=30):
        """Test connection stability over time"""
        print(f"\n⏱️ Testing connection stability for {symbol} {timeframe} ({duration}s)")
        
        ws_url = f"{self.base_url}/ws/chart/{symbol}?timeframe={timeframe}"
        messages_received = 0
        start_time = time.time()
        
        try:
            async with websockets.connect(ws_url) as websocket:
                print(f"✅ Connected for stability test")
                
                while (time.time() - start_time) < duration:
                    try:
                        response = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                        data = json.loads(response)
                        messages_received += 1
                        
                        if messages_received % 5 == 0:
                            print(f"📨 Received {messages_received} messages")
                            
                    except asyncio.TimeoutError:
                        print("⏰ No message received in 5s (normal)")
                        continue
                
                elapsed = time.time() - start_time
                print(f"✅ Stability test completed: {messages_received} messages in {elapsed:.1f}s")
                
                return {
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "duration": elapsed,
                    "messages": messages_received,
                    "stable": True
                }
                
        except Exception as e:
            print(f"❌ Stability test failed: {e}")
            return {
                "symbol": symbol,
                "timeframe": timeframe,
                "duration": time.time() - start_time,
                "messages": messages_received,
                "stable": False,
                "error": str(e)
            }

    async def test_multiple_symbols_parallel(self):
        """Test parallel connections to different symbols"""
        print(f"\n🔀 Testing parallel connections to multiple symbols")
        
        symbols = ["BTC", "ETH", "NVDA", "AAPL"]
        timeframe = "1D"
        
        tasks = []
        for symbol in symbols:
            task = asyncio.create_task(self.test_single_connection(symbol, timeframe))
            tasks.append(task)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        successful = sum(1 for r in results if isinstance(r, dict) and r.get("success"))
        print(f"📊 Parallel connections: {successful}/{len(symbols)} successful")
        
        return results

    async def run_all_tests(self):
        """Run all connection switching tests"""
        print("🚀 Starting Connection Switching Tests")
        print("=" * 60)
        
        all_results = {}
        
        # Test 1: Sequential timeframe switching
        print("\n1️⃣ Sequential Timeframe Switching")
        all_results["sequential"] = await self.test_timeframe_connection_switching("BTC")
        
        # Test 2: Rapid connection switching
        print("\n2️⃣ Rapid Connection Switching")
        all_results["rapid"] = await self.test_rapid_connection_switching("BTC")
        
        # Test 3: Connection stability
        print("\n3️⃣ Connection Stability Test")
        all_results["stability"] = await self.test_connection_stability("BTC", "1D", 15)
        
        # Test 4: Multiple symbols parallel
        print("\n4️⃣ Parallel Symbol Connections")
        all_results["parallel"] = await self.test_multiple_symbols_parallel()
        
        # Summary
        print("\n" + "=" * 60)
        print("📊 TEST SUMMARY")
        print("=" * 60)
        
        if "sequential" in all_results:
            seq_success = sum(1 for r in all_results["sequential"] if r.get("success"))
            seq_total = len(all_results["sequential"])
            print(f"Sequential switching: {seq_success}/{seq_total} passed")
        
        if "rapid" in all_results:
            rapid_success = sum(1 for r in all_results["rapid"] if isinstance(r, dict) and r.get("success"))
            rapid_total = len(all_results["rapid"])
            print(f"Rapid switching: {rapid_success}/{rapid_total} passed")
        
        if "stability" in all_results:
            stable = all_results["stability"].get("stable", False)
            print(f"Stability test: {'✅ PASSED' if stable else '❌ FAILED'}")
        
        if "parallel" in all_results:
            par_success = sum(1 for r in all_results["parallel"] if isinstance(r, dict) and r.get("success"))
            par_total = len(all_results["parallel"])
            print(f"Parallel connections: {par_success}/{par_total} passed")
        
        print("\n🏁 All connection tests completed!")
        return all_results

async def main():
    """Main test runner"""
    tester = ConnectionSwitchingTest()
    
    try:
        await tester.run_all_tests()
    except KeyboardInterrupt:
        print("\n🛑 Tests interrupted by user")
    except Exception as e:
        print(f"❌ Test runner failed: {e}")

if __name__ == "__main__":
    print("🔌 WebSocket Connection Switching Test Script")
    print("Testing connection open/close for timeframe switching")
    print("Production server: trading-production-85d8.up.railway.app")
    print("Press Ctrl+C to stop tests\n")
    
    asyncio.run(main())