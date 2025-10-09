#!/usr/bin/env python3
"""
Test script for WebSocket connection open/close cycles
"""
import asyncio
import websockets
import json
import time

class ConnectionOpenCloseTest:
    def __init__(self, base_url="wss://trading-production-85d8.up.railway.app"):
        self.base_url = base_url
        
    async def test_single_open_close(self, symbol, timeframe):
        """Test single connection open and close"""
        ws_url = f"{self.base_url}/ws/chart/{symbol}?timeframe={timeframe}"
        
        try:
            start_time = time.time()
            async with websockets.connect(ws_url) as websocket:
                connect_time = time.time() - start_time
                
                # Wait for initial data
                response = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                data = json.loads(response)
                
                return {
                    "success": True,
                    "connect_time": connect_time,
                    "data_received": data.get("type") == "chart_update"
                }
                
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def test_rapid_open_close(self, symbol="BTC", cycles=5):
        """Test rapid connection open/close cycles"""
        print(f"🔄 Testing {cycles} rapid open/close cycles for {symbol}")
        
        results = []
        for i in range(cycles):
            print(f"  Cycle {i+1}/{cycles}")
            result = await self.test_single_open_close(symbol, "1D")
            results.append(result)
            await asyncio.sleep(0.5)  # Brief pause between cycles
        
        successful = sum(1 for r in results if r.get("success"))
        print(f"✅ {successful}/{cycles} cycles successful")
        return results

    async def test_timeframe_connection_cycling(self, symbol="BTC"):
        """Test opening connections with different timeframes"""
        print(f"🔀 Testing timeframe connection cycling for {symbol}")
        
        timeframes = ["1h", "4H", "1D", "7D", "1W", "1M"]
        results = []
        
        for tf in timeframes:
            print(f"  Testing {tf}")
            result = await self.test_single_open_close(symbol, tf)
            result["timeframe"] = tf
            results.append(result)
            await asyncio.sleep(1)
        
        successful = sum(1 for r in results if r.get("success"))
        print(f"✅ {successful}/{len(timeframes)} timeframes successful")
        return results

    async def test_connection_stability(self, symbol="BTC", duration=10):
        """Test connection stability over time"""
        print(f"⏱️ Testing connection stability for {duration}s")
        
        ws_url = f"{self.base_url}/ws/chart/{symbol}?timeframe=1D"
        messages_received = 0
        
        try:
            async with websockets.connect(ws_url) as websocket:
                start_time = time.time()
                
                while (time.time() - start_time) < duration:
                    try:
                        await asyncio.wait_for(websocket.recv(), timeout=2.0)
                        messages_received += 1
                    except asyncio.TimeoutError:
                        continue
                
                return {
                    "success": True,
                    "duration": time.time() - start_time,
                    "messages": messages_received
                }
                
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def run_all_tests(self):
        """Run all connection tests"""
        print("🚀 Starting Connection Open/Close Tests")
        print("=" * 50)
        
        # Test 1: Rapid cycles
        print("\n1️⃣ Rapid Open/Close Cycles")
        await self.test_rapid_open_close("BTC", 3)
        
        # Test 2: Timeframe cycling
        print("\n2️⃣ Timeframe Connection Cycling")
        await self.test_timeframe_connection_cycling("BTC")
        
        # Test 3: Connection stability
        print("\n3️⃣ Connection Stability")
        stability_result = await self.test_connection_stability("BTC", 10)
        if stability_result.get("success"):
            print(f"✅ Stable for {stability_result['duration']:.1f}s, {stability_result['messages']} messages")
        else:
            print(f"❌ Stability test failed: {stability_result.get('error')}")
        
        print("\n🏁 Connection tests completed!")

async def main():
    tester = ConnectionOpenCloseTest()
    await tester.run_all_tests()

if __name__ == "__main__":
    print("🔌 WebSocket Connection Open/Close Test")
    print("Production server: trading-production-85d8.up.railway.app\n")
    asyncio.run(main())