#!/usr/bin/env python3
"""
Simple WebSocket timeframe test
"""
import asyncio
import websockets
import json

async def simple_test():
    uri = "wss://trading-production-85d8.up.railway.app/ws/chart/BTC"
    
    async with websockets.connect(uri) as ws:
        print("✅ Connected")
        
        # Test timeframe changes
        timeframes = ["1H", "4H", "1D"]
        
        for tf in timeframes:
            print(f"🔄 Switching to {tf}")
            await ws.send(json.dumps({"type": "change_timeframe", "timeframe": tf}))
            
            # Get response
            msg = await ws.recv()
            data = json.loads(msg)
            current_tf = data.get("timeframe", "?")
            print(f"📊 Response: {current_tf}")
            
            await asyncio.sleep(3)

if __name__ == "__main__":
    asyncio.run(simple_test())