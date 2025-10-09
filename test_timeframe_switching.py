#!/usr/bin/env python3
"""
Test script for WebSocket timeframe switching
"""
import asyncio
import websockets
import json
import time

async def test_timeframe_switching():
    """Test timeframe switching on chart WebSocket"""
    
    # Test configuration
    BASE_URL = "wss://trading-production-85d8.up.railway.app"
    SYMBOL = "BTC"
    TIMEFRAMES = ["1H", "4H", "1D", "7D", "1W"]
    
    uri = f"{BASE_URL}/ws/chart/{SYMBOL}"
    
    print(f"🧪 Testing timeframe switching for {SYMBOL}")
    print(f"🔗 Connecting to: {uri}")
    
    try:
        async with websockets.connect(uri, close_timeout=10) as websocket:
            print("✅ WebSocket connected")
            
            # Listen for initial messages
            message_count = 0
            
            for timeframe in TIMEFRAMES:
                print(f"\n🔄 Switching to timeframe: {timeframe}")
                
                # Send timeframe change message
                change_message = {
                    "type": "change_timeframe",
                    "timeframe": timeframe
                }
                await websocket.send(json.dumps(change_message))
                print(f"📤 Sent timeframe change: {timeframe}")
                
                # Listen for responses for 10 seconds
                start_time = time.time()
                responses_received = 0
                
                while time.time() - start_time < 10:
                    try:
                        # Wait for message with timeout
                        message = await asyncio.wait_for(websocket.recv(), timeout=2.0)
                        data = json.loads(message)
                        
                        if data.get("type") == "chart_update":
                            responses_received += 1
                            current_tf = data.get("timeframe", "unknown")
                            past_count = len(data.get("chart", {}).get("past", []))
                            future_count = len(data.get("chart", {}).get("future", []))
                            
                            print(f"📊 Response #{responses_received}: TF={current_tf}, Past={past_count}, Future={future_count}")
                            
                            # Verify timeframe matches
                            if current_tf == timeframe:
                                print(f"✅ Timeframe correctly switched to {timeframe}")
                            else:
                                print(f"❌ Timeframe mismatch: expected {timeframe}, got {current_tf}")
                        
                        message_count += 1
                        
                    except asyncio.TimeoutError:
                        print("⏰ No message received (timeout)")
                        break
                    except json.JSONDecodeError:
                        print("❌ Invalid JSON received")
                    except Exception as e:
                        print(f"❌ Error receiving message: {e}")
                        break
                
                print(f"📈 Received {responses_received} chart updates for {timeframe}")
                
                # Wait before next timeframe
                await asyncio.sleep(2)
            
            print(f"\n🎯 Test completed! Total messages: {message_count}")
            
            # Send proper close frame
            await websocket.close(code=1000, reason="Test completed")
            
    except websockets.exceptions.ConnectionClosed as e:
        print(f"❌ Connection closed: {e}")
    except Exception as e:
        print(f"❌ Connection error: {e}")
    finally:
        print("🔄 Connection cleanup completed")

async def test_multiple_connections():
    """Test multiple simultaneous connections with different timeframes"""
    
    BASE_URL = "wss://trading-production-85d8.up.railway.app"
    SYMBOLS = ["BTC", "ETH"]
    TIMEFRAMES = ["1H", "1D"]
    
    print(f"\n🔀 Testing multiple connections...")
    
    async def single_connection_test(symbol, timeframe):
        uri = f"{BASE_URL}/ws/chart/{symbol}"
        websocket = None
        try:
            websocket = await websockets.connect(uri, close_timeout=5)
            print(f"✅ Connected: {symbol} ({timeframe})")
            
            # Change timeframe
            change_message = {
                "type": "change_timeframe", 
                "timeframe": timeframe
            }
            await websocket.send(json.dumps(change_message))
            
            # Listen for 5 seconds
            for _ in range(3):
                try:
                    message = await asyncio.wait_for(websocket.recv(), timeout=2.0)
                    data = json.loads(message)
                    if data.get("type") == "chart_update":
                        tf = data.get("timeframe", "?")
                        print(f"📊 {symbol}: {tf}")
                except asyncio.TimeoutError:
                    break
                    
        except Exception as e:
            print(f"❌ {symbol} error: {e}")
        finally:
            if websocket:
                try:
                    await websocket.close(code=1000, reason="Test completed")
                except:
                    pass
    
    # Run multiple connections simultaneously
    tasks = []
    for symbol in SYMBOLS:
        for timeframe in TIMEFRAMES:
            tasks.append(single_connection_test(symbol, timeframe))
    
    await asyncio.gather(*tasks, return_exceptions=True)
    print("🎯 Multiple connection test completed!")

if __name__ == "__main__":
    print("🚀 Starting WebSocket timeframe switching tests...")
    
    # Run single connection test
    asyncio.run(test_timeframe_switching())
    
    # Run multiple connection test
    asyncio.run(test_multiple_connections())
    
    print("✅ All tests completed!")