#!/usr/bin/env python3
"""
WebSocket connection fix for timeframe switching issues
"""

# Add this to your WebSocket endpoints to fix connection issues:

def fix_websocket_connection_state(websocket):
    """Check if WebSocket connection is still active"""
    try:
        if hasattr(websocket, 'client_state'):
            return websocket.client_state.name == 'CONNECTED'
        return True  # Assume connected if no state available
    except:
        return False

async def safe_websocket_send(websocket, data):
    """Safely send data through WebSocket with connection validation"""
    try:
        if fix_websocket_connection_state(websocket):
            await websocket.send_text(data)
            return True
        return False
    except Exception as e:
        print(f"WebSocket send failed: {e}")
        return False

# Key fixes needed:
# 1. Always check connection state before sending
# 2. Properly cleanup connections on timeframe change
# 3. Remove dead connections from active_connections dict
# 4. Cancel background tasks properly

print("WebSocket fixes ready - apply these patterns to your WebSocket endpoints")