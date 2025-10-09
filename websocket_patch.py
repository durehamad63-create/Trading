"""
Apply this patch to fix WebSocket timeframe switching issues
"""

# Add this function to modules/api_routes.py before WebSocket endpoints:

async def validate_websocket_connection(websocket):
    """Check if WebSocket is still connected"""
    try:
        if hasattr(websocket, 'client_state'):
            return websocket.client_state.name == 'CONNECTED'
        # Fallback: try sending a ping
        await websocket.send_text('{"type":"ping"}')
        return True
    except:
        return False

# Replace all websocket.send_text() calls with:
# if await validate_websocket_connection(websocket):
#     await websocket.send_text(message)
# else:
#     # Remove connection and break loop
#     connection_active = False

print("Apply this pattern to fix 'unexpectedly closed' errors")