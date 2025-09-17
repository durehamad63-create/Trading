#!/usr/bin/env python3
"""
Test startup script to verify fixes
"""
import sys
import os

# Add current directory to path
sys.path.append(os.path.dirname(__file__))

def test_imports():
    """Test all critical imports"""
    try:
        print("Testing imports...")
        
        # Test main components
        from modules.ml_predictor import MobileMLModel
        print("✓ ML Predictor imported")
        
        from utils.database_manager import DatabaseManager
        print("✓ Database Manager imported")
        
        from modules.api_routes import setup_routes
        print("✓ API Routes imported")
        
        from realtime_websocket_service import RealTimeWebSocketService
        print("✓ Realtime WebSocket Service imported")
        
        from stock_realtime_service import StockRealtimeService
        print("✓ Stock Realtime Service imported")
        
        print("✓ All imports successful!")
        return True
        
    except Exception as e:
        print(f"✗ Import failed: {e}")
        return False

def test_model_initialization():
    """Test model initialization"""
    try:
        print("\nTesting model initialization...")
        from modules.ml_predictor import MobileMLModel
        model = MobileMLModel()
        print("✓ Model initialized successfully")
        
        # Test a prediction
        prediction = model.predict('BTC')
        print(f"✓ Test prediction successful: {prediction['symbol']}")
        return True
        
    except Exception as e:
        print(f"✗ Model initialization failed: {e}")
        return False

def main():
    """Run all tests"""
    print("🚀 Testing Trading AI Platform startup...")
    
    success = True
    success &= test_imports()
    success &= test_model_initialization()
    
    if success:
        print("\n✅ All tests passed! The platform should start without blocking errors.")
        print("\nTo start the server, run:")
        print("python main.py")
    else:
        print("\n❌ Some tests failed. Check the errors above.")
    
    return success

if __name__ == "__main__":
    main()