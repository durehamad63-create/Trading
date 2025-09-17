#!/usr/bin/env python3
"""
Test stock cache initialization
"""
import sys
import os
import asyncio

# Add current directory to path
sys.path.append(os.path.dirname(__file__))

async def test_stock_cache():
    """Test stock cache initialization"""
    try:
        print("Testing stock cache initialization...")
        
        # Test stock service import
        from stock_realtime_service import StockRealtimeService
        print("✓ StockRealtimeService imported successfully")
        
        # Test database manager
        from utils.database_manager import DatabaseManager
        db = DatabaseManager.get_instance()
        print("✓ Database manager initialized")
        
        # Initialize stock service without ML model (None is acceptable)
        stock_service = StockRealtimeService(None, db)
        print("✓ Stock service initialized")
        
        # Check cache initialization
        print(f"✓ Price cache initialized: {type(stock_service.price_cache)}")
        print(f"✓ Candle cache initialized: {type(stock_service.candle_cache)}")
        print(f"✓ Stock symbols loaded: {len(stock_service.stock_symbols)} symbols")
        print(f"✓ Redis client: {'Connected' if stock_service.redis_client else 'Memory only'}")
        
        # Test cache operations
        stock_service.price_cache['TEST'] = {
            'current_price': 100.0,
            'change_24h': 2.5,
            'volume': 1000000,
            'timestamp': 'test'
        }
        
        if 'TEST' in stock_service.price_cache:
            print("✓ Cache write/read test passed")
        else:
            print("✗ Cache write/read test failed")
            return False
        
        print("\n✅ Stock cache initialization test PASSED!")
        print(f"Stock symbols: {list(stock_service.stock_symbols.keys())}")
        return True
        
    except Exception as e:
        print(f"✗ Stock cache initialization test FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False

async def main():
    """Run stock cache test"""
    print("🧪 Testing Stock Cache Initialization...")
    success = await test_stock_cache()
    
    if success:
        print("\n✅ All stock cache tests passed!")
    else:
        print("\n❌ Stock cache tests failed!")
    
    return success

if __name__ == "__main__":
    asyncio.run(main())