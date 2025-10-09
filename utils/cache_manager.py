"""Centralized cache key management and Redis client"""
import os
import redis
import json
from dotenv import load_dotenv

load_dotenv()

class CacheManager:
    _redis_client = None
    
    @classmethod
    def get_redis_client(cls):
        if cls._redis_client is None:
            try:
                cls._redis_client = redis.Redis(
                    host=os.getenv('REDIS_HOST', 'localhost'),
                    port=int(os.getenv('REDIS_PORT', '6379')),
                    db=0,  # Railway uses single DB
                    password=os.getenv('REDIS_PASSWORD', None) if os.getenv('REDIS_PASSWORD') else None,
                    decode_responses=True,
                    socket_connect_timeout=10,
                    socket_timeout=10
                )
                cls._redis_client.ping()
            except Exception:
                cls._redis_client = None
        return cls._redis_client
    
    @classmethod
    def set_cache(cls, key, value, ttl=60):
        try:
            client = cls.get_redis_client()
            if client:
                client.setex(key, ttl, json.dumps(value, default=str))
        except Exception:
            pass
    
    @classmethod
    def get_cache(cls, key):
        try:
            client = cls.get_redis_client()
            if client:
                data = client.get(key)
                return json.loads(data) if data else None
        except Exception:
            pass
        return None

class CacheKeys:
    # Unified key patterns
    @staticmethod
    def price(symbol, asset_type):
        return f"price:{asset_type}:{symbol}"
    
    @staticmethod
    def prediction(symbol):
        return f"prediction:{symbol}"
    
    @staticmethod
    def market_summary(class_filter="all"):
        return f"market:{class_filter}"
    
    @staticmethod
    def chart_data(symbol, timeframe):
        return f"chart:{symbol}:{timeframe}"
    
    @staticmethod
    def websocket_history(symbol, timeframe):
        return f"ws_history:{symbol}:{timeframe}"