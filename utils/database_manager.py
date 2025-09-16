"""Centralized database connection manager"""
import os
from dotenv import load_dotenv
from database import TradingDatabase

load_dotenv()

class DatabaseManager:
    _instance = None
    
    @classmethod
    def get_instance(cls):
        """Get singleton database instance"""
        if not cls._instance:
            database_url = os.getenv('DATABASE_URL', 'postgresql://postgres:admin123@localhost:5432/trading_db')
            cls._instance = TradingDatabase(database_url)
        return cls._instance
    
    @classmethod
    async def initialize(cls):
        """Initialize database connection"""
        instance = cls.get_instance()
        await instance.connect()
        return instance