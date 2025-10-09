"""
Rate Limiting Middleware
"""
import time
from collections import defaultdict
from fastapi import HTTPException, Request
import asyncio

class RateLimiter:
    def __init__(self):
        self.requests = defaultdict(list)
        self.limits = {
            'default': (100, 60),  # 100 requests per minute
            'websocket': (1000, 60),  # 1000 messages per minute
            'export': (10, 60)  # 10 exports per minute
        }
    
    async def check_rate_limit(self, request: Request, limit_type='default'):
        client_ip = request.client.host
        now = time.time()
        limit, window = self.limits[limit_type]
        
        # Clean old requests
        self.requests[client_ip] = [
            req_time for req_time in self.requests[client_ip] 
            if now - req_time < window
        ]
        
        # Check limit
        if len(self.requests[client_ip]) >= limit:
            raise HTTPException(status_code=429, detail="Rate limit exceeded")
        
        # Add current request
        self.requests[client_ip].append(now)

rate_limiter = RateLimiter()