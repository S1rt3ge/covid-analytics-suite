from functools import lru_cache
from datetime import datetime, timedelta

cache = {}
cache_timeout = {}

def simple_cache(timeout_minutes=5):
    def decorator(func):
        def wrapper(*args, **kwargs):
            key = str(args) + str(kwargs)
            now = datetime.now()
            
            if key in cache and key in cache_timeout:
                if now < cache_timeout[key]:
                    return cache[key]
            
            result = func(*args, **kwargs)
            cache[key] = result
            cache_timeout[key] = now + timedelta(minutes=timeout_minutes)
            return result
        return wrapper
    return decorator