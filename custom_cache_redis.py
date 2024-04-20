import os
import time
import json
from datetime import datetime

import redis
import pandas as pd
from cachetools import TTLCache, cached

def default_serializer(obj):
    if isinstance(obj, datetime):
        return {"__datetime__": True, "as_str": obj.isoformat()}
    if isinstance(obj, pd.DataFrame):
        obj_reset = obj.reset_index(drop=True)
        return {"__DataFrame__": True, "as_dict": obj_reset.to_dict(orient='split')}
    raise TypeError("Object of type '%s' is not serializable" % type(obj).__name__)

def custom_decoder(obj):
    if "__datetime__" in obj:
        return datetime.strptime(obj["as_str"], "%Y-%m-%dT%H:%M:%S.%f")
    if "__DataFrame__" in obj:
        df_data = obj["as_dict"]
        df = pd.DataFrame(df_data["data"], columns=df_data["columns"], index=df_data["index"])
        return df
    return obj

def acquire_lock_redis(redis_conn, lock_key, ttl):
    lock_acquired = redis_conn.setnx(lock_key, "locked")
    if lock_acquired:
        redis_conn.expire(lock_key, ttl)
        return True
    return False

def release_lock_redis(redis_conn, lock_key):
    redis_conn.delete(lock_key)

def read_from_redis(redis_conn, key):
    cached_result = redis_conn.get(key)
    if cached_result is not None:
        return json.loads(cached_result.decode('utf-8'), object_hook=custom_decoder)
    return None

def write_to_redis(redis_conn, key, data, ttl):
    serialized_data = json.dumps(data, default=default_serializer)
    redis_conn.setex(key, ttl, serialized_data)
    
def hashable_key(args, kwargs):
    new_args = tuple(
        arg.to_json(orient='split') if isinstance(arg, pd.DataFrame) else arg
        for arg in args
    )
    new_kwargs = {
        key: value.to_json(orient='split') if isinstance(value, pd.DataFrame) else value
        for key, value in kwargs.items()
    }
    return new_args, frozenset(new_kwargs.items())

def custom_cache(host='localhost', port=6379, db=0, ttl=600):
    use_redis = os.environ["COMPUTER"] == "AWS"
    
    if use_redis:
        redis_conn = redis.StrictRedis(host=host, port=port, db=db)
        
        def decorator(func):
            def wrapper(*args, **kwargs):
                key = f"{func.__name__}-{json.dumps(args, default=default_serializer)}-{json.dumps(kwargs, default=default_serializer)}"
                lock_key = f"{key}-lock"
                result = None
                
                for attempt in range(3):
                    if acquire_lock_redis(redis_conn, lock_key, ttl):
                        try:
                            result = read_from_redis(redis_conn, key)
                            if result is not None:
                                break
                            
                            result = func(*args, **kwargs)
                            write_to_redis(redis_conn, key, result, ttl)
                        except Exception as ex:
                            print(ex)
                            raise ex
                        finally:
                            release_lock_redis(redis_conn, lock_key)
                            break
                        
                    time.sleep(2 * attempt)
                
                return result
            return wrapper
    else:
        cache_local = TTLCache(maxsize=100, ttl=ttl)
        
        def decorator(func):
            @cached(cache_local, key=lambda *args, **kwargs: hashable_key(args, kwargs))
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            
            return wrapper

    return decorator
