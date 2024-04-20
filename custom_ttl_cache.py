import tempfile
from time import time, sleep
from datetime import datetime, date

from cachetools.func import ttl_cache
from diskcache import Cache

# def custom_ttl_cache(ttl=600):
#     tmp_dir = tempfile.mkdtemp()
#     cache = Cache(tmp_dir)
    
#     def decorator(func):
#         def wrapper(*args, **kwargs):
#             key = (func.__name__, args, frozenset(kwargs.items()))
#             cached_value, stored_time = cache.get(key, (None, None))
            
#             if stored_time and time() - stored_time > ttl:
#                 del cache[key]
#                 cached_value = None
            
#             if cached_value is None:
#                 result = func(*args, **kwargs)
#                 cache[key] = (result, time())
#                 return result
#             else:
#                 return cached_value
        
#         return wrapper
    
#     return decorator

def custom_ttl_cache(ttl=600):
    tmp_dir = tempfile.mkdtemp()
    cache = Cache(tmp_dir)
    
    def decorator(func):
        def wrapper(*args, **kwargs):
            key = (func.__name__, args, frozenset(kwargs.items()))
            
            value_time = cache.get(key)
            
            if value_time is not None:
                value, stored_time = value_time
                
                if time() - stored_time <= ttl:
                    return value
            
            result = func(*args, **kwargs)
            cache[key] = (result, time())
            return result
        
        return wrapper
    
    return decorator


@ttl_cache(ttl=600)
def obter_agora():
    sleep(5)
    agora = datetime.now()
    return agora

@ttl_cache(ttl=600)
def obter_diferenca(date: date):
    sleep(5)
    diferenca = datetime.now() - date
    return diferenca

class Teste():
    @custom_ttl_cache()
    def obter_agora(self):
        sleep(5)
        agora = datetime.now()
        return agora

    @custom_ttl_cache(ttl=600)
    def obter_diferenca(self, date: date):
        sleep(5)
        diferenca = datetime.now() - date
        return diferenca

inicio = datetime.now()
teste = Teste()
for i in range(0, 15):
    print(Teste().obter_diferenca(inicio))
