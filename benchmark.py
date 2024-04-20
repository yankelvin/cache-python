import timeit
import tempfile
from time import time
from diskcache import Cache

def custom_ttl_cache2(ttl=600):
    tmp_dir = tempfile.mkdtemp()
    cache = Cache(tmp_dir)
    
    def decorator(func):
        def wrapper(query):
            key = (func.__name__, query)
            
            value_time = cache.get(key)
            
            if value_time is not None:
                value, stored_time = value_time
                
                if time() - stored_time <= ttl:
                    return value
            
            result = func(query)
            cache[key] = (result, time())
            return result
        
        return wrapper
    
    return decorator

# Função de simulação de consulta
@custom_ttl_cache2()
def query_simulation(query):
    # Simula um processamento simples de consulta
    prefix = "Resultado para: "
    return prefix + query

# Benchmark
queries = ["query1", "query2", "query3", "query4", "query5"]

for query in queries:
    print(f"Tempo de execução para a query '{query}':", 
          timeit.timeit(lambda: query_simulation(query), number=100))
