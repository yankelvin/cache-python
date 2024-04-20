import time
import os

from custom_cache_redis import custom_cache

# Configure a variável de ambiente DEBUG para 'true' para usar o Redis, 'false' para usar o cache local
os.environ['COMPUTER'] = 'LOCAL'

# Configure a conexão com o servidor Redis
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0

# Decorar uma função com o decorator cache
@custom_cache(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, ttl=60)
def example_function(arg1, arg2):
    print("Executing example_function...")
    time.sleep(2)  # Simulando um processamento demorado
    return arg1 + arg2

# print(example_function(1, 2))  # Deve imprimir 3
# print(example_function(1, 2))  # Deve imprimir 3 (pegando do cache)

from datetime import datetime
@custom_cache(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, ttl=60)
def example_function2(agora: datetime, depois: datetime):
    print("Executing example_function...")
    time.sleep(2)  # Simulando um processamento demorado
    return agora.timestamp() + depois.timestamp()

# Teste da função decorada
agora = datetime.now()
depois = datetime.now()

# print(example_function2(agora, depois))  # Deve imprimir 3
# print(example_function2(agora, depois))  # Deve imprimir 3 (pegando do cache)

import pandas as pd
@custom_cache(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, ttl=60)
def example_function3(df1: pd.DataFrame, df2: pd.DataFrame):
    print("Executing example_function...")
    time.sleep(2)  # Simulando um processamento demorado
    return float(df1["patrimonio"].iloc[0] + df2["patrimonio"].iloc[0])

homem = {
    "nome": "Joao",
    "patrimonio": 40,
}

mulher = {
    "nome": "Maria",
    "patrimonio": 10,
}

df1 = pd.DataFrame([homem])
df2 = pd.DataFrame([mulher])
# print(example_function3(df1, df2))  # Deve imprimir 3
# print(example_function3(df1, df2))  # Deve imprimir 3 (pegando do cache)

@custom_cache(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, ttl=60)
def example_function4(df1: pd.DataFrame, df2: pd.DataFrame):
    print("Executing example_function...")
    time.sleep(2)  # Simulando um processamento demorado
    return pd.concat([df1, df2])

# print(example_function4(df1, df2))  # Deve imprimir 3
# print(example_function4(df1, df2))  # Deve imprimir 3 (pegando do cache)
