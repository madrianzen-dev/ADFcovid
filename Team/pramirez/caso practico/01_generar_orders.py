# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

from faker import Faker
import pandas as pd
import random
from datetime import datetime
import os

# Inicializar Faker
fake = Faker()

# Crear directorio si no existe
input_path = '/dbfs/mnt/ecommerce_etl/input'
os.makedirs(input_path, exist_ok=True)

# Parámetros para generación
products = ['TV', 'Laptop', 'Tablet', 'Smartphone', 'Monitor']
stores = ['Lima', 'Santiago', 'Bogota', 'Buenos Aires']
countries = ['Peru', 'Chile', 'Colombia', 'Argentina']

# Función para generar un archivo de pedidos
def generate_orders_csv(file_num):
    data = []
    for _ in range(random.randint(5, 10)):
        order = {
            'order_id': f"A{random.randint(1000, 9999)}",
            'store': random.choice(stores),
            'country': random.choice(countries),
            'client_name': fake.name(),
            'product': random.choice(products),
            'quantity': random.randint(1, 5),
            'unit_price': round(random.uniform(100.0, 2000.0), 2),
            'order_date': datetime.today().strftime('%Y-%m-%d')
        }
        data.append(order)
    
    df = pd.DataFrame(data)
    file_date = datetime.today().strftime('%Y%m%d')
    file_name = f"{input_path}/pedidos_{file_date}_{file_num}.csv"
    df.to_csv(file_name, index=False)
    print(f"Archivo guardado: {file_name}")

# Generar 2 a 3 archivos CSV
for i in range(random.randint(2, 3)):
    generate_orders_csv(i+1)
