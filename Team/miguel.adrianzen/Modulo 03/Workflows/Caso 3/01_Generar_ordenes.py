# Databricks notebook source
from faker import Faker
import pandas as pd
import random
from datetime import datetime
import os

fake = Faker()
Faker.seed(123)

# Simular 100 pedidos
data = []
for _ in range(100):
    data.append({
        "order_id": fake.uuid4(),
        "store": random.choice(["Lima", "Santiago", "Bogota", "Buenos Aires"]),
        "country": random.choice(["Peru", "Chile", "Colombia", "Argentina"]),
        "client_name": fake.name(),
        "product": random.choice(["TV", "Laptop", "Tablet", "Celular", "Consola"]),
        "quantity": random.randint(1, 10),
        "unit_price": round(random.uniform(100, 2000), 2),
        "order_date": fake.date_this_year().strftime("%Y-%m-%d")
    })

# Convertir a DataFrame
pdf = pd.DataFrame(data)

# Guardar como archivo CSV simulado
fecha_actual = datetime.now().strftime("%Y%m%d_%H%M%S")
path = f"/dbfs/mnt/ecommerce_etl/input/pedidos_{fecha_actual}.csv"
pdf.to_csv(path, index=False)

print(f" Archivo generado: {path}")
