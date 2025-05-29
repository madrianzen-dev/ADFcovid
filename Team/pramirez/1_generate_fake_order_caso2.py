# Databricks notebook source
# MAGIC %pip install faker

# COMMAND ----------

from faker import Faker
import pandas as pd
import random
from datetime import datetime
import os

fake = Faker()
data = []

for i in range(100):
    data.append({
        "id_pedido": fake.uuid4(),
        "cliente": fake.name(),
        "producto": fake.word(),
        "cantidad": random.randint(1, 20),
        "precio_unitario": round(random.uniform(5, 150), 2),
        "fecha_pedido": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

pdf = pd.DataFrame(data)
fecha_actual = datetime.now().strftime("%Y%m%d_%H%M%S")
directory = "/dbfs/mnt/etl_pedidos/input"
path = f"{directory}/pedidos_prz_{fecha_actual}.csv"

# Create directory if it does not exist
os.makedirs(directory, exist_ok=True)

pdf.to_csv(path, index=False)