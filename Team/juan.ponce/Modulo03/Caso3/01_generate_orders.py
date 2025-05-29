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
        "order_id": fake.uuid4(),
        "store": fake.name(),
        "country": fake.word(),
        "client_name": fake.name(),
        "product": fake.word(),
        "quantity": random.randint(1, 20),
        "unit_price": round(random.uniform(5, 150), 2),
        "order_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    })

pdf = pd.DataFrame(data)
fecha_actual = datetime.now().strftime("%Y%m%d_%H%M%S")
directory = "/dbfs/mnt/etl_pedidos_jp3/input"
path = f"{directory}/pedidos_{fecha_actual}.csv"

# Create directory if it does not exist
os.makedirs(directory, exist_ok=True)

pdf.to_csv(path, index=False)