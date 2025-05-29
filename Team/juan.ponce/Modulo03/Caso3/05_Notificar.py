# Databricks notebook source
import json
import requests

gold_path = "dbfs:/mnt/ecommerce_etl/gold/"
df_gold = spark.read.format("delta").load(gold_path)

# Obtener resumen
df_gold.createOrReplaceTempView("resumen")
resumen = spark.sql("SELECT SUM(num_pedidos) as total_pedidos, SUM(total_ventas) as total_ventas FROM resumen").collect()[0]

mensaje = f"""
*Resumen Diario del ETL de Pedidos*:

Pedidos procesados: {resumen['total_pedidos']}
Total Ventas: ${resumen['total_ventas']:.2f}
"""

# Webhook de Teams (opcional)                  Poner link del Teams
webhook_url = dbutils.secrets.get("webhooks", "teams_url")  

payload = {"text": mensaje}

response = requests.post(
    webhook_url,
    data=json.dumps(payload),
    headers={'Content-Type': 'application/json'}
)

if response.status_code == 200:
    print("Mensaje enviado a Teams.")
else:
    print(f"Error al enviar: {response.status_code} - {response.text}")
