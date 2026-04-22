# Databricks notebook source
# notebooks/bronze/01_ingest_raw.py
# ============================================================
# CAPA BRONZE — Ingesta desde Azure Data Lake Gen2 (Raw)
# Datasets: E-Commerce Sales + Instacart Market Basket
# ============================================================

import sys
sys.path.insert(0, "/Workspace/Repos/<tu-usuario>/databricks_project")

from config.settings import (
    RAW_ECOMMERCE, RAW_ORDERS, RAW_PRODUCTS,
    RAW_ORDER_PRODUCTS, RAW_AISLES, RAW_DEPARTMENTS,
    BRONZE_ECOMMERCE, BRONZE_ORDERS, BRONZE_PRODUCTS,
    BRONZE_ORDER_PRODUCTS, BRONZE_AISLES, BRONZE_DEPARTMENTS,
    DATABASE, WRITE_FORMAT
)
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, input_file_name
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# Crear base de datos
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
print(f"✅ Database '{DATABASE}' lista.")

# ── Función genérica de ingesta Bronze ──────────────────────
def ingest_to_bronze(source_path, bronze_path, table_name):
    print(f"\n📥 Leyendo: {source_path}")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(source_path)
    )
    df = (
        df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
        .withColumn("_layer", lit("bronze"))
    )
    count = df.count()
    print(f"   Registros: {count:,} | Columnas: {df.columns}")
    (
        df.write
        .format(WRITE_FORMAT)
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(bronze_path)
    )
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DATABASE}.{table_name}
        USING DELTA LOCATION '{bronze_path}'
    """)
    print(f"✅ '{table_name}' guardado en Bronze.")
    return count

# ── Ingesta E-Commerce Sales ─────────────────────────────────
# Columnas: Date, Product_Category, Price, Discount,
#           Customer_Segment, Marketing_Spend, Units_Sold
count_ecom = ingest_to_bronze(RAW_ECOMMERCE, BRONZE_ECOMMERCE, "bronze_ecommerce")

# ── Ingesta Instacart Market Basket ──────────────────────────
# orders: order_id, user_id, eval_set, order_number,
#         order_dow, order_hour_of_day, days_since_prior_order
count_orders = ingest_to_bronze(RAW_ORDERS, BRONZE_ORDERS, "bronze_orders")

# products: product_id, product_name, aisle_id, department_id
count_products = ingest_to_bronze(RAW_PRODUCTS, BRONZE_PRODUCTS, "bronze_products")

# order_products: order_id, product_id, add_to_cart_order, reordered
count_op = ingest_to_bronze(RAW_ORDER_PRODUCTS, BRONZE_ORDER_PRODUCTS, "bronze_order_products")

# aisles: aisle_id, aisle
count_aisles = ingest_to_bronze(RAW_AISLES, BRONZE_AISLES, "bronze_aisles")

# departments: department_id, department
count_depts = ingest_to_bronze(RAW_DEPARTMENTS, BRONZE_DEPARTMENTS, "bronze_departments")

# ── Resumen ──────────────────────────────────────────────────
print("\n" + "="*55)
print("📋 RESUMEN BRONZE LAYER")
print("="*55)
print(f"  🛒 E-Commerce Sales   : {count_ecom:>10,} registros")
print(f"  📦 Orders             : {count_orders:>10,} registros")
print(f"  🏷️  Products           : {count_products:>10,} registros")
print(f"  🧾 Order Products     : {count_op:>10,} registros")
print(f"  🗂️  Aisles             : {count_aisles:>10,} registros")
print(f"  🏢 Departments        : {count_depts:>10,} registros")
print(f"  ⏰ Timestamp          : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*55)

spark.table(f"{DATABASE}.bronze_ecommerce").limit(3).display()
spark.table(f"{DATABASE}.bronze_orders").limit(3).display()
spark.table(f"{DATABASE}.bronze_products").limit(3).display()
