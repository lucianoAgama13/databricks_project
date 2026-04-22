# Databricks notebook source
# notebooks/silver/02_clean_transform.py
# ============================================================
# CAPA SILVER — Limpieza, tipado y transformaciones
# Datasets: E-Commerce Sales + Instacart Market Basket
# ============================================================

import sys
sys.path.insert(0, "/Workspace/Repos/<tu-usuario>/databricks_project")

from config.settings import (
    SILVER_ECOMMERCE, SILVER_INSTACART, DATABASE, WRITE_FORMAT
)
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# ── Helper: guardar Silver ───────────────────────────────────
def save_silver(df, path, table_name):
    (
        df.write
        .format(WRITE_FORMAT)
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(path)
    )
    spark.sql(f"CREATE TABLE IF NOT EXISTS {DATABASE}.{table_name} USING DELTA LOCATION '{path}'")
    count = df.count()
    print(f"✅ {table_name:35s} → {count:>8,} registros")
    return count

# ══════════════════════════════════════════════════════════════
# SILVER 1: E-Commerce Sales
# Raw cols: Date, Product_Category, Price, Discount,
#           Customer_Segment, Marketing_Spend, Units_Sold
# ══════════════════════════════════════════════════════════════
print("🔄 Procesando E-Commerce Sales...")

df_ecom = spark.table(f"{DATABASE}.bronze_ecommerce").drop(
    "_ingestion_timestamp", "_source_file", "_layer"
)

# Renombrar a snake_case
df_ecom = (
    df_ecom
    .withColumnRenamed("Date",             "order_date")
    .withColumnRenamed("Product_Category", "category")
    .withColumnRenamed("Price",            "price")
    .withColumnRenamed("Discount",         "discount")
    .withColumnRenamed("Customer_Segment", "customer_segment")
    .withColumnRenamed("Marketing_Spend",  "marketing_spend")
    .withColumnRenamed("Units_Sold",       "units_sold")
)

# Castear tipos
df_ecom = (
    df_ecom
    .withColumn("price",          F.col("price").cast(DoubleType()))
    .withColumn("discount",       F.col("discount").cast(DoubleType()))
    .withColumn("marketing_spend",F.col("marketing_spend").cast(DoubleType()))
    .withColumn("units_sold",     F.col("units_sold").cast(IntegerType()))
)

# Parsear fecha (formato dd-MM-yyyy)
df_ecom = df_ecom.withColumn(
    "order_date",
    F.coalesce(
        F.to_date(F.col("order_date"), "dd-MM-yyyy"),
        F.to_date(F.col("order_date"), "MM/dd/yyyy"),
        F.to_date(F.col("order_date"), "yyyy-MM-dd")
    )
)

# Columnas derivadas de fecha
df_ecom = (
    df_ecom
    .withColumn("order_year",    F.year("order_date"))
    .withColumn("order_month",   F.month("order_date"))
    .withColumn("order_quarter", F.quarter("order_date"))
)

# Revenue neto = price * units_sold * (1 - discount/100)
df_ecom = df_ecom.withColumn(
    "revenue",
    F.round(F.col("price") * F.col("units_sold") * (1 - F.col("discount") / 100), 2)
)

# Normalizar texto
df_ecom = (
    df_ecom
    .withColumn("category",         F.initcap(F.trim(F.col("category"))))
    .withColumn("customer_segment", F.initcap(F.trim(F.col("customer_segment"))))
)

# Eliminar nulos críticos y duplicados
df_ecom = df_ecom.dropna(subset=["order_date", "category", "price", "units_sold"])
df_ecom = df_ecom.dropDuplicates()
df_ecom = df_ecom.withColumn("_layer", F.lit("silver")).withColumn("_silver_ts", F.current_timestamp())

save_silver(df_ecom, SILVER_ECOMMERCE, "silver_ecommerce")

# ══════════════════════════════════════════════════════════════
# SILVER 2: Instacart — Orders
# Raw cols: order_id, user_id, eval_set, order_number,
#           order_dow, order_hour_of_day, days_since_prior_order
# ══════════════════════════════════════════════════════════════
print("\n🔄 Procesando Instacart Orders...")

df_orders = spark.table(f"{DATABASE}.bronze_orders").drop(
    "_ingestion_timestamp", "_source_file", "_layer"
)

df_orders = (
    df_orders
    .withColumn("order_id",                F.col("order_id").cast(IntegerType()))
    .withColumn("user_id",                 F.col("user_id").cast(IntegerType()))
    .withColumn("order_number",            F.col("order_number").cast(IntegerType()))
    .withColumn("order_dow",               F.col("order_dow").cast(IntegerType()))
    .withColumn("order_hour_of_day",       F.col("order_hour_of_day").cast(IntegerType()))
    .withColumn("days_since_prior_order",  F.col("days_since_prior_order").cast(DoubleType()))
)

# Nombre del día
day_names = {0:"Sunday",1:"Monday",2:"Tuesday",3:"Wednesday",4:"Thursday",5:"Friday",6:"Saturday"}
df_orders = df_orders.withColumn(
    "day_name",
    F.when(F.col("order_dow")==0,"Sunday")
     .when(F.col("order_dow")==1,"Monday")
     .when(F.col("order_dow")==2,"Tuesday")
     .when(F.col("order_dow")==3,"Wednesday")
     .when(F.col("order_dow")==4,"Thursday")
     .when(F.col("order_dow")==5,"Friday")
     .otherwise("Saturday")
)

# Franja horaria
df_orders = df_orders.withColumn(
    "time_of_day",
    F.when(F.col("order_hour_of_day").between(6,11),  "Morning")
     .when(F.col("order_hour_of_day").between(12,17), "Afternoon")
     .when(F.col("order_hour_of_day").between(18,21), "Evening")
     .otherwise("Night")
)

# Solo usar eval_set = 'prior' (histórico)
df_orders = df_orders.filter(F.col("eval_set") == "prior")
df_orders = df_orders.dropna(subset=["order_id","user_id"])
df_orders = df_orders.dropDuplicates(["order_id"])
df_orders = df_orders.withColumn("_layer", F.lit("silver")).withColumn("_silver_ts", F.current_timestamp())

path_orders = SILVER_INSTACART + "orders/"
save_silver(df_orders, path_orders, "silver_orders")

# ══════════════════════════════════════════════════════════════
# SILVER 3: Instacart — Products enriquecido con aisle y department
# ══════════════════════════════════════════════════════════════
print("\n🔄 Procesando Instacart Products...")

df_products = spark.table(f"{DATABASE}.bronze_products").drop(
    "_ingestion_timestamp", "_source_file", "_layer"
)
df_aisles = spark.table(f"{DATABASE}.bronze_aisles").drop(
    "_ingestion_timestamp", "_source_file", "_layer"
)
df_depts = spark.table(f"{DATABASE}.bronze_departments").drop(
    "_ingestion_timestamp", "_source_file", "_layer"
)

# Castear IDs
for col in ["product_id", "aisle_id", "department_id"]:
    df_products = df_products.withColumn(col, F.col(col).cast(IntegerType()))
df_aisles   = df_aisles.withColumn("aisle_id",       F.col("aisle_id").cast(IntegerType()))
df_depts    = df_depts.withColumn("department_id",   F.col("department_id").cast(IntegerType()))

# Join para enriquecer productos
df_products_enriched = (
    df_products
    .join(df_aisles, "aisle_id", "left")
    .join(df_depts,  "department_id", "left")
    .withColumn("product_name", F.initcap(F.trim(F.col("product_name"))))
    .dropna(subset=["product_id", "product_name"])
    .dropDuplicates(["product_id"])
)
df_products_enriched = df_products_enriched.withColumn("_layer", F.lit("silver")).withColumn("_silver_ts", F.current_timestamp())

path_products = SILVER_INSTACART + "products/"
save_silver(df_products_enriched, path_products, "silver_products")

# ══════════════════════════════════════════════════════════════
# SILVER 4: Order Products (líneas de pedido)
# ══════════════════════════════════════════════════════════════
print("\n🔄 Procesando Order Products...")

df_op = spark.table(f"{DATABASE}.bronze_order_products").drop(
    "_ingestion_timestamp", "_source_file", "_layer"
)

df_op = (
    df_op
    .withColumn("order_id",          F.col("order_id").cast(IntegerType()))
    .withColumn("product_id",        F.col("product_id").cast(IntegerType()))
    .withColumn("add_to_cart_order", F.col("add_to_cart_order").cast(IntegerType()))
    .withColumn("reordered",         F.col("reordered").cast(IntegerType()))
    .dropna(subset=["order_id","product_id"])
    .dropDuplicates(["order_id","product_id"])
)
df_op = df_op.withColumn("_layer", F.lit("silver")).withColumn("_silver_ts", F.current_timestamp())

path_op = SILVER_INSTACART + "order_products/"
save_silver(df_op, path_op, "silver_order_products")

# ── Resumen ──────────────────────────────────────────────────
print("\n" + "="*55)
print("📋 RESUMEN SILVER LAYER")
print("="*55)
print(f"  ⏰ Timestamp : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*55)

spark.table(f"{DATABASE}.silver_ecommerce").printSchema()
