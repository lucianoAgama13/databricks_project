# Databricks notebook source
# notebooks/gold/03_aggregate_metrics.py
# ============================================================
# CAPA GOLD — Métricas de negocio para Databricks Dashboard
# ============================================================

import sys
sys.path.insert(0, "/Workspace/Repos/<tu-usuario>/databricks_project")

from config.settings import GOLD_PATH, DATABASE, WRITE_FORMAT
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime

spark = SparkSession.builder.getOrCreate()

# Leer Silver
df_ecom = spark.table(f"{DATABASE}.silver_ecommerce")
df_orders = spark.table(f"{DATABASE}.silver_orders")
df_products = spark.table(f"{DATABASE}.silver_products")
df_op = spark.table(f"{DATABASE}.silver_order_products")

# ── Helper: guardar Gold ─────────────────────────────────────
def save_gold(df, table_name, partition_col=None):
    path = f"{GOLD_PATH}{table_name}/"
    writer = df.write.format(WRITE_FORMAT).mode("overwrite").option("overwriteSchema","true")
    if partition_col and partition_col in df.columns:
        writer = writer.partitionBy(partition_col)
    writer.save(path)
    spark.sql(f"DROP TABLE IF EXISTS {DATABASE}.{table_name}")
    spark.sql(f"CREATE TABLE {DATABASE}.{table_name} USING DELTA LOCATION '{path}'")
    count = spark.table(f"{DATABASE}.{table_name}").count()
    print(f"✅ {table_name:40s} → {count:>8,} filas")

# ══════════════════════════════════════════════════════════════
# GOLD 1: Revenue por Categoría (E-Commerce)
# ══════════════════════════════════════════════════════════════
gold_cat = (
    df_ecom
    .groupBy("category")
    .agg(
        F.round(F.sum("revenue"),         2).alias("total_revenue"),
        F.sum("units_sold")                .alias("total_units"),
        F.count("*")                       .alias("num_days"),
        F.round(F.avg("price"),           2).alias("avg_price"),
        F.round(F.avg("discount"),        2).alias("avg_discount"),
        F.round(F.sum("marketing_spend"), 2).alias("total_marketing")
    )
    .withColumn("revenue_per_marketing",
        F.round(F.col("total_revenue") / F.when(F.col("total_marketing")>0, F.col("total_marketing")).otherwise(F.lit(1)), 4)
    )
    .orderBy(F.desc("total_revenue"))
)
save_gold(gold_cat, "gold_revenue_por_categoria")

# ══════════════════════════════════════════════════════════════
# GOLD 2: Tendencia Mensual de Revenue (E-Commerce)
# ══════════════════════════════════════════════════════════════
gold_trend = (
    df_ecom
    .groupBy("order_year", "order_month", "order_quarter")
    .agg(
        F.round(F.sum("revenue"),  2).alias("total_revenue"),
        F.sum("units_sold")         .alias("total_units"),
        F.count("*")                .alias("num_registros"),
        F.round(F.avg("price"),    2).alias("avg_price")
    )
    .withColumn("periodo",
        F.concat(F.col("order_year"), F.lit("-"),
                 F.lpad(F.col("order_month").cast("string"), 2, "0"))
    )
    .orderBy("order_year", "order_month")
)
save_gold(gold_trend, "gold_tendencia_mensual", partition_col="order_year")

# ══════════════════════════════════════════════════════════════
# GOLD 3: Revenue por Segmento de Cliente (E-Commerce)
# ══════════════════════════════════════════════════════════════
gold_seg = (
    df_ecom
    .groupBy("customer_segment")
    .agg(
        F.round(F.sum("revenue"),  2).alias("total_revenue"),
        F.round(F.avg("revenue"),  2).alias("avg_revenue_diario"),
        F.sum("units_sold")         .alias("total_units"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.count("*")                .alias("num_registros")
    )
    .orderBy(F.desc("total_revenue"))
)
save_gold(gold_seg, "gold_revenue_por_segmento")

# ══════════════════════════════════════════════════════════════
# GOLD 4: Top Productos más comprados (Instacart)
# ══════════════════════════════════════════════════════════════
gold_top_products = (
    df_op
    .join(df_products.select("product_id","product_name","aisle","department"), "product_id", "left")
    .groupBy("product_id", "product_name", "aisle", "department")
    .agg(
        F.count("*")               .alias("total_compras"),
        F.sum("reordered")         .alias("total_reorders"),
        F.round(
            F.sum("reordered") / F.count("*"), 4
        ).alias("reorder_rate")
    )
    .orderBy(F.desc("total_compras"))
    .limit(100)
)
save_gold(gold_top_products, "gold_top_productos_instacart")

# ══════════════════════════════════════════════════════════════
# GOLD 5: Ventas por Departamento (Instacart)
# ══════════════════════════════════════════════════════════════
gold_depts = (
    df_op
    .join(df_products.select("product_id","department","aisle"), "product_id", "left")
    .groupBy("department")
    .agg(
        F.count("*")       .alias("total_items_vendidos"),
        F.sum("reordered") .alias("total_reorders"),
        F.countDistinct("product_id").alias("productos_unicos"),
        F.countDistinct("order_id")  .alias("ordenes_con_dept"),
        F.round(F.sum("reordered")/F.count("*"), 4).alias("reorder_rate")
    )
    .orderBy(F.desc("total_items_vendidos"))
)
save_gold(gold_depts, "gold_ventas_por_departamento")

# ══════════════════════════════════════════════════════════════
# GOLD 6: Comportamiento de compra por día y hora (Instacart)
# ══════════════════════════════════════════════════════════════
gold_behavior = (
    df_orders
    .groupBy("day_name", "order_dow", "time_of_day")
    .agg(
        F.count("*")                       .alias("num_ordenes"),
        F.round(F.avg("days_since_prior_order"), 2).alias("avg_dias_entre_compras")
    )
    .orderBy("order_dow")
)
save_gold(gold_behavior, "gold_comportamiento_compra")

# ══════════════════════════════════════════════════════════════
# GOLD 7: KPIs Globales (resumen ejecutivo)
# ══════════════════════════════════════════════════════════════
kpi_ecom = df_ecom.agg(
    F.round(F.sum("revenue"),  2).alias("total_revenue"),
    F.sum("units_sold")         .alias("total_units"),
    F.round(F.avg("price"),    2).alias("avg_price"),
    F.round(F.avg("discount"), 2).alias("avg_discount"),
    F.countDistinct("category").alias("num_categorias"),
    F.count("*")                .alias("num_registros")
).withColumn("dataset", F.lit("ecommerce"))

kpi_instacart = df_op.agg(
    F.count("*")                      .alias("total_items_comprados"),
    F.countDistinct("order_id")        .alias("total_ordenes"),
    F.countDistinct("product_id")      .alias("productos_unicos"),
    F.round(F.avg("reordered")*100, 2).alias("pct_reorden")
).withColumn("dataset", F.lit("instacart"))

save_gold(kpi_ecom.unionByName(kpi_instacart, allowMissingColumns=True), "gold_kpis_globales")

# ── Resumen Final ────────────────────────────────────────────
print("\n" + "="*58)
print("📋 RESUMEN GOLD LAYER")
print("="*58)
tablas = [
    "gold_revenue_por_categoria",
    "gold_tendencia_mensual",
    "gold_revenue_por_segmento",
    "gold_top_productos_instacart",
    "gold_ventas_por_departamento",
    "gold_comportamiento_compra",
    "gold_kpis_globales"
]
for t in tablas:
    try:
        n = spark.table(f"{DATABASE}.{t}").count()
        print(f"  ✅ {t:40s}: {n:>6,} filas")
    except Exception as e:
        print(f"  ⚠️  {t}: {e}")

print(f"\n  ⏰ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("="*58)
print("\n🎯 Listo para Databricks Dashboard.")
print(f"   SELECT * FROM {DATABASE}.<tabla_gold>")
