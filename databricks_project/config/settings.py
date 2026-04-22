# config/settings.py
# ============================================================
# Configuración centralizada del proyecto
# ============================================================

import os

# ── Azure Storage ────────────────────────────────────────────
STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT", "sadatabricksfinal")

def abfss(container: str, path: str = "") -> str:
    base = f"abfss://{container}@{STORAGE_ACCOUNT}.dfs.core.windows.net"
    return f"{base}/{path}" if path else base

# ── Rutas Raw ────────────────────────────────────────────────
RAW_ECOMMERCE        = abfss("raw", "ecommerce/Ecommerce_Sales_Prediction_Dataset.csv")
RAW_ORDERS           = abfss("raw", "instacart/orders.csv")
RAW_PRODUCTS         = abfss("raw", "instacart/products.csv")
RAW_ORDER_PRODUCTS   = abfss("raw", "instacart/order_products__prior.csv")
RAW_AISLES           = abfss("raw", "instacart/aisles.csv")
RAW_DEPARTMENTS      = abfss("raw", "instacart/departments.csv")

# ── Rutas Bronze ─────────────────────────────────────────────
BRONZE_ECOMMERCE      = abfss("bronze", "ecommerce/")
BRONZE_ORDERS         = abfss("bronze", "instacart/orders/")
BRONZE_PRODUCTS       = abfss("bronze", "instacart/products/")
BRONZE_ORDER_PRODUCTS = abfss("bronze", "instacart/order_products/")
BRONZE_AISLES         = abfss("bronze", "instacart/aisles/")
BRONZE_DEPARTMENTS    = abfss("bronze", "instacart/departments/")

# ── Rutas Silver ─────────────────────────────────────────────
SILVER_ECOMMERCE      = abfss("silver", "ecommerce/")
SILVER_INSTACART      = abfss("silver", "instacart/")

# ── Rutas Gold ───────────────────────────────────────────────
GOLD_PATH = abfss("gold", "")

# ── Base de datos ────────────────────────────────────────────
DATABASE     = "medallion_db"
WRITE_FORMAT = "delta"
