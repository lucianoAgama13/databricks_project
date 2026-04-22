# 🏆 Trabajo Final — Ingeniería de Datos con Databricks

Pipeline ETL con arquitectura Medallion en Azure Databricks.

## 📊 Datasets
- **E-Commerce Sales Prediction** (Kaggle)
- **Supermarket / Superstore Bundle** (Kaggle)

## 🏗️ Arquitectura Medallion

```
[Azure Data Lake Gen2 - Raw]
        ↓  (Managed Identity)
   🥉 Bronze  →  Ingesta raw sin transformación
        ↓
   🥈 Silver  →  Limpieza, tipado, deduplicación
        ↓
   🥇 Gold    →  Agregaciones y métricas de negocio
        ↓
[Databricks Dashboard]
```

## 📁 Estructura del Repositorio

```
databricks_project/
├── notebooks/
│   ├── bronze/
│   │   └── 01_ingest_raw.py
│   ├── silver/
│   │   └── 02_clean_transform.py
│   └── gold/
│       └── 03_aggregate_metrics.py
├── config/
│   └── settings.py
├── .github/
│   └── workflows/
│       └── databricks_cicd.yml
└── README.md
```

## 🚀 Setup — Paso a Paso

### 1. Azure: Crear recursos

1. **Resource Group**: `rg-databricks-final`
2. **Azure Data Lake Storage Gen2**:
   - Nombre: `sadatabricksfinal` (único globalmente)
   - Habilitar: *Hierarchical Namespace*
   - Crear contenedores: `raw`, `bronze`, `silver`, `gold`
3. **Azure Databricks Workspace**:
   - Nombre: `adb-workspace-final`
   - Tier: `Premium` (requerido para Unity Catalog y Managed Identity)

### 2. Managed Identity

1. En el Databricks Workspace → **Access Connector for Azure Databricks**:
   - Crear recurso: `databricks-access-connector`
2. En el Storage Account → **IAM** → Agregar rol:
   - Role: `Storage Blob Data Contributor`
   - Assignee: el Access Connector creado

### 3. Databricks: Configurar External Location

En Databricks → **Data** → **External Locations** → New:
```
Credential: usar el Access Connector
URL: abfss://raw@sadatabricksfinal.dfs.core.windows.net/
```

### 4. Databricks: Crear Cluster

- Runtime: `15.4 LTS (Spark 3.5, Scala 2.12)`
- Mode: `Single Node` (para desarrollo/curso)
- Configuración extra (Environment Variables del cluster):
  ```
  STORAGE_ACCOUNT=sadatabricksfinal
  ```

### 5. Subir Datasets

Descargar de Kaggle y subir a ADLS Gen2 contenedor `raw/`:
- `raw/ecommerce/ecommerce_sales.csv`
- `raw/superstore/superstore.csv`

### 6. GitHub Actions — Secrets

En tu repo GitHub → Settings → Secrets → Actions:
```
DATABRICKS_HOST       = https://<tu-workspace>.azuredatabricks.net
DATABRICKS_TOKEN      = <tu-personal-access-token>
DATABRICKS_CLUSTER_ID = <cluster-id>
```

## 📈 Métricas Gold (Visualización)

El notebook Gold genera tablas listas para Databricks Dashboard:
- `gold.ventas_por_categoria` — Revenue por categoría de producto
- `gold.top_regiones` — Top regiones por volumen de ventas
- `gold.tendencia_mensual` — Tendencia de ventas mensual
- `gold.ticket_promedio` — Ticket promedio por segmento de cliente
