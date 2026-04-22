# docs/setup_azure.md
# ============================================================
# GUÍA COMPLETA DE CONFIGURACIÓN — Azure + Databricks
# ============================================================

## PASO 1: Crear Resource Group

1. Ir a **portal.azure.com**
2. Buscar "Resource Groups" → **+ Create**
3. Configurar:
   - Subscription: tu suscripción
   - Resource group: `rg-databricks-final`
   - Region: `East US 2` (o la más cercana)
4. **Review + Create** → **Create**

---

## PASO 2: Crear Azure Data Lake Storage Gen2

1. Buscar "Storage accounts" → **+ Create**
2. Configurar:
   - Resource group: `rg-databricks-final`
   - Storage account name: `sadatabricksfinal` *(debe ser único globalmente)*
   - Region: misma que el Resource Group
   - Performance: `Standard`
   - Redundancy: `LRS` (suficiente para el curso)
3. Tab **Advanced**:
   - ✅ Enable hierarchical namespace ← **MUY IMPORTANTE para ADLS Gen2**
4. **Review + Create** → **Create**

### Crear contenedores en el Storage:
1. Ir al Storage Account creado
2. En el menú izquierdo → **Containers** → **+ Container**
3. Crear los siguientes 4 contenedores:
   - `raw`
   - `bronze`
   - `silver`
   - `gold`

---

## PASO 3: Crear Azure Databricks Workspace

1. Buscar "Azure Databricks" → **+ Create**
2. Configurar:
   - Resource group: `rg-databricks-final`
   - Workspace name: `adb-workspace-final`
   - Region: misma región
   - Pricing Tier: `Premium` ← **Requerido para Managed Identity**
3. **Review + Create** → **Create**
4. Una vez creado → **Launch Workspace**

---

## PASO 4: Configurar Access Connector (Managed Identity)

Este paso permite que Databricks acceda al Storage **sin credenciales hardcodeadas**.

1. En Azure Portal, buscar **"Access Connector for Azure Databricks"** → **+ Create**
2. Configurar:
   - Resource group: `rg-databricks-final`
   - Name: `databricks-access-connector`
   - Region: misma región
3. **Review + Create** → **Create**

### Asignar permisos al Access Connector:
1. Ir a tu **Storage Account** (`sadatabricksfinal`)
2. Menú izquierdo → **Access Control (IAM)** → **+ Add role assignment**
3. Role: `Storage Blob Data Contributor`
4. Assign access to: `Managed identity`
5. Select members: buscar `databricks-access-connector`
6. **Save**

---

## PASO 5: Configurar External Location en Databricks

1. En Databricks Workspace → **Data** (ícono izquierdo) → **External Data** → **Storage Credentials** → **Create credential**
2. Credential type: `Azure Managed Identity`
3. Access connector resource ID: copiar el Resource ID del Access Connector (lo encuentras en Azure Portal → Access Connector → Properties → Resource ID)
4. Luego → **External Locations** → **Create location**:
   - Name: `adls_raw`
   - URL: `abfss://raw@sadatabricksfinal.dfs.core.windows.net/`
   - Storage credential: la que creaste arriba
5. Repetir para `bronze`, `silver`, `gold`

---

## PASO 6: Crear Cluster en Databricks

1. En Databricks → **Compute** → **Create compute**
2. Configurar:
   - Cluster name: `cluster-etl-final`
   - Policy: `Unrestricted`
   - Single node: ✅ (para el curso es suficiente)
   - Databricks runtime: `15.4 LTS (Spark 3.5.0, Scala 2.12)`
   - Node type: `Standard_DS3_v2` (o el más económico disponible)
3. **Advanced options** → **Spark** → **Environment variables**:
   ```
   STORAGE_ACCOUNT=sadatabricksfinal
   ```
4. **Create compute**
5. Anotar el **Cluster ID** (lo necesitarás para GitHub Actions):
   - Ir al cluster → URL del navegador contiene el ID: `.../clusters/<CLUSTER_ID>`

---

## PASO 7: Subir Datasets a ADLS Gen2

### Descargar de Kaggle:
1. Ir a los links del trabajo:
   - https://www.kaggle.com/datasets/nevildhinoja/e-commerce-sales-prediction-dataset
   - https://www.kaggle.com/datasets/amunsentom/supermarket-superstore-dataset-bundle
2. Descargar los CSV

### Subir al Storage:
**Opción A — Azure Portal:**
1. Storage Account → Containers → `raw`
2. Crear carpeta `ecommerce/` → subir CSV de e-commerce
3. Crear carpeta `superstore/` → subir CSV de superstore

**Opción B — Azure Storage Explorer (recomendado):**
1. Descargar Azure Storage Explorer
2. Conectar con tu cuenta
3. Navegar a `raw/` y subir los archivos

---

## PASO 8: Configurar GitHub Actions

### Crear repositorio:
1. Ir a github.com → **New repository**
2. Name: `databricks_project`
3. Visibility: **Public** ← requerido por el trabajo
4. **Create repository**

### Subir código:
```bash
git clone https://github.com/<tu-usuario>/databricks_project
# Copiar todos los archivos del proyecto a la carpeta clonada
git add .
git commit -m "Initial commit - ETL Medallion project"
git push origin main
```

### Configurar Secrets:
1. En GitHub → tu repo → **Settings** → **Secrets and variables** → **Actions**
2. **New repository secret** para cada uno:

| Secret Name | Valor |
|---|---|
| `DATABRICKS_HOST` | `https://<tu-workspace>.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | Token de Databricks (ver abajo) |
| `DATABRICKS_CLUSTER_ID` | ID del cluster creado |

### Generar Databricks Personal Access Token:
1. En Databricks → clic en tu usuario (arriba derecha) → **Settings**
2. **Developer** → **Access tokens** → **Generate new token**
3. Copiar el token (solo se muestra una vez)

---

## PASO 9: Vincular Repo con Databricks (Repos)

1. En Databricks → **Repos** (ícono izquierdo)
2. **Add Repo** → pegar URL de tu repo GitHub
3. Esto permite que los notebooks importen `config/settings.py`

---

## PASO 10: Ejecutar y verificar

1. Ejecutar manualmente desde GitHub: **Actions** → **Databricks ETL Pipeline** → **Run workflow**
2. O ejecutar notebooks directamente en Databricks en orden:
   1. `01_ingest_raw.py`
   2. `02_clean_transform.py`
   3. `03_aggregate_metrics.py`

---

## Crear el Dashboard en Databricks

1. En Databricks → **Dashboards** → **Create Dashboard**
2. Agregar widgets con estas queries SQL:

```sql
-- KPIs globales
SELECT dataset, total_revenue, total_profit, total_transactions, avg_ticket
FROM medallion_db.gold_kpis_globales;

-- Ventas por categoría
SELECT category, source, total_sales, total_profit, profit_margin_pct
FROM medallion_db.gold_ventas_categoria
ORDER BY total_sales DESC;

-- Tendencia mensual
SELECT periodo, source, total_sales, total_profit
FROM medallion_db.gold_tendencia_mensual
ORDER BY periodo;

-- Top regiones
SELECT region, source, total_sales, num_orders
FROM medallion_db.gold_top_regiones
ORDER BY total_sales DESC
LIMIT 10;
```
