# Databricks - Notebooks e SQL

Notebooks e scripts SQL para processamento de dados no Databricks.

## 📁 Estrutura

```
databricks/
├── notebooks/          # Notebooks Python (formato .py)
│   ├── bronze_to_silver.py
│   ├── silver_to_gold.py
│   └── utils.py
├── sql/                # Scripts SQL
│   ├── create_gold_ai_insights.sql
│   └── query_examples.sql
└── README.md
```

## 🔗 Git Sync (Databricks Repos)

Este diretório está conectado ao Databricks via **Repos UI**:

1. Databricks Workspace → Repos → Add Repo
2. URL: `https://github.com/seu-usuario/devradar`
3. Path no workspace: `/Repos/wdodsg@gmail.com/DataRadar`

Quando você faz push no GitHub, pode fazer pull no Databricks Repos para atualizar.

## 📊 Tabelas e Schemas

### Bronze (Raw Data)
- Dados brutos do S3 (`s3://devradar-raw/{env}/reddit/`)
- Lidos diretamente via boto3 ou spark.read.json

### Silver (Cleaned)
- `devradar_silver_posts`: posts limpos e deduplicated
- `devradar_silver_comments`: comentários processados

### Gold (Analytics)
- `gold_ai_insights`: insights gerados por LLM ⬅️ **NOVO!**
- `gold_subreddit_stats`: estatísticas agregadas por subreddit

## 🚀 Executar SQL

### Via Databricks SQL Warehouse (recomendado)
```bash
# Via CLI
databricks sql -e "SELECT * FROM gold_ai_insights LIMIT 10"

# Via notebook
%sql
SELECT * FROM gold_ai_insights;
```

### Via Workspace UI
1. SQL Editor → New Query
2. Copiar conteúdo de `sql/create_gold_ai_insights.sql`
3. Run

## 📝 Notebooks Python

Notebooks no formato `.py` (Databricks source format):

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze to Silver Pipeline

# COMMAND ----------

import pyspark.sql.functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# Read Bronze data
df = spark.read.json("s3://devradar-raw/prod/reddit/...")
```

## 🔧 Lambda Trigger

Lambda executa notebooks via path no Repos:

```python
# lambda/handler.py
notebook_path = "/Repos/wdodsg@gmail.com/DataRadar/notebooks/bronze_to_silver.py"
```

## 📊 Query API Endpoint

FastAPI consulta `gold_ai_insights`:

```python
# app/routers/insights.py
query = """
SELECT * FROM gold_ai_insights
WHERE subreddit = ? AND execution_date = CURRENT_DATE()
"""
```

## 🔄 Workflow

```
S3 Event → Lambda → Databricks Job → Notebook Path
                                       ↓
                              /Repos/.../notebooks/bronze_to_silver.py
                                       ↓
                              Silver Tables (Delta)
                                       ↓
                      (Airflow task: generate_insights.py)
                                       ↓
                              gold_ai_insights (Delta)
```

## 📚 Referências

- [Databricks Repos](https://docs.databricks.com/repos/index.html)
- [Delta Lake](https://docs.delta.io/)
- [Databricks SQL](https://docs.databricks.com/sql/index.html)
