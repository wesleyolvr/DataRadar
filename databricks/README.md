# Databricks - Notebooks e SQL

Notebooks e scripts SQL para processamento de dados no Databricks.

## Estrutura

```
databricks/
├── notebooks/                    # Notebooks Python (formato fonte .py)
│   ├── medallion_pipeline.py    # Entrada do Job/Lambda — Bronze → Silver → Gold
│   ├── medallion_schemas.py     # Schemas StructType (posts / comments)
│   ├── medallion_helpers.py     # S3 (boto3 + secrets), parsing do path
│   └── medallion_transforms.py # Transformações Silver / Gold (DataFrames)
├── sql/
│   ├── create_gold_ai_insights.sql
│   ├── create_gold_ai_insights_v2.sql
│   └── query_examples.sql
└── README.md
```

O `medallion_pipeline.py` usa `%run` para carregar os outros três arquivos na **mesma pasta** do Repo (`./medallion_schemas`, etc.).

## Git Sync (Databricks Repos)

Este diretório pode ser sincronizado via **Repos**:

1. Databricks Workspace → Repos → Add Repo  
2. URL: `https://github.com/<usuario>/devradar` (ou o remoto do seu fork)  
3. Exemplo de path no workspace: `/Repos/wdodsg@gmail.com/DataRadar`

Após `git push` no GitHub, use **Pull** no Repos para atualizar o workspace.

## Tabelas e schemas

### Pipeline medallion (notebook)

Tabelas Delta no schema `default`, atualizadas pelo `medallion_pipeline.py`:

| Camada | Tabela |
|--------|--------|
| Bronze | `devradar_bronze_posts`, `devradar_bronze_comments` |
| Silver | `devradar_silver_posts`, `devradar_silver_comments` |
| Gold | `devradar_gold_subreddit_week`, `devradar_gold_top_commenters` |

**Bronze:** dados lidos do S3 (`s3://devradar-raw/…`) via boto3 + `dbutils.secrets` (escopo `aws_credentials`).

### SQL / LLM (scripts em `sql/`)

- `gold_ai_insights` — definida pelos scripts `create_gold_ai_insights*.sql`; insights gerados por LLM (fluxo separado, ex.: Airflow).

## Executar SQL

### Via Databricks SQL Warehouse (recomendado)

```bash
databricks sql -e "SELECT * FROM gold_ai_insights LIMIT 10"
```

No notebook:

```sql
SELECT * FROM gold_ai_insights;
```

### Via Workspace UI

1. SQL Editor → New Query  
2. Colar o conteúdo de `sql/create_gold_ai_insights.sql` (ou `_v2`)  
3. Run  

## Notebooks Python

Formato **Databricks notebook source** (células separadas por `# COMMAND ----------`; magics com `# MAGIC`).

Exemplo de composição (como no pipeline):

```python
# Databricks notebook source
# MAGIC %run ./medallion_schemas
# COMMAND ----------
from pyspark.sql import functions as F
# ...
```

**Parâmetro do Job/Lambda:** widget `arquivo_novo` — key S3 do arquivo de posts (`raw_*.json`), por exemplo  
`reddit/python/date=2026-03-29/raw_2026-03-29T01_00_50.json`.  
O notebook resolve comentários `comments_*.json` no mesmo diretório.

## Lambda e Databricks Job

O `lambda/handler.py` chama **Jobs Run Now** com `notebook_params.arquivo_novo`; o **caminho do notebook** fica na definição do Job no Databricks, não no Lambda.

Configure o task do Job para apontar para o notebook no Repo. Se o Repo for o projeto **devradar** inteiro (raiz com `databricks/`), o path costuma ser:

`/Repos/<email-ou-org>/<nome-do-repo>/databricks/notebooks/medallion_pipeline.py`

Se você só sincronizar uma subpasta como raiz do Repo, o prefixo antes de `notebooks/` muda — use o caminho exibido no Workspace (botão direito no arquivo → Copy path).

## Query na API (FastAPI)

Exemplo de consulta a insights (ver `app/routers/insights.py`):

```python
query = """
SELECT * FROM gold_ai_insights
WHERE subreddit = ? AND execution_date = CURRENT_DATE()
"""
```

## Workflow (visão geral)

```
S3 (raw) → Lambda → Databricks Job → medallion_pipeline.py
              │                              │
              │                              ├→ Bronze / Silver / Gold (devradar_*)
              │                              │
Airflow / outros ────────────────────────────┴→ gold_ai_insights (SQL + LLM)
```

## Referências

- [Databricks Repos](https://docs.databricks.com/repos/index.html)  
- [Delta Lake](https://docs.delta.io/)  
- [Databricks SQL](https://docs.databricks.com/sql/index.html)  
