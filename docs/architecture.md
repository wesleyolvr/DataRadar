# DataRadar — Arquitetura

## Diagrama

![Diagrama de arquitetura](assets/arquitetura_dataradar.png)

*Fonte Mermaid: [`assets/arquitetura_dataradar.mmd`](assets/arquitetura_dataradar.mmd). Regenerar PNG: [`assets/DIAGRAMAS.md`](assets/DIAGRAMAS.md). Diagrama em texto no [README](../README.md#arquitetura).*

## Visão geral

O DataRadar segue a **Medallion Architecture** (Bronze → Silver → Gold) sobre dados públicos do Reddit, com **insights semânticos** gerados por LLM a partir da camada Silver.

### Bronze (ingestão)

- **Apache Airflow** orquestra a extração (`extract_reddit.py`) contra a API pública do Reddit.
- **Pool `reddit_api`** e backoff limitam concorrência e erros 429.
- JSON bruto particionado no **AWS S3** (`reddit/{subreddit}/date=.../raw_*.json`).
- **Cache e otimização:** decisões de produto (posts/comentários, `num_comments`, contrato S3) estão em [spec: Reddit extraction cache](superpowers/specs/2026-03-31-reddit-extraction-cache-design.md).

### Silver / Gold (processamento)

- **AWS Lambda** reage a novos objetos no S3 e dispara o **Job** no Databricks (`run-now`), passando o parâmetro de notebook `arquivo_novo` (key do objeto `raw_*.json` no S3).
- **PySpark + Delta Lake** (notebooks em `databricks/notebooks/`): o notebook **`medallion_pipeline.py`** é o ponto de entrada do Job; ele carrega `medallion_schemas`, `medallion_helpers` e `medallion_transforms` via `%run`. Tabelas no schema `default` (`devradar_bronze_*`, `devradar_silver_*`, `devradar_gold_*`) com **MERGE** (regra “novo vence”).
- Detalhes de paths no workspace, secrets (`aws_credentials`) e SQL auxiliar: [databricks/README.md](../databricks/README.md).

### Serving e insights

- **Databricks SQL Warehouse**: o **FastAPI** usa o conector SQL para Silver/Gold em tempo real no dashboard local.
- **Insights IA**: script `scripts/generate_insights.py` lê Silver via SQL, chama **Groq** (Llama 3.1), grava `app/static/data.json`; o endpoint de pipeline e o deploy estático (ex.: **Vercel**) consomem esse JSON.
- **Frontend**: HTML/CSS/JS em `app/static/`.

## Fluxo resumido

1. Airflow (agendado ou manual) → Reddit API → S3 Bronze.
2. S3 → Lambda → job Databricks → Silver/Gold (Delta).
3. FastAPI ← SQL Warehouse (dados tabulares); `data.json` ← batch Groq (insights).
4. Dashboard ← FastAPI e/ou snapshot estático.

## Componentes principais

| Área | Onde está |
|------|-----------|
| DAGs e extração | `airflow/dags/`, `airflow/scripts/extract_reddit.py` |
| Lambda S3 → Databricks Job | `lambda/handler.py` (parâmetro `arquivo_novo`; path do notebook fica na definição do Job) |
| Pipeline medallion (entrada do Job) | `databricks/notebooks/medallion_pipeline.py` (+ `medallion_schemas.py`, `medallion_helpers.py`, `medallion_transforms.py`) |
| SQL Databricks (ex.: `gold_ai_insights`) | `databricks/sql/` |
| API e dashboard | `app/` |
| Insights batch | `scripts/generate_insights.py` |
