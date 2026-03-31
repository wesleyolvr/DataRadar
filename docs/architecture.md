# DataRadar — Arquitetura

## Diagrama

![Diagrama de arquitetura](assets/arquitetura_dataradar.png)

*(Versão editável: export do Excalidraw em `docs/assets/`; ver também o diagrama em texto no [README](../README.md#arquitetura).)*

## Visão geral

O DataRadar segue a **Medallion Architecture** (Bronze → Silver → Gold) sobre dados públicos do Reddit, com **insights semânticos** gerados por LLM a partir da camada Silver.

### Bronze (ingestão)

- **Apache Airflow** orquestra a extração (`extract_reddit.py`) contra a API pública do Reddit.
- **Pool `reddit_api`** e backoff limitam concorrência e erros 429.
- JSON bruto particionado no **AWS S3** (`reddit/{subreddit}/date=.../raw_*.json`).

### Silver / Gold (processamento)

- **AWS Lambda** reage a novos objetos no S3 e dispara o job no **Databricks** (`run-now`).
- **PySpark + Delta Lake**: tabelas Silver (posts/comentários limpos) e Gold (agregações por subreddit/semana, top commenters, etc.), com **MERGE** (regra “novo vence”).

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
| Lambda S3 → Databricks | `lambda/handler.py` |
| Pipeline medallion (referência) | `databricks/jobs/medallion_pipeline.py` |
| API e dashboard | `app/` |
| Insights batch | `scripts/generate_insights.py` |
