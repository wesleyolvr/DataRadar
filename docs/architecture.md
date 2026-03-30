# DevRadar — Arquitetura

## Visão Geral

O DevRadar implementa a Medallion Architecture para processar dados de comunidades tech do Reddit:

### Bronze (Ingestão)
- **Airflow** orquestra a extração via API pública do Reddit
- Posts e comentários são salvos como JSON no **disco local** e opcionalmente no **S3**
- Particionamento: `reddit/{subreddit}/date=YYYY-MM-DD/raw_{timestamp}.json`
- Cache de comentários evita re-extrair posts que não mudaram

### Silver (Processamento) — em desenvolvimento
- **Lambda** detecta novos arquivos no S3 e dispara jobs no **Databricks**
- Deduplicação cross-batch, schema validation, extração de ferramentas mencionadas

### Gold (Agregação) — em desenvolvimento
- Rankings de ferramentas, tendências temporais, análise de sentimento

### API + Dashboard
- **FastAPI** expõe os dados Bronze do disco local
- Frontend estático com explorador de posts, stats por subreddit
- Silver/Gold atualmente mockados com regex sobre dados Bronze

## Fluxo de Dados

1. Airflow (cron ou manual) → `extract_reddit.py` → API pública Reddit
2. Posts/comentários → JSON no disco → upload S3 (opcional)
3. S3 Event → Lambda → Databricks Job (Silver/Gold)
4. FastAPI lê JSONs do disco → API REST → Dashboard

## Componentes

### `airflow/scripts/extract_reddit.py`
Módulo core de extração. Funções puras que retornam dados em memória — sem I/O local.
Lida com paginação, rate-limiting (429), retry com backoff, deduplicação de posts e comentários.

### `airflow/dags/`
Três DAGs:
- `dag_reddit_ingestion.py` — versão original, subreddits fixos
- `dag_reddit_ingestion_local.py` — parametrizável, disco + S3 opcional
- `dag_reddit_scheduled.py` — execução horária automática

### `app/services/bronze_reader.py`
Lê JSONs Bronze do filesystem. Deduplica por ID mantendo o snapshot mais recente.
Suporta paginação e sorting.

### `app/services/mock_layers.py`
Simula Silver/Gold usando regex de ferramentas sobre dados Bronze.
Dicionário de ~35 ferramentas tech com aliases.

### `lambda/handler.py`
Lambda acionada por S3 Event. Filtra `raw_*.json`, valida path, dispara Databricks Jobs API.
