# DevRadar — Setup Completo

## 1. Variáveis de Ambiente

Copie o template e preencha:

```bash
cp .env.example .env
```

| Variável | Descrição | Obrigatória |
|----------|-----------|:-----------:|
| `AWS_ACCESS_KEY_ID` | Chave de acesso AWS | Só para S3 |
| `AWS_SECRET_ACCESS_KEY` | Secret key AWS | Só para S3 |
| `AWS_DEFAULT_REGION` | Região AWS | Só para S3 |
| `DEVRADAR_S3_BUCKET` | Nome do bucket S3 | Só para S3 |

## 2. Airflow (Docker)

```bash
cd airflow
docker compose up -d
```

- Acesse: http://localhost:8080
- Login: `admin` / `admin`
- DAGs disponíveis:
  - `devradar_reddit_ingestion_local` — trigger manual, parametrizável
  - `devradar_reddit_scheduled` — execução horária automática

## 3. API + Dashboard

```bash
cd app
pip install -r requirements.txt
uvicorn main:app --reload --port 8000
```

- API: http://localhost:8000/api/health
- Dashboard: http://localhost:8000

## 4. Testes

```bash
pip install pytest ruff
pytest tests/ -v
```

## 5. Lint

```bash
ruff check .
```

## 6. Utilitários

```bash
# Trigger manual da DAG
python scripts/trigger_dag.py dataengineering python rust

# Teste de upload S3 (requer variáveis de ambiente configuradas)
python scripts/test_s3_upload.py
```
