# 🚀 Plano de Automação Completa — DataRadar

> **Objetivo:** Transformar projeto de portfólio manual em pipeline 100% automatizado  
> **Custo:** $0/mês (free tiers)  
> **Status:** 🟡 Em Implementação  
> **Última atualização:** 2026-04-13

---

## 📊 Resumo de Decisões

| # | Área | Decisão | Ferramenta/Estratégia |
|---|------|---------|----------------------|
| 1 | Orquestração | Airflow em produção 24/7 | **Astronomer free tier** |
| 2 | AI Insights | Automação geração LLM | **Task no Airflow** (fim da DAG) |
| 3 | API/Frontend | Deploy produção | **Vercel** (frontend) + **Render** (API) |
| 3B | Storage Insights | Substituir data.json | **Tabela `gold_ai_insights`** (Databricks) |
| 4 | IaC | Infraestrutura como código | **Terraform** |
| 5 | CI/CD | Estratégia deploy | **Workflows separados + path filters** |
| 6 | Observability | Monitoramento/alertas | **Grafana Cloud free tier** |
| 7 | Databricks | Deploy notebooks | **Databricks Repos** (Git sync UI) |
| 8 | Ambientes | Dev/Staging/Prod | **Namespace em recursos** (dev/prod) |
| 9 | Secrets | Gestão centralizada | **AWS SSM Parameter Store + GitHub Secrets** |
| 10 | Custos | Otimização sustentável | DAG **6h**, insights **1x/dia**, S3 lifecycle |
| 11 | Docs | Para recrutadores | **ADRs + deployment guide + demo live** |

---

## 🎯 Arquitetura Final (Totalmente Automatizada)

### Antes (Estado Atual - Manual)
```
┌─────────────────────────────────────────────────────┐
│              PROCESSO MANUAL                         │
└─────────────────────────────────────────────────────┘

1. Você sobe Docker Compose (Airflow local)
2. Você roda script: python scripts/generate_insights.py
3. Você roda API: uvicorn main:app
4. Dashboard só funciona em localhost
5. Infra criada via console AWS (não reproduzível)
6. Zero monitoramento
```

### Depois (Automatizado - Meta)
```
┌─────────────────────────────────────────────────────┐
│         PIPELINE TOTALMENTE AUTOMATIZADO             │
└─────────────────────────────────────────────────────┘

GitHub (push) → CI/CD → Deploy automático todos componentes
                   ↓
    ┌──────────────┴──────────────┐
    │   Astronomer (Airflow 24/7) │ ← Schedule: 6h
    └──────────────┬──────────────┘
                   ↓
    Reddit → S3 Bronze → Lambda → Databricks (Silver/Gold)
                                       ↓
                            gold_ai_insights (Delta Table)
                                       ↓
    ┌──────────────────────────────────────────┐
    │  Render (API) ← Databricks SQL Warehouse │
    └────────────┬─────────────────────────────┘
                 ↓
    Vercel (Dashboard) ← API → Usuários
                 ↓
    Grafana Cloud (Monitoring 24/7)
```

---

## 📦 Fases de Implementação

### 🟢 Fase 1: Infraestrutura (Terraform)
**Duração:** ~2h  
**Ação Humana:** ⚠️ Configurar credenciais AWS localmente  
**Status:** 🔴 Pendente

**Componentes:**
- [ ] S3 bucket (namespace dev/prod)
- [ ] Lambda function + IAM roles
- [ ] S3 event notifications
- [ ] SSM Parameter Store (secrets)
- [ ] Terraform state (S3 backend)

**Outputs:**
- Bucket: `devradar-raw`
- Lambda: `devradar-s3-trigger-prod`
- SSM paths: `/devradar/prod/*`

---

### 🟢 Fase 2: Databricks - Camada Gold AI
**Duração:** ~1h  
**Ação Humana:** ⚠️ Executar SQL no Databricks workspace  
**Status:** 🔴 Pendente

**Componentes:**
- [ ] Criar tabela `gold_ai_insights` (Delta)
- [ ] Testar INSERT/MERGE
- [ ] Conectar notebooks via Repos

**SQL:**
```sql
CREATE TABLE gold_ai_insights (
  subreddit STRING,
  insight_type STRING,
  item_name STRING,
  mentions INT,
  context STRING,
  generated_at TIMESTAMP,
  execution_date DATE
) USING DELTA
PARTITIONED BY (execution_date)
LOCATION 's3://devradar-raw/gold/ai_insights/';
```

---

### 🟢 Fase 3: Airflow → Astronomer
**Duração:** ~3h  
**Ação Humana:** ⚠️ Criar conta Astronomer + conectar GitHub  
**Status:** 🔴 Pendente

**Componentes:**
- [ ] Criar workspace Astronomer
- [ ] Configurar `Dockerfile` Astronomer-compatible
- [ ] Refactor DAG: schedule 6h (era 2h30)
- [ ] Nova task: `generate_insights` → escreve em `gold_ai_insights`
- [ ] Deploy via CLI: `astro deploy`

**Mudanças código:**
```python
# airflow/dags/dag_reddit_scheduled.py
schedule=timedelta(hours=6)  # era hours=2, minutes=30

# Nova task ao final
@task
def generate_insights(**context):
    # Lê Silver via Databricks SQL
    # Chama Groq LLM
    # INSERT INTO gold_ai_insights
    pass
```

---

### 🟢 Fase 4: API Refactor + Deploy Render
**Duração:** ~2h  
**Ação Humana:** ⚠️ Criar conta Render + conectar GitHub  
**Status:** 🔴 Pendente

**Componentes:**
- [ ] Remover `app/static/data.json` (obsoleto)
- [ ] Novo endpoint: `GET /api/insights`
- [ ] Configurar CORS (aceitar Vercel domain)
- [ ] Criar `render.yaml`
- [ ] Deploy automático via Git

**Novo endpoint:**
```python
@app.get("/api/insights")
def get_insights(subreddit: str = None):
    # Query: SELECT * FROM gold_ai_insights
    # WHERE subreddit = ? ORDER BY mentions DESC
    return {"insights": [...]}
```

---

### 🟢 Fase 5: Frontend Deploy Vercel
**Duração:** ~1h  
**Ação Humana:** ⚠️ Criar conta Vercel + conectar GitHub  
**Status:** 🔴 Pendente

**Componentes:**
- [ ] Criar `vercel.json` (proxy API)
- [ ] Atualizar frontend: buscar `/api/insights` (não mais data.json)
- [ ] Deploy automático via Git

**Proxy config:**
```json
{
  "rewrites": [
    {"source": "/api/(.*)", "destination": "https://dataradar-api.onrender.com/api/$1"}
  ]
}
```

---

### 🟢 Fase 6: CI/CD Multi-Componente
**Duração:** ~3h  
**Ação Humana:** ⚠️ Adicionar secrets no GitHub  
**Status:** 🔴 Pendente

**Workflows:**
- [ ] `.github/workflows/deploy-infra.yml` (Terraform)
- [ ] `.github/workflows/deploy-airflow.yml` (Astronomer)
- [ ] `.github/workflows/deploy-api.yml` (Render)
- [ ] `.github/workflows/deploy-frontend.yml` (Vercel)
- [ ] Atualizar `ci.yml` (lint + test)

**GitHub Secrets necessários:**
```
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY
ASTRONOMER_TOKEN
RENDER_API_KEY
VERCEL_TOKEN
DATABRICKS_TOKEN
```

---

### 🟢 Fase 7: Databricks Notebooks Versionados
**Duração:** ~2h  
**Ação Humana:** ⚠️ Conectar Repos no Databricks UI  
**Status:** ✅ FEITO (você já conectou!)

**Componentes:**
- [x] Criar `databricks/notebooks/` no repo
- [x] Conectar via Databricks Repos UI
- [x] Migrar código notebooks existentes (`medallion_pipeline.py` + módulos `%run`)
- [x] Atualizar Job Databricks: task notebook apontando para `.../databricks/notebooks/medallion_pipeline` (o Lambda só envia `arquivo_novo`; path fica no Job)

---

### 🟢 Fase 8: Observability (Grafana Cloud)
**Duração:** ~3h  
**Ação Humana:** ⚠️ Criar conta Grafana Cloud  
**Status:** 🔴 Pendente

**Componentes:**
- [ ] Setup Grafana Cloud workspace
- [ ] Conectar Astronomer → OpenTelemetry → Grafana
- [ ] Conectar CloudWatch → Grafana integration
- [ ] Criar dashboards:
  - Pipeline health (DAG success rate)
  - Data volume (posts/dia)
  - LLM metrics (tokens, latência)
- [ ] Configurar alertas (email)

---

### 🟢 Fase 9: Documentação Profissional
**Duração:** ~2h  
**Ação Humana:** ⚠️ Gravar video demo (opcional)  
**Status:** 🔴 Pendente

**Componentes:**
- [ ] ADRs (Architecture Decision Records)
- [ ] Deployment guide (zero-to-hero)
- [ ] Cost breakdown (free tier limits)
- [ ] README updates (badges, demo links)
- [ ] Screenshots/diagramas antes-depois

---

## 🎬 Ordem de Execução Recomendada

```
Sprint 1 (Fundação - 6h)
├── Fase 1: Terraform infra
├── Fase 2: Tabela gold_ai_insights
└── Fase 7: Databricks notebooks

Sprint 2 (Deploy Componentes - 6h)
├── Fase 3: Airflow → Astronomer
├── Fase 4: API → Render
└── Fase 5: Frontend → Vercel

Sprint 3 (Automação - 6h)
├── Fase 6: CI/CD workflows
└── Fase 8: Grafana monitoring

Sprint 4 (Polish - 2h)
└── Fase 9: Documentação

Total: ~20 horas (3-4 dias trabalhando)
```

---

## ⚠️ Ações Manuais Necessárias (Checklist)

### Pré-requisitos (fazer antes de começar)

- [ ] **AWS CLI configurado** localmente (`aws configure`)
- [ ] **Terraform instalado** (`terraform --version`)
- [ ] **Criar conta Astronomer** (https://www.astronomer.io/)
- [ ] **Criar conta Render** (https://render.com/)
- [ ] **Criar conta Vercel** (https://vercel.com/)
- [ ] **Criar conta Grafana Cloud** (https://grafana.com/products/cloud/)

### Durante implementação

- [ ] **Fase 1:** Executar `terraform init && terraform apply` local
- [ ] **Fase 2:** Executar SQL CREATE TABLE no Databricks
- [ ] **Fase 3:** Rodar `astro login && astro deploy`
- [ ] **Fase 4:** Conectar repo GitHub no Render dashboard
- [ ] **Fase 5:** Conectar repo GitHub no Vercel dashboard
- [ ] **Fase 6:** Adicionar 6 secrets no GitHub Settings
- [ ] **Fase 8:** Configurar integrations Grafana Cloud

---

## 📈 Métricas de Sucesso

**Critérios para considerar projeto "automatizado":**

- ✅ Push `main` → deploy automático todos componentes (< 10min)
- ✅ Pipeline roda 4x/dia sem intervenção manual
- ✅ Dashboard acessível publicamente 24/7
- ✅ Grafana mostra métricas em tempo real
- ✅ Infra reproduzível: `git clone → terraform apply → done`
- ✅ Zero custos mensais (dentro free tiers)

---

## 🔗 Recursos

- [Terraform AWS Provider](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)
- [Astronomer Docs](https://docs.astronomer.io/)
- [Databricks Repos](https://docs.databricks.com/repos/index.html)
- [Render Deploy](https://render.com/docs/deploy-fastapi)
- [Vercel Deploy](https://vercel.com/docs)
- [Grafana Cloud](https://grafana.com/docs/grafana-cloud/)

---

## 🏁 Status Atual

**Progresso:** 0/9 fases completas (0%)

**Próximo passo:** → Fase 1 (Terraform infraestrutura)

**Bloqueios:** Nenhum

**Última atualização:** 2026-04-13
