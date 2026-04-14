# Deploy Strategy - DataRadar Automation

## Componentes e Métodos de Deploy

| Componente | Método Deploy | Trigger | Frequência Mudanças |
|-----------|---------------|---------|---------------------|
| **Terraform (AWS infra)** | Manual local | Quando infra muda | Raro (1x/mês) |
| **Airflow (DAGs)** | Manual CLI: `astro deploy -f` | Quando DAG muda | Ocasional (1x/semana) |
| **API (Render)** | Auto via Git | Push `main` | Frequente (várias/dia) |
| **Frontend (Vercel)** | Auto via Git | Push `main` | Frequente (várias/dia) |
| **Databricks notebooks** | Git sync (Repos UI) | Manual pull no workspace | Raro (1x/semana) |

---

## Razão da Estratégia

### Por quê Airflow é Manual?

**Limitações free tier Astronomer:**
- Workspace tokens expiram (1h)
- GitHub Action oficial desatualizada
- API keys não disponíveis (plan pago)
- Deploy é rápido local: `astro deploy -f` (2 min)

**Trade-off:** Deploy manual Airflow é aceitável porque:
- ✅ DAGs mudam raramente (vs app code)
- ✅ CLI local = 2 minutos
- ✅ Evita complexidade CI/CD desnecessária
- ✅ Free tier foca em runtime, não CI/CD

### Por quê Render/Vercel Auto?

- ✅ Git-based deploy nativo (zero config)
- ✅ App code muda frequentemente
- ✅ Frontend/API precisam estar sempre sincronizados
- ✅ Free tier robusto para auto-deploy

---

## Workflow Completo

### Mudança em DAG Airflow:

```bash
# 1. Editar código
vi airflow/dags/dag_reddit_scheduled.py

# 2. Testar local (opcional)
cd airflow
astro dev start  # roda Airflow local

# 3. Deploy produção
astro deploy -f  # 2 minutos

# 4. Commit
git add airflow/
git commit -m "feat: update DAG schedule to 6h"
git push
```

### Mudança em API/Frontend:

```bash
# 1. Editar código
vi app/main.py

# 2. Commit + push
git add app/
git commit -m "feat: add new endpoint"
git push

# 3. Auto-deploy (Render + Vercel detectam push)
# Aguardar 2-3 min
```

### Mudança em Infraestrutura:

```bash
# 1. Editar Terraform
vi terraform/lambda.tf

# 2. Aplicar
cd terraform
terraform plan
terraform apply

# 3. Commit
git add terraform/
git commit -m "infra: update Lambda timeout"
git push
```

---

## CI/CD Mantido

Workflows que **permanecem automatizados**:

```
.github/workflows/
├── ci.yml              ✅ Lint + test (sempre)
└── deploy-lambda.yml   ✅ Deploy Lambda (auto)
```

Workflows **removidos** (não funcionam free tier):
- ❌ `deploy-airflow.yml` - substituído por `astro deploy -f` manual
- ❌ `deploy-infra.yml` - Terraform local é mais seguro
- ❌ `deploy-api.yml` - Render auto-deploya via Git
- ❌ `deploy-frontend.yml` - Vercel auto-deploya via Git

---

## Demonstração de Habilidades

**O que demonstra:**
- ✅ **Pragmatismo** - escolhe ferramenta certa pro contexto
- ✅ **Trade-offs conscientes** - não over-engineer
- ✅ **Free tier mastery** - maximiza recursos gratuitos
- ✅ **Hybrid approach** - manual onde faz sentido, auto onde vale pena

**Para recrutadores:**
> "Implementei pipeline de dados 100% em produção usando free tiers. Deploy de app code é automatizado via Git (Render/Vercel). Orquestração (Airflow) usa CLI deploy (2min) pois DAGs mudam raramente. Demonstra pragmatismo em escolher automação onde agrega valor vs overhead de manutenção."
