# DataRadar — Melhorias Pendentes no Pipeline

> Anotações de débitos técnicos, otimizações e features futuras.
> Atualizado em: 2026-03-30

---

## Ciclo Atual: Estabilização (em andamento)

> Decisões tomadas via interrogatório grill-me em 2026-03-30.
> Objetivo: tornar o projeto publicável no GitHub, profissional e pronto para produção.
> Escopo: zero features novas — só organização, segurança e qualidade.

### Git + GitHub
- [x] Inicializar repo Git só em `devradar/` (repo independente)
- [x] Branch strategy: `main` + feature branches (GitHub Flow), PRs para histórico
- [x] Licença MIT
- [x] `.gitignore` completo (adicionar `.venv/`, `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`, `.vscode/`, `.idea/`, `dist/`, `*.egg-info`)
- [x] Mover `test_s3_upload.py` e `trigger_dag.py` para `scripts/`, limpar `load_dotenv` hardcoded
- [x] README enxuto (~100-150 linhas) com diagrama Mermaid de arquitetura + detalhes em `docs/`

### Segurança de Credenciais
- [x] Rotacionar chave AWS antes de publicar no GitHub
- [x] Criar `.env.example` na raiz com placeholders (manter `airflow/.env.example` separado)
- [x] Lambda: manter env vars no console por agora; documentar migração para SSM Parameter Store como próximo passo

### Testes Automatizados
- [x] Framework: pytest, `tests/` na raiz, estrutura flat com `conftest.py`
- [x] Testar funções puras (sem mocks HTTP): `extract_reddit.py`, `mock_layers.py`, `lambda/handler.py`, `bronze_reader.py`
- [x] `bronze_reader.py`: testes com `tmp_path` e JSONs fake para deduplicação/paginação

### CI/CD
- [x] GitHub Actions: lint (ruff) + testes (pytest) a cada push/PR
- [x] Badge de "tests passing" no README

---

## Próximo Ciclo: Features

### Ingestão (Airflow)

- [ ] **Cache de posts**: hoje só comentários têm cache. Adicionar controle para não extrair posts se já rodou nos últimos N minutos (evita snapshots idênticos consecutivos)
- [ ] **Retry inteligente por subreddit**: se um subreddit falha, hoje ele é pulado silenciosamente. Implementar alerta (Slack/email) quando um subreddit falha 3x seguidas
- [ ] **Rate limit adaptativo**: o sleep fixo de 2s entre requests funciona, mas poderia ler o header `X-Ratelimit-Remaining` da API do Reddit e ajustar dinamicamente
- [ ] **Compressão no upload S3**: os JSONs são enviados sem compressão. Usar gzip antes do `put_object` para reduzir custo de storage e transferência
- [ ] **Particionamento S3 por hora**: hoje é `date=YYYY-MM-DD`. Com execução horária, considerar `date=YYYY-MM-DD/hour=HH` para evitar diretórios com muitos arquivos

### Lambda

- [ ] **Batch processing**: hoje cada `raw_*.json` dispara 1 Run no Databricks. Se o volume crescer, usar SQS para acumular eventos e disparar 1 Run batch com todos os subreddits
- [ ] **Dead Letter Queue (DLQ)**: se o Lambda falha (Databricks offline, token expirado), o evento é perdido. Configurar DLQ no SQS para reprocessamento
- [ ] **Filtro de duplicatas**: se o Airflow fizer retry e subir o mesmo arquivo 2x, o Lambda vai acionar 2 Runs. Adicionar idempotência (ex: checar no DynamoDB se o arquivo já foi processado)
- [ ] **Monitoramento**: criar alarme no CloudWatch para quando o Lambda falha (SNS -> email)
- [ ] **Migrar secrets para SSM Parameter Store** (gratuito, criptografado) — `DATABRICKS_TOKEN`, `DATABRICKS_DOMAIN`, `JOB_ID`

### Databricks (Medallion Pipeline)

- [ ] **Deduplicação incremental na Silver**: hoje usa `dropDuplicates(["id"])` dentro do batch, mas não verifica duplicatas entre execuções. Implementar MERGE/UPSERT em Delta para deduplicação cross-batch
- [ ] **Schema evolution**: se o Reddit mudar o formato da API, o pipeline quebra. Adicionar `mergeSchema=true` no write e validação de schema no read
- [ ] **NLP na Silver**: extrair ferramentas/tecnologias mencionadas no título e body (regex + dicionário). Campos como `tools_mentioned`, `sentiment_score`
- [ ] **Gold — Tendências temporais**: criar mart de "trending tools" comparando semana atual vs anterior (crescimento de menções)
- [ ] **Gold — Análise de sentimento**: usar TextBlob ou modelo leve para classificar posts/comentários como positivo/negativo/neutro
- [ ] **Processar todos os subreddits em 1 Run**: em vez de 11 Runs enfileirados, listar tudo no S3 para o `date=` do dia e processar de uma vez (reduz overhead de cluster startup)
- [ ] **Tabelas Silver como Managed Tables no Unity Catalog**: se migrar da Community Edition, registrar no catálogo para melhor governança

### Frontend / API

- [ ] **Exibir dados Silver e Gold reais**: hoje as abas Silver e Gold mostram dados mockados. Quando a Silver estiver estável, conectar a API para ler das tabelas Delta (ou de um PostgreSQL/Supabase alimentado pelo Gold)
- [ ] **Dashboard de monitoramento**: mostrar no frontend o status das últimas execuções do pipeline (Airflow + Databricks), com indicadores de saúde
- [ ] **Histórico de execuções agendadas**: a aba Agendamento mostra só as últimas 5 runs. Adicionar paginação e filtro por data
- [ ] **Busca nos posts**: adicionar campo de busca full-text na aba Bronze Explorer

### Infraestrutura

- [ ] **Terraform/IaC**: hoje tudo foi criado pelo Console AWS. Migrar Lambda, IAM policies e S3 event notifications para Terraform para reprodutibilidade
- [ ] **Ambiente de staging**: hoje tudo roda no mesmo bucket e tabelas. Separar `dataradar-raw-dev` e `dataradar-raw-prod`

### Performance / Custo

- [ ] **Lifecycle policy no S3**: dados Bronze com mais de 90 dias podem ir para S3 Glacier (reduz custo ~90%)
- [ ] **Airflow — reduzir intervalo**: avaliar se 1h é frequente demais para o volume de dados. Se os posts não mudam tanto, 3h ou 6h pode ser suficiente
- [ ] **Databricks — cluster autoterm**: garantir que o cluster desliga automaticamente após N minutos de inatividade para não consumir créditos

---

*Prioridade do próximo ciclo: Deduplicação Silver > NLP > Dashboard de monitoramento*
