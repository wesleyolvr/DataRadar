-- DataRadar - Tabela Gold AI Insights
-- Armazena insights gerados por LLM (Groq/Llama) sobre cada subreddit
-- Substitui o arquivo estático data.json

-- Drop table se já existir (cuidado em produção!)
-- DROP TABLE IF EXISTS gold_ai_insights;

CREATE TABLE IF NOT EXISTS gold_ai_insights (
  subreddit STRING COMMENT 'Nome do subreddit (ex: dataengineering)',
  insight_type STRING COMMENT 'Tipo de insight: trending_tools, pain_points, ou solutions',
  item_name STRING COMMENT 'Nome da ferramenta, tópico ou solução',
  mentions INT COMMENT 'Número de menções/ocorrências identificadas',
  context STRING COMMENT 'Contexto em 1 frase (português BR)',
  generated_at TIMESTAMP COMMENT 'Timestamp de geração do insight',
  execution_date DATE COMMENT 'Data da execução do pipeline (particionamento)',
  model_version STRING COMMENT 'Versão do modelo LLM usado (ex: llama-3.1-8b-instant)'
)
USING DELTA
PARTITIONED BY (execution_date)
COMMENT 'Camada Gold - AI Insights por subreddit (trending tools, pain points, solutions)'
LOCATION 's3://devradar-raw/gold/ai_insights/';

-- Índices para otimizar queries comuns
-- Delta tables criam automaticamente índices Z-ORDER quando necessário

-- Exemplo de query para popular (referência)
-- INSERT INTO gold_ai_insights VALUES
-- ('dataengineering', 'trending_tools', 'Apache Airflow', 15, 'Menções em discussões sobre orquestração', current_timestamp(), current_date(), 'llama-3.1-8b-instant');

-- Query de validação após criação
SELECT 
  COUNT(*) as total_insights,
  COUNT(DISTINCT subreddit) as total_subreddits,
  COUNT(DISTINCT insight_type) as total_types,
  MAX(execution_date) as latest_execution
FROM gold_ai_insights;
