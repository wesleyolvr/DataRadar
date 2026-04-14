-- DataRadar - Exemplos de Queries para gold_ai_insights

-- 1. Buscar insights de um subreddit específico (mais recente)
SELECT 
  insight_type,
  item_name,
  mentions,
  context,
  generated_at
FROM gold_ai_insights
WHERE subreddit = 'dataengineering'
  AND execution_date = (SELECT MAX(execution_date) FROM gold_ai_insights)
ORDER BY insight_type, mentions DESC;

-- 2. Top 10 ferramentas mais mencionadas (todas comunidades)
SELECT 
  item_name as tool,
  SUM(mentions) as total_mentions,
  COUNT(DISTINCT subreddit) as communities_count
FROM gold_ai_insights
WHERE insight_type = 'trending_tools'
  AND execution_date = (SELECT MAX(execution_date) FROM gold_ai_insights)
GROUP BY item_name
ORDER BY total_mentions DESC
LIMIT 10;

-- 3. Pain points mais comuns (últimos 7 dias)
SELECT 
  subreddit,
  item_name as pain_point,
  mentions,
  context,
  execution_date
FROM gold_ai_insights
WHERE insight_type = 'pain_points'
  AND execution_date >= CURRENT_DATE() - INTERVAL '7' DAY
ORDER BY execution_date DESC, mentions DESC;

-- 4. Comparar tendências: semana atual vs anterior
WITH current_week AS (
  SELECT 
    item_name,
    SUM(mentions) as current_mentions
  FROM gold_ai_insights
  WHERE insight_type = 'trending_tools'
    AND execution_date >= CURRENT_DATE() - INTERVAL '7' DAY
  GROUP BY item_name
),
previous_week AS (
  SELECT 
    item_name,
    SUM(mentions) as previous_mentions
  FROM gold_ai_insights
  WHERE insight_type = 'trending_tools'
    AND execution_date BETWEEN CURRENT_DATE() - INTERVAL '14' DAY AND CURRENT_DATE() - INTERVAL '7' DAY
  GROUP BY item_name
)
SELECT 
  COALESCE(c.item_name, p.item_name) as tool,
  COALESCE(c.current_mentions, 0) as current_week,
  COALESCE(p.previous_mentions, 0) as previous_week,
  COALESCE(c.current_mentions, 0) - COALESCE(p.previous_mentions, 0) as trend
FROM current_week c
FULL OUTER JOIN previous_week p ON c.item_name = p.item_name
ORDER BY trend DESC
LIMIT 20;

-- 5. Insights completos para API endpoint (formato JSON-ready)
SELECT 
  subreddit,
  COLLECT_LIST(
    STRUCT(
      insight_type,
      item_name,
      mentions,
      context,
      generated_at
    )
  ) as insights
FROM gold_ai_insights
WHERE execution_date = (SELECT MAX(execution_date) FROM gold_ai_insights)
GROUP BY subreddit
ORDER BY subreddit;

-- 6. Estatísticas por subreddit
SELECT 
  subreddit,
  COUNT(*) as total_insights,
  SUM(CASE WHEN insight_type = 'trending_tools' THEN 1 ELSE 0 END) as tools_count,
  SUM(CASE WHEN insight_type = 'pain_points' THEN 1 ELSE 0 END) as pains_count,
  SUM(CASE WHEN insight_type = 'solutions' THEN 1 ELSE 0 END) as solutions_count,
  MAX(generated_at) as last_updated
FROM gold_ai_insights
WHERE execution_date = (SELECT MAX(execution_date) FROM gold_ai_insights)
GROUP BY subreddit
ORDER BY total_insights DESC;

-- 7. Health check - detectar subreddits sem insights recentes
SELECT 
  s.subreddit,
  MAX(i.execution_date) as last_insight_date,
  DATEDIFF(CURRENT_DATE(), MAX(i.execution_date)) as days_since_update
FROM (
  SELECT DISTINCT subreddit FROM devradar_silver_posts
) s
LEFT JOIN gold_ai_insights i ON s.subreddit = i.subreddit
GROUP BY s.subreddit
HAVING days_since_update > 7 OR days_since_update IS NULL
ORDER BY days_since_update DESC;
