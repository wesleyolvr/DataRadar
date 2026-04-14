"""Generate AI insights task for Airflow DAG - writes to gold_ai_insights table."""

from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import datetime

from airflow.decorators import task

logger = logging.getLogger(__name__)

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")
DATABRICKS_WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")

MODEL = "llama-3.1-8b-instant"
DELAY_BETWEEN_CALLS = 2

PROMPT_TEMPLATE = """Analyze posts/comments from r/{subreddit}. Return JSON with 3 categories:
- trending_tools: tools/libs/frameworks mentioned (key: "name")
- pain_points: problems/frustrations discussed (key: "topic")
- solutions: recommendations proposed (key: "topic")

Each item: name/topic (max 5 words), mentions (int), context (1 sentence in Portuguese BR).
Top 3 per category. Empty array if none. ONLY valid JSON, no markdown.

Schema: {{"trending_tools":[{{"name":"...","mentions":N,"context":"..."}}],"pain_points":[{{"topic":"...","mentions":N,"context":"..."}}],"solutions":[{{"topic":"...","mentions":N,"context":"..."}}]}}

--- r/{subreddit} DATA ---
{content}
"""


def _execute_databricks_query(query: str) -> list[dict]:
    """Execute SQL query on Databricks and return results as dicts."""
    from databricks import sql
    
    with sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}",
        access_token=DATABRICKS_TOKEN,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _get_subreddits_with_data() -> list[str]:
    """Get list of subreddits that have data in Silver."""
    results = _execute_databricks_query(
        "SELECT subreddit, COUNT(*) as cnt FROM devradar_silver_posts "
        "GROUP BY subreddit HAVING cnt >= 3 ORDER BY cnt DESC"
    )
    return [r["subreddit"] for r in results]


def _get_content_for_subreddit(
    sub: str,
    posts_limit: int = 15,
    comments_limit: int = 20,
    max_chars: int = 6000,
) -> str:
    """Fetch posts and comments content from Silver tables."""
    posts = _execute_databricks_query(
        f"SELECT title, selftext FROM devradar_silver_posts "
        f"WHERE subreddit = '{sub}' ORDER BY score DESC LIMIT {posts_limit}"
    )
    
    comments = _execute_databricks_query(
        f"SELECT body FROM devradar_silver_comments "
        f"WHERE subreddit = '{sub}' ORDER BY score DESC LIMIT {comments_limit}"
    )
    
    parts = []
    for p in posts:
        text = p.get("title", "")
        if p.get("selftext"):
            text += f" | {p['selftext'][:150]}"
        parts.append(text)
    
    for c in comments:
        if c.get("body"):
            parts.append(c["body"][:120])
    
    return "\n".join(parts)[:max_chars]


def _call_groq(subreddit: str, content: str) -> dict | None:
    """Call Groq API to generate insights."""
    from openai import OpenAI
    
    client = OpenAI(base_url="https://api.groq.com/openai/v1", api_key=GROQ_API_KEY)
    prompt = PROMPT_TEMPLATE.format(subreddit=subreddit, content=content)
    
    for attempt in range(3):
        try:
            resp = client.chat.completions.create(
                model=MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                max_tokens=800,
                response_format={"type": "json_object"},
            )
            text = resp.choices[0].message.content
            parsed = json.loads(text)
            
            if isinstance(parsed, dict) and {
                "trending_tools", "pain_points", "solutions"
            }.intersection(parsed.keys()):
                return parsed
                
            logger.warning(f"r/{subreddit}: Estrutura inesperada, tentativa {attempt+1}")
            continue
            
        except Exception as e:
            err_str = str(e)
            if "429" in err_str:
                wait = 60
                match = re.search(r"try again in (\d+(?:\.\d+)?)s", err_str)
                if match:
                    wait = int(float(match.group(1))) + 1
                logger.warning(f"r/{subreddit}: Rate limit, aguardando {wait}s...")
                time.sleep(wait)
                continue
            
            logger.error(f"r/{subreddit}: Erro Groq - {err_str[:150]}")
            return None
    
    return None


def _write_insights_to_gold(subreddit: str, insights: dict, execution_date: str) -> None:
    """Write insights to gold_ai_insights table."""
    from databricks import sql
    
    rows = []
    for insight_type in ["trending_tools", "pain_points", "solutions"]:
        items = insights.get(insight_type, [])
        for item in items:
            if insight_type == "trending_tools":
                item_name = item.get("name", "")
            else:
                item_name = item.get("topic", "")
            
            rows.append((
                subreddit,
                insight_type,
                item_name,
                item.get("mentions", 0),
                item.get("context", ""),
                datetime.now(),
                execution_date,
                MODEL,
            ))
    
    if not rows:
        logger.warning(f"r/{subreddit}: Nenhum insight para inserir")
        return
    
    with sql.connect(
        server_hostname=DATABRICKS_HOST,
        http_path=f"/sql/1.0/warehouses/{DATABRICKS_WAREHOUSE_ID}",
        access_token=DATABRICKS_TOKEN,
    ) as conn:
        with conn.cursor() as cursor:
            # MERGE para evitar duplicatas
            cursor.executemany(
                """
                MERGE INTO gold_ai_insights AS target
                USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?)) AS source(
                    subreddit, insight_type, item_name, mentions, context, 
                    generated_at, execution_date, model_version
                )
                ON target.subreddit = source.subreddit 
                   AND target.insight_type = source.insight_type
                   AND target.item_name = source.item_name
                   AND target.execution_date = source.execution_date
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """,
                rows
            )
    
    logger.info(f"r/{subreddit}: {len(rows)} insights inseridos na tabela Gold")


@task
def generate_insights(**context) -> dict:
    """Generate AI insights for all subreddits and write to gold_ai_insights table."""
    execution_date = context["ds"]
    
    if not GROQ_API_KEY:
        logger.error("GROQ_API_KEY não configurada - pulando geração de insights")
        return {"status": "skipped", "reason": "missing_groq_key"}
    
    if not all([DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID]):
        logger.error("Databricks credentials não configuradas - pulando insights")
        return {"status": "skipped", "reason": "missing_databricks_creds"}
    
    logger.info("Buscando subreddits com dados no Databricks...")
    subreddits = _get_subreddits_with_data()
    logger.info(f"Encontrados {len(subreddits)} subreddits para processar")
    
    processed = 0
    errors = 0
    
    for i, sub in enumerate(subreddits, 1):
        logger.info(f"[{i}/{len(subreddits)}] Processando r/{sub}...")
        
        content = _get_content_for_subreddit(sub)
        if len(content) < 100:
            logger.warning(f"r/{sub}: Pouco conteúdo, pulando")
            continue
        
        insights = _call_groq(sub, content)
        if insights:
            _write_insights_to_gold(sub, insights, execution_date)
            processed += 1
            
            t = len(insights.get("trending_tools", []))
            p = len(insights.get("pain_points", []))
            s = len(insights.get("solutions", []))
            logger.info(f"r/{sub}: OK ({t}t {p}p {s}s)")
        else:
            errors += 1
            logger.error(f"r/{sub}: Falha ao gerar insights")
        
        if i < len(subreddits):
            time.sleep(DELAY_BETWEEN_CALLS)
    
    logger.info(
        f"Geração de insights completa: {processed} sucesso, {errors} erros"
    )
    
    return {
        "status": "completed",
        "processed": processed,
        "errors": errors,
        "total": len(subreddits),
    }
