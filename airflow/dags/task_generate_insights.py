"""Generate AI insights task for Airflow DAG — writes to gold_ai_insights table."""

from __future__ import annotations

import json
import logging
import os
import re
import time
from datetime import UTC, datetime

from airflow.decorators import task

logger = logging.getLogger(__name__)

MODEL = "llama-3.1-8b-instant"
DELAY_BETWEEN_CALLS = 2

PROMPT_TEMPLATE = (
    "Analyze posts/comments from r/{subreddit}. Return JSON with 3 categories:\n"
    '- trending_tools: tools/libs/frameworks mentioned (key: "name")\n'
    '- pain_points: problems/frustrations discussed (key: "topic")\n'
    '- solutions: recommendations proposed (key: "topic")\n\n'
    "Each item: name/topic (max 5 words), mentions (int), context (1 sentence in Portuguese BR).\n"
    "Top 3 per category. Empty array if none. ONLY valid JSON, no markdown.\n\n"
    'Schema: {{"trending_tools":[{{"name":"...","mentions":N,"context":"..."}}],'
    '"pain_points":[{{"topic":"...","mentions":N,"context":"..."}}],'
    '"solutions":[{{"topic":"...","mentions":N,"context":"..."}}]}}\n\n'
    "--- r/{subreddit} DATA ---\n{content}"
)


def _get_db_connection():
    """Create a Databricks SQL connection using env vars read at runtime."""
    from databricks import sql

    return sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"],
        http_path=f"/sql/1.0/warehouses/{os.environ['DATABRICKS_WAREHOUSE_ID']}",
        access_token=os.environ["DATABRICKS_TOKEN"],
    )


def _execute_databricks_query(query: str, params: dict | None = None) -> list[dict]:
    """Execute SQL query on Databricks and return results as dicts."""
    with _get_db_connection() as conn, conn.cursor() as cursor:
        cursor.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row, strict=False)) for row in cursor.fetchall()]


def _get_subreddits_with_data() -> list[str]:
    """Get list of subreddits that have data in Silver."""
    results = _execute_databricks_query(
        "SELECT subreddit, COUNT(*) as cnt FROM devradar_silver_posts "
        "GROUP BY subreddit HAVING cnt >= 3 ORDER BY cnt DESC",
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
        "SELECT title, selftext FROM devradar_silver_posts "
        "WHERE subreddit = :sub ORDER BY score DESC LIMIT :lim",
        {"sub": sub, "lim": posts_limit},
    )

    comments = _execute_databricks_query(
        "SELECT body FROM devradar_silver_comments "
        "WHERE subreddit = :sub ORDER BY score DESC LIMIT :lim",
        {"sub": sub, "lim": comments_limit},
    )

    parts: list[str] = []
    for p in posts:
        text = p.get("title", "")
        selftext = p.get("selftext")
        if selftext:
            text += f" | {selftext[:150]}"
        parts.append(text)

    for c in comments:
        body = c.get("body")
        if body:
            parts.append(body[:120])

    return "\n".join(parts)[:max_chars]


def _call_groq(subreddit: str, content: str) -> dict | None:
    """Call Groq API to generate insights."""
    from openai import OpenAI

    api_key = os.environ.get("GROQ_API_KEY", "")
    client = OpenAI(base_url="https://api.groq.com/openai/v1", api_key=api_key)
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

            expected_keys = {"trending_tools", "pain_points", "solutions"}
            if isinstance(parsed, dict) and expected_keys.intersection(parsed.keys()):
                return parsed

            logger.warning(
                "r/%s: Estrutura inesperada, tentativa %d", subreddit, attempt + 1,
            )

        except Exception as exc:
            err_str = str(exc)
            if "429" in err_str:
                match = re.search(r"try again in (\d+(?:\.\d+)?)s", err_str)
                wait = int(float(match.group(1))) + 1 if match else 60
                logger.warning("r/%s: Rate limit, aguardando %ds...", subreddit, wait)
                time.sleep(wait)
                continue

            logger.error("r/%s: Erro Groq - %s", subreddit, err_str[:150])
            return None

    return None


def _write_insights_to_gold(
    subreddit: str, insights: dict, execution_date: str,
) -> None:
    """Write insights to gold_ai_insights table via individual INSERT statements."""
    now = datetime.now(tz=UTC)
    inserted = 0

    with _get_db_connection() as conn, conn.cursor() as cursor:
        for insight_type in ("trending_tools", "pain_points", "solutions"):
            items = insights.get(insight_type, [])
            for item in items:
                item_name = item.get("name", "") if insight_type == "trending_tools" else item.get("topic", "")
                mentions = item.get("mentions", 0)
                context = item.get("context", "")

                cursor.execute(
                    """
                    MERGE INTO gold_ai_insights AS target
                    USING (SELECT
                        :sub AS subreddit,
                        :itype AS insight_type,
                        :iname AS item_name,
                        :mentions AS mentions,
                        :ctx AS context,
                        :gen_at AS generated_at,
                        :exec_date AS execution_date,
                        :model AS model_version
                    ) AS source
                    ON target.subreddit = source.subreddit
                       AND target.insight_type = source.insight_type
                       AND target.item_name = source.item_name
                       AND target.execution_date = source.execution_date
                    WHEN MATCHED THEN UPDATE SET
                        mentions = source.mentions,
                        context = source.context,
                        generated_at = source.generated_at,
                        model_version = source.model_version
                    WHEN NOT MATCHED THEN INSERT *
                    """,
                    {
                        "sub": subreddit,
                        "itype": insight_type,
                        "iname": item_name,
                        "mentions": mentions,
                        "ctx": context,
                        "gen_at": now.isoformat(),
                        "exec_date": execution_date,
                        "model": MODEL,
                    },
                )
                inserted += 1

    logger.info("r/%s: %d insights inseridos na tabela Gold", subreddit, inserted)


@task(retries=1, retry_delay=60)
def generate_insights(**context) -> dict:
    """Generate AI insights for all subreddits and write to gold_ai_insights table."""
    execution_date = context["ds"]

    groq_key = os.environ.get("GROQ_API_KEY", "")
    db_host = os.environ.get("DATABRICKS_HOST", "")
    db_token = os.environ.get("DATABRICKS_TOKEN", "")
    db_wh = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")

    if not groq_key:
        logger.error("GROQ_API_KEY não configurada — pulando geração de insights")
        return {"status": "skipped", "reason": "missing_groq_key"}

    if not all([db_host, db_token, db_wh]):
        logger.error("Databricks credentials não configuradas — pulando insights")
        return {"status": "skipped", "reason": "missing_databricks_creds"}

    logger.info("Buscando subreddits com dados no Databricks...")
    subreddits = _get_subreddits_with_data()
    logger.info("Encontrados %d subreddits para processar", len(subreddits))

    processed = 0
    errors = 0

    for i, sub in enumerate(subreddits, 1):
        logger.info("[%d/%d] Processando r/%s...", i, len(subreddits), sub)

        content = _get_content_for_subreddit(sub)
        if len(content) < 100:
            logger.warning("r/%s: Pouco conteúdo, pulando", sub)
            continue

        insights = _call_groq(sub, content)
        if insights:
            _write_insights_to_gold(sub, insights, execution_date)
            processed += 1

            t = len(insights.get("trending_tools", []))
            p = len(insights.get("pain_points", []))
            s = len(insights.get("solutions", []))
            logger.info("r/%s: OK (%dt %dp %ds)", sub, t, p, s)
        else:
            errors += 1
            logger.error("r/%s: Falha ao gerar insights", sub)

        if i < len(subreddits):
            time.sleep(DELAY_BETWEEN_CALLS)

    logger.info(
        "Geração de insights completa: %d sucesso, %d erros", processed, errors,
    )

    return {
        "status": "completed",
        "processed": processed,
        "errors": errors,
        "total": len(subreddits),
    }
