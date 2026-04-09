"""Gera insights por subreddit usando Groq + dados do Databricks."""

from __future__ import annotations

import json
import os
import re
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))


def _load_env_file() -> None:
    path = os.path.join(os.path.dirname(__file__), "..", ".env")
    if not os.path.isfile(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, val = line.partition("=")
            key, val = key.strip(), val.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = val


_load_env_file()

from openai import OpenAI  # noqa: E402
from services.databricks_client import _execute_query, _rows_to_dicts  # noqa: E402

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
MODEL = "llama-3.1-8b-instant"  # 500K TPD, 14.4K RPD no free plan
DELAY_BETWEEN_CALLS = 2
DATA_JSON_PATH = os.path.join(os.path.dirname(__file__), "..", "app", "static", "data.json")

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


def get_subreddits_with_data() -> list[str]:
    rows, cols = _execute_query(
        "SELECT subreddit, COUNT(*) as cnt FROM devradar_silver_posts "
        "GROUP BY subreddit ORDER BY cnt DESC"
    )
    return [r[0] for r in rows if r[1] >= 3]


def get_content_for_subreddit(
    sub: str,
    posts_limit: int = 15,
    comments_limit: int = 20,
    max_chars: int = 6000,
    selftext_clip: int = 150,
    comment_clip: int = 120,
) -> str:
    posts_rows, posts_cols = _execute_query(
        f"SELECT title, selftext FROM devradar_silver_posts "
        f"WHERE subreddit = '{sub}' ORDER BY score DESC LIMIT {posts_limit}"
    )
    posts = _rows_to_dicts(posts_rows, posts_cols)

    comments_rows, comments_cols = _execute_query(
        f"SELECT body FROM devradar_silver_comments "
        f"WHERE subreddit = '{sub}' ORDER BY score DESC LIMIT {comments_limit}"
    )
    comments = _rows_to_dicts(comments_rows, comments_cols)

    parts = []
    for p in posts:
        text = p.get("title", "")
        if p.get("selftext"):
            text += f" | {p['selftext'][:selftext_clip]}"
        parts.append(text)

    for c in comments:
        if c.get("body"):
            parts.append(c["body"][:comment_clip])

    return "\n".join(parts)[:max_chars]


def _parse_retry_after(err_str: str) -> int:
    """Extrai retry-after do erro ou retorna default."""
    match = re.search(r"try again in (\d+(?:\.\d+)?)s", err_str)
    if match:
        return int(float(match.group(1))) + 1
    match = re.search(r"retry.after.*?(\d+)", err_str, re.IGNORECASE)
    if match:
        return int(match.group(1)) + 1
    return 60


def call_groq(client: OpenAI, subreddit: str, content: str) -> dict | None:
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
            if isinstance(parsed, dict) and {"trending_tools", "pain_points", "solutions"}.intersection(parsed.keys()):
                return parsed
            print(f"    Estrutura inesperada, tentativa {attempt+1}...")
            continue
        except Exception as e:
            err_str = str(e)
            if "429" in err_str:
                wait = _parse_retry_after(err_str)
                print(f"    Rate limit, aguardando {wait}s...")
                time.sleep(wait)
                continue
            if "json_validate_failed" in err_str and attempt < 2:
                print("    JSON invalido, retentando...")
                time.sleep(2)
                continue
            print(f"    ERRO: {err_str[:150]}")
            return None
    return None


def load_existing_insights() -> dict:
    if os.path.exists(DATA_JSON_PATH):
        try:
            with open(DATA_JSON_PATH, encoding="utf-8") as f:
                return json.load(f).get("insights", {})
        except Exception:
            pass
    return {}


def _parse_cli() -> tuple[bool, int, str | None, int, int]:
    """Retorna (force, limit, subreddit_only, posts_limit, comments_limit)."""
    force = "--force" in sys.argv
    limit = 0
    sub_only: str | None = None
    posts_limit = 15
    comments_limit = 20
    argv = sys.argv[1:]
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--subreddit" or a == "-s":
            if i + 1 < len(argv):
                sub_only = argv[i + 1].strip().lower()
                i += 2
                continue
            print("Uso: --subreddit NOME")
            sys.exit(1)
        if a.startswith("--subreddit="):
            sub_only = a.split("=", 1)[1].strip().lower()
            i += 1
            continue
        if a in ("--posts", "-p") and i + 1 < len(argv) and argv[i + 1].isdigit():
            posts_limit = int(argv[i + 1])
            i += 2
            continue
        if a in ("--comments", "-c") and i + 1 < len(argv) and argv[i + 1].isdigit():
            comments_limit = int(argv[i + 1])
            i += 2
            continue
        if a.isdigit():
            limit = int(a)
        i += 1

    if sub_only and not re.match(r"^[a-zA-Z0-9_]+$", sub_only):
        print("Nome de subreddit invalido.")
        sys.exit(1)

    return force, limit, sub_only, posts_limit, comments_limit


def main() -> None:
    if not GROQ_API_KEY:
        print("GROQ_API_KEY nao configurada. Defina via env var.")
        sys.exit(1)

    force, limit, sub_only, posts_limit, comments_limit = _parse_cli()

    existing = load_existing_insights()
    print(f"Insights existentes: {len(existing)} subreddits")

    print("Buscando subreddits com dados no Databricks...")
    if sub_only:
        subs = [sub_only]
        # Modo um subreddit: mais amostras (Groq limita tamanho total do request; ver max_chars abaixo)
        if "--posts" not in sys.argv and "-p" not in sys.argv:
            posts_limit = max(posts_limit, 80)
        if "--comments" not in sys.argv and "-c" not in sys.argv:
            comments_limit = max(comments_limit, 120)
    else:
        subs = get_subreddits_with_data()
        if limit:
            subs = subs[:limit]

    if not force:
        subs = [s for s in subs if s not in existing]

    print(f"  {len(subs)} subreddits para processar\n")
    if not subs:
        print("Nada a fazer. Use --force para regenerar todos.")
        return

    # Groq retorna 413 se o prompt exceder o limite do modelo; ~12k chars de dados costuma caber.
    max_chars = 12000 if sub_only else 6000
    selftext_clip = 280 if sub_only else 150
    comment_clip = 200 if sub_only else 120

    client = OpenAI(base_url="https://api.groq.com/openai/v1", api_key=GROQ_API_KEY)
    all_insights = dict(existing)
    new_count = 0

    for i, sub in enumerate(subs, 1):
        print(f"[{i}/{len(subs)}] r/{sub}...", end=" ", flush=True)
        content = get_content_for_subreddit(
            sub,
            posts_limit=posts_limit,
            comments_limit=comments_limit,
            max_chars=max_chars,
            selftext_clip=selftext_clip,
            comment_clip=comment_clip,
        )
        if len(content) < 100:
            print("pouco conteudo, pulando.")
            continue

        insights = call_groq(client, sub, content)
        if insights and isinstance(insights, dict):
            all_insights[sub] = insights
            new_count += 1
            t = len(insights.get("trending_tools", []))
            p = len(insights.get("pain_points", []))
            s = len(insights.get("solutions", []))
            print(f"OK ({t}t {p}d {s}s)")
        else:
            print("falhou.")

        if i < len(subs):
            time.sleep(DELAY_BETWEEN_CALLS)

    if os.path.exists(DATA_JSON_PATH):
        with open(DATA_JSON_PATH, encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = {}

    data["insights"] = all_insights
    with open(DATA_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)

    print(f"\nPronto: {new_count} novos, {len(all_insights)} total em data.json.")


if __name__ == "__main__":
    main()
