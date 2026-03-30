"""Endpoints para disparar ingestão manual e gerenciar DAG agendada."""

from __future__ import annotations

import asyncio
import logging
import os

import httpx
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/ingest", tags=["ingest"])

AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://localhost:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASS", "admin")
DAG_ID = "devradar_reddit_ingestion_local"
SCHEDULED_DAG_ID = "devradar_reddit_scheduled"
VARIABLE_KEY = "devradar_subreddits"


class IngestRequest(BaseModel):
    subreddits: list[str] = Field(..., min_length=1, examples=[["rust", "golang"]])
    sort: str = Field("hot", pattern="^(hot|new|top|rising)$")
    max_pages: int = Field(3, ge=1, le=10)
    extract_comments: bool = Field(True, description="Extrair comentários?")
    min_comments: int = Field(5, ge=1, le=100)
    top_k_comments: int = Field(50, ge=1, le=300)
    comment_depth: int = Field(3, ge=1, le=10)
    upload_s3: bool = Field(False, description="Enviar para S3 após salvar?")


@router.post("/trigger")
async def trigger_ingestion(req: IngestRequest):
    endpoint = f"{AIRFLOW_URL}/api/v1/dags/{DAG_ID}/dagRuns"

    payload = {
        "conf": {
            "subreddits": req.subreddits,
            "sort": req.sort,
            "max_pages": req.max_pages,
            "extract_comments": req.extract_comments,
            "min_comments": req.min_comments,
            "top_k_comments": req.top_k_comments,
            "comment_depth": req.comment_depth,
            "upload_s3": req.upload_s3,
        }
    }

    logger.info(
        "Disparando DAG %s — subreddits=%s sort=%s pages=%d comments=%s s3=%s",
        DAG_ID, req.subreddits, req.sort, req.max_pages,
        req.extract_comments, req.upload_s3,
    )

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                endpoint,
                json=payload,
                auth=(AIRFLOW_USER, AIRFLOW_PASS),
                headers={"Content-Type": "application/json"},
            )
    except httpx.ConnectError as exc:
        raise HTTPException(
            status_code=503,
            detail="Airflow não está acessível. Verifique se o Docker está rodando.",
        ) from exc

    if resp.status_code in (200, 201):
        data = resp.json()
        return {
            "status": "triggered",
            "dag_run_id": data.get("dag_run_id"),
            "state": data.get("state"),
            "subreddits": req.subreddits,
            "airflow_url": f"{AIRFLOW_URL}/dags/{DAG_ID}/grid",
        }

    raise HTTPException(
        status_code=resp.status_code,
        detail=resp.json() if resp.headers.get("content-type", "").startswith("application/json") else resp.text,
    )


@router.get("/dag-run/{dag_run_id}")
async def dag_run_progress(dag_run_id: str):
    """Consulta o progresso de uma DAG Run (estado geral + tasks)."""
    auth = (AIRFLOW_USER, AIRFLOW_PASS)

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            run_resp, tasks_resp = await asyncio.gather(
                client.get(
                    f"{AIRFLOW_URL}/api/v1/dags/{DAG_ID}/dagRuns/{dag_run_id}",
                    auth=auth,
                ),
                client.get(
                    f"{AIRFLOW_URL}/api/v1/dags/{DAG_ID}/dagRuns/{dag_run_id}/taskInstances",
                    auth=auth,
                ),
            )
    except httpx.ConnectError as exc:
        raise HTTPException(status_code=503, detail="Airflow não acessível.") from exc

    if run_resp.status_code != 200:
        raise HTTPException(status_code=run_resp.status_code, detail=run_resp.text)

    run = run_resp.json()
    tasks_raw = tasks_resp.json().get("task_instances", []) if tasks_resp.status_code == 200 else []

    task_order = [
        "get_subreddits", "extract", "validate",
        "save_local", "extract_and_save_comments", "upload_to_s3",
    ]
    task_labels = {
        "get_subreddits": "Lendo parâmetros",
        "extract": "Extraindo posts",
        "validate": "Validando dados",
        "save_local": "Salvando posts",
        "extract_and_save_comments": "Extraindo comentários",
        "upload_to_s3": "Enviando para S3",
    }

    tasks_by_id: dict[str, list] = {}
    for t in tasks_raw:
        tid = t.get("task_id", "")
        tasks_by_id.setdefault(tid, []).append(t)

    steps = []
    for tid in task_order:
        instances = tasks_by_id.get(tid, [])
        if not instances:
            steps.append({
                "task_id": tid,
                "label": task_labels.get(tid, tid),
                "state": "no_status",
                "count": 0,
            })
            continue

        states = [i.get("state") for i in instances]
        if all(s == "success" for s in states):
            agg = "success"
        elif any(s == "failed" for s in states):
            agg = "failed"
        elif any(s == "running" for s in states):
            agg = "running"
        elif any(s in ("queued", "scheduled") for s in states):
            agg = "queued"
        elif any(s == "upstream_failed" for s in states):
            agg = "upstream_failed"
        else:
            agg = states[0] if states else "no_status"

        steps.append({
            "task_id": tid,
            "label": task_labels.get(tid, tid),
            "state": agg,
            "count": len(instances),
        })

    return {
        "dag_run_id": dag_run_id,
        "state": run.get("state"),
        "start_date": run.get("start_date"),
        "end_date": run.get("end_date"),
        "steps": steps,
    }


@router.get("/dag-status")
async def dag_status():
    """Verifica se o Airflow está acessível e a DAG existe."""
    endpoint = f"{AIRFLOW_URL}/api/v1/dags/{DAG_ID}"
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            resp = await client.get(
                endpoint,
                auth=(AIRFLOW_USER, AIRFLOW_PASS),
            )
        if resp.status_code == 200:
            dag = resp.json()
            return {
                "airflow": "online",
                "dag_id": DAG_ID,
                "is_paused": dag.get("is_paused", True),
            }
        return {"airflow": "online", "dag_id": DAG_ID, "error": resp.text}
    except httpx.ConnectError:
        return {"airflow": "offline", "dag_id": DAG_ID}


# ---------------------------------------------------------------------------
# Scheduled DAG management
# ---------------------------------------------------------------------------

class ScheduledSubredditsRequest(BaseModel):
    subreddits: list[str] = Field(..., min_length=1, examples=[["dataengineering", "python", "rust"]])


@router.get("/scheduled/status")
async def scheduled_status():
    """Retorna estado da DAG agendada + lista de subreddits cadastrados."""
    auth = (AIRFLOW_USER, AIRFLOW_PASS)
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            dag_resp, var_resp = await asyncio.gather(
                client.get(
                    f"{AIRFLOW_URL}/api/v1/dags/{SCHEDULED_DAG_ID}",
                    auth=auth,
                ),
                client.get(
                    f"{AIRFLOW_URL}/api/v1/variables/{VARIABLE_KEY}",
                    auth=auth,
                ),
            )
    except httpx.ConnectError as exc:
        raise HTTPException(status_code=503, detail="Airflow não acessível.") from exc

    is_paused = True
    schedule = None
    if dag_resp.status_code == 200:
        dag_data = dag_resp.json()
        is_paused = dag_data.get("is_paused", True)
        schedule = dag_data.get("schedule_interval", {})
        if isinstance(schedule, dict):
            schedule = schedule.get("value", "0 */1 * * *")

    subreddits = ["dataengineering", "python"]
    if var_resp.status_code == 200:
        import json
        raw = var_resp.json().get("value", "[]")
        try:
            subreddits = json.loads(raw) if isinstance(raw, str) else raw
        except (json.JSONDecodeError, TypeError):
            pass

    last_runs = []
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            runs_resp = await client.get(
                f"{AIRFLOW_URL}/api/v1/dags/{SCHEDULED_DAG_ID}/dagRuns",
                params={"limit": 5, "order_by": "-start_date"},
                auth=auth,
            )
        if runs_resp.status_code == 200:
            for run in runs_resp.json().get("dag_runs", []):
                last_runs.append({
                    "dag_run_id": run.get("dag_run_id"),
                    "state": run.get("state"),
                    "start_date": run.get("start_date"),
                    "end_date": run.get("end_date"),
                })
    except httpx.ConnectError:
        pass

    return {
        "dag_id": SCHEDULED_DAG_ID,
        "is_paused": is_paused,
        "schedule": schedule,
        "subreddits": subreddits,
        "last_runs": last_runs,
    }


@router.put("/scheduled/subreddits")
async def update_scheduled_subreddits(req: ScheduledSubredditsRequest):
    """Atualiza a lista de subreddits na Airflow Variable."""
    import json

    auth = (AIRFLOW_USER, AIRFLOW_PASS)
    value = json.dumps(req.subreddits)

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.patch(
                f"{AIRFLOW_URL}/api/v1/variables/{VARIABLE_KEY}",
                json={"key": VARIABLE_KEY, "value": value},
                auth=auth,
            )
            if resp.status_code == 404:
                resp = await client.post(
                    f"{AIRFLOW_URL}/api/v1/variables",
                    json={"key": VARIABLE_KEY, "value": value},
                    auth=auth,
                )
    except httpx.ConnectError as exc:
        raise HTTPException(status_code=503, detail="Airflow não acessível.") from exc

    if resp.status_code in (200, 201):
        logger.info("Variable %s atualizada: %s", VARIABLE_KEY, req.subreddits)
        return {"status": "updated", "subreddits": req.subreddits}

    raise HTTPException(status_code=resp.status_code, detail=resp.text)


@router.post("/scheduled/toggle")
async def toggle_scheduled_dag():
    """Pausa ou despausa a DAG agendada."""
    auth = (AIRFLOW_USER, AIRFLOW_PASS)

    try:
        async with httpx.AsyncClient(timeout=8) as client:
            dag_resp = await client.get(
                f"{AIRFLOW_URL}/api/v1/dags/{SCHEDULED_DAG_ID}",
                auth=auth,
            )
            if dag_resp.status_code != 200:
                raise HTTPException(status_code=dag_resp.status_code, detail=dag_resp.text)

            current = dag_resp.json().get("is_paused", True)
            new_state = not current

            patch_resp = await client.patch(
                f"{AIRFLOW_URL}/api/v1/dags/{SCHEDULED_DAG_ID}",
                json={"is_paused": new_state},
                auth=auth,
            )
    except httpx.ConnectError as exc:
        raise HTTPException(status_code=503, detail="Airflow não acessível.") from exc

    if patch_resp.status_code == 200:
        state_label = "pausada" if new_state else "ativa"
        logger.info("DAG %s agora está %s", SCHEDULED_DAG_ID, state_label)
        return {"dag_id": SCHEDULED_DAG_ID, "is_paused": new_state, "label": state_label}

    raise HTTPException(status_code=patch_resp.status_code, detail=patch_resp.text)
