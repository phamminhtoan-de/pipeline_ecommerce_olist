import json
import os
import urllib.request


def on_task_failure_alert(context):
    webhook_url = os.getenv("ALERT_WEBHOOK_URL", "").strip()
    if not webhook_url:
        return

    ti = context.get("task_instance") or context.get("ti")
    dag = context.get("dag")
    dag_id = ti.dag_id if ti else (dag.dag_id if dag else "olist_medallion_elt")
    task_id = ti.task_id if ti else "unknown_task"
    run_id = context.get("run_id", "unknown_run")
    logical_date = context.get("logical_date") or context.get("execution_date")
    try_number = getattr(ti, "try_number", "?")

    message = (
        f"Airflow DAG thất bại tại task `{task_id}` sau tất cả các lần retry\n"
        f"- DAG: `{dag_id}`\n"
        f"- Run ID: `{run_id}`\n"
        f"- Logical Date: `{logical_date}`\n"
        f"- Số lần thử: {try_number}"
    )
    payload = json.dumps({"content": message, "text": message}).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass
