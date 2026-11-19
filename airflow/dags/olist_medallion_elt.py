from datetime import timedelta
import json
import os
import urllib.request

import pendulum

try:
    from airflow.sdk import DAG, Param
except ImportError:
    from airflow.models.dag import DAG
    from airflow.models.param import Param

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator


PROJECT_DIR = os.getenv("ETL_PROJECT_DIR", "/opt/project")
ETL_PYTHON = os.getenv("ETL_PYTHON_BIN", "/opt/etl_venv/bin/python")
DBT_PROJECT_DIR = f"{PROJECT_DIR}/dbt_transform"
DBT_CLI = f"{ETL_PYTHON} -m dbt.cli.main --no-use-colors --log-path ../logs"
DBT_ENSURE_DEPS = f'if [ ! -d "{DBT_PROJECT_DIR}/dbt_packages/dbt_utils" ]; then {DBT_CLI} deps; fi'


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
        f"🚨 Airflow DAG thất bại tại task `{task_id}` sau tất cả các lần retry\n"
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


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": on_task_failure_alert,
}

with DAG(
    dag_id="olist_medallion_elt",
    description="Olist E-Commerce Medallion Data Pipeline (Bronze -> Silver -> Snapshot -> Gold)",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
    max_active_runs=1,
    tags=["olist", "medallion", "elt", "dbt", "bronze-silver-gold"],
    params={
        "full_refresh": Param(
            False,
            type="boolean",
            description="Chạy dbt với cờ --full-refresh để tái tạo toàn bộ bảng Silver & Gold",
        ),
    },
) as dag:
    preflight_check = BashOperator(
        task_id="preflight_check",
        bash_command=f"{ETL_PYTHON} main.py --preflight",
        cwd=PROJECT_DIR,
        retries=3,
        retry_delay=timedelta(seconds=15),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=2),
    )

    extract_load_bronze = BashOperator(
        task_id="extract_load_bronze",
        bash_command=f"{ETL_PYTHON} main.py",
        cwd=PROJECT_DIR,
        retries=2,
        retry_delay=timedelta(minutes=1),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=5),
    )

    dbt_silver = BashOperator(
        task_id="dbt_silver",
        bash_command=(
            f"{DBT_ENSURE_DEPS} && {DBT_CLI} run --select staging "
            "{{ '--full-refresh' if params.full_refresh else '' }}"
        ),
        cwd=DBT_PROJECT_DIR,
        retries=1,
        retry_delay=timedelta(minutes=1),
    )

    dbt_test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=f"{DBT_CLI} test --select staging source:*",
        cwd=DBT_PROJECT_DIR,
        retries=0,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT_CLI} snapshot",
        cwd=DBT_PROJECT_DIR,
        retries=1,
        retry_delay=timedelta(minutes=1),
    )

    dbt_gold = BashOperator(
        task_id="dbt_gold",
        bash_command=(
            f"{DBT_CLI} run --select marts "
            "{{ '--full-refresh' if params.full_refresh else '' }}"
        ),
        cwd=DBT_PROJECT_DIR,
        retries=1,
        retry_delay=timedelta(minutes=1),
    )

    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command=f"{DBT_CLI} test --exclude staging source:*",
        cwd=DBT_PROJECT_DIR,
        retries=0,
    )

    (
        preflight_check
        >> extract_load_bronze
        >> dbt_silver
        >> dbt_test_silver
        >> dbt_snapshot
        >> dbt_gold
        >> dbt_test_gold
    )
