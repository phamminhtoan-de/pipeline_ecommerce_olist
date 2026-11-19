from datetime import timedelta
import json
import os
import urllib.request

import pendulum

try:
    from airflow.sdk import DAG, Param, TaskGroup
except ImportError:
    from airflow.models.dag import DAG
    from airflow.models.param import Param
    from airflow.utils.task_group import TaskGroup

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator


PROJECT_DIR = os.getenv("ETL_PROJECT_DIR", "/opt/project")
ETL_PYTHON = os.getenv("ETL_PYTHON_BIN", "/opt/etl_venv/bin/python")
DBT_PROJECT_DIR = f"{PROJECT_DIR}/dbt_transform"
DBT_CLI = f"{ETL_PYTHON} -m dbt.cli.main --no-use-colors --log-path ../logs"
DBT_ENSURE_DEPS = f'if [ ! -d "{DBT_PROJECT_DIR}/dbt_packages/dbt_utils" ]; then {DBT_CLI} deps; fi'

BRONZE_TABLES = [
    "customers",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "sellers",
]

SILVER_MODELS = [
    "stg_customers",
    "stg_orders",
    "stg_order_items",
    "stg_order_payments",
    "stg_order_reviews",
    "stg_products",
    "stg_sellers",
]

GOLD_DIM_BASE_MODELS = ["dim_date", "dim_location", "dim_product"]
GOLD_DIM_DEPENDENT_MODELS = ["dim_customer", "dim_seller"]
GOLD_FACT_MODELS = ["fact_orders", "fact_sales"]
GOLD_ALL_MODELS = GOLD_DIM_BASE_MODELS + GOLD_DIM_DEPENDENT_MODELS + GOLD_FACT_MODELS
GOLD_BUSINESS_RULES = [
    "assert_delivered_orders_have_delivery_date",
    "assert_fact_orders_match_sales_amount",
    "assert_fact_orders_match_sales_items_count",
    "assert_order_dates_chronology",
    "assert_sales_amounts_positive",
    "assert_valid_review_score",
]


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
    max_active_tasks=3,
    tags=["olist", "medallion", "elt", "dbt", "bronze-silver-gold"],
    params={
        "full_refresh": Param(
            False,
            type="boolean",
            description="Chạy dbt với cờ --full-refresh để tái tạo toàn bộ bảng Silver & Gold",
        ),
    },
) as dag:
    check_source_sqlserver = BashOperator(
        task_id="check_source_sqlserver",
        bash_command=f"{ETL_PYTHON} main.py --preflight-source",
        cwd=PROJECT_DIR,
        retries=3,
        retry_delay=timedelta(seconds=15),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=2),
    )

    check_target_postgres = BashOperator(
        task_id="check_target_postgres",
        bash_command=f"{ETL_PYTHON} main.py --preflight-target",
        cwd=PROJECT_DIR,
        retries=3,
        retry_delay=timedelta(seconds=15),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=2),
    )

    init_bronze_schema = BashOperator(
        task_id="init_bronze_schema",
        bash_command=f"{ETL_PYTHON} main.py --init-schema",
        cwd=PROJECT_DIR,
        retries=2,
        retry_delay=timedelta(seconds=15),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=2),
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=DBT_ENSURE_DEPS,
        cwd=DBT_PROJECT_DIR,
        retries=1,
        retry_delay=timedelta(seconds=30),
    )

    with TaskGroup(
        group_id="extract_load_bronze",
        tooltip="Trích xuất và nạp song song 7 bảng từ SQL Server vào PostgreSQL Bronze",
    ) as extract_load_bronze:
        for table_name in BRONZE_TABLES:
            BashOperator(
                task_id=f"bronze_{table_name}",
                bash_command=f'{ETL_PYTHON} main.py --table {table_name} --skip-validate --run-id "{{{{ run_id }}}}"',
                cwd=PROJECT_DIR,
                retries=2,
                retry_delay=timedelta(minutes=1),
                retry_exponential_backoff=True,
                max_retry_delay=timedelta(minutes=5),
            )

    validate_bronze = BashOperator(
        task_id="validate_bronze",
        bash_command=f'{ETL_PYTHON} main.py --validate-only --run-id "{{{{ run_id }}}}"',
        cwd=PROJECT_DIR,
        retries=1,
        retry_delay=timedelta(seconds=30),
    )

    dbt_test_bronze_source = BashOperator(
        task_id="dbt_test_bronze_source",
        bash_command=f"{DBT_CLI} test --select source:* --target-path /tmp/dbt_test_bronze_source",
        cwd=DBT_PROJECT_DIR,
        retries=0,
    )

    dbt_source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT_CLI} source freshness --target-path /tmp/dbt_source_freshness",
        cwd=DBT_PROJECT_DIR,
        retries=0,
    )

    with TaskGroup(
        group_id="dbt_silver",
        tooltip="Làm sạch, chuẩn hóa & dedup song song 7 model tầng Silver",
    ) as dbt_silver:
        for model_name in SILVER_MODELS:
            BashOperator(
                task_id=f"silver_{model_name}",
                bash_command=(
                    f"{DBT_CLI} run --select {model_name} --target-path /tmp/dbt_{model_name} "
                    "{{ '--full-refresh' if params.full_refresh else '' }}"
                ),
                cwd=DBT_PROJECT_DIR,
                retries=1,
                retry_delay=timedelta(minutes=1),
            )

    dbt_test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=f"{DBT_CLI} test --select staging --exclude source:* --target-path /tmp/dbt_test_silver",
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

    with TaskGroup(
        group_id="dbt_gold",
        tooltip="Xây dựng Star Schema tầng Gold theo thứ tự phụ thuộc Dimension -> Fact",
    ) as dbt_gold:
        gold_tasks = {}
        for model_name in GOLD_ALL_MODELS:
            gold_tasks[model_name] = BashOperator(
                task_id=f"gold_{model_name}",
                bash_command=(
                    f"{DBT_CLI} run --select {model_name} --target-path /tmp/dbt_{model_name} "
                    "{{ '--full-refresh' if params.full_refresh else '' }}"
                ),
                cwd=DBT_PROJECT_DIR,
                retries=1,
                retry_delay=timedelta(minutes=1),
            )

        gold_tasks["dim_location"] >> [gold_tasks["dim_customer"], gold_tasks["dim_seller"]]
        [
            gold_tasks["dim_date"],
            gold_tasks["dim_location"],
            gold_tasks["dim_customer"],
        ] >> gold_tasks["fact_orders"]
        [
            gold_tasks["dim_date"],
            gold_tasks["dim_location"],
            gold_tasks["dim_customer"],
            gold_tasks["dim_product"],
            gold_tasks["dim_seller"],
        ] >> gold_tasks["fact_sales"]

    dbt_test_gold_schema = BashOperator(
        task_id="dbt_test_gold_schema",
        bash_command=f"{DBT_CLI} test --select marts,test_type:generic --target-path /tmp/dbt_test_gold_schema",
        cwd=DBT_PROJECT_DIR,
        retries=0,
    )

    with TaskGroup(
        group_id="dbt_test_business_rules",
        tooltip="Kiểm tra riêng từng quy tắc nghiệp vụ tầng Gold (6 singular tests)",
    ) as dbt_test_business_rules:
        for rule_name in GOLD_BUSINESS_RULES:
            BashOperator(
                task_id=f"rule_{rule_name}",
                bash_command=f"{DBT_CLI} test --select {rule_name} --target-path /tmp/dbt_test_{rule_name}",
                cwd=DBT_PROJECT_DIR,
                retries=0,
            )

    check_target_postgres >> init_bronze_schema
    [check_source_sqlserver, init_bronze_schema] >> extract_load_bronze
    extract_load_bronze >> [validate_bronze, dbt_test_bronze_source, dbt_source_freshness]
    dbt_deps >> [dbt_test_bronze_source, dbt_source_freshness]
    [validate_bronze, dbt_test_bronze_source] >> dbt_silver
    dbt_silver >> dbt_test_silver >> dbt_snapshot >> dbt_gold
    dbt_gold >> [dbt_test_gold_schema, dbt_test_business_rules]
