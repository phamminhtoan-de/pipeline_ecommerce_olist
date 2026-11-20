import pendulum

try:
    from airflow.sdk import DAG, Param
except ImportError:
    from airflow.models.dag import DAG
    from airflow.models.param import Param

from olist_tasks.bronze_stage import build_bronze_stage
from olist_tasks.callbacks import on_task_failure_alert
from olist_tasks.gold_stage import build_gold_stage
from olist_tasks.silver_stage import build_silver_stage

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
    bronze = build_bronze_stage()
    silver = build_silver_stage()
    gold = build_gold_stage()

    silver["dbt_deps"] >> [bronze["dbt_test_bronze_source"], bronze["dbt_source_freshness"]]
    [bronze["validate_bronze"], bronze["bronze_quality_gate"]] >> silver["dbt_silver"]
    silver["dbt_snapshot"] >> gold["dbt_gold"]
