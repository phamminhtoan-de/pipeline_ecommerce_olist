from datetime import timedelta

try:
    from airflow.sdk import TaskGroup
except ImportError:
    from airflow.utils.task_group import TaskGroup

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator

from olist_tasks.config import (
    DBT_CLI,
    DBT_PROJECT_DIR,
    ETL_PYTHON,
    GOLD_ALL_MODELS,
    GOLD_BUSINESS_RULES,
    GOLD_RULE_TARGET_DIRS,
    PROJECT_DIR,
)


def build_gold_stage():
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

    collect_gold_errors = BashOperator(
        task_id="collect_gold_errors",
        bash_command=(
            f'{ETL_PYTHON} -m src.monitoring.collect_errors '
            f'--collect --layer gold --run-id "{{{{ run_id }}}}" '
            f'--target-dir /tmp/dbt_test_gold_schema {GOLD_RULE_TARGET_DIRS}'
        ),
        cwd=PROJECT_DIR,
        trigger_rule="all_done",
        retries=1,
        retry_delay=timedelta(seconds=15),
    )

    gold_quality_gate = BashOperator(
        task_id="gold_quality_gate",
        bash_command=(
            f'{ETL_PYTHON} -m src.monitoring.collect_errors '
            f'--quality-gate --layer gold --run-id "{{{{ run_id }}}}" '
            f'--target-dir /tmp/dbt_test_gold_schema {GOLD_RULE_TARGET_DIRS}'
        ),
        cwd=PROJECT_DIR,
        retries=0,
    )

    dbt_gold >> [dbt_test_gold_schema, dbt_test_business_rules]
    [dbt_test_gold_schema, dbt_test_business_rules] >> collect_gold_errors >> gold_quality_gate

    return {
        "dbt_gold": dbt_gold,
        "gold_quality_gate": gold_quality_gate,
    }
