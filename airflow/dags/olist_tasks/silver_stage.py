from datetime import timedelta

try:
    from airflow.sdk import TaskGroup
except ImportError:
    from airflow.utils.task_group import TaskGroup

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator

from olist_tasks.config import DBT_CLI, DBT_ENSURE_DEPS, DBT_PROJECT_DIR, ETL_PYTHON, PROJECT_DIR, SILVER_MODELS


def build_silver_stage():
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=DBT_ENSURE_DEPS,
        cwd=DBT_PROJECT_DIR,
        retries=1,
        retry_delay=timedelta(seconds=30),
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

    collect_silver_errors = BashOperator(
        task_id="collect_silver_errors",
        bash_command=(
            f'{ETL_PYTHON} -m src.monitoring.collect_errors '
            '--collect --layer silver --run-id "{{ run_id }}" --target-dir /tmp/dbt_test_silver'
        ),
        cwd=PROJECT_DIR,
        trigger_rule="all_done",
        retries=1,
        retry_delay=timedelta(seconds=15),
    )

    silver_quality_gate = BashOperator(
        task_id="silver_quality_gate",
        bash_command=(
            f'{ETL_PYTHON} -m src.monitoring.collect_errors '
            '--quality-gate --layer silver --run-id "{{ run_id }}" --target-dir /tmp/dbt_test_silver'
        ),
        cwd=PROJECT_DIR,
        retries=0,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT_CLI} snapshot",
        cwd=DBT_PROJECT_DIR,
        retries=1,
        retry_delay=timedelta(minutes=1),
    )

    dbt_silver >> dbt_test_silver >> collect_silver_errors >> silver_quality_gate >> dbt_snapshot

    return {
        "dbt_deps": dbt_deps,
        "dbt_silver": dbt_silver,
        "dbt_snapshot": dbt_snapshot,
    }
