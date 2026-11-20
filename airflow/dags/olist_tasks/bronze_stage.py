from datetime import timedelta

try:
    from airflow.sdk import TaskGroup
except ImportError:
    from airflow.utils.task_group import TaskGroup

try:
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow.operators.bash import BashOperator

from olist_tasks.config import BRONZE_TABLES, DBT_CLI, DBT_PROJECT_DIR, ETL_PYTHON, PROJECT_DIR


def build_bronze_stage():
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

    collect_bronze_errors = BashOperator(
        task_id="collect_bronze_errors",
        bash_command=(
            f'{ETL_PYTHON} -m src.monitoring.collect_errors '
            '--collect --layer bronze --run-id "{{ run_id }}" --target-dir /tmp/dbt_test_bronze_source'
        ),
        cwd=PROJECT_DIR,
        trigger_rule="all_done",
        retries=1,
        retry_delay=timedelta(seconds=15),
    )

    bronze_quality_gate = BashOperator(
        task_id="bronze_quality_gate",
        bash_command=(
            f'{ETL_PYTHON} -m src.monitoring.collect_errors '
            '--quality-gate --layer bronze --run-id "{{ run_id }}" --target-dir /tmp/dbt_test_bronze_source'
        ),
        cwd=PROJECT_DIR,
        retries=0,
    )

    check_target_postgres >> init_bronze_schema
    [check_source_sqlserver, init_bronze_schema] >> extract_load_bronze
    extract_load_bronze >> [validate_bronze, dbt_test_bronze_source, dbt_source_freshness]
    dbt_test_bronze_source >> collect_bronze_errors >> bronze_quality_gate

    return {
        "dbt_test_bronze_source": dbt_test_bronze_source,
        "dbt_source_freshness": dbt_source_freshness,
        "validate_bronze": validate_bronze,
        "bronze_quality_gate": bronze_quality_gate,
    }
