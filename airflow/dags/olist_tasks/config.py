import os

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
GOLD_RULE_TARGET_DIRS = " ".join(f"/tmp/dbt_test_{rule_name}" for rule_name in GOLD_BUSINESS_RULES)
