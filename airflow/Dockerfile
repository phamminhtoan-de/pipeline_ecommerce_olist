FROM apache/airflow:3.1.0-python3.12

USER root

# 1. Cài đặt Microsoft ODBC Driver 18 cho SQL Server trên Debian 12 (Bookworm)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gnupg2 \
    unixodbc-dev \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Tạo môi trường ảo độc lập (/opt/etl_venv) cho code ETL & dbt
#    Tránh tuyệt đối xung đột thư viện giữa Airflow (constraints riêng) và dự án (pandas 3, SQLAlchemy 2.1, dbt)
COPY requirements.txt /tmp/etl_requirements.txt
RUN python3 -m venv /opt/etl_venv \
    && /opt/etl_venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/etl_venv/bin/pip install --no-cache-dir -r /tmp/etl_requirements.txt \
    && chown -R airflow:root /opt/etl_venv \
    && rm -f /tmp/etl_requirements.txt

USER airflow
