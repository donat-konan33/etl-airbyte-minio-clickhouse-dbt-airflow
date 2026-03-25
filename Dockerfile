FROM python:3.9.20-slim

ARG DEBIAN_FRONTEND=noninteractive

ENV PYTHONUNBUFFERED=1
ENV AIRFLOW_HOME=/app/airflow
ENV DBT_DIR=$AIRFLOW_HOME/dbt_project
ENV DBT_TARGET_DIR=$DBT_DIR/target
ENV DBT_PROFILES_DIR=$DBT_DIR
ENV DBT_VERSION=1.8.7

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR $AIRFLOW_HOME

COPY scripts scripts

# COPY airflow_auth_config airflow_auth_config # define since airflow 3.0.0

COPY airbyte airbyte

COPY README.md README.md

COPY project_functions project_functions

COPY pyproject.toml poetry.lock ./

RUN chmod +x scripts/entrypoint.sh scripts/init_connections.sh

RUN pip3 install --upgrade --no-cache-dir pip \
    && pip3 install poetry \
    && poetry install --with airflow --without dev,api
