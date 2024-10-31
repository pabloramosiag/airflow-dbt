FROM apache/airflow:2.10.2
USER root

USER airflow

COPY --chown=airflow . .

# Setup dbt for the example project
RUN pip install dbt-core==1.8.2 dbt-postgres==1.8.2

RUN dbt deps --project-dir /opt/airflow/dbt_scripts