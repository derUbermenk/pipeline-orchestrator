FROM apache/airflow:3.3.2-python3.12

ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12

# apache-airflow-providers-cncf-kubernetes: gives KubernetesPodOperator
# pyyaml: yaml.safe_load — usually already present as an Airflow dependency,
#   pinned explicitly here so it's not left to chance
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# pipeline_orchestrator: your shared internal package (PROJECTS_DIR, etc.)
# Installed separately since it's likely private/internal, not on public PyPI
# COPY pipeline_orchestrator /opt/pipeline_orchestrator
# RUN pip install --no-cache-dir /opt/pipeline_orchestrator

COPY --chown=airflow:root pipeline_orchestrator /opt/pipeline_orchestrator
RUN pip install --no-cache-dir /opt/pipeline_orchestrator