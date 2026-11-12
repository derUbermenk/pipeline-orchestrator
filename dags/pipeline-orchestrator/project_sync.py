from __future__ import annotations

import os

import pendulum
from airflow.decorators import dag
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

# ---- config -----------------------------------------------------------------
IMAGE = os.environ.get("PROJECT_SYNC_IMAGE", "project-sync:latest")
CONFIG_LOCATION = os.environ.get("PROJECT_SYNC_CONFIG", "s3://my-bucket/path/projects.yaml")

# pipeline-orchestrator owns this DAG, so its pods run in this namespace
NAMESPACE = "pipeline-orchestrator"

# One PVC rooted at <repo>/pipelines, holding:
#   dags/      pipeline-orchestrator/ (tracked code) + projects/ (parsers, written by this sync)
#   projects/  per-project configs the parsers read (written by this sync)
# Local (kind): backed by a hostPath PV. Prod: backed by a static EFS PV.
# The name must match the PVC manifest in kubernetes-configs/storage.
PIPELINES_PVC = "sera-pipelines"

# run the pod as the same user as Airflow so it can read what the pod writes
AIRFLOW_UID = int(os.environ.get("AIRFLOW_UID", "50000"))

GIT_KEYS_SECRET = "project-sync-git-keys"
XCOM_FILE = "/airflow/xcom/return.json"  # the XCom sidecar only reads this path


# ---- volumes ----------------------------------------------------------------
# The code only names the claim. The PV / StorageClass decides what backs it.
pipelines_volume = k8s.V1Volume(
    name="pipelines",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=PIPELINES_PVC),
)

# Secret volume: one file per project key, in RAM, only visible to this pod.
# The script copies the key to a private 0600 file before calling ssh.
git_keys_volume = k8s.V1Volume(
    name="git-keys",
    secret=k8s.V1SecretVolumeSource(secret_name=GIT_KEYS_SECRET, default_mode=0o444),
)

VOLUMES = [git_keys_volume, pipelines_volume]
VOLUME_MOUNTS = [
    k8s.V1VolumeMount(name="git-keys", mount_path="/secrets/git", read_only=True),
    # pipelines/dags -> /dags (the script writes parsers to /dags/projects/<project>)
    k8s.V1VolumeMount(name="pipelines", mount_path="/dags", sub_path="dags"),
    # pipelines/projects -> /projects (the script clones each project here)
    k8s.V1VolumeMount(name="pipelines", mount_path="/projects", sub_path="projects"),
]


# ---- DAG --------------------------------------------------------------------
@dag(
    dag_id="project_sync",
    schedule=None,
    start_date=pendulum.datetime(2026, 11, 1, tz="Asia/Manila"),
    catchup=False,
    tags=["sera", "sync"],
)
def project_sync():
    # 1. Read the config and emit [{"project", "url", "branch"}, ...] as XCom.
    #    Needs S3 read access (IRSA / service account in prod), no shared volumes.
    discover = KubernetesPodOperator(
        task_id="discover",
        name="project-sync-discover",
        namespace=NAMESPACE,
        image=IMAGE,
        # a locally loaded kind image is never in a registry: don't try to pull it
        image_pull_policy="IfNotPresent",
        arguments=["--sync_config", "--config_location", CONFIG_LOCATION],
        env_vars={"XCOM_PATH": XCOM_FILE},
        do_xcom_push=True,
        get_logs=True,
        on_finish_action="delete_pod",
    )

    # 2. One pod per project: clone to /projects/<project>, copy parser to /dags/projects/<project>
    sync = KubernetesPodOperator.partial(
        task_id="sync",
        name="project-sync-project",
        namespace=NAMESPACE,
        image=IMAGE,
        image_pull_policy="IfNotPresent",
        volumes=VOLUMES,
        volume_mounts=VOLUME_MOUNTS,
        env_vars={
            "GIT_KEY_DIR": "/secrets/git",
            "PROJECT_DAGS_DIR": "/dags/projects",
            "HOME": "/tmp",  # git/ssh need a writable home when running as non-root
        },
        security_context={"runAsUser": AIRFLOW_UID, "runAsGroup": 0},
        get_logs=True,
        on_finish_action="delete_pod",
        max_active_tis_per_dag=4,
    ).expand(
        arguments=discover.output.map(
            lambda i: [
                "--sync_project",
                "--project", i["project"],
                "--url", i["url"],
                "--branch", i["branch"],
                "--dest", f"/projects/{i['project']}",
            ]
        ),
    )

    discover >> sync


project_sync()