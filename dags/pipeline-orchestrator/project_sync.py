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

# "hostpath" for local dev (kind/minikube/Docker Desktop), "pvc" for EFS-backed prod
STORAGE = os.environ.get("SERA_STORAGE", "hostpath")

# node-side folders for local dev (must match the cluster's extra mounts)
DAGS_HOST_PATH = os.environ.get("SERA_DAGS_HOST_PATH", "/mnt/dags")
PROJECTS_HOST_PATH = os.environ.get("SERA_PROJECTS_HOST_PATH", "/mnt/projects")

# PVC names for prod (both backed by the EFS that EC2 also mounts)
DAGS_PVC = "dags-efs"
PROJECTS_PVC = "projects-efs"

# run the pod as the same user as Airflow so it can read what the pod writes
AIRFLOW_UID = int(os.environ.get("AIRFLOW_UID", "50000"))

GIT_KEYS_SECRET = "project-sync-git-keys"
XCOM_FILE = "/airflow/xcom/return.json"  # the XCom sidecar only reads this path


# ---- volumes ----------------------------------------------------------------
def shared_volume(name: str, host_path: str, claim: str) -> k8s.V1Volume:
    if STORAGE == "pvc":
        return k8s.V1Volume(
            name=name,
            persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name=claim),
        )
    return k8s.V1Volume(
        name=name,
        host_path=k8s.V1HostPathVolumeSource(path=host_path, type="DirectoryOrCreate"),
    )


# Secret volume: one file per project key, in RAM, only visible to this pod.
# The script copies the key to a private 0600 file before calling ssh.
git_keys_volume = k8s.V1Volume(
    name="git-keys",
    secret=k8s.V1SecretVolumeSource(secret_name=GIT_KEYS_SECRET, default_mode=0o444),
)

VOLUMES = [
    git_keys_volume,
    shared_volume("dags", DAGS_HOST_PATH, DAGS_PVC),
    shared_volume("projects", PROJECTS_HOST_PATH, PROJECTS_PVC),
]
VOLUME_MOUNTS = [
    k8s.V1VolumeMount(name="git-keys", mount_path="/secrets/git", read_only=True),
    k8s.V1VolumeMount(name="dags", mount_path="/dags"),
    k8s.V1VolumeMount(name="projects", mount_path="/projects"),
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
        arguments=["--sync_config", "--config_location", CONFIG_LOCATION],
        env_vars={"XCOM_PATH": XCOM_FILE},
        do_xcom_push=True,
        get_logs=True,
        on_finish_action="delete_pod",
    )

    # 2. One pod per project: clone to /projects/<project>, copy parser to /dags
    sync = KubernetesPodOperator.partial(
        task_id="sync",
        name="project-sync-project",
        namespace=NAMESPACE,
        image=IMAGE,
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