"""project-sync: container entry point for the pipeline-orchestrator sync pods.

Modes
-----
--sync_config  --config_location LOC
    Read the projects YAML and write [{"project", "url", "branch"}, ...] to the
    XCom file (path taken from the XCOM_PATH env var, set by the DAG).

--sync_project --project P --url U --branch B --dest D [--parser_path REL]
    Clone/refresh the repo into D using the project's deploy key, then copy the
    project's parser file into DAGS_DIR.

Environment
-----------
XCOM_PATH    required for --sync_config (the DAG sets /airflow/xcom/return.json)
GIT_KEY_DIR  where the Secret is mounted (default /secrets/git)
DAGS_DIR     where parser files are copied (default /dags)
KNOWN_HOSTS  optional known_hosts file; otherwise /etc/ssh/ssh_known_hosts is used
"""
import os
import pwd
import argparse
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import filecmp

import glob
# import boto3
import yaml

PROJECT_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")


def check_project_name(project: str) -> str:
    """Project names become file names and paths, so keep them boring."""
    if not PROJECT_RE.match(project):
        raise ValueError(f"invalid project name: {project!r}")
    return project


def sync_config(config_location: str) -> list[dict]:
    """
    config_location examples:
      "s3://my-bucket/path/projects.yaml"  -> read from S3
      "/config/projects.yaml"              -> read from local disk
    Returns [{"project": ..., "url": ..., "branch": ...}, ...]
    """
    logging.info(f"Reading config from {config_location}")

    if config_location.startswith("s3://"):
        bucket, _, key = config_location[len("s3://"):].partition("/")
        body = boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()
    else:
        with open(config_location, "rb") as f:
            body = f.read()

    config = yaml.safe_load(body)
    if not isinstance(config, dict):
        raise ValueError("config must be a mapping of project -> {url, branch}")

    projects = []
    for project, cfg in config.items():
        check_project_name(str(project))
        if not isinstance(cfg, dict) or not {"url", "branch"} <= cfg.keys():
            raise ValueError(f"{project}: missing url or branch")
        projects.append({"project": project, "url": cfg["url"], "branch": cfg["branch"]})
    return projects


def _git_env(project: str) -> tuple[dict, str]:
    """Env for git over SSH, using the project's deploy key from the mounted Secret."""
    key_src = os.path.join(os.environ.get("GIT_KEY_DIR", "/secrets/git"), project)
    if not os.path.isfile(key_src):
        raise FileNotFoundError(f"no deploy key for project '{project}' at {key_src}")

    # ssh rejects keys with loose permissions, so use a private 0600 copy
    fd, key = tempfile.mkstemp(prefix="deploy_key_")
    os.close(fd)
    shutil.copyfile(key_src, key)
    os.chmod(key, 0o600)

    ssh = f"ssh -i {key} -o IdentitiesOnly=yes -o StrictHostKeyChecking=yes"
    if os.environ.get("KNOWN_HOSTS"):
        ssh += f" -o UserKnownHostsFile={os.environ['KNOWN_HOSTS']}"

    env = {**os.environ, "GIT_SSH_COMMAND": ssh, "GIT_TERMINAL_PROMPT": "0"}
    return env, key


def sync_project(project: str, url: str, branch: str, dest: str,
                 parser_path: str | None = None) -> None:
    """Clone or refresh the repo into dest, then copy its parser file into DAGS_DIR."""
    check_project_name(project)
    env, key = _git_env(project)
    ensure_passwd_entry()
    try:
        logging.info(f"Syncing {project}: {url} @ {branch} -> {dest}")
        if os.path.isdir(os.path.join(dest, ".git")):
            subprocess.run(["git", "-C", dest, "fetch", "--depth", "1", "origin", branch],
                           check=True, env=env)
            subprocess.run(["git", "-C", dest, "reset", "--hard", "FETCH_HEAD"],
                           check=True, env=env)
        else:
            subprocess.run(["git", "clone", "--depth", "1", "--branch", branch, url, dest],
                           check=True, env=env)
    finally:
        os.remove(key)

    copy_parsers(project, dest, parser_path)


def copy_parsers(project: str, dest: str, pattern: str | None = None) -> None:
    """Copy every parser file matching `pattern` (relative to dest) into DAGS_DIR."""
    sources: dict[str, str] = {}
    if not pattern:
        pattern = "dags/*_parser.py"
    logging.info(f'pattern {pattern}, dest {dest}')
    for p in sorted(glob.glob(os.path.join(dest, pattern), recursive=True)):
        if not os.path.isfile(p):
            continue
        name = os.path.basename(p)
        if name in sources:
            raise ValueError(f"{project}: two parser files named {name}: {sources[name]} and {p}")
        sources[name] = p
    if not sources:  # refuse to continue, so a bad pattern can't wipe the folder
        raise FileNotFoundError(f"{project}: no parser files match {pattern!r} in {dest}")

    dags_dir = os.path.realpath(os.environ.get("PROJECT_DAGS_DIR", "/dags/projects"))
    target_dir = os.path.join(dags_dir, project)
    if os.path.dirname(os.path.realpath(target_dir)) != dags_dir:  # safety guard
        raise ValueError(f"{project}: target {target_dir} escapes {dags_dir}")
    os.makedirs(target_dir, exist_ok=True)

    # 2. what is there now
    existing = set(os.listdir(target_dir))
    wanted = set(sources)

    # 3. add new files and update changed ones (unchanged files are left untouched)
    for name, src in sorted(sources.items()):
        target = os.path.join(target_dir, name)
        if os.path.isfile(target) and filecmp.cmp(src, target, shallow=False):
            logging.info(f"Unchanged: {name}")
            continue
        tmp = target + ".tmp"          # copy then rename, so no half-written file is parsed
        shutil.copyfile(src, tmp)
        os.replace(tmp, target)
        logging.info(f"{'Updated' if name in existing else 'Added'}: {name}")

    # 4. remove everything that is not in the incoming set
    for name in sorted(existing - wanted):
        path = os.path.join(target_dir, name)
        if os.path.isdir(path) and not os.path.islink(path):
            shutil.rmtree(path)
        else:
            os.remove(path)
        logging.info(f"Removed: {name}")

def write_xcom(value) -> None:
    path = os.environ["XCOM_PATH"]  # set by the DAG; fails loudly if missing
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(value, f)


def ensure_passwd_entry():
    uid = os.getuid()
    try:
        pwd.getpwuid(uid)
    except KeyError:
        home = os.environ.get("HOME", "/tmp")
        with open("/etc/passwd", "a") as f:
            f.write(f"projectsync:x:{uid}:0:project-sync:{home}:/sbin/nologin\n")


def main() -> None:
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)

    p = argparse.ArgumentParser()
    mode = p.add_mutually_exclusive_group(required=True)
    mode.add_argument("--sync_config", action="store_true")
    mode.add_argument("--sync_project", action="store_true")
    p.add_argument("--config_location")
    p.add_argument("--project")
    p.add_argument("--url")
    p.add_argument("--branch")
    p.add_argument("--dest")
    p.add_argument("--parser_path", help="parser file path relative to the repo root")
    args = p.parse_args()

    if args.sync_config:
        if not args.config_location:
            p.error("--sync_config requires --config_location")
        write_xcom(sync_config(args.config_location))
    else:
        if not (args.project and args.url and args.branch and args.dest):
            p.error("--sync_project requires --project, --url, --branch and --dest")
        sync_project(args.project, args.url, args.branch, args.dest, args.parser_path)


if __name__ == "__main__":
    main()