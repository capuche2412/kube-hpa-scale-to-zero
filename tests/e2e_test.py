import json
import logging
import subprocess
from pathlib import Path
from time import sleep

import pytest

from main import SYNC_INTERVAL

TESTS_PATH = Path(__file__).parent
MANIFESTS_PATH = TESTS_PATH / "manifests"

TIMEOUT = SYNC_INTERVAL * 6

logger = logging.getLogger(__name__)


def run(*, command: list[str], **kw_args) -> str:
    try:
        return subprocess.check_output(command, timeout=300, text=True, stderr=subprocess.PIPE, **kw_args)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        logger.exception(f"Running the command failed: {e.stderr}")
        raise e


@pytest.fixture(scope="session")
def setup():
    try:
        run(
            command=[
                "helm",
                "repo",
                "add",
                "prometheus-community",
                "https://prometheus-community.github.io/helm-charts",
            ]
        )
        run(
            command=[
                "helm",
                "install",
                "prometheus",
                "prometheus-community/prometheus",
                "--values",
                f"{MANIFESTS_PATH}/prometheus-values.yaml",
                "--wait",
            ]
        )
        run(
            command=[
                "helm",
                "install",
                "prometheus-adapter",
                "prometheus-community/prometheus-adapter",
                "--values",
                f"{MANIFESTS_PATH}/prometheus-adapter-values.yaml",
                "--wait",
            ]
        )

        run(command=["kubectl", "apply", "-f", f"{MANIFESTS_PATH}/metrics-generator.yaml", "--wait=true"])
        yield
    finally:
        run(command=["helm", "delete", "prometheus", "--wait"])
        run(command=["helm", "delete", "prometheus-adapter", "--wait"])
        run(command=["kubectl", "delete", "-f", f"{MANIFESTS_PATH}/metrics-generator.yaml", "--wait=true"])


def deploy_target(manifest: str):
    run(command=["kubectl", "apply", "-f", f"{MANIFESTS_PATH}/{manifest}", "--wait=true"])


def delete_target(manifest: str):
    run(command=["kubectl", "delete", "-f", f"{MANIFESTS_PATH}/{manifest}", "--wait=true"])


def run_scaler(scale_up_stabilization_window: int = 0, scale_down_stabilization_window: int = 0):
    if scale_up_stabilization_window != 0:
        return subprocess.Popen(
            [
                "python",
                f"{TESTS_PATH.parent}/main.py",
                "--scale-up-stabilization-window",
                str(scale_up_stabilization_window),
            ]
        )
    elif scale_down_stabilization_window != 0:
        return subprocess.Popen(
            [
                "python",
                f"{TESTS_PATH.parent}/main.py",
                "--scale-down-stabilization-window",
                str(scale_down_stabilization_window),
            ]
        )
    else:
        return subprocess.Popen(["python", f"{TESTS_PATH.parent}/main.py"])


def set_foo_metric_value(value: int):
    run(
        command=[
            "kubectl",
            "patch",
            "deployment",
            "metrics-generator",
            "-p",
            json.dumps({"spec": {"template": {"metadata": {"labels": {"foo_metric_value": str(value)}}}}}),
        ]
    )
    run(command=["kubectl", "rollout", "status", "deployment", "metrics-generator"])


def wait_scale(*, kind: str, name: str, replicas: int):
    run(
        command=[
            "kubectl",
            "wait",
            f"--for=jsonpath={{.spec.replicas}}={replicas}",
            kind,
            name,
            f"--timeout={TIMEOUT}s",
        ]
    )


@pytest.mark.parametrize("target_name, kind", [("target-1", "deployment"), ("target-2", "statefulset")])
def test_target(setup, target_name: str, kind: str):
    set_foo_metric_value(0)

    deploy_target(f"{target_name}.yaml")

    # The intial replicas count is 1
    wait_scale(kind=kind, name=target_name, replicas=1)

    khstz = run_scaler()

    try:
        # The initial metric value is 0, it should scale the target to 0
        wait_scale(kind=kind, name=target_name, replicas=0)

        # Increase the metric value
        set_foo_metric_value(10)

        # The deloyment was revived anf the HPA was able to scale it up
        wait_scale(kind=kind, name=target_name, replicas=3)
    finally:
        khstz.kill()
        delete_target(f"{target_name}.yaml")


@pytest.mark.parametrize(
    "target_name, kind, scale_stabilization_window, wait_duration",
    [("target-1", "deployment", 15, 5), ("target-2", "statefulset", 15, 30)],
)
def test_target_with_scale_up_stabilization(
    setup, target_name: str, kind: str, scale_stabilization_window: int, wait_duration: int
):
    set_foo_metric_value(0)

    deploy_target(f"{target_name}.yaml")

    # The intial replicas count is 1
    wait_scale(kind=kind, name=target_name, replicas=1)

    khstz = run_scaler(scale_up_stabilization_window=scale_stabilization_window)

    try:

        # The initial metric value is 0, it should scale the target to 0
        wait_scale(kind=kind, name=target_name, replicas=0)

        # Increase the metric value
        set_foo_metric_value(10)

        sleep(wait_duration)

        if wait_duration < scale_stabilization_window:
            # The deloyment was revived but the HPA was not able to scale it up yet.
            wait_scale(kind=kind, name=target_name, replicas=0)
        else:
            # The deloyment was revived and the HPA was able to scale it up.
            wait_scale(kind=kind, name=target_name, replicas=3)
    finally:
        khstz.kill()
        delete_target(f"{target_name}.yaml")


@pytest.mark.parametrize(
    "target_name, kind, scale_stabilization_window, wait_duration",
    [("target-1", "deployment", 15, 5), ("target-2", "statefulset", 15, 30)],
)
def test_target_with_scale_down_stabilization(
    setup, target_name: str, kind: str, scale_stabilization_window: int, wait_duration: int
):
    set_foo_metric_value(10)

    deploy_target(f"{target_name}.yaml")

    # The intial replicas count is 3
    wait_scale(kind=kind, name=target_name, replicas=3)

    khstz = run_scaler(scale_down_stabilization_window=scale_stabilization_window)

    try:

        # Decrease the metric value
        set_foo_metric_value(0)

        sleep(wait_duration)

        if wait_duration < scale_stabilization_window:
            # The deployment or the statefulset must be up before the end of the stabilization_window.
            wait_scale(kind=kind, name=target_name, replicas=3)
        else:
            # The deployment or the statefulset must go down to zero after the end of the stabilization_window.
            wait_scale(kind=kind, name=target_name, replicas=0)
    finally:
        khstz.kill()
        delete_target(f"{target_name}.yaml")
