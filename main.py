import argparse
import json
import logging
import os
import threading
from dataclasses import dataclass
from time import sleep, time

import kubernetes
from kubernetes import watch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
LOGGER = logging.getLogger(__name__)


def load_kubernetes_config() -> None:
    try:
        kubernetes.config.load_incluster_config()
    except kubernetes.config.ConfigException:
        kubernetes.config.load_kube_config()


load_kubernetes_config()
AUTOSCALING_V1 = kubernetes.client.AutoscalingV1Api()
APP_V1 = kubernetes.client.AppsV1Api()
DYNAMIC = kubernetes.dynamic.DynamicClient(kubernetes.client.api_client.ApiClient())


@dataclass(slots=True, kw_only=True)
class HPA:
    name: str
    namespace: str
    metric_value_path: str
    target_kind: str
    target_name: str
    scale_up_stabilization_window: int
    scale_down_stabilization_window: int


class MetricNotFound(Exception):
    pass


SYNC_INTERVAL = 10
STABILIZATION_CHECK_INTERVAL = 1
HPAs: dict[str, HPA] = {}


def watch_metrics() -> None:
    """
    periodically watches metrics of HPA and scale the targets accordingly if needed.
    """

    # TODO: See if we can use Kubernetes's watch mechanism
    def _watch():
        try:
            while True:
                for hpa in list(HPAs.values()):
                    update_target(hpa)
                sleep(SYNC_INTERVAL)
        except Exception as exc:
            LOGGER.exception(f"Exiting because of: {exc}")
            os._exit(1)

    threading.Thread(target=_watch, daemon=True).start()


def watch_hpa(args) -> None:
    LOGGER.info(f"Will watch HPA with {args.hpa_label_selector=} in {args.hpa_namespace=}.")
    LOGGER.info(f"The scale_up_stabilization_window is set to {args.scale_up_stabilization_window}s.")
    LOGGER.info(f"The scale_down_stabilization_window is set to {args.scale_down_stabilization_window}s.")
    while True:
        try:
            w = watch.Watch()
            for event in w.stream(
                AUTOSCALING_V1.list_namespaced_horizontal_pod_autoscaler,
                args.hpa_namespace,
                label_selector=args.hpa_label_selector,
            ):
                update_hpa(
                    event["object"].metadata,
                    scale_up_stabilization_window=args.scale_up_stabilization_window,
                    scale_down_stabilization_window=args.scale_down_stabilization_window,
                )
        except kubernetes.client.exceptions.ApiException as exc:
            if exc.status != 410:
                raise exc


def update_hpa(metadata, scale_up_stabilization_window, scale_down_stabilization_window) -> None:
    """
    inserts/updates/deletes the HPA to/in/from HPAs.
    """
    hpa_namespace, hpa_name = metadata.namespace, metadata.name
    namespaced_name = f"{hpa_namespace}/{hpa_name}"
    try:
        hpa = AUTOSCALING_V1.read_namespaced_horizontal_pod_autoscaler(namespace=hpa_namespace, name=hpa_name)
        HPAs[namespaced_name] = HPA(
            name=hpa_name,
            namespace=hpa_namespace,
            metric_value_path=build_metric_value_path(hpa),
            target_kind=hpa.spec.scale_target_ref.kind,
            target_name=hpa.spec.scale_target_ref.name,
            scale_up_stabilization_window=scale_up_stabilization_window,
            scale_down_stabilization_window=scale_down_stabilization_window,
        )
    except kubernetes.client.exceptions.ApiException as exc:
        if exc.status != 404:
            raise exc
        LOGGER.info(f"HPA {hpa_namespace}/{hpa_name} was not found, will forget about it.")
        HPAs.pop(namespaced_name, None)


def build_metric_value_path(hpa) -> str:
    """
    returns the Kube API path to retrieve the custom.metrics.k8s.io used metric.
    """
    metrics = json.loads(hpa.metadata.annotations["autoscaling.alpha.kubernetes.io/metrics"])
    try:
        custom_metric = next(m["object"] for m in metrics if m["type"] == "Object")
        assert not custom_metric.get("selector")
        target = custom_metric["target"]
        assert target["kind"] == "Service"
    except (StopIteration, AssertionError) as e:
        LOGGER.exception("Only supports ONE CUSTOM metric without selector based on service for now.")
        raise e

    service_namespace = hpa.metadata.namespace
    service_name = target["name"]
    metric_name = custom_metric["metricName"]

    return f"apis/custom.metrics.k8s.io/v1beta1/namespaces/{service_namespace}/services/{service_name}/{metric_name}"


def get_needed_replicas(metric_value_path) -> int | None:
    """
    returns 0 if the metric value is 0, and 1 otherwise (HPA will take care of scaling up if needed)
    raise MetricNotFound, if the needed replicas cannot be determined.
    """
    try:
        # We suppose the MetricValueList does contain one item
        return min(int(DYNAMIC.request("GET", metric_value_path).items[0].value), 1)
    except kubernetes.client.exceptions.ApiException as exc:
        match exc.status:
            case 404 | 503 | 403:
                LOGGER.exception(f"Could not get Custom metric at {metric_value_path}: {exc}")
                raise MetricNotFound()
            case _:
                raise exc


def get_replicas(*, hpa, read_scale) -> (int, int):
    try:
        scale = read_scale(namespace=hpa.namespace, name=hpa.name)
    except kubernetes.client.exceptions.ApiException as exc:
        if exc.status != 404:
            raise exc
        LOGGER.warning(f"{hpa.namespace}/{hpa.name} was not found.")

    current_replicas = scale.status.replicas
    needed_replicas = get_needed_replicas(hpa.metric_value_path)

    return current_replicas, needed_replicas


def update_target(hpa: HPA) -> None:

    match hpa.target_kind:
        case "Deployment":
            read_scale = APP_V1.read_namespaced_deployment_scale
            patch_scale = APP_V1.patch_namespaced_deployment_scale
        case "StatefulSet":
            read_scale = APP_V1.read_namespaced_stateful_set_scale
            patch_scale = APP_V1.patch_namespaced_stateful_set_scale
        case _:
            raise ValueError(f"Target kind {hpa.target_kind} not supported.")

    try:
        if scaling_is_needed(hpa=hpa, read_scale=read_scale):
            scale = read_scale(namespace=hpa.namespace, name=hpa.name)
            current_replicas, needed_replicas = get_replicas(hpa=hpa, read_scale=read_scale)
            scale.spec.replicas = needed_replicas
            patch_scale(namespace=hpa.namespace, name=hpa.name, body=scale)
            LOGGER.info(
                f"{hpa.target_kind} {hpa.namespace}/{hpa.name} was scaled {current_replicas=}->{needed_replicas=}."
            )
        else:
            current_replicas, needed_replicas = get_replicas(hpa=hpa, read_scale=read_scale)
            LOGGER.info(
                f"No need to scale {hpa.target_kind} {hpa.namespace}/{hpa.name} {current_replicas=} {needed_replicas=}."
            )
    except kubernetes.client.exceptions.ApiException as exc:
        if exc.status != 404:
            raise exc
        LOGGER.warning(f"{hpa.target_kind} {hpa.namespace}/{hpa.name} was not found.")
    except MetricNotFound:
        LOGGER.error(f"Will not update {hpa.target_kind} {hpa.namespace}/{hpa.target_name}.")


def scaling_up_is_needed(current_replicas, needed_replicas):
    return current_replicas < needed_replicas and needed_replicas == 1


def scaling_down_is_needed(current_replicas, needed_replicas):
    return current_replicas > needed_replicas and needed_replicas == 0


def scaling_is_needed(*, hpa, read_scale) -> bool:
    """
    check if the metrics is scale up/down is relevant for the stabilization window duration
    """

    current_replicas, needed_replicas = get_replicas(hpa=hpa, read_scale=read_scale)

    if scaling_up_is_needed(current_replicas, needed_replicas):
        stabilization_window = hpa.scale_up_stabilization_window
    elif scaling_down_is_needed(current_replicas, needed_replicas):
        stabilization_window = hpa.scale_down_stabilization_window
    else:
        return False

    if stabilization_window != 0:

        stabilization_end_time = time() + stabilization_window

        LOGGER.info(
            f"{hpa.target_kind} {hpa.namespace}/{hpa.name} will be scaled ({current_replicas=}->{needed_replicas=}). "
            "Waiting for stabilization..."
        )

        while time() < stabilization_end_time:

            current_replicas, needed_replicas = get_replicas(hpa=hpa, read_scale=read_scale)

            if bool(current_replicas) == bool(needed_replicas):
                LOGGER.info(f"{hpa.target_kind} {hpa.namespace}/{hpa.name} scale is canceled due to stabilization.")
                return False

            sleep(STABILIZATION_CHECK_INTERVAL)

    return True


def parse_cli_args():
    parser = argparse.ArgumentParser(
        description="kube-hpa-scale-to-zero. Check https://github.com/machine424/kube-hpa-scale-to-zero"
    )
    parser.add_argument(
        "--hpa-namespace",
        dest="hpa_namespace",
        default="default",
        help="namespace where the HPA live. (default: 'default' namespace)",
    )
    parser.add_argument(
        "--hpa-label-selector",
        dest="hpa_label_selector",
        default="",
        help="label_selector to get HPA to watch, 'foo=bar,bar=foo' e.g. (default: empty string to select all)",
    )
    # TODO: Remove when spec.behavior.scaleUp.stabilizationWindowSeconds can be retreived with the K8s Python API
    parser.add_argument(
        "--scale-up-stabilization-window",
        dest="scale_up_stabilization_window",
        default="0",
        help="scale_up_stabilization_window restricts the flapping of replica count while scaling up (default: 0)",
        type=int,
    )
    # TODO: Remove when spec.behavior.scaleDown.stabilizationWindowSeconds can be retreived with the K8s Python API
    parser.add_argument(
        "--scale-down-stabilization-window",
        dest="scale_down_stabilization_window",
        default="0",
        help="scale_down_stabilization_window restricts the flapping of replica count while scaling up (default: 0)",
        type=int,
    )

    return parser.parse_args()


if __name__ == "__main__":
    cli_args = parse_cli_args()
    watch_metrics()
    watch_hpa(cli_args)
