from __future__ import annotations

from typing import Sequence
from kubernetes import client
from airflow.models import BaseOperator
from airflow.sensors.base import BaseSensorOperator
from provider.k8s.hook import CustomKubernetesHook
from airflow.utils.context import Context
from airflow.exceptions import AirflowException
import datetime


class CustomSparkK8sOperator(BaseOperator):
    """
    Creates sparkApplication object in kubernetes cluster:

    .. seealso::
        For more detail about Spark Application Object have a look at the reference:
        https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/v1beta2-1.1.0-2.4.5/docs/api-docs.md#sparkapplication

    :param application_file: Defines Kubernetes 'custom_resource_definition' of 'sparkApplication' as either a
        path to a '.yaml' file, '.json' file, YAML string or JSON string.
    :param namespace: kubernetes namespace to put sparkApplication
    :param kubernetes_conn_id: The :ref:`kubernetes connection id <howto/connection:kubernetes>`
        for the to Kubernetes cluster.
    :param api_group: kubernetes api group of sparkApplication
    :param api_version: kubernetes api version of sparkApplication
    """

    template_fields: Sequence[str] = ("application_file", "namespace")
    template_ext: Sequence[str] = (".yaml", ".yml", ".json")
    ui_color = "#f4a460"

    def __init__(
            self,
            *,
            application_file: str,
            namespace: str | None = None,
            kubernetes_conn_id: str = "kubernetes_default",
            api_group: str = "sparkoperator.k8s.io",
            api_version: str = "v1beta2",
            additional_args: list | None = [],
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.application_file = application_file
        self.namespace = namespace
        self.kubernetes_conn_id = kubernetes_conn_id
        self.api_group = api_group
        self.api_version = api_version
        self.plural = "sparkapplications"
        self.additional_args = additional_args

    def execute(self, context: Context):
        execution_date = (context['dag_run'].logical_date + datetime.timedelta(1)).strftime("%Y-%m-%d")
        conf = context['dag_run'].conf
        arguments = conf.get('arguments', [])
        additional_args = [f'--execution_date={execution_date}'] + self.additional_args
        if arguments:
            additional_args += arguments
        hook = CustomKubernetesHook(conn_id=self.kubernetes_conn_id)
        self.log.info("Creating sparkApplication")
        response = hook.create_custom_object(
            additional_args=additional_args,
            group=self.api_group,
            version=self.api_version,
            plural=self.plural,
            body=self.application_file,
            namespace=self.namespace,
        )
        return response


class CustomSparkK8sOperator_trigger(BaseOperator):
    """
    Creates sparkApplication object in kubernetes cluster:

    .. seealso::
        For more detail about Spark Application Object have a look at the reference:
        https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/v1beta2-1.1.0-2.4.5/docs/api-docs.md#sparkapplication

    :param application_file: Defines Kubernetes 'custom_resource_definition' of 'sparkApplication' as either a
        path to a '.yaml' file, '.json' file, YAML string or JSON string.
    :param namespace: kubernetes namespace to put sparkApplication
    :param kubernetes_conn_id: The :ref:`kubernetes connection id <howto/connection:kubernetes>`
        for the to Kubernetes cluster.
    :param api_group: kubernetes api group of sparkApplication
    :param api_version: kubernetes api version of sparkApplication
    """

    template_fields: Sequence[str] = ("application_file", "namespace")
    template_ext: Sequence[str] = (".yaml", ".yml", ".json")
    ui_color = "#f4a460"

    def __init__(
            self,
            *,
            application_file: str,
            namespace: str | None = None,
            kubernetes_conn_id: str = "kubernetes_default",
            api_group: str = "sparkoperator.k8s.io",
            api_version: str = "v1beta2",
            additional_args: list | None = [],
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.application_file = application_file
        self.namespace = namespace
        self.kubernetes_conn_id = kubernetes_conn_id
        self.api_group = api_group
        self.api_version = api_version
        self.plural = "sparkapplications"
        self.additional_args = additional_args

    def execute(self, context: Context):
        execution_date = context['dag_run'].logical_date.strftime("%Y-%m-%d")
        conf = context['dag_run'].conf
        arguments = conf.get('arguments', [])
        additional_args = [f'--execution_date={execution_date}'] + self.additional_args
        if arguments:
            additional_args += arguments
        hook = CustomKubernetesHook(conn_id=self.kubernetes_conn_id)
        self.log.info("Creating sparkApplication")
        response = hook.create_custom_object(
            additional_args=additional_args,
            group=self.api_group,
            version=self.api_version,
            plural=self.plural,
            body=self.application_file,
            namespace=self.namespace,
        )
        return response


class CustomSparkKubernetesSensor(BaseSensorOperator):
    """
    Checks sparkApplication object in kubernetes cluster:

    .. seealso::
        For more detail about Spark Application Object have a look at the reference:
        https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/blob/v1beta2-1.1.0-2.4.5/docs/api-docs.md#sparkapplication

    :param application_name: spark Application resource name
    :param namespace: the kubernetes namespace where the sparkApplication reside in
    :param container_name: the kubernetes container name where the sparkApplication reside in
    :param kubernetes_conn_id: The :ref:`kubernetes connection<howto/connection:kubernetes>`
        to Kubernetes cluster.
    :param attach_log: determines whether logs for driver pod should be appended to the sensor log
    :param api_group: kubernetes api group of sparkApplication
    :param api_version: kubernetes api version of sparkApplication
    """

    template_fields: Sequence[str] = ("application_name", "namespace")
    FAILURE_STATES = ("FAILED", "UNKNOWN")
    SUCCESS_STATES = ("COMPLETED",)

    def __init__(
            self,
            *,
            application_name: str,
            attach_log: bool = False,
            namespace: str | None = None,
            container_name: str = "spark-kubernetes-driver",
            kubernetes_conn_id: str = "kubernetes_default",
            api_group: str = "sparkoperator.k8s.io",
            api_version: str = "v1beta2",
            **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.application_name = application_name
        self.attach_log = attach_log
        self.namespace = namespace
        self.container_name = container_name
        self.kubernetes_conn_id = kubernetes_conn_id
        self.hook = CustomKubernetesHook(conn_id=self.kubernetes_conn_id)
        self.api_group = api_group
        self.api_version = api_version

    def _log_driver(self, application_state: str, response: dict) -> None:
        if not self.attach_log:
            return
        status_info = response["status"]
        if "driverInfo" not in status_info:
            return
        driver_info = status_info["driverInfo"]
        if "podName" not in driver_info:
            return
        driver_pod_name = driver_info["podName"]
        namespace = response["metadata"]["namespace"]
        log_method = self.log.error if application_state in self.FAILURE_STATES else self.log.info
        try:
            log = ""
            for line in self.hook.get_pod_logs(
                    driver_pod_name, namespace=namespace, container=self.container_name
            ):
                log += line.decode()
            log_method(log)
        except client.rest.ApiException as e:
            self.log.warning(
                "Could not read logs for pod %s. It may have been disposed.\n"
                "Make sure timeToLiveSeconds is set on your SparkApplication spec.\n"
                "underlying exception: %s",
                driver_pod_name,
                e,
            )

    def poke(self, context: Context) -> bool:
        self.log.info("Poking: %s", self.application_name)
        response = self.hook.get_custom_object(
            group=self.api_group,
            version=self.api_version,
            plural="sparkapplications",
            name=self.application_name,
            namespace=self.namespace,
        )
        try:
            application_state = response["status"]["applicationState"]["state"]
        except KeyError:
            return False
        if self.attach_log and application_state in self.FAILURE_STATES + self.SUCCESS_STATES:
            self._log_driver(application_state, response)
        if application_state in self.FAILURE_STATES:
            raise AirflowException(f"Spark application failed with state: {application_state}")
        elif application_state in self.SUCCESS_STATES:
            self.log.info("Spark application ended successfully")
            return True
        else:
            self.log.info("Spark application is still in state: %s", application_state)
            return False
