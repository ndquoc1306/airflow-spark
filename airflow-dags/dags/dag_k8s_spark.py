from __future__ import annotations

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

with DAG(
    'airflow-call-spark-on-k8s',
    default_args={"max_active_runs": 1},
    description="submit spark-pi as sparkApplication on kubernetes",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 11, 20),
    catchup=False,
) as dag:
    # [START SparkKubernetesOperator_DAG]
    t1 = SparkKubernetesOperator(
        task_id="spark_cf0001",
        kubernetes_conn_id='k8s',
        application_file="config_spark.yaml",
        namespace='spark-operator',
        do_xcom_push=True,
        dag=dag,
    )

    t2 = SparkKubernetesSensor(
        task_id="check_spark_app",
        kubernetes_conn_id='k8s',
        application_name="cf0001",
        namespace='spark-operator',
        dag=dag,
    )
    t1 >> t2

