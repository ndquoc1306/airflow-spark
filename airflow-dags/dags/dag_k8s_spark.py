from __future__ import annotations
from airflow.operators.empty import EmptyOperator
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from provider.k8s.operator import CustomSparkK8sOperator_trigger
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
        'airflow-call-spark-on-k8s',
        default_args={"max_active_runs": 1},
        description="submit spark-pi as sparkApplication on kubernetes",
        schedule=None,
        start_date=datetime(2021, 11, 20),
        catchup=False,
) as dag:
    # [START SparkKubernetesOperator_DAG]
    start = EmptyOperator(task_id='Start')
    end = EmptyOperator(task_id='End')
    t1 = CustomSparkK8sOperator_trigger(
        task_id="spark_cf0001",
        trigger_rule="all_success",
        depends_on_past=False,
        kubernetes_conn_id='k8s',
        retries=3,
        application_file="config_spark.yaml",
        namespace='spark-operator',
        api_group="sparkoperator.k8s.io",
        api_version="v1beta2",
        do_xcom_push=True,
        dag=dag,
    )
    merge_stg_to_dwh = 'call stg.merge_cf0001_stg_to_dwh();'
    call_produce = PostgresOperator(
        task_id='merge_stg_to_dwh',
        sql=merge_stg_to_dwh,
        postgres_conn_id='dwh',
        autocommit=True
    )

    start >> t1 >> call_produce >> end
