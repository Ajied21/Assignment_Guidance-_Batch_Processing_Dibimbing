from datetime import timedelta
from pathlib import Path
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Fungsi DAG utama
@dag(
    description="Dhoifullah Luth Majied",
    dag_id=Path(__file__).stem,
    schedule_interval=None,
    dagrun_timeout=timedelta(minutes=60),
    start_date=days_ago(1),
    default_args={
        "owner": "Ajied(WhatsApp), Ajied(Email), dibimbing",
    },
    owner_links={
        "Ajied(WhatsApp)": "https://wa.me/+6287787106077",
        "Ajied(Email)": "mailto:dhoifullah.luthmajied05@gmail.com",
        "dibimbing": "https://dibimbing.id/",
    },
    tags=["Assignment: Batch Processing by Ajied batch 7"],
)
def spark_ETL_Simple_Agregasi_Dibimbing():

    # Task dan alur eksekusi DAG
    start_task = EmptyOperator(task_id="start_task")

    # Gunakan SparkSubmitOperator langsung dalam DAG
    etl = SparkSubmitOperator(
        application="/spark-scripts/spark_ETL_Analisis_Churn-Retention_Pelanggan_Berdasarkan_Aktivitas_Transaksi_Terakhir.py",
        conn_id="spark_main",
        task_id="spark_submit_task",
        jars="/spark-scripts/jars/jars_postgresql-42.2.20.jar",
    )

    end_task = EmptyOperator(task_id="end_task")
    
    # Definisikan alur eksekusi
    start_task >> etl >> end_task

spark_ETL_Simple_Agregasi_Dibimbing()