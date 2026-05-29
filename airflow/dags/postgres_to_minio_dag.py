from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "samir",
    "start_date": datetime(2026, 5, 28)
}

dag = DAG(
    dag_id="postgres_to_minio_pipeline",
    default_args=default_args,
    schedule=None,
    catchup=False
)

spark_job = BashOperator(
    task_id="run_spark_job",
    bash_command="""
docker exec spark_container \
/opt/spark/bin/spark-submit \
--jars /opt/jars/postgresql-42.7.11.jar,/opt/extra_jars/hadoop-aws-3.3.4.jar,/opt/extra_jars/aws-java-sdk-bundle-1.12.262.jar \
/opt/spark_jobs/postgres_to_minio.py
""",
    dag=dag
)

spark_job