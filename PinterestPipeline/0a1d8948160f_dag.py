from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from datetime import datetime, timedelta

# Obfuscating for GitHub commit
fn = 'jareen'[::-1]
sn = 'lusda'[::-1]
pr = 'liamg'[::-1]
ud = 'f0618498d1a0'[::-1]

notebook_task = {
    'notebook_path': f"/Users/{fn}.{sn}@{pr}.com/{ud}_PinterestDataPipeline"
}

notebook_params = {
    'Variable': 5
}

default_args = {
    'owner': 'neeraj',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=20)
}

with DAG(
    dag_id='0a1d8948160f_dag',
    start_date=datetime(2023, 12, 20, 16),
    schedule_interval='@daily',
    catchup=False,
    default_args=default_args
) as dag:
    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='databricks_default',
        existing_cluster_id='1108-162752-8okw8dgg',
        notebook_task=notebook_task,
    )

    opr_submit_run

print(notebook_task)
