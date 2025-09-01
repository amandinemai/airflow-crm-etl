from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from etl import crm_etl  

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="crm_etl_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    extract = PythonOperator(
        task_id="extract_data",
        python_callable=crm_etl.extract_data,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=lambda ti: crm_etl.transform_data(
            ti.xcom_pull(task_ids="extract_crm_data")
        ),
    )

    load = PythonOperator(
        task_id="load_to_db",
        python_callable=lambda ti: crm_etl.load_to_db(
            ti.xcom_pull(task_ids="transform_crm_data")
        ),
    )

    extract >> transform >> load
