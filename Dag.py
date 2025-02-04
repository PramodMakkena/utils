import os
import json
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator

project_id = os.environ["GCP_PROJECT"]
bucket = os.environ["GCS_BUCKET"]
file = "gfcc_dam_dc_config_PM.json"
file_path = Path(__file__).parents[1] / 'gfcc_dam_config_files' / file

with open(file_path, 'r') as f:
    dag_config = json.load(f)

global_config_file_path = "gs://{}/dags/{}"
global_config_file_name = global_config_file_path.split("/")[-1].split(".")[0]

default_args = {
    "owner": dag_config["owner"],
    "start_date": datetime.strptime(dag_config["start_date"], "%Y-%m-%d"),
    "email_on_failure": dag_config["email_on_failure"],
    "tags": dag_config["tags"],
}

with DAG(dag_id=dag_config["dag_id"], default_args=default_args, catchup=dag_config["catchup"],
         schedule_interval=dag_config["schedule_interval"], description="DAM Data Copier",
         params={"f_name": "", "email_dl": ""}) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="DAM_Cluster",
        cluster_name="DAM_Cluster_data_copier",
        idle_delete_ttl=1200
    )

    data_copier_args = {
        "global_config_file_name": global_config_file_name,
        "project_id": project_id,
        "bucket": bucket,
        "f_name": "{{ params.f_name }}",
        "email_dl": "{{ params.email_dl }}"
    }

    PYSPARK_JOB = {
        "main_python_file_uri": main_python_file,
        "python_file_uris": [utility_file_path, global_config_file_path],
        "file_uris": [config_file_path],
        "args": [str(data_copier_args)]
    }

    submit_data_copier_job = DataprocSubmitJobOperator(
        task_id="data_copier",
        cluster_name="DAM_Cluster_data_copier",
        project_id=project_id,
        pyspark_job=PYSPARK_JOB
    )

    def send_email(**context):
        email_dl = context["dag_run"].conf.get("email_dl", "")

        if email_dl and len(str(email_dl)) > 0:
            to_gl = email_dl

        run_id = context["dag_run"].run_id
        file_path = gs://{project_id}/{csv_path}/{file_name}

        email_notification = EmailOperator(
            task_id="email_notification",
            to=to_gl,
            subject=f"DAG completed successfully - {context['dag']}",
            html_content=f"This DAG run for data copier has finished.<br>Latest DAG Run Details:<br>"
                         f"DAG run date: {run_id} <br>"
                         # Need to add the code here for printing the location of the CSV file which we get from calling the BQ_to_CSV function in utility file
                         f"File path: {file_path} <br>"
                         f"GCS location: {file_path}"
        )

        email_notification.execute(context=context)

    send_email_notification = PythonOperator(
        task_id="send_email_notification",
        provide_context=True,
        python_callable=send_email,
        dag=dag
    )
