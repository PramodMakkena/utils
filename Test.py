import os
import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)


# ------------------------------------------------------------------------------
# 1. Read in config files (global_config_file and possibly a local config file).
#    Adjust paths as needed for your environment.
# ------------------------------------------------------------------------------
PROJECT_ID = os.environ.get("GCP_PROJECT", "my_project")
BUCKET = os.environ.get("GCS_BUCKET", "my_bucket")

# Example: 
#   file_path = Path(__file__).parents[1] / "gfcc_dam_config_files" / "gfcc_dam_config.json"
# Or if you read from GCS or anywhere else, adapt as needed:
global_config_file_path = "/path/to/gfcc_dam_global_config.json"

with open(global_config_file_path, "r") as f:
    global_config_data = json.load(f)

# Optionally read a second config file if your DAG logic requires it:
# local_config_path = "/path/to/gfcc_dam_config_PM.json"
# with open(local_config_path, "r") as f2:
#     local_config_data = json.load(f2)


# ------------------------------------------------------------------------------
# 2. Define default_args for the DAG
# ------------------------------------------------------------------------------
default_args = {
    'owner': global_config_data.get("owner", "default_owner"),
    'start_date': datetime.strptime(global_config_data.get("start_date", "2023-01-01"), "%Y-%m-%d"),
    'email_on_failure': global_config_data.get("email_on_failure", []),
    'email_on_retry': [],
    'retries': 0
}

# ------------------------------------------------------------------------------
# 3. Create the DAG
#    schedule_interval, catchup, etc. can be read from config if desired
# ------------------------------------------------------------------------------
with DAG(
    dag_id="gfcc_dam_data_copier_dag_PM",
    description="DAM Data Copier DAG",
    default_args=default_args,
    schedule_interval=global_config_data.get("schedule_interval", "@daily"),
    catchup=False
) as dag:

    # --------------------------------------------------------------------------
    # 4. Create the Dataproc cluster
    # --------------------------------------------------------------------------
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_DAM_cluster",
        project_id=PROJECT_ID,
        cluster_name="DAM_cluster_data_copier",
        # Add your cluster_config etc. as needed
        # cluster_config=...
        dag=dag,
        idle_delete_ttl=1200
    )

    # --------------------------------------------------------------------------
    # 5. Submit the Dataproc job (e.g., PySpark job)
    # --------------------------------------------------------------------------
    Pyspark_Job = {
        "main_python_file_uri": "gs://my_project_id/gfcc_dam_script/gfcc_dam_common_utility_configuration/gfcc_dam_utility.py",
        "args": [
            # Example arguments
            "--config_file", "gs://my_bucket/path/to/config.json",
        ],
    }

    submit_data_copier_job = DataprocSubmitJobOperator(
        task_id="data_copier",
        project_id=PROJECT_ID,
        cluster_name="DAM_cluster_data_copier",
        job_type="pyspark_job",
        pyspark_job=Pyspark_Job,
        dag=dag,
    )

    # --------------------------------------------------------------------------
    # 6. Define a Python callable that sends email to both config-file emails
    #    AND DAG-run parameter emails
    # --------------------------------------------------------------------------
    def send_email(**context):
        """
        Send email to addresses defined in the config file plus
        any email passed in through DAG run conf (e.g. 'email_d1' key).
        """
        # 6a. Pull the email from DAG run conf (if set when triggering the DAG)
        dag_run_conf_email = context["dag_run"].conf.get("email_d1")

        # 6b. Pull emails from global_config_data (adjust key as needed)
        config_email = global_config_data.get("email", "")  # or "email_on_success"

        # 6c. Combine them (EmailOperator accepts a list of recipients)
        to_recipients = []
        if config_email:
            # If config_email is itself a list, extend; if string, append
            if isinstance(config_email, list):
                to_recipients.extend(config_email)
            else:
                to_recipients.append(config_email)

        if dag_run_conf_email:
            to_recipients.append(dag_run_conf_email)

        # 6d. Build the subject / body
        run_id = context['dag_run'].run_id
        ts = context['ts']
        subject = f"DAG completed successfully - {context['dag'].dag_id}"
        html_content = (
            f"This DAG run for data copier has finished.<br>"
            f"Latest DAG Run Details:<br>"
            f"<b>DAG Run date</b>: {ts}<br>"
            f"<b>DAG Run ID</b>: {run_id}"
        )

        # 6e. Actually send email
        email_notification = EmailOperator(
            task_id="email_notification",
            to=to_recipients,
            subject=subject,
            html_content=html_content,
            dag=dag,
        )
        email_notification.execute(context=context)

    # --------------------------------------------------------------------------
    # 7. PythonOperator to call our send_email function
    # --------------------------------------------------------------------------
    send_email_notification = PythonOperator(
        task_id="send_email_notification",
        provide_context=True,
        python_callable=send_email,
        dag=dag,
    )

    # --------------------------------------------------------------------------
    # 8. Set the task ordering
    # --------------------------------------------------------------------------
    create_cluster >> submit_data_copier_job >> send_email_notification
