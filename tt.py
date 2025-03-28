import os
import json
import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from lumi.dataprocCreateClusterOperator import DataprocCreateClusterOperator
from lumi.dataprocSubmitJobOperator import DataprocSubmitJobOperator
from lumi.dataprocDeleteClusterOperator import DataprocDeleteClusterOperator

# Environment variables
project_id = os.environ["GCP_PROJECT"]
bucket = os.environ["GCS_BUCKET"]

# Load config
file = 'gfcc_dam_dc_master_table_config.json'
file_path = Path(__file__).parents[1] / 'gfcc_dam_config_files' / file
with open(file_path, 'r') as f:
    config = json.load(f)
dag_config = config["dag_config"]

# Define paths
main_python_file = f"gs://{project_id}/gfcc_dam_script/gfcc_dam_common_utility_configuration/gfcc_dam_dc_master_table.py"
utility_file_path = f"gs://{project_id}/gfcc_dam_script/gfcc_dam_common_utility_configuration/gfcc_dam_utility.py"
config_file_path = f"gs://{bucket}/dags/lumiteam-usecase/gfcc_dam_config_files/gfcc_dam_dc_master_table_config.json"
global_config_file_path = f"gs://{bucket}/dags/lumiteam-usecase/gfcc_dam_config_files/gfcc_dam_global_config.py"
global_config_file_name = global_config_file_path.split("/")[-1].split(".")[0]

# DAG default args
default_args = {
    'owner': dag_config['owner'],
    'start_date': datetime.strptime(dag_config['start_date'], "%Y-%m-%d"),
    'email': dag_config['email'],
    'email_on_failure': dag_config['email_on_failure'],
    'tags': dag_config['tags']
}

with DAG(
    dag_id=dag_config['dag_id'],
    default_args=default_args,
    catchup=dag_config['catchup'],
    schedule_interval=dag_config['schedule_interval'],
    description='DAM Data Copier Master Table',
    params={
        "master_table_file_name": "",
        "table_ids": ""
    }
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="DAM_Cluster",
        cluster_name="DAM_Cluster_data_copier_Master_table",
        idle_delete_ttl=1200
    )

    data_copier_master_table_args = {
        "global_config_file_name": global_config_file_name,
        "project_id": project_id,
        "bucket": bucket,
        "master_table_file_name": "{{ params.master_table_file_name }}",
        "table_ids": "{{ params.table_ids }}"
    }

    PYSPARK_JOB = {
        "main_python_file_uri": main_python_file,
        "python_file_uris": [global_config_file_path, utility_file_path],
        "file_uris": [config_file_path],
        "args": [str(data_copier_master_table_args)]
    }

    master_table_run_job = DataprocSubmitJobOperator(
        task_id="master_table_creation",
        cluster_name="DAM_Cluster_data_copier_Master_table",
        project_id=project_id,
        pyspark_job=PYSPARK_JOB
    )

    create_cluster >> master_table_run_job