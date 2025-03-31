from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
from datetime import datetime, timedelta
import json
import os
import sys
import time
import importlib
from pathlib import Path
from lumi.dataprocSubmitJobOperator import DataprocSubmitJobOperator
from lumi.dataprocCreateClusterOperator import DataprocCreateClusterOperator
from airflow.models.param import Param
from airflow.models.xcom_arg import XComArg

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from gfcc_dam_common_utility_configuration.gfcc_dam_email_utility_dynamic import failXcomPopulateDynamic, \
    successXcomPopulateDynamic, dynamicTaskSendNotificationAndReturnBranch
from gfcc_dam_common_utility_configuration.gfcc_dam_dag_utility import validate_run_func

cur_dt = datetime.now()
project_id = os.environ["GCP_PROJECT"]
bucket = os.environ["GCS_BUCKET"]

DAG_DIR = Path(__file__).parent
file = "gfcc_test_automation_config.json"
file_path = DAG_DIR / file
conf_file = Path(file_path)
with open(conf_file, 'r') as f:
    config = json.load(f)

dag_config = config['dag_conf']
cluster_config = config['cluster_conf']
automation_config = config['automation_conf']
destination_emails = config["destination_emails"][project_id]

global_config_file_path = f"gs://{bucket}/{automation_config['required_files']['global_config_file_path']}"
global_config_file_name = global_config_file_path.split("/")[-1].split(".")[0]

dam_utility_file_path = f"gs://{project_id}/{automation_config['required_files']['dam_utility_file_path']}"
automation_utility_path = f"gs://{project_id}/{automation_config['required_files']['automation_utility_path']}"
json_creation_file_path = f"gs://{project_id}/{automation_config['required_files']['json_creation_file_path']}"
execution_file_path = f"gs://{project_id}/{automation_config['required_files']['execution_file_path']}"
consolidation_automation_utility_path = f"gs://{project_id}/{automation_config['required_files']['consolidation_automation_utility_path']}"

PYSPARK_JOB1 = {
    "main_python_file_uri": json_creation_file_path,
    "python_file_uris": [
        dam_utility_file_path,
        global_config_file_path
    ],
    "args": [
        "{{ params.release_version }}",
        "{{ params.sprint }}",
        "{{ params.class_id }}",
        "{{ params.exec_testcases }}",
        "{{ params.test_category }}",
        "{{ params.test_block }}",
        "{{ run_id }}",
        global_config_file_name,
        project_id
    ]
}

# Default arguments for the DAG
default_args = {
    "owner": dag_config["owner"],
    "depends_on_past": dag_config["depends_on_past"],
    "start_date": dag_config["start_date"],
    "retries": dag_config["retries"],
    "retry_delay": timedelta(seconds=dag_config["retry_delay_seconds"]),
    "on_failure_callback": failXcomPopulateDynamic,
    "on_success_callback": successXcomPopulateDynamic
}

# Define the first DAG
with DAG(
    dag_id=f"{dag_config['dag_id']}",
    default_args=default_args,
    schedule_interval=dag_config["schedule_interval"],
    description=dag_config["description"],
    params={
        "release_version": Param("", type="string", description="PTI details to be added as release_version e.g. 24.4,24.5"),
        "sprint": Param("", type="string", description="Sprint details to be added as current sprint e.g. 24.4.17"),
        "exec_testcases": Param("", description="Test case numbers to be executed e.g. empty value or 1 or 1,2,3 or 4,5"),
        "test_block": Param("", description="Block needs to be tested (takes precedence over testcases) e.g. empty value or SENANE, SEADDRESS"),
        "class_id": Param("", type="string", description="Class ID to be added e.g. MERC for Merchant, CONS for Consumer"),
        "test_category": Param("", type="string", description="Test Category to be added e.g. SIT, UT"),
        "table_prefix": Param("", type="string", description="Table prefix to be added if needed e.g. DELTA, HIBERNATION")
    },
) as dag:
    def generate_list(class_id, run_id, **context):
    gcs_file_system = gcsfs.GCSFileSystem(project=project_id)
    file_path = f"gs://{project_id}/gfcc_dam_output/SIT_CONFIG/{class_id}_{run_id}.json"
    read_file = gcs_file_system.open(file_path)

    config = json.load(read_file)
    print('config:', config)
    tasks = config["TASK_CONFIG"]
    task_id = tasks["TASK"]
    result = []
    task_list = []

    for task in task_id:
        print("TASK_ID:", task["task_id"])

        args_dict_extraction = {
            "curr_dt": str(cur_dt),
            "sprint": "{{ params.sprint }}",
            "project_id": project_id,
            "task_block": task["task_id"],
            "test_no": str(task["test_number"]),
            "classid": class_id,
            "runid": run_id,
            "global_config_file_name": global_config_file_name
        }

        block_name = task["task_id"]
        PYSPARK_JOB = {
            "main_python_file_uri": execution_file_path,
            "python_file_uris": [
                dam_utility_file_path,
                automation_utility_path,
                global_config_file_path,
                consolidation_automation_utility_path
            ],
            "args": [str(args_dict_extraction)]
        }

        parameter_op = {
            "cluster_name": cluster_config["cluster_name"],
            "pyspark_job": PYSPARK_JOB
        }

        print('PARAMETER_OP:', parameter_op)
        result.append(parameter_op)
        task_list.append(block_name)

    print('task_list:', task_list)
    index_list = list(range(len(task_list)))
    print('index_list:', index_list)

    context['ti'].xcom_push(key="task_list_indices", value=index_list)
    return result
    
    # This function validates the user before proceeding
validate_run = PythonOperator(
    task_id="validate_run",
    python_callable=validate_run_func,
    retries=0,
    provide_context=True,
    op_kwargs={
        "project_id": project_id,
        "dag_id": dag_config["dag_id"],
        "passed_dag_id": "{{ dag_run.conf.get('passed_dag_id') }}"
    }
)

create_cluster = DataprocCreateClusterOperator(
    task_id="Create_cluster",
    cluster_name=cluster_config["cluster_name"],
    idle_delete_ttl=6000
)

create_json_file = DataprocSubmitJobOperator(
    task_id='Get_testcases_to_execute',
    pyspark_job=PYSPARK_JOB1,
    retries=0
)

# delay_trigger_task = PythonOperator(
#     task_id='delay_trigger_task',
#     python_callable=lambda: time.sleep(100)
# )

generate_list_task = PythonOperator(
    task_id='Create_dynamic_testing_tasks',
    python_callable=generate_list,
    provide_context=True,
    op_kwargs={"class_id": "{{params.class_id}}", "run_id": "{{run_id}}"},
    dag=dag,
    retries=0
)

submit_job = DataprocSubmitJobOperator.partial(
    task_id='Execute_testcases'
).expand_kwargs(
    XComArg(generate_list_task)
)

task_list = [
    {
        "task_name": "validate_run",
        "dynamic": False,
        "task_index": []
    },
    {
        "task_name": "Get_testcases_to_execute",
        "dynamic": False,
        "task_index": []
    },
    {
        "task_name": "Create_dynamic_testing_tasks",
        "dynamic": False,
        "task_index": []
    },
    {
        "task_name": "Execute_testcases",
        "dynamic": True,
        "task_index": "{{ti.xcom_pull(task_ids='Create_dynamic_testing_tasks', key='task_list_indices')}}"
    }
]

notifications_branching = PythonOperator(
    task_id='notifications_branching',
    python_callable=dynamicTaskSendNotificationAndReturnBranch,
    provide_context=True,
    trigger_rule='all_done',
    email_on_failure=True,
    op_kwargs={'task_list': task_list, 'destination_emails': destination_emails, 'dynamic_task': True}
)

validate_run >> create_cluster >> create_json_file >> generate_list_task >> submit_job >> notifications_branching