import sys
import json
import pandas as pd
import os
from datetime import datetime
from google.cloud import bigquery, storage
from gfcc_dam_utility import *
import gfcc_dam_utility

global dtm

def getArgs():
    try:
        args_dict = eval(sys.argv[1])
        project_id = args_dict["project_id"]
        bucket = args_dict["bucket"]
        master_table_file_name = args_dict["master_table_file_name"]
        dag_table_ids = args_dict["table_ids"]

        print("args", sys.argv)

        config_subpath = f"gs://{bucket}/dags/lumiteam-usecase/gfcc_dam_config_files/"
        dc_config_file = "gfcc_dam_dc_master_table_config.json"
        gfcc_dam_utility.global_config_file_name = args_dict["global_config_file_name"]

        lumi_id = getLumiSpace(project_id)
        log_msg("project_id: " + str(project_id))
        log_msg("bucket: " + str(bucket))
        log_msg("lumi_id: " + str(lumi_id))

        return project_id, bucket, config_subpath, dc_config_file, lumi_id, master_table_file_name, dag_table_ids
    except Exception as e:
        err_str = "Exception in getArgs: " + str(e)
        throw_exception(err_str)

def readconfig(config_path):
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
            table_ids = config["table_ids"]
            csv_location = config["csv_loc"]
            return table_ids, csv_location
    except Exception as e:
        err_str = "Exception in readconfig: " + str(e)
        throw_exception(err_str)

def read_csv_from_gcs(project_id, bucket, csv_location):
    try:
        client = storage.Client()
        bucket_obj = client.bucket(bucket)
        blob = bucket_obj.blob(f"{csv_location}master_table.csv")
        data = blob.download_as_text()
        df = pd.read_csv(pd.compat.StringIO(data))
        return df
    except Exception as e:
        err_str = "Exception reading CSV from GCS: " + str(e)
        throw_exception(err_str)

def readCSV(bq_client, project_id, bucket, csv_location, dag_table_ids):
    try:
        df = read_csv_from_gcs(project_id, bucket, csv_location)
        global dtm
        dtm = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        log_msg(dtm)

        valid_run = 1
        if len(dag_table_ids) > 0:
            table_ids = dag_table_ids.split(",")
            print("table_ids:", table_ids)
        else:
            log_msg("ERROR: table_id not provided in DAG parameter, kindly provide and re-run")
            valid_run = 0

        if valid_run == 1:
            for val in table_ids:
                if val == "all":
                    data = df
                else:
                    data = df[df["table_id"] == val]

                for _, row in data.iterrows():
                    table_id = row["table_id"]
                    table_name = str(row["table_name"])
                    pks = row["pks"] if pd.notna(row["pks"]) else ""
                    log_msg("table_id: " + str(table_id))
                    log_msg("table_name: " + str(table_name))
                    log_msg("pks: " + str(pks))
                    create_master_table(bq_client, project_id, table_name, pks)
    except Exception as e:
        err_str = "Exception in readCSV: " + str(e)
        throw_exception(err_str)

def create_master_table(bq_client, project_id, table_name, pks):
    try:
        if table_name and table_name.lower() != "none":
            ddl_master_tbl = f"""
            CREATE TABLE IF NOT EXISTS `{project_id}.data.master_table` (
                table_name STRING NOT NULL,
                primary_keys STRING,
                PRIMARY KEY(table_name) NOT ENFORCED
            )
            """
            execute_bq(bq_client, ddl_master_tbl)

            merge_ddl = f"""
            MERGE `{project_id}.data.master_table` AS target
            USING (SELECT '{table_name}' AS table_name, '{pks}' AS primary_keys) AS source
            ON target.table_name = source.table_name
            WHEN MATCHED THEN UPDATE SET target.primary_keys = source.primary_keys
            WHEN NOT MATCHED THEN INSERT (table_name, primary_keys) 
            VALUES (source.table_name, source.primary_keys)
            """
            execute_bq(bq_client, merge_ddl)
        else:
            log_msg("ERROR: Values for table_name are not given. Please update the master_table.csv appropriately.")
    except Exception as e:
        err_str = "Exception in insert_master_tbl: " + str(e)
        throw_exception(err_str)

def main():
    log_msg("Starting main function")
    global project_id, bucket, config_subpath, dc_config_file, csv_location, master_table_file_name, dag_table_ids, dtm

    project_id, bucket, config_subpath, dc_config_file, lumi_id, master_table_file_name, dag_table_ids = getArgs()

    bq_client = bigquery.Client()
    local_config_path = os.path.join("gfcc_dam_config_files", dc_config_file)
    table_ids, csv_location = readconfig(local_config_path)
    readCSV(bq_client, project_id, bucket, csv_location, dag_table_ids)

if __name__ == "__main__":
    main()
    
    
    
    
import os
import json
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Import your local function that was previously executed on Dataproc
# Ensure `main()` is defined inside this script or imported correctly
from gfcc_dam_dc_master_table import main

# Load environment variables
project_id = os.environ["GCP_PROJECT"]
bucket = os.environ["GCS_BUCKET"]

# Load config
file = 'gfcc_dam_dc_master_table_config.json'
file_path = Path(__file__).parents[1] / 'gfcc_dam_config_files' / file
with open(file_path, 'r') as f:
    config = json.load(f)

dag_config = config["dag_config"]

# DAG default args
default_args = {
    'owner': dag_config['owner'],
    'start_date': datetime.strptime(dag_config['start_date'], "%Y-%m-%d"),
    'email': dag_config['email'],
    'email_on_failure': dag_config['email_on_failure'],
    'tags': dag_config['tags']
}

# Define the DAG
with DAG(
    dag_id=dag_config['dag_id'],
    default_args=default_args,
    catchup=dag_config['catchup'],
    schedule_interval=dag_config['schedule_interval'],
    description='DAM Data Copier Master Table (Local Python Execution)',
    params={
        "master_table_file_name": "",
        "table_ids": ""
    }
) as dag:

    run_master_table_job = PythonOperator(
        task_id="run_master_table_job",
        python_callable=main
    )

    run_master_table_job