import sys
import json
from gfcc_dam_utility import *
import gfcc_dam_utility
from google.cloud import storage, bigquery
from pyspark.sql.types import StringType, IntegerType, DecimalType, BooleanType, TimestampType, LongType, DateType
from pyspark.sql.functions import regexp_replace, col, to_timestamp
from datetime import datetime
import pandas as pd
import os

global dttm

def getArgs():
    try:
        # Fetching all the arguments
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

def readconfig():
    try:
        with open(dc_config_file, "r") as f:
            config = json.load(f)
            table_ids = config["table_ids"]
            csv_location = config["csv_loc"]
            return table_ids, csv_location
    except Exception as e:
        err_str = "Exception in readconfig: " + str(e)
        throw_exception(err_str)

def readCSV(spark):
    try:
        dc_csv_location_path = f"gs://{project_id}/{csv_location}master_table.csv"
        log_msg(dc_csv_location_path)
        df = readFromBucket(spark, dc_csv_location_path, "csv")
        dtm = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        log_msg(dttm)

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
                    data = df.collect()
                else:
                    data = df.filter(df.table_id == val).collect()
                
                for row in data:
                    table_id = row.table_id
                    table_name = str(row.table_name)
                    pks = row.pks if pd.notna(row.pks) else ""
                    log_msg("table_id: " + str(table_id))
                    log_msg("table_name: " + str(table_name))
                    log_msg("pks: " + str(pks))
                    create_master_table(spark, table_name, pks)
    except Exception as e:
        err_str = "Exception in readCSV: " + str(e)
        throw_exception(err_str)

def create_master_table(spark, table_name, pks):
    try:
        if all(var != "None" for var in [table_name]):
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
            WHEN NOT MATCHED THEN INSERT (table_name, primary_keys) VALUES (source.table_name, source.primary_keys)
            """
            execute_bq(bq_client, merge_ddl)
        else:
            log_msg("ERROR: Values for table_name is/are not given. Please update the master_table.csv with appropriate values for this table_id")
    except Exception as e:
        err_str = "Exception in insert_master_tbl: " + str(e)
        throw_exception(err_str)

def main():
    log_msg("Starting main function")
    global spark, project_id, bucket, config_subpath, dc_config_file, csv_location, master_table_file_name, dag_table_ids, table_ids, table_id, csv_path, dtm

    project_id, bucket, config_subpath, dc_config_file, lumi_id, master_table_file_name, dag_table_ids = getArgs()
    project_ds = getproject_ds(project_id)
    lumi_ds = getlumi_ds(project_id)

    # Creating spark and bigquery client connection
    spark = getSparkSession()
    bq_client = getBQClient()
    table_ids, csv_location = readconfig()
    readCSV(spark)

if __name__ == "__main__":
    main()