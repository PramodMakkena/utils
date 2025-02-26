import sys
import json
from gfcc_dam_utility import *
import gfcc_dam_utility
from google.cloud import storage
from pyspark.sql.types import StringType, IntegerType, DecimalType, BooleanType, TimestampType, LongType, DateType
from pyspark.sql.functions import regexp_replace, col, to_timestamp
from datetime import datetime
import pandas as pd
import ast

global dttm


def getArgs():
    log_msg("------------------ Starting getArgs function to get arguments ------------------")
    try:
        # Fetching all the arguments
        args_dict = eval(sys.argv[1])
        project_id = args_dict["project_id"]
        bucket = args_dict["bucket"]
        f_name = args_dict["f_name"]
        print("args ", sys.argv)

        # config_subpath is just an example of subpath usage
        config_subpath = f"gs://{bucket}/dags/lumiteam-usecase/gfcc_dam_config_files/"
        dc_config_file = 'gfcc_dam_dc_config.json'

        # Reading Global Config
        gfcc_dam_utility.global_config_file_name = args_dict["global_config_file_name"]
        lumi_id = getLumiSpace(project_id)

        # Printing the received arguments
        log_msg('project_id: ' + str(project_id))
        log_msg('bucket: ' + str(bucket))
        log_msg('lumi_id: ' + str(lumi_id))
        log_msg("------------------ Getting arguments completed ------------------")
        return project_id, bucket, config_subpath, dc_config_file, lumi_id, f_name

    except Exception as e:
        err_str = "Exception in getArgs: " + str(e)
        throw_exception(err_str)


def readconfig():
    log_msg("------------------ Starting reading configurations to get variables from config  ------------------")
    try:
        with open(dc_config_file, 'r') as f:
            config = json.load(f)
        dc_ids = config['dc_ids']
        csv_location = config['csv_loc']
        return dc_ids, csv_location

    except Exception as e:
        err_str = "Exception in readconfig: " + str(e)
        throw_exception(err_str)


def readCSV(spark):
    """
    Reads the CSV with data_copier instructions, iterates over each dc_id,
    and processes each entry. If src_db, src_ds, src_tbl is missing or does not exist,
    it will log an error and continue with the next entry.
    """
    try:
        # Determine CSV location
        if len(f_name) > 0:
            dc_csv_location_path = f"gs://{project_id}{csv_location}{f_name}"
        else:
            dc_csv_location_path = f"gs://{project_id}{csv_location}data_copier.csv"

        log_msg(dc_csv_location_path)

        # Read CSV into DataFrame
        df = readfrombucket(spark, dc_csv_location_path, "csv")
        if df.count() == 0:
            log_msg("All rows are invalid. DAG stopped.")
            return

        # Generate a UTC timestamp for suffix naming
        dttm = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        print("Current timestamp for backups: ", dttm)

        data = df.collect()

        for row in data:
            tbl_name = str(row.tbl_name)
            pk = str(row.pk)
            clmn_names = str(row.clmn_names)

            try:
                create_tbl(
                    tbl_name,
                    pk,
                    clmn_names
                )
            except Exception as e:
                # This ensures that if something fails inside create_tbl,
                # we only skip this iteration and continue with next row.
                log_msg(f"ERROR while processing table: {tbl_name}. Skipping this record. Error details: {e}")
                continue

    except Exception as e:
        err_str = "Exception in readCSV: " + str(e)
        throw_exception(err_str)



def create_tbl(
        tbl_name,
        pk,
        clmn_names
):
    try:
        log_msg("------------------ Creating tables------------------")
        final_schema = {}
        primary_key_constraint = f", PRIMARY KEY({pk})" if pk else ""
        for col_info in clmn_names.split(","):
            col_info = col_info.strip()
            col, col_datatype = col_info.split(":")
            col = col.strip()
            col_datatype = col_datatype.strip()
            final_schema[col] = col_datatype

        col_definitions = "{" + ",".join(f"\"{col}\":\"{dtype}\""
                                         for col, dtype in final_schema.items()) + "}"

        ddl_str = "INSERT INTO `data.master_table` (table_name, schema_definition, primary_keys) VALUES({},{},{})".format(
            tbl_name, col_definitions, primary_key_constraint)
        print(ddl_str)
        bq_client.query(ddl_str)

        print(ddl_str)
        bq_client.query(ddl_str)

    except Exception as e:
        err_str = "Exception in create_tbl: " + str(e)
        throw_exception(err_str)


def main():
    log_msg("------------------ Starting main function ------------------")
    global spark, project_id, bucket, config_subpath, dc_config_file, csv_location
    global f_name, project_ds, lumi_id, bq_client, lumi_ds, dc_ids
    # These might be used if declared globally, or you can store them in a local scope

    # 1. Get parameters
    project_id, bucket, config_subpath, dc_config_file, lumi_id, f_name = getArgs()

    # 2. Get project DS & LUMI DS from config
    project_ds = getproject_ds(project_id)
    lumi_ds = getlumi_ds(project_id)

    # 3. Initialize Spark & BQ client
    spark = get_conn_spark()
    bq_client = get_conn_bq()

    # 4. Set up a temporary GCS bucket for BQ to Spark
    temp_bucket = project_id + "-temp"
    tempBucket(spark, bucket=temp_bucket)

    # 5. Read the config JSON (dc_config_file) to get CSV location and dc_ids
    dc_ids, csv_location = readconfig()
    print(dc_ids)
    print(csv_location)
    print(f_name)

    # 6. Read the data_copier CSV and process instructions
    readCSV(spark)


if __name__ == '__main__':
    main()
