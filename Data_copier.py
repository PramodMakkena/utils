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

        # Loop through all the dc_ids from config
        for val in dc_ids:
            # "all" - process all rows in the CSV
            if val == "all":
                data = df.collect()
            elif val != "none":
                data = df.filter(df.dc_id == val).collect()
            else:
                log_msg("No data copied..")
                continue

            # Process each row for the current dc_id
            for row in data:
                dc_id = row.dc_id
                src_db = str(row.src_db)
                src_ds = str(row.src_ds)
                src_tbl = str(row.src_tbl)
                tgt_db = row.tgt_db
                tgt_ds = row.tgt_ds
                tgt_tbl = row.tgt_tbl
                col_list = str(row.col_list).replace('~', ',')
                if col_list == 'None':
                    col_list = '*'
                key_cols = str(row.key_cols)
                key_vals = str(row.key_vals)
                csv_path = str(row.csv_path)
                crt_rep = row.load_type
                limit_val = str(row.limit)
                file_name = str(row.file_name)
                schema_json = str(row.schema_json)

                # Print/log for debugging
                print(f"dc_id = {dc_id}")

                # If we reach here, we can proceed
                try:
                    create_tbl(
                        spark,
                        crt_rep,
                        tgt_db,
                        tgt_ds,
                        tgt_tbl,
                        src_db,
                        src_ds,
                        src_tbl,
                        col_list,
                        key_cols,
                        key_vals,
                        csv_path,
                        dttm,
                        limit_val,
                        file_name,
                        schema_json
                    )
                except Exception as e:
                    # This ensures that if something fails inside create_tbl,
                    # we only skip this iteration and continue with next row.
                    log_msg(f"ERROR while processing dc_id: {dc_id}. Skipping this record. Error details: {e}")
                    continue

    except Exception as e:
        err_str = "Exception in readCSV: " + str(e)
        throw_exception(err_str)


def createBkpTbl(tgt_db, tgt_ds, tgt_tbl, dttm):
    try:
        print("inside backup table function")
        # Example: creating a backup in the `temp` dataset (adjust as needed)
        bkp_tbl = (
                "create or replace table "
                + tgt_db
                + ".temp."
                + tgt_tbl
                + "_"
                + dttm
                + " as (select * from "
                + tgt_db
                + "."
                + tgt_ds
                + "."
                + tgt_tbl
                + ")"
        )
        print(bkp_tbl)
        sqlexecute_bq(bq_client, bkp_tbl)
    except Exception as e:
        err_str = "Exception in createBkpTbl" + str(e)
        print(err_str)


def checkTableExist(tgt_db, tgt_ds, tgt_tbl):
    """
    (If you use google.cloud.bigquery, you might prefer gfcc_dam_utility.check_table_exists)
    This is just an example in case you want a local function,
    but gfcc_dam_utility.check_table_exists is recommended.
    """
    try:
        print("Check if table exists...")
        # If using googleapiclient discovery: bq_client.tables().get(...)
        # If using google.cloud.bigquery:
        #   table_id = f"{tgt_db}.{tgt_ds}.{tgt_tbl}"
        #   bq_client.get_table(table_id)
        #   Return True if no exception
        bq_client.tables().get(
            projectId=tgt_db,
            datasetId=tgt_ds,
            tableId=tgt_tbl
        ).execute()
        return True
    except Exception as e:
        err_str = "Exception in checkTableExist: " + str(e)
        print(err_str)
        return False


def getWhereCond(tgt_ds, tgt_tbl, src_db, src_ds, src_tbl, col_list, key_cols, key_vals):
    try:
        print("Inside getWhereCond")
        where_str = ""
        if key_cols != 'None':
            kcols = key_cols.split('~~')
            key_col_values = key_vals.split('~~')
            sqlstr = ""
            key_col_ind = 0
            where_cond = ""
            for col in kcols:
                sqlstr = (
                        "select data_type from `"
                        + src_db
                        + "."
                        + src_ds
                        + ".INFORMATION_SCHEMA.COLUMNS` "
                        + "where table_name='"
                        + src_tbl
                        + "' and lower(column_name)= lower('"
                        + col
                        + "')"
                )
                res = sqlexecute_bq(bq_client, sqlstr)
                res_op = res.result()

                col_data_type = None
                for row in res_op:
                    col_data_type = str(row.data_type)

                col_val = str(key_col_values[key_col_ind]).split('#~#')
                col_values = ""
                for i, value in enumerate(col_val):
                    if i == 0:
                        col_values = f"cast('{value}' as {col_data_type})"
                    else:
                        col_values += f', cast("{value}" as {col_data_type})'
                if key_col_ind == 0:
                    where_cond = col + " in( " + col_values + ")"
                else:
                    where_cond = where_cond + " and " + col + " in( " + col_values + ")"
                key_col_ind += 1

            where_str = " where " + where_cond
        return where_str
    except Exception as e:
        err_str = "Exception in getWhereCond: " + str(e)
        throw_exception(err_str)


def getLimit(limit_val, default=False):
    try:
        if limit_val == 'None':
            limit_val = min(int(limit_val), 10000) if default else int(limit_val)
        else:
            limit_val = 1000 if default else None
        return f"Limit {limit_val}" if limit_val is not None else None
    except Exception as e:
        err_str = "Exception in getLimit: " + str(e)
        throw_exception(err_str)


def get_schema_dict(bq_client, lumi_id, lumi_ds, tgt_tbl):
    try:
        col_nm_str = ""
        data_type_str = ""

        schema_ddl = f"""select string_agg(column_name) as col_nm_str, 
                                string_agg(data_type) as data_type_str from(
                        select column_name, data_type, ordinal_position 
                        from `{lumi_id}.{lumi_ds}.INFORMATION_SCHEMA.COLUMNS`
                        where table_name = '{tgt_tbl}' 
                        order by ordinal_position)"""

        res = sqlexecute_bq(bq_client, schema_ddl)

        for row in res:
            if str(row.col_nm_str) != 'None':
                col_nm_str = row.col_nm_str
                data_type_str = row.data_type_str
            else:
                log_msg(f"ERROR: Table does not exist in the project space. Please check the table name provided")
                valid_run = 0

        if valid_run != 0:
            col_nm_list = col_nm_str.split(",")
            data_type_list = data_type_str.split(",")
            schema_definition = json.dumps(dict(zip(col_nm_list, data_type_list)))
            return schema_definition

    except Exception as e:
        err_str = "Exception in getting schema dictionary: " + str(e)
        throw_exception(err_str)


def create_tbl(
        spark, load_type, tgt_db, tgt_ds, tgt_tbl, csv_path, file_name, schema_json
):
    try:
        file_name = "{}_Ch.csv".format(tgt_tbl) if (not file_name or file_name.lower() == "none") else file_name

        if load_type.upper() == "TL":
            if all(var != "None" for var in [tgt_db, tgt_ds, tgt_tbl, file_name, csv_path]):
                file_path = "gs://{}/{}".format(csv_path, file_name)
                df = read_from_bucket(spark, file_path, "csv")
                csv_col_list = [x.lower() for x in df.columns]
                master_schema_dict = {}
                tgt_tbl_col_list = []
                tgt_tbl_chk_flag = 0
                valid_run = 1

                # schema_tracker_select_query = f"""
                #     SELECT schema_definition
                #     FROM `{project_id}.data.master_table`
                #     WHERE table_name = '{tgt_tbl}'
                # """
                # result = sql_execute_bq(bq_client, schema_tracker_select_query)
                # schema_definition = None

                # for row in result:
                schema_definition = get_schema_dict(bq_client, project_id, tgt_ds, tgt_tbl)

                if schema_definition and schema_definition.lower() != "none":
                    master_schema_dict = ast.literal_eval(schema_definition.lower())
                else:
                    log_msg(
                        f"ERROR: Either schema_definition is missing in master table OR table entry not present inside master table")
                    valid_run = 0

                if valid_run != 0:
                    ddl_col_str = f"""
                        SELECT STRING_AGG(column_name) as col_str 
                        FROM `{project_id}.{tgt_ds}.INFORMATION_SCHEMA.COLUMNS` 
                        WHERE table_name = '{tgt_tbl}'
                    """
                    res = sql_execute_bq(bq_client, ddl_col_str)

                    for row in res:
                        if str(row.col_str) != "None":
                            tgt_tbl_col_list = row.col_str.split(",")
                            tgt_tbl_chk_flag = 1
                            log_msg(f"INFO: Target Table `{project_id}.{tgt_ds}.{tgt_tbl}` already exists")
                        else:
                            log_msg(
                                f"INFO: Target Table `{project_id}.{tgt_ds}.{tgt_tbl}` doesn’t exist, will be newly created")

                col_chk_list = list(set(tgt_tbl_col_list) - set(csv_col_list))
                col_chk_list_t = list(set(csv_col_list) - set(tgt_tbl_col_list))

                if len(col_chk_list) > 0 or len(col_chk_list_t) > 0:
                    log_msg(f"ERROR: Target table schema is not matching with data.csv file")

                if valid_run != 0:
                    csv_data_type_list = []
                    for key in csv_col_list:
                        if key in master_schema_dict:
                            csv_data_type_list.append(master_schema_dict.get(key))
                        else:
                            log_msg(f"ERROR: column {key} not present in schema definition inside master table!")
                            return
                    schema_dict = json.dumps(dict(zip(csv_col_list, csv_data_type_list)))
                    schema_fields = []
                    for col_name, col_type in schema_dict.items():
                        schema_fields.append(bigquery.SchemaField(col_name, col_type))
                    load_job_config = bigquery.LoadJobConfig(schema=schema_fields,
                                                             source_format=bigquery.SourceFormat.CSV,
                                                             write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                                                             skip_leading_rows=1)
                    dest_tbl_id = f"{project_id}.{dest_ds}.{tgt_tbl}"
                    load_job = bq_client.load_table_from_uri(file_path, dest_tbl_id, job_config=load_job_config)
                    load_job.result()
                    log_msg(f"INFO: Loaded {file_path} into {dest_tbl_id} using master schema.")
                else:
                    log_msg(
                        "ERROR: Either target table details or file_name/csv_path details is/are missing in input file")

        elif load_type.upper() == "BQ_TO_CSV":
            if all(var != "None" for var in [src_db, src_ds, src_tbl, csv_path]):
                chk_flag = 0
                where_str = ""
                if key_vals != "None":
                    if key_cols != "None":
                        where_str = getWhereCond(tgt_ds, tgt_tbl, src_db, src_ds, src_tbl, col_list, key_cols, key_vals)
                    else:
                        chk_flag = 1
                        log_msg("ERROR: key_vals is present but key_cols is missing in input file")
                        return
                if chk_flag != 1:
                    limit_str = getLimit(limit_val, True)
                    sqlstr = "SELECT " + col_list + " FROM " + src_db + "." + src_ds + "." + src_tbl + where_str + limit_str
                    bq_to_csv(bq_client, project_id, csv_path, file_name, sqlstr)
            else:
                log_msg(
                    f"ERROR: Values for src_db/src_ds/src_tbl/csv_path is/are not given. Please update the CSV with appropriate values for this dc_id")
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
