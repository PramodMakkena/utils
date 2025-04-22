import os
import sys
import json
from datetime import datetime
import pandas as pd
import numpy as np
from google.cloud import bigquery
from google.cloud import storage
from send_email import send_email

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import gfcc_dam_config_files.gfcc_dam_global_config as global_config
from gfcc_dam_common_utility_configuration.gfcc_dam_dag_utility import log_msg, sqlexecute_bq, readfrombucket, \
    bq_to_csv, throw_exception, get_schema_dict, read_json

global dttm


def getArgs(table_prefix, bucket, project_id):
    log_msg("Starting getArgs function to fetch arguments")
    try:
        file_prefix = "" if table_prefix == "None" else "_"
        config_subpath = f"gs://{bucket}/dags/lumiteam-usecases/gfcc_dam_config_files/"
        config_file = f"gfcc_dam_dc_config.json"
        lum_id = f"{globalconfig.lums_spec[project_id]}"
        config_file_path = f"{project_id}_{bucket}_{lum_id}_{table_prefix}"
        return config_file_path
    except Exception as e:
        throw_exception(f"Exception in getArgs: {e}")


def readcsv(dag_id, dc_ids, project_id, j_name, table_prefix, csv_location):
    try:
        dtm = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        chk_dc_ids = []
        log_msg("dc_ids: " + str(dc_ids))

        # Split dc_ids
        dc_ids = dc_ids.split(",")
        log_msg("dc_ids (split): " + str(dc_ids))

        # Read request CSV file from GCS
        dc_csv_location_path = f"gs://{project_id}/{csv_location}/{j_name}"
        log_msg(f"dc_csv_location_path: {dc_csv_location_path}")
        df = readfrombucket(dc_csv_location_path, "csv")
        df = df.dropna(subset=['dc_id']).reset_index(drop=True)

        if 'all' in dc_ids:
            dc_ids = df["dc_id"].unique().tolist()
        dc_ids = [int(k) for k in dc_ids]
        print("dc_ids (int):", dc_ids)

        for dc_id in dc_ids:
            valid_run = True
            start_dttm = datetime.utcnow().timestamp()
            for _, row in df.iterrows():
                if int(row["dc_id"]) == dc_id:
                    user_email = 'None' if row["user_email"] == '' else str(row["user_email"])
                    src_ds = 'None' if row["src_ds"] == '' else str(row["src_ds"])
                    src_db = 'None' if row["src_db"] == '' else str(row["src_db"])
                    src_tbl = 'None' if row["src_tbl"] == '' else str(row["src_tbl"])
                    tgt_ds = 'None' if row["tgt_ds"] == '' else str(row["tgt_ds"])
                    tgt_tbl = 'None' if row["tgt_tbl"] == '' else str(row["tgt_tbl"])
                    col_list = 'None' if row["col_list"] == '' else str(row["col_list"])
                    col_list = str(row["col_list"]).replace('~', ',') if str(row["col_list"]) != 'None' else '*'
                    where_cond = 'None' if row["where_cond"] == '' else str(row["where_cond"])
                    rfc_ds = 'None' if row["rfc_ds"] == '' else str(row["rfc_ds"])
                    rfc_tbl = 'None' if row["rfc_tbl"] == '' else str(row["rfc_tbl"])
                    rfc_key = 'None' if row["rfc_key"] == '' else str(row["rfc_key"])
                    limit_val = 'None' if row["limit"] == '' else str(int(row["limit"]))
                    csv_path = 'None' if row["csv_path"] == '' else str(row["csv_path"])
                    if valid_run:
                        status, failed_rsn, schema_changed, cols_str = create_tbl(
                            project_id, table_prefix, load_type, tgt_ds, tgt_tbl, src_ds,
                            src_db, src_tbl, col_list, where_cond, csv_path, dtm, limit_val,
                            file_name, rfc_ds, rfc_tbl, rfc_key
                        )
                    else:
                        status = 'failed'
                        schema_changed = False
                        cols_str = ''
                    dc_audit_table(project_id, table_prefix, dc_id, user_email, status, load_type, tgt_ds, tgt_tbl,
                                   tgt_tbl_name, file_name, failed_rsn,
                                   schema_changed, cols_str, start_dttm)

        if schema_changed:
            if load_type == "SNAPSHOT":
                # email_id = "slp.dam.test.automate+aaaapd3amrrak5g4aa4ss4dave@exp.org.slack.com"
                email_id = "Priyanka.chitalkar@wegexp.com"
                log_msg(f"INFO: Schema changed for {tgt_tbl_name} and email sent to {email_id}")
                send_notification(email_id, tgt_tbl_name, cols_str)

        if len(failed_rsn) > 0:
            chk_dc_ids.append(0)
        else:
            chk_dc_ids.append(1)

    if 1 in chk_dc_ids:
        log_msg("At least one dc_id successfully ran")
    else:
        throw_exception("All provided dc_ids are failed, so failing DAG")

    except Exception as e:
    throw_exception(f"Exception in readCSV: {e}")


def getWhere(where_cond):
    try:
        return '' if str(where_cond) == 'None' else where_cond
    except Exception as e:
        throw_exception(f"Exception in getWhere: {e}")


def getWhereCond(rfc_tbl, rfc_key):
    try:
        where_str = ""
        if ".csv" in rfc_tbl:
            pass
        else:
            print(f"Reading table: {rfc_tbl} for join condition based on rfc_key: {rfc_key}")
            if rfc_key and ":" in rfc_key:
                source_column, rfc_column = map(str.strip, rfc_key.split(':'))
                if source_column and rfc_column:
                    where_str = f"WHERE a.{source_column} = b.{rfc_column}"
                else:
                    print(f"Invalid rfc_key format: {rfc_key}. Expected 'source_column:rfc_column'.")
            else:
                print(f"Invalid rfc_key format: {rfc_key}. Expected 'source_column:rfc_column'.")

        return where_str
    except Exception as e:
        throw_exception( "Exception in getWhereCond: " + str(e))

def getLimit(limit_val):
    try:
        limit_val = int(limit_val) if int(limit_val) < 10000 else 10000
        return f" limit {limit_val}"
    except Exception as e:
        throw_exception(f"Exception in getLimit: {e}")


def get_df(project_id, csv_path, file_name):
    # Gets the path of given data.csv file and its columns names
    file_path = "gs://{}/{}/{}".format(project_id, csv_path, file_name)
    df = readfrombucket(file_path, "csv")
    csv_col_list = [k.lower() for k in df.columns]
    return file_path, csv_col_list


def create_tbl(project_id, table_prefix, load_type, tgt_ds, tgt_tbl, src_ds, src_db, src_tbl, col_list, where_cond,
               csv_path, dttm, limit_val, file_name, rfc_ds, rfc_tbl, rfc_key):
    log_msg("Creating table...")
    try:
        file_name = "{}.csv".format(str(src_tbl).strip()) if not file_name.lower() in file_name else file_name
        schema_changed = False
        csv_col_chk_str = ''

        if load_type.upper() == "SNAPSHOT":
            if all(v != "None" for v in [tgt_ds, tgt_tbl, file_name, csv_path]):
                return snapshot_table(project_id, table_prefix, tgt_ds, tgt_tbl, file_name, csv_path, schema_changed,
                                      csv_col_chk_str)
            else:
                log_msg("ERROR: Either target table details or file_name/csv_path details are missing in input file")
                return 'failed', False, '', ''

        elif load_type.upper() == "TI":
            if all(v != "None" for v in [tgt_ds, tgt_tbl, file_name, csv_path]):
                return ti_table(project_id, tgt_ds, tgt_tbl, file_name, csv_path, schema_changed, csv_col_chk_str)
            else:
                log_msg("ERROR: Either target table details or file_name/csv_path details are missing in input file")
                return 'failed', False, '', ''

        elif load_type.upper() == "BQ_TO_CSV":
            if all(v != "None" for v in [src_db, src_ds, src_tbl, col_list, csv_path]):
                if rfc_tbl:
                    where_str = getWhereCond(tgt_ds, tgt_tbl, src_db, src_ds, src_tbl, col_list, rfc_tbl,rfc_ds,rfc_key)
                else:
                    where_str = getWhere(where_cond)

                limit_str = getLimit(limit_val)
                if rfc_tbl:
                    join_condition = where_str.replace("WHERE ",
                                                       "") if where_str else ""
                    query = f"""
                        SELECT {col_list}
                        FROM {src_db}.{src_ds}.{src_tbl} a
                        JOIN {src_db}.{rfc_ds}.{rfc_tbl} b ON {join_condition}
                        {limit_str}
                        """
                bq_to_csv(project_id, src_db, src_ds, src_tbl, col_list, where_str, limit_str, csv_path)


                        
                return 'success', False, '', ''
            else:
                log_msg("ERROR: Either source table detail columns or csv_path details are missing in input file")
                return 'failed', False, '', ''

        else:
            return fail("ERROR: Please provide valid load_type (SNAPSHOT, TI, BQ_TO_CSV)")
    except Exception as e:
        err_str = "Exception in create_tbl: " + str(e)
        throw_exception(err_str)


def bq_to_csv(client: bigquery.Client, project_id: str, csv_path: str, file_name: str, query: str, sep: str = None):
    try:
        log_msg("Writing BQ query results to csv")
        csv_path = "/gfcc_dam_script/csv/" if (not csv_path or csv_path.lower() == "none") else csv_path
        file_path = "gs://{}/{}/{}".format(project_id, csv_path, file_name)
        log_msg(f"GCS Location: {file_path}")
        query_job = sqlexecute_bq(client, query)
        df = query_job.to_dataframe()
        sep = "," if sep is None else sep
        return df.to_csv(file_path, index=False, sep=sep)
    except Exception as e:
        log_msg("Exception: Writing BQ query results to csv in bq_to_csv " + str(e))


def datacopier_main(project_id, bucket, f_name, dag_dc_ids, table_prefix, csv_location, **kwargs):
    log_msg("Starting main function")
    global config_subpath, dc_config_file, project_ds, lumi_ds, bq_client, lumi_id, dc_ids, src_db, src_ds, src_tbl, tgt_ds, tgt_tbl, col_list, where_cond, load_type, csv_path, dttm, start_dttm, rfc_ds, rfc_tbl, rfc_key

    config_subpath, dc_config_file, lumi_id, table_prefix = getArgs(table_prefix, bucket, project_id)
    project_ds = global_config.project_ds[project_id]
    lumi_ds = global_config.lumi_ds[project_id]
    bq_client = bqClient_impersonation()
    log_msg(csv_location)
    readCSV(dag_dc_ids, project_id, f_name, table_prefix, csv_location)
