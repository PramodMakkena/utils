import sys
import json
from gfcc_dam_utility import *
import gfcc_dam_utility
from google.cloud import storage
from pyspark.sql.types import StringType, IntegerType, DecimalType, BooleanType, TimestampType,LongType,DateType
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
        print( "args ",sys.argv)
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
    try:
        if len(f_name)>0:
            dc_csv_location_path = f"gs://{project_id}{csv_location}{f_name}"
        else:
            dc_csv_location_path = f"gs://{project_id}{csv_location}data_copier.csv"

        log_msg(dc_csv_location_path)

        df = readfrombucket(spark, dc_csv_location_path, "csv")

        dttm = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        print(dttm)

        for val in dc_ids:
            # "all" - To create all tables in data_copier.csv
            if val == "all":
                data = df.collect()
            elif val != "none":
                data = df.filter(df.dc_id == val).collect()
            else:
                log_msg("No data copied..")

            for row in data:
                dc_id=row.dc_id
                src_db=str(row.src_db)
                src_ds=str(row.src_ds)
                src_tbl=str(row.src_tbl)
                tgt_db=row.tgt_db
                tgt_ds=row.tgt_ds
                tgt_tbl=row.tgt_tbl
                col_list=str(row.col_list).replace('~',',')
                if col_list == 'None':
                    col_list = '*'
                key_cols=str(row.key_cols)
                key_vals=str(row.key_vals)
                csv_path = str(row.csv_path)
                crt_rep=row.load_type
                limit_val =str(row.limit)
                file_name = str(row.file_name)
                schema_json =str(row.schema_json)

                print("dc_id="+ str(dc_id))
                print("src_db="+ src_db)
                print("src_tbl="+ src_tbl)
                print("col_list="+ col_list)
                print("key_cols="+ key_cols)
                print("key_vals="+ key_vals)
                print("csv_path="+ csv_path)
                print("create_replace="+ crt_rep)

                create_tbl(spark,crt_rep,tgt_db,tgt_ds,tgt_tbl,src_db,src_ds,src_tbl,col_list,key_cols,key_vals,csv_path,dttm,limit_val,file_name,schema_json)
                

    except Exception as e:
        err_str = "Exception in readCSV: " + str(e)
        throw_exception(err_str)

def createBkpTbl(tgt_db,tgt_ds,tgt_tbl,dttm):
    try:
        print("inside backup table function")
        ##bkp_tbl = "create or replace table " + tgt_db + "." + tgt_ds + "." + tgt_tbl + "_" + dttm + " as (select * from "+ tgt_db + "." + tgt_ds + "." + tgt_tbl + ")"
        bkp_tbl = "create or replace table " + tgt_db + ".temp" + "." + tgt_tbl + "_" + dttm + " as (select * from "+ tgt_db + "." + tgt_ds + "." + tgt_tbl + ")"

        print(bkp_tbl)
        sqlexecute_bq(bq_client, bkp_tbl)
    except Exception as e:
        err_str = "Exception in createBkpTbl" + str(e)
        print(err_str)

def checkTableExist(tgt_db,tgt_ds,tgt_tbl):
    try:
        print("Check if table exists...")
        bq_client.tables().get(
            projectId=tgt_db,
            datasetId=tgt_ds,
            tableId=tgt_tbl).execute()
        return True
    except Exception as e:
        err_str = "Exception in checkTableExist: " + str(e)
        print(err_str)
        return False

def createDDL(ddl_str,tgt_db,tgt_ds,tgt_tbl,src_db,src_ds,src_tbl,col_list):
    try:
        print("Create DDL...")
        ddl_str = ddl_str + tgt_db + "." + tgt_ds + "." + tgt_tbl + " as (select " + col_list + " from " + src_db + "." + src_ds + "." + src_tbl + " limit 0)"
        print(ddl_str)
        sqlexecute_bq(bq_client, ddl_str)
    except Exception as e:
        err_str = "Exception in createDDL: " + str(e)
        throw_exception(err_str)

def getWhereCond(tgt_ds,tgt_tbl,src_db,src_ds,src_tbl,col_list,key_cols,key_vals):
    try:

        ##data_types = ['INTEGER','INT64','FLOAT','FLOAT64']

        print("Inside getWhereCond")
        where_str=""
        if key_cols!='None':
            kcols = key_cols.split('~~')
            print(kcols)
            key_col_values = key_vals.split('~~')
            print(key_col_values)
            sqlstr = ""
            key_col_ind=0
            where_cond = ""
            for col in kcols:
                print(col)
                sqlstr = "select data_type from `" + src_db + "." + src_ds + "." + "INFORMATION_SCHEMA.COLUMNS` where table_name='" + src_tbl + "' and lower(column_name)= lower('" + col + "')"
                print(sqlstr)
                res = sqlexecute_bq(bq_client, sqlstr)
                res_op = res.result()
                print("res_op",res_op)
                col_values = ""
                print(str(key_col_values[key_col_ind]))
                col_val = str(key_col_values[key_col_ind]).split('#~#')
                print(col_val)
                for row in res_op:
                    col_data_type = str(row.data_type)
                    #print(col_data_type)
                    ##if col_data_type in data_types:
                print("col_data_type",col_data_type)
                i = 0
                for value in col_val:
                    if i==0:
                        col_values = f"cast('{value}' as {col_data_type})"
                    else:
                        col_values = f'{col_values}, cast("{value}" as {col_data_type})'
                    i += 1
                if key_col_ind==0:
                    where_cond = col + " in( " + col_values + ")"
                else:
                    where_cond =  where_cond + " and " + col + " in( " + col_values + ")"
                key_col_ind += 1

            where_str = " where "+ where_cond
        return where_str
    except Exception as e:
        err_str = "Exception in getWhereCond: " + str(e)
        throw_exception(err_str)

def getLimit(limit_val):
    try:
        print("Inside getLimit")
        print(limit_val)

        if limit_val!='None':
            limit_str=" limit " + str(limit_val)
        else:
            limit_str=""

        print(limit_str)
        return limit_str

    except Exception as e:
        err_str = "Exception in getLimit: " + str(e)
        throw_exception(err_str)

def insertTbl(tgt_db,tgt_ds,tgt_tbl,src_db,src_ds,src_tbl,col_list,where_str,limit_str):
    try:
        print("inside insertTbl")
        ins_str = "insert into " + tgt_db + "." + tgt_ds + "." + tgt_tbl + " select " + col_list + " from " + src_db + "." + src_ds + "." + src_tbl + where_str + limit_str
        print(ins_str)
        ins_res = sqlexecute_bq(bq_client, ins_str)
    except Exception as e:
        err_str = "Exception in insertTbl: " + str(e)
        throw_exception(err_str)

def deleteTbl(tgt_db,tgt_ds,tgt_tbl,where_str):
    try:
        print("inside deleteTbl")
        del_str = "delete from " + tgt_db + "." + tgt_ds + "." + tgt_tbl + where_str
        print(del_str)
        del_res = sqlexecute_bq(bq_client, del_str)
    except Exception as e:
        err_str = "Exception in deleteTbl: " + str(e)
        throw_exception(err_str)
def create_tbl(spark,crt_rep,tgt_db,tgt_ds,tgt_tbl,src_db,src_ds,src_tbl,col_list,key_cols,key_vals,csv_path,dttm,limit_val,file_name,schema_json):
    log_msg("------------------ Creating tables------------------")
    try:
        file_name = "{}_{}.csv".format(src_tbl, dttm) if (not file_name or file_name.lower() == "none") else file_name

        if crt_rep.upper() == "SNAPSHOT":
            where_str = getWhereCond(tgt_ds, tgt_tbl, src_db, src_ds, src_tbl, col_list, key_cols, key_vals)
            limit_str = getLimit(limit_val)
            
            snapshot(bq_client,project_id,col_list,src_db,src_ds,src_tbl,csv_path,file_name,tgt_db,tgt_ds,tgt_tbl,where_str,limit_str)
        elif crt_rep == "TL":
            where_str = getWhereCond(tgt_ds, tgt_tbl, src_db, src_ds, src_tbl, col_list, key_cols, key_vals)
            limit_str = getLimit(limit_val)
            sqlstr = "select {} from `{}.{}.{}`{}{}".format(col_list,src_db,src_ds,src_tbl,where_str,limit_str)
            temp_tbl = "{}_temp_{}".format(tgt_tbl,dttm)

            bq_to_csv(bq_client, project_id, csv_path, file_name, sqlstr)
            df = readfrombucket(spark, "gs://{}{}{}".format(project_id, csv_path, file_name), "csv")
            writetablebq(df, project_id, tgt_ds, temp_tbl, "overwrite")
            truncate_load(bq_client, tgt_db, tgt_ds, tgt_tbl, temp_tbl)
        elif crt_rep == "BQ_TO_CSV":
            
            where_str = getWhereCond(tgt_ds,tgt_tbl,src_db,src_ds,src_tbl,col_list,key_cols,key_vals)
            limit_str = getLimit(limit_val)
            sqlstr = "select " + col_list + " from " + src_db + "." + src_ds + "." + src_tbl + where_str + limit_str

            bq_to_csv(bq_client, project_id, csv_path, file_name, sqlstr)
        elif crt_rep == "CSV_TO_BQ":
            file_path = f"gs://{project_id}/{csv_path}{file_name}"
            df = readfrombucket(spark, file_path, "csv")
            print(schema_json)
            csv_to_bq(df,schema_json,project_id,tgt_ds,tgt_tbl)

    except Exception as e:
        err_str = "Exception in create_tbl: " + str(e)
        throw_exception(err_str)


def main():
    log_msg("------------------ Starting main function ------------------")
    global spark, project_id, bucket, config_subpath, dc_config_file,  csv_location, f_name, project_ds, lumi_id, bq_client, lumi_ds, dc_ids, dc_id, src_db, src_ds, src_tbl, tgt_db, tgt_ds, tgt_tbl, col_list, key_cols, key_vals, crt_rep, csv_path, dttm

    project_id, bucket, config_subpath, dc_config_file, lumi_id, f_name = getArgs()

    project_ds = getproject_ds(project_id)
    lumi_ds = getlumi_ds(project_id)

    # Creating spark and bigquery client connection
    spark = get_conn_spark()
    bq_client = get_conn_bq()

    # Creating temporary bucket
    temp_bucket = project_id + "-temp"
    tempBucket(spark, bucket=temp_bucket)

    dc_ids, csv_location = readconfig()
    print(dc_ids)
    print(csv_location)
    print(f_name)

    readCSV(spark)


if __name__ == '__main__':
    main()
