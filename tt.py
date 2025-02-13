import logging
import json
from importlib import import_module
import pyspark as pyspark
from pyspark.sql.functions import udf, lit, col
from pyspark.sql.types import StringType, IntegerType, DecimalType, BooleanType, TimestampType, LongType, DateType
import datetime
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import ast

global_config_file_name = None

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


################################################################################################
##    Title      - Run bigquery sql.py
##    Author     - GRC
##    Function   - To run any bigquery sql
################################################################################################

def log_msg(str1: str, *args: str):
    # TO print the looging statement debug being highest and critical being lowest
    log_level = {"debug": logger.debug, "error": logger.error,
                 "warning": logger.warning, "critical": logger.critical, "info": logger.info}
    level = args[0] if args else "info"
    str1 = str(str1)
    message = "LOG ->" + str(datetime.datetime.now()) + ":" + str1
    log_level[level](message)


def tempBucket(spark: pyspark.sql.SparkSession, bucket: str):
    # Use the Cloud Storage bucket for temporary BigQuery export data used by the connector.
    spark.conf.set('temporaryGcsBucket', bucket)


def throw_exception(err_str: str):
    log_msg(str(err_str), "error")
    raise Exception(err_str)


def readdatabq(spark: pyspark.sql.SparkSession, query: str) -> pyspark.sql.dataframe.DataFrame:
    # Use to load data based on provided query from bigquery table
    try:
        log_msg('Running query: {}'.format(query))
        spark.conf.set("viewsEnabled", "true")
        spark.conf.set("materializationDataset", "temp")
        df = spark.read.format('bigquery').load(query)
        return df
    except Exception as e:
        log_msg('Exception: Executing query in readDataBq' + str(e))
        throw_exception(e)


def readtablebq(spark: pyspark.sql.SparkSession, project_id: str, bq_dataset: str,
                bq_table: str) -> pyspark.sql.dataframe.DataFrame:
    # Use to read entire table from bigquery
    try:
        log_msg('Loading data from : {}.{}.{}'.format(project_id, bq_dataset, bq_table))
        df = spark.read.format('bigquery') \
            .option('table', '{}.{}.{}'.format(project_id, bq_dataset, bq_table)) \
            .load()
        return df
    except Exception as e:
        log_msg('Exception: Executing query in readTableBq' + str(e))
        throw_exception(e)


def writetablebq(df: pyspark.sql.dataframe.DataFrame, project_id: str, bq_dataset: str, bq_table: str, write_mode: str):
    # Use to write dataframe back to bigquery table
    try:
        log_msg('Writing value of dataframe to bigquery table: {}.{}.{}'.format(project_id, bq_dataset, bq_table))
        df.write.format('bigquery').option('table', '{}.{}.{}'.format(project_id, bq_dataset, bq_table)).mode(
            write_mode).save()
    except Exception as e:
        log_msg('Exception: Executing query in writetablebq' + str(e))
        throw_exception(e)


def readfrombucket(spark: pyspark.sql.SparkSession, file_path: str,
                   file_format: str) -> pyspark.sql.dataframe.DataFrame:
    # Use to read file from GCS bucket
    try:
        log_msg('Reading file from GCS bucket {}'.format(file_path))
        if file_format == "csv":
            df = spark.read.option("header", "true").option("multiline", "true").format(file_format).load(file_path)
        else:
            df = spark.read.format(file_format).load(file_path)
        return df
    except Exception as e:
        log_msg('Exception: Executing query in readfrombucket' + str(e))
        throw_exception(e)


def writetobucket(df: pyspark.sql.dataframe.DataFrame, file_path: str, file_format: str):
    # Use to write dataframe to GCS bucket
    try:
        log_msg('Writing dataframe to GCS bucket {}'.format(file_path))
        df.write.mode("overwrite").format(file_format).save(file_path)
    except Exception as e:
        log_msg('Exception: Executing query in writetobucket' + str(e))
        throw_exception(e)


def sqlexecute_spark(spark: pyspark.sql.SparkSession, sql_str: str) -> pyspark.sql.dataframe.DataFrame:
    # Use to execute any spark sql query
    df = ""
    log_msg(sql_str)
    try:
        df = spark.sql(sql_str)
        return df
    except Exception as e:
        log_msg('Exception: Executing HQL in sqlExecute' + str(e))
        throw_exception(e)


def get_conn_spark() -> pyspark.sql.SparkSession:
    try:
        spark = pyspark.sql.SparkSession.builder.appName("grc_odl_connection_setup").enableHiveSupport().getOrCreate()
        log_msg('-------------------------------------')
        log_msg(spark.sparkContext.applicationId)
        log_msg('-------------------------------------')
        sqlstr = """set spark.hadoop.hive.exec.dynamic.partition.mode=nonstrict"""
        sqlexecute_spark(spark, sqlstr)
        sqlstr1 = """set spark.sql.parquet.datetimeRebaseModeInWrite = CORRECTED"""
        sqlexecute_spark(spark, sqlstr1)
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    except Exception as e:
        log_msg('Exception:' + str(e))
        throw_exception(e)


def readETLLoadTableBQ(client: bigquery.Client, project_id: str, dataset: str,
                       etlodlid: str) -> bigquery.table.RowIterator:
    # Used to read config table to get all details for each required block using BigQuery Client
    try:
        config = import_module(global_config_file_name).config_tables
        masterEtlQuery = f"""SELECT dataset_id,db_filepath,table_file_name,phase,actual_query,query_sequence 
						FROM `{project_id}.{dataset}.{config["config_main_table"]}` a 
						join `{project_id}.{dataset}.{config["config_query_table"]}` b on a.etlodl_id = b.etlodl_id 
						where a.etlodl_id="{etlodlid}" order by query_sequence asc"""
        df = client.query(masterEtlQuery)
        return df
    except Exception as e:
        log_msg('Exception: in function dataExtraction' + str(e))
        throw_exception(e)


def get_conn_bq() -> bigquery.Client:
    # Used to create BigQuery Client Session
    try:
        client = bigquery.Client()
        log_msg("BigQuery Client Connection Established")
        log_msg('-------------------------------------')
        return client
    except Exception as e:
        log_msg('Exception:' + str(e))
        throw_exception(e)


def sqlexecute_bq(client: bigquery.Client, sql_query: str) -> bigquery.table.RowIterator:
    # Used to run any query on BigQuery Table
    try:
        log_msg("sql_query - " + sql_query)
        query_job = client.query(sql_query)
        query_job.result()
        if query_job.errors:
            for error in query_job.errors:
                error_msg = error["message"]
                throw_exception("BigQuery Error: {}".format(error_msg))
        else:
            log_msg("SQL Query Executed Successfully")
        return query_job
    except Exception as e:
        log_msg('Exception: Executing query in dmlTblBq' + str(e))
        throw_exception(e)


def dfreturn_bq(client: bigquery.Client, sql_query: str) -> bigquery.table.RowIterator:
    # Used to run any query on BigQuery Table and return a DF
    try:
        log_msg("sql_query - " + sql_query)
        df = client.query(sql_query)
        return df
    except Exception as e:
        log_msg('Exception: Executing query in dmlTblBq' + str(e))
        throw_exception(e)


def getproject_ds(project_id: str) -> str:
    # Used to get the project dataset
    try:
        project_ds_v = import_module(global_config_file_name).project_ds
        if project_id in project_ds_v.keys():
            return project_ds_v[project_id]
        else:
            throw_exception("Project Dataset Not Found")
    except Exception as e:
        log_msg('Exception: Executing query in getproject_ds' + str(e))
        throw_exception(e)


def getlumi_ds(project_id: str) -> str:
    try:
        lumi_ds_v = import_module(global_config_file_name).lumi_ds
        if project_id in lumi_ds_v.keys():
            return lumi_ds_v[project_id]
        else:
            throw_exception("Project Dataset Not Found")
    except Exception as e:
        log_msg('Exception: Executing query in getlumi_ds' + str(e))
        throw_exception(e)


def getLumiSpace(project_id: str) -> str:
    # Used to get the Lumi central warehouse with respect to the project_id
    try:
        lumi_space_v = import_module(global_config_file_name).lumi_space
        if project_id in lumi_space_v.keys():
            return lumi_space_v[project_id]
        else:
            throw_exception("Project Space Not Found")
    except Exception as e:
        log_msg('Exception: Executing query in getLumiSpace' + str(e))
        throw_exception(e)


def check_table_exists(client: bigquery.Client, project_id: str, dataset: str, table: str):
    try:
        table_id = "{project_id}.{dataset}.{table}".format(project_id=project_id, dataset=dataset, table=table)
        log_msg(
            "Checking table {project_id}.{dataset}.{table} if exists.".format(project_id=project_id, dataset=dataset,
                                                                              table=table))
        table = client.get_table(table_id)  # Make an API request.
        log_msg("Table {} already exists.".format(table))
    except NotFound:
        log_msg("Table {} is not found.".format(table))


def get_num_rows(client: bigquery.Client, project_id: str, dataset: str, table: str) -> int:
    try:
        table_id = "{project_id}.{dataset}.{table}".format(project_id=project_id, dataset=dataset, table=table)
        log_msg(
            "Checking table {project_id}.{dataset}.{table} properties.".format(project_id=project_id, dataset=dataset,
                                                                               table=table))
        table = client.get_table(table_id)  # Make an API request.
        row_num = table.num_rows
        log_msg("Table has {} rows".format(row_num))
        return row_num
    except NotFound:
        log_msg("Table {} is not found.".format(table))


def get_tbl_schema(client: bigquery.Client, project_id: str, dataset: str, table: str) -> bigquery.schema.SchemaField:
    try:
        table_id = "{project_id}.{dataset}.{table}".format(project_id=project_id, dataset=dataset, table=table)
        log_msg(
            "Checking table {project_id}.{dataset}.{table} properties.".format(project_id=project_id, dataset=dataset,
                                                                               table=table))
        table = client.get_table(table_id)  # Make an API request.
        log_msg("Table description: {}".format(table.description))
        log_msg("Table schema: {}".format(table.schema))
        return table.schema
    except NotFound:
        log_msg("Table {} is not found.".format(table))


def get_column_names(client: bigquery.Client, project_id: str, dataset: str, table_name: str) -> list:
    sql_query = f""" SELECT column_name from `{project_id}.{dataset}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` AS paths WHERE paths.table_name = '{table_name}' """
    query_job = sqlexecute_bq(client, sql_query)
    columns = [row.column_name.lower() for row in query_job.result()]
    return columns


def cancel_job(client: bigquery.Client, job_id: str):
    try:
        log_msg("Cancelling job: {}".format(job_id))
        job = client.cancel_job(job_id, location="us")
    except NotFound:
        log_msg("Job {} is not found.".format(job_id))


""" Consolidation Script Functions"""


def generate_sde_tag_col(column_name: str, sde_role: str, lumi_space_name: str, lumi_ds: str) -> str:
    print(column_name, sde_role)
    if sde_role is None:
        return column_name
    else:
        return f"`{lumi_space_name}.{lumi_ds}`.decrypt_sde('{sde_role}', {column_name}) as {column_name} "


def generate_query(spark: pyspark.sql.SparkSession, project_id: str, schema: str,
                   tableName: str) -> pyspark.sql.dataframe.DataFrame:
    sql_query = f"SELECT column_name, description as sde_tag from `{project_id}.{schema}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` AS paths WHERE paths.table_name = '{tableName}' order by sde_tag desc;"

    print("********************* Triggering Query ********************* ")
    query_df = readdatabq(spark, sql_query)
    query_df.show(100, truncate=False)  # Can be removed later

    lumi_project_name = getLumiSpace(project_id)
    lumi_ds = "dw"  # getlumi_ds(project_id)

    udf_generate_sde_tag_col = udf(generate_sde_tag_col, StringType())
    final_df = query_df.withColumn("decrypt_string",
                                   udf_generate_sde_tag_col(query_df["column_name"], query_df["sde_tag"],
                                                            lit(lumi_project_name), lit(lumi_ds))).filter(
        "sde_tag IS NOT NULL AND trim(sde_tag) <> '' ")
    final_df.show(100, truncate=False)  # Can be removed later

    return final_df


def get_dict_from_df(inputDf: pyspark.sql.dataframe.DataFrame, dictKeyCol: str, dictValueCol: str) -> dict:
    convDict = inputDf.toPandas().to_dict(orient='list')
    listLength = len(convDict[dictKeyCol])

    decryptDict = dict()
    for x in range(listLength):
        decryptDict[convDict[dictKeyCol][x]] = convDict[dictValueCol][x]

    log_msg(f"\nDecrypt Tag Dictionary ->\n{decryptDict}")
    return decryptDict


def string_replacer(inputString: str, replaceDict: dict) -> str:
    for oldCol, newName in replaceDict.items():
        inputString = inputString.replace(oldCol, newName)
    return inputString


def alias_changer(colName: str, replaceDict: dict, decryptDict: dict) -> str:
    replaceDictKeys = list(replaceDict.keys())
    decryptDictKeys = list(decryptDict.keys())

    if colName in decryptDictKeys and colName in replaceDictKeys:
        return decryptDict[colName].replace(f"as {colName}", f"AS {replaceDict[colName]}")
    elif colName in replaceDictKeys:
        return f"{colName} AS {replaceDict[colName]}"
    elif colName in decryptDictKeys:
        return decryptDict[colName]
    else:
        return colName


def get_regex_replace_string(replaceKeyList: list) -> str:
    replaceRegexString = ''
    if len(replaceKeyList) > 0:
        replaceKeyListRegex = ['[,]*"{}":{{}}[,]*'.format(key) for key in replaceKeyList]
        replaceRegexString = "|".join(replaceKeyListRegex)
    log_msg(f"INFO: Replace Regex String ->\n{replaceRegexString}")
    return replaceRegexString


def get_block_replace_null(mapping_from_field: dict) -> tuple:
    replace_str_list = []
    for k in mapping_from_field.keys():
        replace_str_list.append('[,]*"' + k + '":{}[,]*')
        replace_str_list.append('[,]*"' + k + r'":\[\{\}\][,]*')
    return mapping_from_field.keys(), "|".join(replace_str_list)


def open_json_file(file_path: str) -> dict:
    '''
    function to open json file
    params: file_path
    return: json file data
    '''
    try:
        with open(file_path) as json_file:
            json_data = json.load(json_file)
            return json_data
    except Exception as e:
        log_msg("Exception in open_json_file: " + str(e))
        throw_exception(e)


def getproject_tempds(project_id: str) -> str:
    # Used to get the project temp dataset
    temp_ds = None
    try:
        project_tempds_v = import_module(global_config_file_name).project_tempds
        if project_id in project_tempds_v.keys():
            temp_ds = project_tempds_v[project_id]
        else:
            throw_exception("Project Temp Dataset Not Found")
    except Exception as e:
        log_msg('Exception: Executing in getproject_tempds' + str(e))
        throw_exception(e)
    return temp_ds


def bq_to_csv(client: bigquery.Client, project_id: str, csv_path: str,
              file_name: str, query: str, sep: str = None):
    try:
        log_msg("Writing BQ query results to csv")
        log_msg("Sql statement: {}".format(query))
        csv_path = "/gfcc_dam_script/csv/" if (not csv_path or csv_path.lower() == "none") else csv_path
        file_path = "gs://{}{}{}".format(project_id, csv_path, file_name)
        log_msg("GCS location: {}".format(file_path))

        query_job = sqlexecute_bq(client, query)
        df = query_job.to_dataframe()
        if df[['src_db', 'src_tbl']].isnull().any().any():
            error_msg = "Null values found in `src_db` or `src_tbl`. Execution stopped."
            log_msg(error_msg)
            return {"status": "error", "message": error_msg}
        sep = "," if sep is None else sep

        return df.to_csv(file_path, index=False, sep=sep)
    except Exception as e:
        log_msg("Exception: Writing BQ query results to csv in bq_to_csv " + str(e))


def csv_to_bq(df: pyspark.sql.dataframe.DataFrame, schema_json: str, project_id: str, tgt_ds: str, tgt_tbl: str):
    # Transforms a df based on a schema and loads it into a BQ table
    try:
        if schema_json:
            schema = ast.literal_eval(schema_json)
            print(schema)
            for key, value in schema.items():
                if value.upper() in ["INTEGER", "INT", "INT64"]:
                    df = df.withColumn(key, col(key).cast(LongType()))
                elif value.upper() in ["FLOAT", "NUMERIC", "FLOAT64"]:
                    df = df.withColumn(key, col(key).cast(DecimalType(30, 9)))
                elif value.upper() == "DATE":
                    df = df.withColumn(key, col(key).cast(DateType()))
                elif value.upper() in ["BOOLEAN", "BOOL"]:
                    df = df.withColumn(key, col(key).cast(BooleanType()))
                elif value.upper() == "TIMESTAMP":
                    df = df.withColumn(key, col(key).cast(TimestampType()))
                elif value.upper() == "DATETIME":
                    df = df.withColumn(key, col(key).cast(TimestampType()))
        prj_id = f"{project_id}"
        bq_dataset = f"{tgt_ds}"
        bq_table = f"{tgt_tbl}"
        writetablebq(df, prj_id, bq_dataset, bq_table, "overwrite")
    except Exception as e:
        log_msg("Exception: Writing CSV data to BQ table in csv_to_bq " + str(e))
        throw_exception(e)


def truncate_load(client: bigquery.Client, project_id: str, bq_dataset: str,
                  bq_table: str, temp_bq_table: str):
    try:
        truncate_load_str = """
				create or replace table `{}.{}.{}` as 
				(select * from `{}.{}.{}`);
			""".format(project_id, bq_dataset, bq_table, project_id, bq_dataset, temp_bq_table)
        log_msg("Truncate load statement: {}".format(truncate_load_str))

        sqlexecute_bq(client, truncate_load_str)

        drop_str = "drop table `{}.{}.{}`;".format(project_id, bq_dataset, temp_bq_table)
        log_msg("Drop table statement: {}".format(drop_str))

        sqlexecute_bq(client, drop_str)
    except Exception as e:
        log_msg("Exception in truncate_load: {}".format(e))


def get_schema_from_mastertable(client: bigquery.Client, project_id: str, table_name: str):
    query = f"""
        SELECT schema_definition, primary_keys
        FROM `{project_id}.data.mastertable`
        WHERE tablename = '{table_name}'
    """
    schema = client.query(query)
    result = schema.result()

    schema_dict = {}
    primary_key = []

    for row in result:
        schema_json = json.loads(row["schema_definition"])
        primary_key = row["primary_keys"]
        schema_dict.update(schema_json)

    return schema_dict, primary_key

def snapshot(client: bigquery.Client, project_id: str, table_col_list: str, bq_src_db: str, bq_src_dataset: str,
             bq_src_table: str,
             csv_path: str, file_name: str, bq_target_db: str, bq_target_dataset: str, bq_target_table: str,
             where_str: str = None, limit_str: str = None):
    try:
        sqlstr = "select {} from `{}.{}.{}` {} {}".format(table_col_list, bq_src_db, bq_src_dataset, bq_src_table,
                                                          where_str, limit_str)
        bq_to_csv(client, project_id, csv_path, file_name, sqlstr)
        temp_tbl_nm = f"temp_{bq_src_table}"
        ddl_create_str = "create or replace table "
        ddl_temp_str = "{} `{}.{}.{}` as ({});".format(ddl_create_str, bq_target_db, bq_target_dataset, temp_tbl_nm,
                                                       sqlstr)
        sqlexecute_bq(client, ddl_temp_str)

        temp_tgt_tbl_nm = f"temp_{bq_target_table}"
        sqlbkstr = "select * from `{}.{}.{}`".format(bq_target_db, bq_target_dataset, bq_target_table)
        ddl_backup = "{} `{}.{}.{}` as ({});".format(ddl_create_str, bq_target_db, bq_target_dataset, temp_tgt_tbl_nm,
                                                       sqlbkstr)
        sqlexecute_bq(client, ddl_backup)

        ddl_drop_tgt_table = "drop table " + bq_target_db + "." + bq_target_dataset + "." + bq_target_table
        sqlexecute_bq(client, ddl_drop_tgt_table)

        # get table schema
        existing_schema_map, primary_key = get_schema_from_mastertable(client, project_id, temp_tgt_tbl_nm)
        final_schema = {}
        primary_key_constraint = f", PRIMARY KEY({primary_key})" if primary_key else ""

        # create final col list with col list being used as file name list for columns
        for col in table_col_list.split(","):
            col = col.strip()
            if col in existing_schema_map:
                final_schema[col] = existing_schema_map[col]
            else:
                final_schema[col] = "STRING"

        col_definitions = ", ".join(f"{col} {dtype}" for col, dtype in final_schema.items())

        # create target table with dadatype from target table if already exist else default datatype as string for new columns
        ddl_str_tgt = "create table if not exists `{}.{}.{}` ({}) {}".format(
            bq_target_db, bq_target_dataset, bq_target_table, col_definitions, primary_key_constraint
        )
        print(ddl_str_tgt)
        sqlexecute_bq(client, ddl_str_tgt)

        # select temp target table
        sqlstrtgt = "SELECT {} FROM `{}.{}.{}`".format(
            table_col_list, bq_target_db, bq_target_dataset, temp_tgt_tbl_nm)

        # insert into target table from temp target table
        ddl_tgt_str = "INSERT INTO `{}.{}.{}` ({}) {}".format(
            bq_target_db, bq_target_dataset, bq_target_table, table_col_list, sqlstrtgt)
        sqlexecute_bq(client, ddl_tgt_str)

        # Drop temp target table
        ddl_drop_tgt_table = "drop table " + bq_target_db + "." + bq_target_dataset + "." + temp_tgt_tbl_nm
        sqlexecute_bq(client, ddl_drop_tgt_table)

        if len(where_str) > 0:
            del_str = "delete from " + bq_target_db + "." + bq_target_dataset + "." + bq_target_table + where_str
            sqlexecute_bq(client, del_str)
        ins_str = "insert into " + bq_target_db + "." + bq_target_dataset + "." + bq_target_table + " select " + table_col_list + " from " + bq_target_db + "." + bq_target_dataset + "." + temp_tbl_nm
        sqlexecute_bq(client, ins_str)
        ddl_drop_table = "drop table " + bq_target_db + "." + bq_target_dataset + "." + temp_tbl_nm
        sqlexecute_bq(client, ddl_drop_table)
    except Exception as e:
        log_msg("Exception in snapsot " + str(e))
