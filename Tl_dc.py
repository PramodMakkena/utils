import ast
from google.cloud import bigquery

def sqlexecute_bq(bq_client, query_str):
    """
    Helper function to execute a query via BigQuery client
    and yield results (like your existing sqlexecute_bq).
    """
    query_job = bq_client.query(query_str)
    return query_job.result()

def log_msg(msg):
    """
    Simple placeholder for your existing logging method.
    """
    print(msg)

def process_tl_load(project_id, tgt_db, tgt_ds, tgt_tbl, file_name, csv_path, load_type):
    """
    Example function for the 'TL' branch, using bqclient and master table schema.
    """

    # Only proceed if all needed details are present
    if (
        tgt_db != 'None'
        and tgt_ds != 'None'
        and tgt_tbl != 'None'
        and file_name != 'None'
        and csv_path != 'None'
        and load_type == "TL"
    ):
        # Construct the GCS file path
        file_path = f"gs://{csv_path}/{file_name}"

        # Initialize BQ client
        bq_client = bigquery.Client(project=project_id)

        valid_run = 0
        master_schema_dict = {}

        # 1) Fetch the schema definition from your master table
        schema_tracker_select_query = f"""
        SELECT schema_definition
        FROM `{project_id}.data.master_table`
        WHERE table_name = '{tgt_tbl}'
        """
        result = sqlexecute_bq(bq_client, schema_tracker_select_query)

        # 2) Parse the master schema from the query result
        schema_definition = None
        for row in result:
            schema_definition = row.schema_definition  # e.g., stored as JSON or Python dict-string

        if schema_definition and str(schema_definition) != 'None':
            # Convert the stored text/dict/JSON into a Python dict
            master_schema_dict = ast.literal_eval(schema_definition)
        else:
            log_msg("ERROR: Either schema_definition is missing in master table OR table entry not present inside master table")
            valid_run = 1

        # 3) If the master schema is present, perform the load
        if valid_run != 1:
            # Convert master_schema_dict {col_name: bq_type, ...} into BigQuery SchemaField objects
            schema_fields = []
            for col_name, col_type in master_schema_dict.items():
                schema_fields.append(bigquery.SchemaField(col_name, col_type))

            # Configure the load job to enforce the master schema
            load_job_config = bigquery.LoadJobConfig(
                schema=schema_fields,
                source_format=bigquery.SourceFormat.CSV,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, 
                skip_leading_rows=1  # if your CSV has a header row
                # Set other CSV options if needed (e.g. field_delimiter, quote_character, etc.)
            )

            # The target table
            table_id = f"{project_id}.{tgt_ds}.{tgt_tbl}"

            # 4) Kick off the load job from GCS -> BigQuery
            load_job = bq_client.load_table_from_uri(
                file_path,
                table_id,
                job_config=load_job_config
            )
            load_job.result()  # Waits for the job to complete

            log_msg(f"INFO: Loaded {file_path} into {table_id} (overwriting) using master schema.")
        else:
            log_msg("ERROR: Load job not initiated due to missing or invalid schema.")
    else:
        log_msg("ERROR: Either target table details or file_name/csv_path details are missing in input.")





import ast
from google.cloud import bigquery

elif load_type == "TL":
    if (tgt_db != 'None' and tgt_ds != 'None' and tgt_tbl != 'None' and file_name != 'None' and csv_path != 'None'):
        file_path = f"gs://{project_id}/{csv_path}/{file_name}"
        valid_run = 0
        master_schema_dict = {}

        # Fetch Schema Definition from Master Table
        schema_tracker_select_query = f"""
        SELECT schema_definition FROM `{project_id}.data.master_table`
        WHERE table_name = '{tgt_tbl}'
        """
        result = sqlexecute_bq(bq_client, schema_tracker_select_query)

        schema_definition = None
        for row in result:
            schema_definition = row.schema_definition
            if schema_definition and str(schema_definition) != 'None':
                master_schema_dict = ast.literal_eval(schema_definition)  # Convert stored schema string to dict
            else:
                log_msg(f"ERROR: Either schema_definition is missing in master table OR table entry not present inside master table")
                valid_run = 1

        if valid_run != 1:
            schema_fields = []
            csv_df = readfrombucket(spark, file_path, "csv")  # Read CSV into DataFrame
            csv_col_list = csv_df.columns

            updated_col_list = []
            csv_data_type_list = []

            for col_name, col_type in master_schema_dict.items():
                if col_name in csv_col_list:
                    schema_fields.append(bigquery.SchemaField(col_name, col_type))  # Define schema for BQ
                    updated_col_list.append(col_name)  # Maintain order as per master schema
                else:
                    log_msg(f"ERROR: Column {col_name} is missing from input CSV but present in master schema")

            # Ensure DataFrame follows master schema
            if updated_col_list:
                csv_df = csv_df[updated_col_list]  # Reorder columns as per schema
                csv_df = cast_df_fields(csv_df, dict(zip(updated_col_list, [col.field_type for col in schema_fields])))  # Cast types

                # Load Data into BigQuery
                bq_project_id = f"{project_id}"
                bq_dataset = f"{tgt_ds}"
                bq_table = f"{tgt_tbl}"

                job_config = bigquery.LoadJobConfig(
                    schema=schema_fields,
                    source_format=bigquery.SourceFormat.CSV,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                    skip_leading_rows=1
                )

                load_job = bq_client.load_table_from_dataframe(csv_df, f"{bq_project_id}.{bq_dataset}.{bq_table}", job_config=job_config)
                load_job.result()  # Wait for job completion

                log_msg(f"INFO: Loaded {file_name} into {bq_table} (overwriting) using master schema.")
            else:
                log_msg(f"ERROR: No valid columns found for loading into BigQuery.")

        else:
            log_msg(f"ERROR: Schema validation failed, data not written to BigQuery")
    else:
        log_msg(f"ERROR: Either target table details or file_name/csv_path details are missing in input file")


elif crt_rep == "TL":
            where_str = getWhereCond(tgt_ds, tgt_tbl, src_db, src_ds, src_tbl, col_list, key_cols, key_vals)
            limit_str = getLimit(limit_val)
            sqlstr = "select {} from `{}.{}.{}`{}{}".format(
                col_list, src_db, src_ds, src_tbl, where_str, limit_str
            )
            temp_tbl = "{}_temp_{}".format(tgt_tbl, dttm)

            df = spark.read.option("header", "true").option("multiline", "true").format("csv").load(f"gs://{file_name}")
            
            table_col_list = df.columns
            ddl_create_str = "create or replace table "
            sqlbkstr = "select * from `{}.{}.{}`".format(tgt_db, tgt_ds, tgt_tbl)
            ddl_backup = "{} `{}.{}.{}` as ({});".format(ddl_create_str, tgt_db, tgt_ds,
                                                         temp_tbl,
                                                         sqlbkstr)
            bq_client.query( ddl_backup)

            ddl_drop_tgt_table = "drop table " + tgt_db + "." + tgt_ds + "." + tgt_tbl
            bq_client.query( ddl_drop_tgt_table)

            # get table schema
            existing_schema_map, primary_key = get_schema_from_mastertable(client, project_id, temp_tgt_tbl_nm)
            final_schema = {}
            primary_key_constraint = f", PRIMARY KEY({primary_key})" if primary_key else ""

            # create final col list with col list being used as file name list for columns
            for col in table_col_list:
                col = col.strip()
                if col in existing_schema_map:
                    final_schema[col] = existing_schema_map[col]
                else:
                    final_schema[col] = "STRING"

            col_definitions = ", ".join(f"{col} {dtype}" for col, dtype in final_schema.items())

            # create target table with datatype from target table if already exist else default datatype as string for
            # new columns
            ddl_str_tgt = "create table if not exists `{}.{}.{}` ({}) {}".format(
                tgt_db, tgt_ds, tgt_tbl, col_definitions, primary_key_constraint
            )
            print(ddl_str_tgt)
            bq_client.query( ddl_str_tgt)

            # select temp target table
            sqlstrtgt = "SELECT {} FROM `{}.{}.{}`".format(
                table_col_list, tgt_db, tgt_ds, temp_tbl)

            # insert into target table from temp target table
            ddl_tgt_str = "INSERT INTO `{}.{}.{}` ({}) {}".format(
                tgt_db, tgt_ds, tgt_tbl, table_col_list, sqlstrtgt)
            bq_client.query( ddl_tgt_str)

            # Drop temp target table
            ddl_drop_tgt_table = "drop table " + tgt_db + "." + tgt_ds + "." + temp_tbl
            bq_client.query( ddl_drop_tgt_table)




add below function also in data copier:
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
