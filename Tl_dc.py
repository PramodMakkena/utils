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
