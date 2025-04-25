def bq_to_csv_table(project_id, src_db, src_ds, src_tbl, file_name, csv_path, col_list, where_cond, limit_val, schema_changed, csv_col_chk_str, rfc_ds, rfc_tbl, rfc_key, rfc_schema_json):
    limit_str = getLimit(limit_val)

    if rfc_tbl and ".csv" in rfc_tbl and rfc_schema_json:
        temp_table_id = createTempTable(bq_client, project_id, rfc_tbl, rfc_schema_json, csv_path)
        rfc_tbl = temp_table_id.split(".")[-1]
        rfc_ds = "temp"
        where_clause = getOnJoins(bq_client, project_id, rfc_ds, rfc_tbl, rfc_key)
        join_condition = where_clause.replace("WHERE ", "") if where_clause else ""
        where_str = getWhereCond(where_cond)
        prefixed_col_list = ",".join([f"a.{col}" for col in col_list.split(",")])
        sqlstr = f"""
            SELECT {prefixed_col_list}
            FROM `{src_db}.{src_ds}.{src_tbl}` a
            JOIN `{project_id}.temp.{rfc_tbl}` b ON {join_condition}{where_str}
            {limit_str}
        """

    elif len(rfc_tbl) > 0 and rfc_tbl != "None":
        where_clause = getOnJoins(bq_client, project_id, rfc_ds, rfc_tbl, rfc_key)
        join_condition = where_clause.replace("WHERE ", "") if where_clause else ""
        where_str = getWhereCond(where_cond)
        prefixed_col_list = ",".join([f"a.{col}" for col in col_list.split(",")])
        sqlstr = f"""
            SELECT {prefixed_col_list}
            FROM `{src_db}.{src_ds}.{src_tbl}` a
            JOIN `{project_id}.{rfc_ds}.{rfc_tbl}` b ON {join_condition}{where_str}
            {limit_str}
        """

    else:
        where_str = getWhereCond(where_cond)
        sqlstr = f"SELECT {col_list} FROM `{src_db}.{src_ds}.{src_tbl}`{where_str}{limit_str}"

    failed_rsn = bq_to_csv(bq_client, project_id, csv_path, file_name, sqlstr)

    if "_temp" in rfc_tbl:
        deleteTempTable(bq_client, f"{project_id}.temp.{rfc_tbl}")

    if failed_rsn:
        return fail_fn(failed_rsn, schema_changed, csv_col_chk_str)

    return "success", "", schema_changed, csv_col_chk_str
    
def process_rfc_key(rfc_key, rfc_column=None):
    try:
        if rfc_key:
            rfc_map = json.loads(rfc_key)
            source_column = list(rfc_map.keys())
            rfc_column = list(rfc_map.values())
            if len(source_column) != len(rfc_column):
                log_msg("Mismatch in number of source and rfc columns.")
                return ""

            join_condition = []
            for src, rfc in zip(source_column, rfc_column):
                join_condition.append(f"a.{src} = b.{rfc}")
            return f"({' AND '.join(join_condition)})"
        else:
            log_msg(f"Invalid rfc_key format: {rfc_key}. Expected JSON format.")
            return ""
    except Exception as e:
        throw_exception("Exception in process_rfc_key: " + str(e))


def deleteTempTable(bq_client, table_id):
    try:
        bq_client.delete_table(table_id, not_found_ok=True)
        log_msg(f"Deleted temporary table: {table_id}")
    except Exception as e:
        err_str = "Exception in deleteTempTable: " + str(e)
        throw_exception(err_str)
        
def createTempTable(bq_client, project_id, rfc_tbl, rfc_schema_json, csv_path):
    try:
        if rfc_tbl and ".csv" in rfc_tbl and rfc_schema_json:
            file_path = f"gs://{project_id}/{csv_path}/{rfc_tbl}"
            table_id = f"{project_id}.temp.{rfc_tbl.split('.')[0].replace('.csv', '')}_temp"
            schema_dict = ast.literal_eval(rfc_schema_json)
            schema_fields = [bigquery.SchemaField(column_name, column_datatype) for column_name, column_datatype in schema_dict.items()]
            schema = bigquery.Table(table_id, schema=schema_fields)
            table = bq_client.create_table(table, exists_ok=True)
            
            df = readFromBucket(file_path, "csv")
            extra_columns = list(set(df.columns) - set(schema_dict.keys()))
            
            if len(extra_columns) > 0:
                log_msg(f"INFO: Extra columns in CSV file but not defined in Schema JSON and will be ignored: {extra_columns}")
            df = df[[col for col in df.columns if col in schema_dict.keys()]]
            
            load_job = bq_client.load_table_from_dataframe(
                df, table_id,
                job_config=bigquery.LoadJobConfig(
                    schema=schema_fields,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
                )
            )
            load_job.result()
            log_msg(f"Temporary table created: {table_id}")
            return table_id
        else:
            log_msg("Temporary table creation skipped as rfc_tbl is not a CSV file or rfc_schema_json is None")
    except Exception as e:
        throw_exception(f"Exception in createTempTable: " + str(e))

def getOnJoins(bq_client, project_id, rfc_ds, rfc_tbl, rfc_key):
    try:
        where_str = ""
        table_ref = f"{project_id}.{rfc_ds}.{rfc_tbl}"
        table = bq_client.get_table(table_ref)
        rfc_columns = [field.name for field in table.schema]
        log_msg(f"INFO: Columns in RFC table: {rfc_columns}")
        where_str = process_rfc_key(rfc_key, rfc_columns)
        return where_str
    except Exception as e:
        throw_exception(f"Exception in getOnJoins: " + str(e))

def create_tbl(project_id, table_prefix, load_type, tgt_ds, tgt_tbl, bq_tbl, 
               src_db, src_ds, src_tbl, col_list, where_cond, csv_path, dttm, 
               limit_val, file_name, csv_path, schema_changed, csv_col_chk_str, 
               rfc_ds, rfc_tbl, rfc_key, rfc_schema_json):
    log_msg("Starting create_tbl logic...")
    try:
        file_name = "{}.csv".format(str(bq_tbl), dttm) if not file_name or file_name.lower() == "none" else file_name
        schema_changed = False
        csv_col_chk_str = ""

        if load_type.upper() == "SNAPSHOT":
            if all(var != "None" for var in [tgt_ds, tgt_tbl, file_name, csv_path, schema_changed]):
                return snapshot_table(project_id, tgt_ds, tgt_tbl, csv_path, file_name, schema_changed, csv_col_chk_str)
            else:
                return fail_fn("ERROR: Either target table details or file_name/csv_path details is/are missing in input file", schema_changed, csv_col_chk_str)

        elif load_type.upper() == "LT":
            if all(var != "None" for var in [tgt_ds, tgt_tbl, file_name, csv_path]):
                return lt_table(project_id, tgt_ds, tgt_tbl, file_name, csv_path, schema_changed, csv_col_chk_str)
            else:
                return fail_fn("ERROR: Either target table details or file_name/csv_path details is/are missing in input file", schema_changed, csv_col_chk_str)

        elif load_type.upper() == "BQ_TO_CSV":
            if all(var != "None" for var in [src_db, src_ds, src_tbl, col_list, where_cond, limit_val, 
                                             file_name, csv_path]):
                return bq_to_csv_table(project_id, src_db, src_ds, src_tbl, file_name, csv_path, col_list, 
                                       where_cond, limit_val, schema_changed, csv_col_chk_str, 
                                       rfc_ds, rfc_tbl, rfc_key, rfc_schema_json)
            else:
                return fail_fn("ERROR: Either source table details or columns or csv_path details is/are missing in input file", schema_changed, csv_col_chk_str)

    except Exception as e:
        throw_exception("Exception in create_tbl: " + str(e))