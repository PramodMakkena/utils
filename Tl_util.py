def truncate_load(spark: pyspark.sql.SparkSession, client: bigquery.Client,
                  bq_target_db: str, bq_target_dataset: str, bq_target_table: str, temp_tgt_tbl_nm: str, file_name: str,
                  project_id: str):
    try:
        df = readfrombucket(spark, f"gs://{file_name}", "csv")
        table_col_list = df.columns
        ddl_create_str = "create or replace table "
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


