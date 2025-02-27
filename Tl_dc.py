def create_master_table(spark, lumi_id, lumi_ds, table_name, pks):
    """
    Creates a master table in the database if it does not exist.
    """
    log_msg("--- Inserting into master table ---")
    
    try:
        # Initialize variables
        col_nm_list, data_type_list, schema_definition = "", "", ""
        valid_run = 1

        # Define the SQL query to fetch table schema information
        schema_ddl = f"""
            SELECT STRING_AGG(column_name) AS col_nm_str,
                   STRING_AGG(data_type) AS data_type_str
            FROM `{project_id}.{lumi_id}.{lumi_ds}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_name}'
            ORDER BY ordinal_position
        """
        
        # Execute schema query
        res = sqlexecute_bq(bq_client, schema_ddl)
        
        # Extract column names and data types from result
        for row in res:
            if row.col_nm_str:
                col_nm_list = row.col_nm_str
                data_type_list = row.data_type_str
            else:
                log_msg("ERROR: Either table is not present in Lumi OR table exists without schema")
                valid_run = 0
        
        # Generate schema definition if valid
        if valid_run:
            col_nm_list = col_nm_list.split(",")
            data_type_list = data_type_list.split(",")
            schema_definition = str(dict(zip(col_nm_list, data_type_list)))
            json_schema = json.dumps(schema_definition)
            
            # Delete existing entry if any
            delete_ddl = f"DELETE FROM `{project_id}.data.master_table` WHERE table_name = '{table_name}'"
            sqlexecute_bq(bq_client, delete_ddl)
            
            # Insert new entry
            insert_ddl = f"""
                INSERT INTO `{project_id}.data.master_table`
                (table_name, primary_keys, schema_definition)
                VALUES ('{table_name}', '{pks}', '{json_schema}')
            """
            sqlexecute_bq(bq_client, insert_ddl)

    except Exception as e:
        err_str = "Exception in insert_master_tbl: " + str(e)
        throw_exception(err_str)

def create_master_table(spark, lumi_id, lumi_ds, table_name, pks):
    """
    Creates or updates an entry in the master table with schema information for the specified table.
    """
    # Log the start of the master table creation process
    log_msg("inserting into master table-----------")
    try:
        # Initialize strings for column names and data types
        col_nm_str = ""
        data_type_str = ""
        valid_run = 1

        # Create the master table if it does not exist
        ddl_master_tbl = f"""CREATE TABLE IF NOT EXISTS `{project_id}.data.master_table` (
                                table_name STRING NOT NULL,
                                primary_keys STRING,
                                schema_definition STRING,
                                PRIMARY KEY(table_name) NOT ENFORCED
                             )"""
        sqlexecute_bq(bq_client, ddl_master_tbl)

        # Build a query to retrieve all column names and data types for the specified table
        schema_ddl = f"""SELECT STRING_AGG(column_name) AS col_nm_str,
                                STRING_AGG(data_type) AS data_type_str
                         FROM (
                                SELECT column_name, data_type, ordinal_position
                                FROM `{lumi_id}.{lumi_ds}.INFORMATION_SCHEMA.COLUMNS`
                                WHERE table_name = '{table_name}'
                                ORDER BY ordinal_position
                         )"""
        # Execute the schema query and get results
        res = sqlexecute_bq(bq_client, schema_ddl)

        # Extract concatenated column names and data types from the result
        for row in res:
            if str(row.col_nm_str) != 'None':
                col_nm_str = row.col_nm_str
                data_type_str = row.data_type_str
            else:
                valid_run = 0

        # If no schema information is found, log an error and exit
        if valid_run != 1:
            log_msg(f"ERROR: Either table is not present in {lumi_id} or table exists without schema")
            return

        # Construct schema definition as a dictionary and convert it to a JSON string
        col_nm_list = col_nm_str.split(",")
        data_type_list = data_type_str.split(",")
        schema_definition = dict(zip(col_nm_list, data_type_list))
        schema_json = json.dumps(schema_definition)

        # Remove any existing entry for this table in the master table
        delete_ddl = f"DELETE FROM `{project_id}.data.master_table` WHERE table_name = '{table_name}'"
        sqlexecute_bq(bq_client, delete_ddl)

        # Insert the new entry into the master table with table name, primary keys, and schema definition
        insert_ddl = f"""INSERT INTO `{project_id}.data.master_table` (table_name, primary_keys, schema_definition)
                         VALUES ('{table_name}', '{pks}', '{schema_json}')"""
        sqlexecute_bq(bq_client, insert_ddl)

    except Exception as e:
        # Log the exception and raise an error with a descriptive message
        err_str = "Exception in insert_master_tbl: " + str(e)
        throw_exception(err_str)


def create_master_table(spark, lumi_id, lumi_ds, table_name, pks):
    """
    Optimized function to create or update the master table schema information efficiently.
    """
    log_msg("--- Optimizing insertion into master table ---")
    try:
        # Prepare master table creation query
        ddl_master_tbl = f"""
            CREATE TABLE IF NOT EXISTS `{project_id}.data.master_table` (
                table_name STRING NOT NULL,
                primary_keys STRING,
                schema_definition STRING,
                PRIMARY KEY(table_name) NOT ENFORCED
            )
        """
        sqlexecute_bq(bq_client, ddl_master_tbl)

        # Optimized schema retrieval using ARRAY_AGG for better performance
        schema_ddl = f"""
            SELECT ARRAY_AGG(column_name ORDER BY ordinal_position) AS col_names,
                   ARRAY_AGG(data_type ORDER BY ordinal_position) AS data_types
            FROM `{lumi_id}.{lumi_ds}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_name}'
        """
        res = sqlexecute_bq(bq_client, schema_ddl)

        # Process result efficiently
        if res and res[0].col_names:
            schema_definition = dict(zip(res[0].col_names, res[0].data_types))
            json_schema = json.dumps(schema_definition)
        else:
            log_msg(f"ERROR: Table '{table_name}' not found or has no schema.")
            return

        # Use MERGE instead of separate DELETE & INSERT for performance gains
        merge_ddl = f"""
            MERGE `{project_id}.data.master_table` AS target
            USING (SELECT '{table_name}' AS table_name, '{pks}' AS primary_keys, '{json_schema}' AS schema_definition) AS source
            ON target.table_name = source.table_name
            WHEN MATCHED THEN UPDATE SET target.primary_keys = source.primary_keys, target.schema_definition = source.schema_definition
            WHEN NOT MATCHED THEN INSERT (table_name, primary_keys, schema_definition) VALUES (source.table_name, source.primary_keys, source.schema_definition)
        """
        sqlexecute_bq(bq_client, merge_ddl)

    except Exception as e:
        err_str = "Optimized Exception in insert_master_tbl: " + str(e)
        throw_exception(err_str)
