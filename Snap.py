import pandas as pd
import sqlite3  # Using SQLite for demonstration; replace with your database connector

def snapshot(csv_file_path, target_table_name, master_table_name, db_connection):
    """
    Snapshot function to handle schema changes and data backup.

    Args:
        csv_file_path (str): Path to the CSV file containing the new column list.
        target_table_name (str): Name of the target table in the database.
        master_table_name (str): Name of the master table containing schema definitions.
        db_connection: Database connection object.
    """
    # Read the CSV file to get the new column list
    csv_columns = pd.read_csv(csv_file_path).columns.tolist()

    # Fetch schema definition and primary key from the master table
    cursor = db_connection.cursor()
    cursor.execute(f"SELECT schema_definition, PK FROM {master_table_name} WHERE table_name = ?", (target_table_name,))
    result = cursor.fetchone()

    if not result:
        raise ValueError(f"Table {target_table_name} not found in master table {master_table_name}.")

    schema_definition, primary_key = result
    target_columns = [col.split()[0] for col in schema_definition.split(",")]  # Extract column names from schema definition

    # Compare CSV columns with target table columns
    if set(csv_columns) != set(target_columns):
        print("Column mismatch detected. Handling schema change...")

        # Backup existing data to a temporary table
        temp_table_name = f"temp_{target_table_name}"
        cursor.execute(f"CREATE TABLE {temp_table_name} AS SELECT * FROM {target_table_name};")
        print(f"Backed up data to temporary table {temp_table_name}.")

        # Drop the existing target table
        cursor.execute(f"DROP TABLE {target_table_name};")
        print(f"Dropped table {target_table_name}.")

        # Create a new target table with the updated schema
        new_schema_definition = ", ".join([f"{col} TEXT" for col in csv_columns])  # Assuming all columns are TEXT for simplicity
        cursor.execute(f"CREATE TABLE {target_table_name} ({new_schema_definition}, PRIMARY KEY ({primary_key}));")
        print(f"Created new table {target_table_name} with updated schema.")

        # Insert data from the temporary table back into the new target table
        common_columns = set(csv_columns).intersection(set(target_columns))
        if common_columns:
            common_columns_str = ", ".join(common_columns)
            cursor.execute(f"INSERT INTO {target_table_name} ({common_columns_str}) SELECT {common_columns_str} FROM {temp_table_name};")
            print(f"Restored data from {temp_table_name} to {target_table_name}.")

        # Drop the temporary table
        cursor.execute(f"DROP TABLE {temp_table_name};")
        print(f"Dropped temporary table {temp_table_name}.")

    else:
        print("No schema changes detected. Proceeding without modifications.")

    db_connection.commit()
    cursor.close()

# Example usage
if __name__ == "__main__":
    # Connect to the database (SQLite for demonstration)
    conn = sqlite3.connect("example.db")

    # Define paths and table names
    csv_file = "new_columns.csv"
    target_table = "target_table"
    master_table = "master_table"

    # Call the snapshot function
    snapshot(csv_file, target_table, master_table, conn)

    # Close the database connection
    conn.close()

import csv
from google.cloud import bigquery

def snapshot(
    client: bigquery.Client,
    project_id: str,
    bq_src_db: str,
    bq_src_dataset: str,
    bq_src_table: str,
    csv_path: str,
    file_name: str,
    bq_target_db: str,
    bq_target_dataset: str,
    bq_target_table: str,
    table_master_db: str,
    table_master_dataset: str,
    table_master_table: str,
    where_str: str = None,
    limit_str: str = None
):
    """
    Extended snapshot function that:
      - Reads a new column list from a CSV file
      - Retrieves corresponding data types from a 'table master'
      - Compares the CSV column list with the target table’s existing schema
      - If CSV has more columns, backs up the target table data in a temp table
        and recreates the target table with the new columns
      - Finally executes the snapshot logic
    """

    try:
        # 1. Read the new column list from CSV
        new_col_list = []
        with open(csv_path + "/" + file_name, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            # Assuming CSV file has one column name per row, or
            # a header row with columns in row[0], row[1], etc.
            for row in reader:
                # Adjust indexing depending on your CSV format
                if row:  # skip empty lines
                    new_col_list.append(row[0].strip())

        # 2. Read the schema definitions (and PK, etc.) from the table master
        #    The assumption here is that table master has columns:
        #      - table_name
        #      - schema_definition (with col_name:datatype)
        #      - pk (some primary key definition, if needed)
        #    Adapt queries as needed for your real schema.
        master_query = f"""
            SELECT schema_definition
            FROM `{project_id}.{table_master_dataset}.{table_master_table}`
            WHERE table_name = '{bq_target_table}'
        """
        master_job = client.query(master_query)
        master_result = master_job.result()
        schema_map = {}
        for row in master_result:
            # parse row['schema_definition'] which might contain JSON or
            # "col1:STRING,col2:INTEGER,..."
            # For example, if stored as simple CSV "col1:STRING,col2:INTEGER", parse it:
            schema_pairs = row["schema_definition"].split(",")
            for pair in schema_pairs:
                col, dtype = pair.strip().split(":")
                schema_map[col.strip()] = dtype.strip()

        # 3. Get existing target table’s schema
        full_target_table_name = f"{project_id}.{bq_target_dataset}.{bq_target_table}"
        try:
            target_table_ref = client.get_table(full_target_table_name)
            existing_columns = [field.name for field in target_table_ref.schema]
        except Exception:
            # If table doesn't exist, just treat existing_columns as empty
            existing_columns = []

        # 4. Compare columns. If CSV has more columns (or differs), we recreate.
        new_set = set(new_col_list)
        existing_set = set(existing_columns)
        # Condition: If new columns are not the same, or new columns are bigger
        if new_set != existing_set and new_set.issuperset(existing_set):
            # (a) Move/backup old table to a temp table
            temp_table_name = bq_target_table + "_bkp"
            temp_full_name = f"{project_id}.{bq_target_dataset}.{temp_table_name}"
            print(f"Backing up table {full_target_table_name} to {temp_full_name} ...")

            # Drop temp if it already exists
            try:
                client.delete_table(temp_full_name)
            except:
                pass  # If table doesn’t exist, ignore

            # Copy target table to temp
            backup_job = client.copy_table(full_target_table_name, temp_full_name)
            backup_job.result()  # Wait for job to complete

            # (b) Drop old table
            try:
                client.delete_table(full_target_table_name)
            except:
                pass

            # (c) Create new target table with updated schema
            # Construct a bigquery.SchemaField list from new_col_list & schema_map
            new_schema_fields = []
            for col in new_col_list:
                # default to STRING if the col wasn't in schema_map
                col_type = schema_map.get(col, "STRING")
                new_schema_fields.append(bigquery.SchemaField(col, col_type))
            
            table = bigquery.Table(full_target_table_name, schema=new_schema_fields)
            created_table = client.create_table(table)
            print(f"Created new table {created_table.full_table_id} with updated schema.")

            # (d) Move data from temp to new table if needed
            #   - Only copy those columns that still exist, ignoring new ones for now
            common_cols = list(set(existing_columns).intersection(new_col_list))
            if common_cols:
                col_str = ",".join(common_cols)
                insert_sql = f"""
                    INSERT INTO `{full_target_table_name}` ({col_str})
                    SELECT {col_str}
                    FROM `{temp_full_name}`
                """
                insert_job = client.query(insert_sql)
                insert_job.result()
                print("Copied old data into new target table for the overlapping columns.")

            # Optionally drop temp backup to clean up
            # client.delete_table(temp_full_name)

        # 5. Perform the actual snapshot logic from your original code
        #    (Below is just a skeleton of the logic you had.)
        sqlstr = f"SELECT * FROM `{bq_src_db}.{bq_src_dataset}.{bq_src_table}`"
        # bq_to_csv(...) or your existing logic
        # create or replace table ...
        # etc.

        # Example:
        temp_tbl_nm = "temp_bq_src_table"
        ddl_create_str = (
            "CREATE OR REPLACE TABLE `{}.{}.{}` AS ( {} )"
            .format(bq_target_db, bq_target_dataset, temp_tbl_nm, sqlstr)
        )
        # client.query(ddl_create_str).result()  # Example of creating a temp

        # Next steps, e.g. upsert/insert the data from `temp_tbl_nm` into your `bq_target_table`
        # … your existing logic

        print("Snapshot completed.")

    except Exception as e:
        print("Exception in snapshot:", str(e))
        # Handle or raise as appropriate


# ------------------------------------------------------------------------
# EXAMPLE OF HOW TO CALL THE UPDATED SNAPSHOT FUNCTION (SECOND IMAGE AREA)
# Suppose you have a separate function create_tbl or some code that
# eventually calls snapshot. You might do something like:

def create_tbl(
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
    schema_json,
    bq_client,    # pass in the BigQuery client
    project_id    # and so on...
):
    print("---------- Creating tables ----------")
    try:
        # If we’re using “SNAPSHOT” as a command
        if crt_rep.upper() == "SNAPSHOT":
            where_str = getWhereCond(tgt_ds, tgt_tbl, src_db, src_ds, src_tbl, col_list, key_cols, key_vals)
            limit_str = getLimit(limit_val)

            # Notice we now call our updated snapshot function, passing extra
            # arguments referencing the table master details, etc.
            snapshot(
                client=bq_client,
                project_id=project_id,
                bq_src_db=src_db,
                bq_src_dataset=src_ds,
                bq_src_table=src_tbl,
                csv_path=csv_path,
                file_name=file_name,
                bq_target_db=tgt_db,
                bq_target_dataset=tgt_ds,
                bq_target_table=tgt_tbl,
                table_master_db="my_master_db",
                table_master_dataset="my_master_dataset",
                table_master_table="table_master",
                where_str=where_str,
                limit_str=limit_str
            )

        elif crt_rep == "TL": 
            # Or other logic
            pass

    except Exception as e:
        print("Exception in create_tbl:", str(e))
        raise
