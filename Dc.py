def readCSV(spark):
    limit_val = str(row.limit)
    file_name = str(row.file_name)
    user_email = str(row.user_email)

    log_msg("dc_id=" + str(dc_id))

    if len(str(load_type)) == 0 or str(load_type) == 'None':
        log_msg("ERROR: Please provide valid load_type")
        failed_rsn = "ERROR: Please provide valid load_type"
        valid_run = 0
    else:
        valid_run = 1  # Ensure valid_run is set correctly if load_type is valid

    if valid_run == 1:  # Ensure the function executes only if valid_run is 1
        if user_email and user_email != 'None':  # More robust check for valid email
            # Call create_tbl function
            status, failed_rsn, schema_changed, cols_str = create_tbl(
                spark, load_type, tgt_db, tgt_ds, tgt_tbl, src_db, src_ds, 
                src_tbl, col_list, where_cond, csv_path, dtm, limit_val, file_name
            )

            if len(failed_rsn) > 0:
                chk_dc_ids.append(0)  # Failure case
            else:
                chk_dc_ids.append(1)  # Success case

            dc_audit_table(dc_id, user_email, status, load_type, tgt_tbl, file_name, failed_rsn, schema_changed, cols_str)
        else:
            log_msg("ERROR: Please provide email and re-run the dc_id")
            failed_rsn = "ERROR: Please provide email and re-run the dc_id"
            valid_run = 0

    if valid_run == 0:
        status = 'failed'
        schema_changed = False
        cols_str = ""
        chk_dc_ids.append(0)

        # Log failed execution
        dc_audit_table(dc_id, user_email, status, load_type, tgt_tbl, file_name, failed_rsn, schema_changed, cols_str)

    print(chk_dc_ids)
    if 1 in chk_dc_ids:
        log_msg("At least one dc_id successfully ran")
    else:
        throw_exception("All provided dc_ids failed, so failing DAG!")

except Exception as e:
    err_str = "Exception in readCSV: " + str(e)
    throw_exception(err_str)
