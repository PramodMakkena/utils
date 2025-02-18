elif crt_rep == "TL":
            where_str = getWhereCond(tgt_ds, tgt_tbl, src_db, src_ds, src_tbl, col_list, key_cols, key_vals)
            limit_str = getLimit(limit_val)
            sqlstr = "select {} from `{}.{}.{}`{}{}".format(
                col_list, src_db, src_ds, src_tbl, where_str, limit_str
            )
            temp_tbl = "{}_temp_{}".format(tgt_tbl, dttm)

            bq_to_csv(bq_client, project_id, csv_path, file_name, sqlstr)
            df = readfrombucket(spark, f"gs://{project_id}{csv_path}{file_name}", "csv")
            writetablebq(df, project_id, tgt_ds, temp_tbl, "overwrite")
            truncate_load(spark, bq_client, tgt_db, tgt_ds, tgt_tbl, temp_tbl, project_id, file_name)
