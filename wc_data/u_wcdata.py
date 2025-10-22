from fmctools import fmccfg, fmccog, fmccsv, fmcmsx, fmcpol, fmcsql
import polars as pl
import tomllib


def pick_data(df, rename_dict, select_list):
    result = df
    result = result.rename(rename_dict)
    result = result.select(select_list)
    return result


def main():
    cfg = fmccfg.read_toml("wcdata_cfg.toml")
    FMCSQL = fmcsql.SQLServer(cfg["sql"])
    api_key = cfg["cognito"]["api_key"]
    form_id = cfg["wcdata"]["form_id"]
    wcforms = fmccog.get_form_ret_df(api_key, form_id)
    wcforms = wcforms.with_columns(
        submission_date=pl.col("Entry_DateSubmitted").str.slice(0, 10)
    )
    wcforms = pick_data(
        wcforms, cfg["wcdata"]["col_rename"], cfg["wcdata"]["col_select"]
    )
    print(wcforms)
    fmccsv.writecsv_from_frame(wcforms, "wc_cog_forms.csv")
    print("writing to s_cog_wc_annual_reports")
    wcforms.write_database(
        connection=FMCSQL.polars_conn_2,
        table_name="s_cog_wc_annual_reports",
        engine="sqlalchemy",
        if_table_exists="replace",
    )
    fmcsql.execute_sql(FMCSQL.pyodbc_conn, "EXEC u_wc_annual_reports")
    fmcsql.execute_sql(FMCSQL.pyodbc_conn, "DROP TABLE s_cog_wc_annual_reports")


if __name__ == "__main__":
    main()
