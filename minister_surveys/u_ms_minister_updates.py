from fmctools import fmccfg, fmccsv, fmcmsx, fmcsql
import polars
import pyodbc
import time
import tomllib
import win32com.client


def main():
    cfg = fmccfg.read_toml("minister_surveys_cfg.toml")
    FMCSQL = fmcsql.SQLServer(cfg["sql"])
    surveys = fmcmsx.read_xlsx(
        cfg["excel_source"]["file"], cfg["excel_source"]["table"]
    )
    fmccsv.writecsv_from_frame(surveys, "ministers_surveys.csv")
    surveys = surveys.rename(cfg["mu_schema"])
    backup = fmcsql.return_query(
        FMCSQL.polars_conn, f'SELECT * FROM {cfg["mu_dest_table"]["table"]}'
    )
    fmccsv.writecsv_from_frame(backup, "ministers_surveys_bk.csv")
    fmcsql.execute_sql(
        FMCSQL.pyodbc_conn, f"""DELETE FROM {cfg["mu_dest_table"]["table"]}"""
    )
    fmcsql.insert_to_table(FMCSQL.pyodbc_conn, surveys, cfg["mu_dest_table"]["table"])
    fmcsql.execute_sql(FMCSQL.pyodbc_conn, "EXECUTE u_ministers_surveys")
    fmcsql.execute_sql(FMCSQL.pyodbc_conn, "DELETE FROM s_ministers_surveys")
    fmcmsx.refresh_excel(cfg["msx_reports"]["mailing_list_path"])


if __name__ == "__main__":
    main()
