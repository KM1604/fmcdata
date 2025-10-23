from fmctools import fmccfg, fmccsv, fmcmsx, fmcpol, fmcsql
import polars as pl
import pyodbc
import tomllib
import win32com.client


def ny_index():
    cfg = fmccfg.read_toml("nyhart_cfg.toml")
    FMCSQL = fmcsql.SQLServer(cfg["sql"])
    try:
        nyindex = fmcmsx.read_xlsx(
            cfg["ny_index"]["src_filename"], cfg["ny_index"]["src_tablename"]
        )
    except:
        print(f'Name the table {cfg["ny_index"]["src_tablename"]}.')
        nyindex = None
        return nyindex
    nyindex = nyindex.rename(cfg["ny_index"]["schema"])
    nyindex = fmcpol.blank_to_nulls(nyindex)
    nyindex = nyindex.filter(pl.col("employee_id").is_not_null())
    nyindex = nyindex.filter(
        pl.col("employee_id").is_in(cfg["ny_salary"]["bad_ny_ids"]).not_()
    )
    print(nyindex)
    fmccsv.writecsv_from_frame(nyindex, "s_ny_pension_index.csv")
    nyindex.write_database(
        connection=FMCSQL.polars_conn_2,
        table_name=cfg["ny_index"]["temp_table"],
        engine="sqlalchemy",
        if_table_exists="replace",
    )
    fmcsql.execute_sql(FMCSQL.pyodbc_conn, "EXEC u_ny_pension_index")
    fmcsql.execute_sql(
        FMCSQL.pyodbc_conn, f"""DELETE FROM {cfg["ny_index"]["temp_table"]}"""
    )


def ny_salary():
    cfg = fmccfg.read_toml("nyhart_cfg.toml")
    FMCSQL = fmcsql.SQLServer(cfg["sql"])
    try:
        nysalaries = fmcmsx.read_xlsx(
            cfg["ny_salary"]["src_filename"], cfg["ny_salary"]["src_tablename"]
        )
    except:
        print(f'Name the table {cfg["ny_salary"]["src_tablename"]}.')
        nysalaries = None
        return nysalaries
    nysalaries = nysalaries.rename(cfg["ny_salary"]["schema"])
    nysalaries = fmcpol.blank_to_nulls(nysalaries)
    nysalaries = nysalaries.filter(pl.col("employee_id").is_not_null())
    print(nysalaries)
    nysalaries = nysalaries.filter(
        pl.col("employee_id").is_in(cfg["ny_salary"]["bad_ny_ids"]).not_()
    )
    print(nysalaries)
    fmccsv.writecsv_from_frame(nysalaries, "s_ny_salary_history.csv")
    nysalaries.write_database(
        connection=FMCSQL.polars_conn_2,
        table_name=cfg["ny_salary"]["temp_table"],
        engine="sqlalchemy",
        if_table_exists="replace",
    )
    fmcsql.execute_sql(FMCSQL.pyodbc_conn, "EXEC u_ny_salary_history")
    fmcsql.execute_sql(
        FMCSQL.pyodbc_conn, f"""DELETE FROM {cfg["ny_salary"]["temp_table"]}"""
    )


def main():
    ny_index()
    ny_salary()


if __name__ == "__main__":
    main()
