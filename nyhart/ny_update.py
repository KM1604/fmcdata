from fmctools import fmccfg, fmccsv, fmcmsx, fmcsql
import polars as pl
import pyodbc
import tomllib
import win32com.client


def ny_salary_update():
    cfg = fmccfg.read_toml("nyhart_cfg.toml")
    FMCSQL = fmcsql.SQLServer(cfg["sql"])
    try:
        nysalaries = fmcmsx.read_xlsx(
            cfg["ny_salary"]["src_filename"], cfg["ny_salary"]["src_tablename"]
        )
    except:
        print(
            f'Did you name the table {cfg["ny_salary"]["src_tablename"]}? Because it failed.'
        )
        nysalaries = None
        return nysalaries
    fmccsv.writecsv_from_frame(nysalaries, "SalaryHistoryReport.csv")

    backup = fmcsql.return_query(
        FMCSQL.polars_conn, f'SELECT * FROM {cfg["ny_salary"]["bk_table"]}'
    )
    fmccsv.writecsv_from_frame(backup, f'{cfg["ny_salary"]["bk_table"]}.csv')

    nysalaries = nysalaries.rename(cfg["ny_salary_schema"])
    fmcsql.execute_sql(
        FMCSQL.pyodbc_conn, f'DELETE FROM {cfg["ny_salary"]["temp_table"]}'
    )
    fmcsql.insert_to_table(
        FMCSQL.pyodbc_conn, nysalaries, cfg["ny_salary"]["temp_table"]
    )
    fmcsql.execute_sql(FMCSQL.pyodbc_conn, cfg["ny_salary"]["u_script"])


def ny_index_update():
    cfg = fmccfg.read_toml("ny_update.toml")
    FMCSQL = fmcsql.SQLServer(cfg["sql"])
    try:
        nyindex = fmcmsx.read_xlsx(
            cfg["ny_index"]["src_filename"],
            cfg["ny_index"]["src_tablename"],
        )
    except:
        print(
            f'Did you name the index table {cfg["ny_index"]["src_tablename"]}? Because it failed.'
        )
        nyindex = None
        return nyindex
    fmccsv.writecsv_from_frame(nyindex, "PensionIndex.csv")

    backup = fmcsql.return_query(
        FMCSQL.polars_conn, f'SELECT * FROM {cfg["ny_index"]["table"]}'
    )
    fmccsv.writecsv_from_frame(backup, f'{cfg["ny_index"]["table"]}.csv')

    nyindex = nyindex.rename(cfg["ny_index_schema"])

    # remove test employee ids - contain problematic dummy data
    nyindex.with_columns(
        polars.remove(polars.col("employee_id").is_in(cfg["ny_salary"]["bad_ny_ids"]))
    )

    fmcsql.execute_sql(
        FMCSQL.pyodbc_conn, f'DELETE FROM {cfg["ny_index"]["temp_table"]}'
    )
    fmcsql.insert_to_table(FMCSQL.pyodbc_conn, nyindex, cfg["ny_index"]["temp_table"])
    fmcsql.execute_sql(FMCSQL.pyodbc_conn, cfg["ny_index"]["u_script"])

def blank_to_nulls(frame):
    nulled = frame.with_columns(
                pl.when(pl.col(pl.String).str.len_chars() == 0)
                .then(None)
                .otherwise(pl.col(pl.String))
                .name.keep()
                )
    print(nulled)
    return nulled


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
    nysalaries = blank_to_nulls(nysalaries)
    nysalaries = nysalaries.filter(pl.col("employee_id").is_not_null())
    print(nysalaries)
    nysalaries = nysalaries.filter(pl.col("employee_id").is_in(cfg["ny_salary"]["bad_ny_ids"]).not_())
    print(nysalaries)
    fmccsv.writecsv_from_frame(nysalaries, "s_ny_salary_history.csv")
    nysalaries.write_database(
        connection=FMCSQL.polars_conn_2,
        table_name=cfg["ny_salary"]["temp_table"],
        engine="sqlalchemy",
        if_table_exists="replace"
        )


def main():
    ny_salary()


if __name__ == "__main__":
    main()
