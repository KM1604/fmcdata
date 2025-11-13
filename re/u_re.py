import csv
from fmctools import fmccfg, fmccsv, fmcpol, fmcsql
import tomllib
import polars
import pyodbc

def re_table_update(cfg_value, sql):
    dest_table = fmcsql.return_query(sql.polars_conn, f'''SELECT * FROM {cfg_value["table"]}''')
    fmccsv.writecsv_from_frame(dest_table, cfg_value["bk_filename"])
    csv_source = cfg_value["src_filename"]
    try:
        csv_encoding = cfg_value["src_encoding"]
    except Exception as e: 
        csv_encoding = None
        print(repr(e))
    print("\n")
    print(csv_encoding)
    print("\n")
    source = fmccsv.csv_to_df(cfg_value["src_filename"], csv_encoding) 
    source = fmcpol.blank_to_nulls(source)
    source = fmcpol.truncate_strings(source, 190)
    source = source.rename(cfg_value["schema"])
    print(source)
    source.write_database(
        connection=sql.polars_conn_2,
        table_name=cfg_value["s_table"],
        engine="sqlalchemy",
        if_table_exists="replace"
        )
    fmcsql.execute_sql(sql.pyodbc_conn, cfg_value["u_script"])
    fmcsql.execute_sql(sql.pyodbc_conn, f'''DROP TABLE {cfg_value["s_table"]}''')

def main():
    cfg = fmccfg.read_toml("re_cfg.toml")
    FMCSQL = fmcsql.SQLServer(cfg["sql"])

    re_table_update(cfg["km0017"], FMCSQL)
    re_table_update(cfg["km0020"], FMCSQL)
    re_table_update(cfg["km0021"], FMCSQL)
    re_table_update(cfg["km0022"], FMCSQL)
    re_table_update(cfg["km0023"], FMCSQL) 
    re_table_update(cfg["km0024"], FMCSQL)
    re_table_update(cfg["km0025"], FMCSQL)
    re_table_update(cfg["km0026"], FMCSQL)
    re_table_update(cfg["km0027"], FMCSQL)
    re_table_update(cfg["km0028"], FMCSQL)
    fmcsql.execute_sql(FMCSQL.pyodbc_conn, f'''EXEC u_all''')

if __name__ == "__main__":
    main()
