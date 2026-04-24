import csv
import tomllib
import polars as pl
import pyodbc

from fmccfg import read_toml
from fmccsv import csv_to_df, writecsv_from_frame
from fmcpol import blank_to_nulls, truncate_strings
from fmcsql import execute_sql, return_query, SQLServer


def km0029():
    cfg = read_toml("re_cfg.toml")
    fmcusa_gl = SQLServer(cfg["sql"])

    big_atts = csv_to_df("km0029_attributes.CSV")
    print(big_atts)
    atts = big_atts.with_columns(
        pl.coalesce(
            pl.when(
                "Constituent Specific Attributes "
                + pl.col("Constituent Attribute Category")
                + " Description"
                == col
            ).then(col)
            for col in big_atts.columns
        ).alias("Constituent Attribute Description")
    )
    atts = atts.with_columns(
        pl.coalesce(
            pl.when(
                "Constituent Specific Attributes "
                + pl.col("Constituent Attribute Category")
                + " Date"
                == col
            ).then(col)
            for col in big_atts.columns
        ).alias("Constituent Attribute Date")
    )
    atts = atts.select(
        [
            "System Record ID",
            "Constituent ID",
            "Constituent Attribute Date",
            "Constituent Attribute Category",
            "Constituent Attribute Description",
        ]
    )
    atts = blank_to_nulls(atts)
    atts = atts.unique(maintain_order=True)
    print(atts)
    writecsv_from_frame(atts, "small_atts.csv")

    atts.write_database(
        connection=fmcusa_gl.polars_conn_2,
        table_name=cfg["km0029"]["s_table"],
        engine="sqlalchemy",
        if_table_exists="replace",
    )
    execute_sql(fmcusa_gl.pyodbc_conn, cfg["km0029"]["u_script"])
    execute_sql(fmcusa_gl.pyodbc_conn, f"""DROP TABLE {cfg['km0029']['s_table']}""")

    return atts


def re_table_update(cfg_value, sql):
    dest_table = return_query(
        sql.polars_conn, f"""SELECT * FROM {cfg_value["table"]}"""
    )
    writecsv_from_frame(dest_table, cfg_value["bk_filename"])
    csv_source = cfg_value["src_filename"]
    try:
        csv_encoding = cfg_value["src_encoding"]
    except Exception as e:
        csv_encoding = None
        print(repr(e))
    print("\n")
    print(csv_encoding)
    print("\n")
    source = csv_to_df(cfg_value["src_filename"], csv_encoding)
    source = blank_to_nulls(source)
    source = truncate_strings(source, 190)
    source = source.rename(cfg_value["schema"])
    print(source)
    source.write_database(
        connection=sql.polars_conn_2,
        table_name=cfg_value["s_table"],
        engine="sqlalchemy",
        if_table_exists="replace",
    )
    execute_sql(sql.pyodbc_conn, cfg_value["u_script"])
    execute_sql(sql.pyodbc_conn, f"""DROP TABLE {cfg_value["s_table"]}""")


def main():
    cfg = read_toml("re_cfg.toml")
    FMCSQL = SQLServer(cfg["sql"])

    re_table_update(cfg["km0017"], FMCSQL)
    re_table_update(cfg["km0020"], FMCSQL)
    re_table_update(cfg["km0021"], FMCSQL)
    re_table_update(cfg["km0022"], FMCSQL)
    re_table_update(cfg["km0023"], FMCSQL)
    re_table_update(cfg["km0024"], FMCSQL)
    re_table_update(cfg["km0025"], FMCSQL)
    # made unnecessary by km0029
    # re_table_update(cfg["km0026"], FMCSQL)
    re_table_update(cfg["km0027"], FMCSQL)
    re_table_update(cfg["km0028"], FMCSQL)

    # km0029 because attributes are stupid
    km0029()

    execute_sql(FMCSQL.pyodbc_conn, f"""EXEC u_all""")


if __name__ == "__main__":
    main()
