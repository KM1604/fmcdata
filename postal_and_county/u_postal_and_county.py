import polars as pl
import pyodbc
import sqlite3
import tomllib


class SQLServer:
    def __init__(self, cfg_dict):
        self.usr = cfg_dict["usr"]
        self.pw = cfg_dict["pw"]
        self.server = cfg_dict["server"]
        self.db = cfg_dict["db"]
        self.port = cfg_dict["port"]
        self.read_flags = cfg_dict["read_flags"]
        self.read_flags_2 = cfg_dict["read_flags_2"]
        self.driver = cfg_dict["driver"]
        self.polars_conn = (
            f"mssql://{self.usr}:{self.pw}@"
            f"{self.server}:{self.port}/"
            f"{self.db}?{self.read_flags}"
        )
        self.polars_conn_2 = (
            f"mssql://{self.usr}:{self.pw}@"
            f"{self.server}:{self.port}/"
            f"{self.db}?{self.read_flags_2}"
        )
        self.pyodbc_conn = (
            f"DRIVER={self.driver};"
            f"SERVER={self.server},{self.port};"
            f"DATABASE={self.db};"
            f"UID={self.usr};"
            f"PWD={self.pw};"
        )


def csv_to_df(path, encoding=None):
    if encoding:
        df = pl.read_csv(
            source=path,
            separator=",",
            quote_char='"',
            has_header=True,
            encoding=encoding,
        )
    else:
        df = pl.read_csv(source=path, separator=",", quote_char='"', has_header=True)
    print(df)
    return df


def execute_sql(conn_str, script):
    print(f"executing {script}...")
    connection = pyodbc.connect(conn_str)
    cursor = connection.cursor()
    cursor.execute(script)
    connection.commit()
    print(f"{script} executed")


def read_toml(path):
    try:
        with open(path, "rb") as f:
            cfg = tomllib.load(f)
            print(cfg["readme"])
    except Exception as e:
        print("reading toml failed")
        print(repr(e))
        cfg = {}
    return cfg


def return_query(conn_string, query):
    df = pl.read_database_uri(query, conn_string)
    print(df)
    return df


def main():
    con = sqlite3.connect("postal_and_county.db")
    con.close()

    cfg = read_toml("postal_and_county_cfg.toml")
    fmcusagl = SQLServer(cfg.get("sql"))
    prod_table = return_query(fmcusagl.polars_conn, f"SELECT * FROM postal_and_county")
    prod_table.write_database(
        table_name="postal_and_county",
        connection="sqlite:///postal_and_county.db",
        if_table_exists="replace",
    )
    fips = csv_to_df(cfg.get("postal_and_county").get("src_filename"))
    fips = fips.with_columns(pl.lit(0).alias("pop_rank"))
    fips.write_database(
        table_name="csv_src",
        connection="sqlite:///postal_and_county.db",
        if_table_exists="replace",
    )
    fips.write_database(
        table_name=cfg.get("postal_and_county").get("tgt_table"),
        connection=fmcusagl.polars_conn_2,
        engine="sqlalchemy",
        if_table_exists="replace",
    )
    execute_sql(fmcusagl.pyodbc_conn, "exec u_postal_and_county")


if __name__ == "__main__":
    main()
