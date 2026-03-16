import json
import polars as pl
import pyodbc
import requests
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


def execute_sql(conn_str, script):
    print(f"executing {script}...")
    conn = pyodbc.connect(conn_str)
    csr = connection.cursor()
    csr.execute(script)
    conn.commit()
    csr.close()
    conn.close()
    print(f"{script} executed")


def get_form_ret_df(api_key, form_id, view_id=1):
    r = requests.get(
        f"https://www.cognitoforms.com/api/odata/Forms({form_id})/Views({view_id})/Entries",
        headers={"Authorization": f"Bearer {api_key}"},
    )
    print(r)
    entries = json.loads(r.text)
    form_entries = pl.DataFrame(entries["value"])
    print(form_entries)
    return form_entries


def read_toml(path):
    try:
        with open(path, "rb") as f:
            cfg = tomllib.load(f)
            print(cfg["readme"])
    except Exception as e:
        print("reading {path} failed")
        print(repr(e))
        cfg = {}
    return cfg


def return_query(conn_string, query):
    df = pl.read_database_uri(query, conn_string)
    print(df)
    return df


def writecsv_from_frame(frame, filename):
    print(f"writing data to {filename}")
    frame.write_csv(
        file=filename,
        separator=",",
        quote_char='"',
        float_scientific=False,
    )
    print(f"{filename} written")


def main():
    cfg = read_toml("cis_cfg.toml")
    fmcusa_gl = SQLServer(cfg["sql"])
    con = sqlite3.connect("cis.db")
    con.close()

    # backup existing table to local SQLite db
    pre_cis = return_query(
        fmcusa_gl.polars_conn, f"SELECT * FROM {cfg['cis']['s_table']}"
    )
    pre_cis.write_database(
        table_name="pre_cis", connection="sqlite:///cis.db", if_table_exists="replace"
    )

    # pull responses, write to temp on fmcusa_gl and local SQLite db
    cis_responses = get_form_ret_df(cfg["cognito"]["api_key"], cfg["cis"]["form_id"])
    cis_responses.write_database(
        table_name="cog_cis", connection="sqlite:///cis.db", if_table_exists="replace"
    )
    cis_responses.write_database(
        table_name=cfg["cis"]["s_table"],
        connection=fmcusa_gl.polars_conn_2,
        engine="sqlalchemy",
        if_table_exists="replace",
    )



if __name__ == "__main__":
    main()
