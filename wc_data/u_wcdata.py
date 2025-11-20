import json
import polars as pl
import pyodbc
import requests
import tomllib


def pick_data(df, rename_dict, select_list):
    result = df
    result = result.rename(rename_dict)
    result = result.select(select_list)
    return result


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


def writecsv_from_frame(frame, filename):
    print(f"writing data to {filename}")
    frame.write_csv(
        file=filename,
        separator=",",
        quote_char='"',
        float_scientific=False,
    )
    print(f"{filename} written")


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
    connection = pyodbc.connect(conn_str)
    cursor = connection.cursor()
    cursor.execute(script)
    connection.commit()
    print(f"{script} executed")


def main():
    cfg = read_toml("wcdata_cfg.toml")
    FMCSQL = SQLServer(cfg["sql"])
    api_key = cfg["cognito"]["api_key"]
    form_id = cfg["wcdata"]["form_id"]
    wcforms = get_form_ret_df(api_key, form_id)
    wcforms = wcforms.with_columns(
        submission_date=pl.col("Entry_DateSubmitted").str.slice(0, 10)
    )
    wcforms = pick_data(
        wcforms, cfg["wcdata"]["col_rename"], cfg["wcdata"]["col_select"]
    )
    print(wcforms)
    writecsv_from_frame(wcforms, "wc_cog_forms.csv")
    print("writing to s_cog_wc_annual_reports")
    wcforms.write_database(
        connection=FMCSQL.polars_conn_2,
        table_name="s_cog_wc_annual_reports",
        engine="sqlalchemy",
        if_table_exists="replace",
    )
    execute_sql(FMCSQL.pyodbc_conn, "EXEC u_wc_annual_reports")
    execute_sql(FMCSQL.pyodbc_conn, "DROP TABLE s_cog_wc_annual_reports")


if __name__ == "__main__":
    main()
