import json
import polars as pl
import pyodbc
import requests
import time
import tomllib
import win32com.client

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
        print("reading toml failed")
        print(repr(e))
        cfg = {}
    return cfg


def refresh_excel(path):
    xlapp = win32com.client.DispatchEx("Excel.Application")
    try:
        print(f'''opening {path}''')
        Workbook = xlapp.Workbooks.Open(path)
        print('Waiting 5 seconds because MS is slow')
        time.sleep(5)
        print('refreshing data connections')
        Workbook.RefreshAll()
        xlapp.CalculateUntilAsyncQueriesDone()
        Workbook.Close(SaveChanges=1)
        xlapp.Quit()
    except:
        print('refresh failed')
    else:
        print('refresh complete, save and quit')


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
    cfg = read_toml("car_cfg.toml")
    FMCSQL = SQLServer(cfg["sql"])
    entries = get_form_ret_df(cfg["cognito"]["api_key"], cfg["car"]["form_id"])
    entries = entries.rename(cfg["car"]["schema"])
    entries = entries.cast({"dataYear": pl.UInt64})
    entries = (
        entries.with_columns(pl.col("submissionName") + pl.col("dataYear") * 1000000)
        .cast({"submissionName": pl.UInt64})
        .cast({"submissionName": pl.String})
    )
    entries = entries.select(cfg["car"]["cols"])
    writecsv_from_frame(entries, "2025_car.csv")
    entries.write_database(
        table_name="s_cog_car",
        connection=FMCSQL.polars_conn_2,
        engine="sqlalchemy",
        if_table_exists="replace",
    )
    car = return_query(FMCSQL.polars_conn, "SELECT * FROM churchAnnualReport")
    writecsv_from_frame(car, "car_bk.csv")
    execute_sql(FMCSQL.pyodbc_conn, "EXECUTE u_cog_car")
    execute_sql(FMCSQL.pyodbc_conn, "DROP TABLE s_cog_car")
    execute_sql(FMCSQL.pyodbc_conn, "EXECUTE u_domo_church_annual_reports")
    refresh_excel('C:\\Users\\kevin.eccles\\Free Methodist Church USA\\WMC Team - Documents\\ADM - Administration Files\\Database Administration\\Source Files\\Annual Reports\\annual_report_reminders.xlsx')

if __name__ == "__main__":
    main()
