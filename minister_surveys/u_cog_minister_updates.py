import json
import polars as pl
import pyodbc
import requests
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


def add_race_cols(df):
    result = df.with_columns(
        race_native=pl.when(pl.col("Race").str.contains("American"))
        .then(1)
        .otherwise(0),
        race_asian=pl.when(pl.col("Race").str.contains("Asian")).then(1).otherwise(0),
        race_black=pl.when(pl.col("Race").str.contains("Black")).then(1).otherwise(0),
        race_pacific=pl.when(pl.col("Race").str.contains("Pacific"))
        .then(1)
        .otherwise(0),
        race_white=pl.when(pl.col("Race").str.contains("White")).then(1).otherwise(0),
        race_other=pl.when(pl.col("Race").str.contains("Other")).then(1).otherwise(0),
        race_decline=pl.when(pl.col("Race").str.contains("Prefer"))
        .then(1)
        .otherwise(0),
    )
    return result


def add_eth_cols(df):
    result = df
    result = result.with_columns(
        eth_hispanic=pl.when(pl.col("Hispanic").str.contains("Yes "))
        .then(1)
        .when(pl.col("Hispanic").str.contains("Prefer"))
        .then(2)
        .otherwise(0),
        eth_arab=pl.when(pl.col("Arab").str.contains("Yes "))
        .then(1)
        .when(pl.col("Arab").str.contains("Prefer"))
        .then(2)
        .otherwise(0),
    )
    return result


def add_gender_col(df):
    result = df
    result = result.with_columns(
        gender=pl.when(pl.col("Gender").str.contains("Male"))
        .then(pl.lit("Male"))
        .when(pl.col("Gender").str.contains("Female"))
        .then(pl.lit("Female"))
        .otherwise(None)
    )
    return result


def add_consent_cols(df):
    result = df
    result = result.with_columns(
        phone_consent=pl.when(pl.col("PhoneConsent").str.contains("Yes"))
        .then(1)
        .otherwise(0),
        email_consent=pl.when(pl.col("EmailConsent").str.contains("Yes"))
        .then(1)
        .otherwise(0),
    )
    return result


def execute_sql(conn_str, script):
    print(f"executing {script}...")
    conn = pyodbc.connect(conn_str)
    csr = conn.cursor()
    csr.execute(script)
    conn.commit()
    csr.close()
    conn.close()
    print(f"{script} executed")


def fix_dates(df):
    result = df
    result = result.with_columns(
        date_birth=pl.date("Year", "Month", "Day"),
        submission_date=pl.col("Entry_DateSubmitted").str.slice(0, 10),
    )
    result = result.cast({"submission_date": pl.Date})
    return result


def format_conference(df):
    result = df
    result = result.with_columns(
        conference_name=pl.col("Conference").str.split(" - ").list.first(),
        conference_id=pl.col("Conference").str.split(" - ").list.last(),
    )
    return result


def format_surveys(df, col_rename, col_select):
    result = df
    result = add_race_cols(result)
    result = add_eth_cols(result)
    result = add_gender_col(result)
    result = add_consent_cols(result)
    result = fix_dates(result)
    result = format_conference(result)
    result = result.rename(col_rename)
    result = result.select(col_select)
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
    cfg = read_toml("minister_surveys_cfg.toml")
    FMCSQL = SQLServer(cfg["sql"])
    api_key = cfg["cognito"]["api_key"]
    form_id = cfg["cogministersurveys"]["form_id"]
    surveys = get_form_ret_df(api_key, form_id)
    surveys = format_surveys(
        surveys,
        cfg["cogministersurveys"]["col_rename"],
        cfg["cogministersurveys"]["col_select"],
    )
    print(surveys)
    writecsv_from_frame(surveys, "cog_minister_surveys.csv")
    print("local csv written...")
    surveys.write_database(
        table_name="s_cog_minister_surveys",
        connection=FMCSQL.polars_conn_2,
        engine="sqlalchemy",
        if_table_exists="replace",
    )
    print("s_cog_minister_surveys updated...")
    execute_sql(FMCSQL.pyodbc_conn, "exec u_ministers_surveys")
    execute_sql(FMCSQL.pyodbc_conn, "DROP TABLE s_cog_minister_surveys")
    # fmcmsx.refresh_excel(cfg["msx_reports"]["mailing_list_path"])


if __name__ == "__main__":
    main()
