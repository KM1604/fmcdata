import csv
import polars
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


def post_csv_to_form(
    cognito_api, src_filename, form_id, import_mode, email, match_on=None
):
    with open(src_filename, "r") as s:
        src = s.read()
    data_contents = {
        "ImportMode": import_mode,
        "Email": email,
    }
    if match_on != None:
        data_contents["MatchEntriesUsing"] = match_on
    r = requests.post(
        f"https://www.cognitoforms.com/api/forms/{form_id}/import-entries",
        headers={"Authorization": f"Bearer {cognito_api}"},
        data={"ImportMode": import_mode, "Email": email, "MatchEntriesUsing": match_on},
        files={"files": (src_filename, src, "text/csv")},
    )
    return r


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
    df = polars.read_database_uri(query, conn_string)
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



cfg = read_toml("cognito_cfg.toml")
megmac = cfg["megmacactions"]

SQL = SQLServer(cfg["sql"])

query = megmac["query"]
df = return_query(SQL.polars_conn, query)
filename = megmac["src_filename"]
writecsv_from_frame(df, filename)
cog_api = cfg["cognito"]["api_key"]

r = post_csv_to_form(
    cog_api,
    filename,
    megmac["form_id"],
    megmac["import_mode"],
    megmac["email"],
    megmac["match_on"],
)
print(r)
print(r.headers)
print(r.text)
