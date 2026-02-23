import csv
import tomllib
import polars
import pyodbc

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

def read_xlsx(path, table):
    print(f'''accessing {table} in {path}''')
    frame = polars.read_excel(
                source=path,
                table_name=table,
                has_header=True
                )
    print(frame)
    return frame

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



def sp_emp():
    cfg = read_toml("sp_cfg.toml")
    FMCSQL = SQLServer(cfg["sql"])

    bk_sp_emp = return_query(
        FMCSQL.polars_conn,
        f'SELECT * FROM {cfg["sp_emp"]["bk_table"]}'
        )
    writecsv_from_frame(
        bk_sp_emp,
        cfg["sp_emp"]["bk_dest"]
        )
    sp_emp = read_xlsx(
        cfg["sp_emp"]["src_filename"],
        cfg["sp_emp"]["src_table"]
        )

    sp_emp.write_database(
        table_name=cfg["sp_emp"]["tgt_table"],
        connection=FMCSQL.polars_conn_2,
        engine="sqlalchemy",
        if_table_exists="replace"
        )
    execute_sql(
        FMCSQL.pyodbc_conn,
        f'EXECUTE {cfg["sp_emp"]["u_script"]}'
        )
    execute_sql(
        FMCSQL.pyodbc_conn,
        f'DROP TABLE {cfg["sp_emp"]["tgt_table"]}'
        )


def sp_res():
    cfg = read_toml("sp_cfg.toml")
    FMCSQL = SQLServer(cfg["sql"])

    bk_sp_emp = return_query(
        FMCSQL.polars_conn,
        f'SELECT * FROM {cfg["sp_res"]["bk_table"]}'
        )
    writecsv_from_frame(
        bk_sp_emp,
        cfg["sp_res"]["bk_dest"]
        )
    sp_res = read_xlsx(
        cfg["sp_res"]["src_filename"],
        cfg["sp_res"]["src_table"]
        )
    sp_res.write_database(
        table_name=cfg["sp_res"]["tgt_table"],
        connection=FMCSQL.polars_conn_2,
        engine="sqlalchemy",
        if_table_exists="replace"
        )

    execute_sql(
        FMCSQL.pyodbc_conn,
        f'EXECUTE {cfg["sp_res"]["u_script"]}'
        )

    execute_sql(
        FMCSQL.pyodbc_conn,
        f'DROP TABLE {cfg["sp_res"]["tgt_table"]}'
        )


def main():
    sp_emp()
    sp_res()
    cfg = read_toml("sp_cfg.toml")
    FMCSQL = SQLServer(cfg["sql"])
    execute_sql(
        FMCSQL.pyodbc_conn,
        f'EXECUTE u_domo_staff_pulse'
        )

if __name__ == "__main__":
    main()
