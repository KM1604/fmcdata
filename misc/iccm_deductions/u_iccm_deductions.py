import polars as pl
import pyodbc
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
    print(f'{script}...', end='')
    connection = pyodbc.connect(conn_str)
    cursor = connection.cursor()
    cursor.execute(script)
    connection.commit()
    print('complete')

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

def main():
    cfg = read_toml("iccm_cfg.toml")
    fmcusa_gl = SQLServer(cfg["sql"])
    
    iccm = pl.read_excel(
        source='2025_iccm_matched_giving.xlsx',
        table_name='deductionsEPP_ICCM'
    )
    print(iccm)
    rows_written = iccm.write_database(
        table_name = 's_iccm_deductions',
        connection = fmcusa_gl.polars_conn_2,
        if_table_exists = 'replace',
        engine = 'sqlalchemy'
        )
    print(f'{rows_written} rows written to s_iccm_deductions')
    execute_sql(
        fmcusa_gl.pyodbc_conn,
        'exec u_iccm_deductions'
        )
    execute_sql(
        fmcusa_gl.pyodbc_conn,
        'DROP TABLE s_iccm_deductions'
        )


if __name__ == '__main__':
    main()
