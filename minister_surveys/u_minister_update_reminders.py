from fmctools import fmccfg, fmccsv, fmcmsx, fmcsql
import polars
import pyodbc
import time
import tomllib
import win32com.client


def main():
    cfg = fmccfg.read_toml("minister_updates.toml")
    FMCSQL = fmcsql.SQLServer(cfg["sql"])
    fmcmsx.refresh_excel(cfg["msx_reports"]["mailing_list_path"])


if __name__ == "__main__":
    main()
