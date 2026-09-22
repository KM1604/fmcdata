import argparse
from pathlib import *
import pyodbc
import sys
from tomllib import *

from SQLDB import SQLDB

parser = argparse.ArgumentParser(
    description="capturing trust and encode vars for conn string output",
    epilog="for use with fmcusa_gl primarily",
)

parser.add_argument(
    "-e",
    "--encrypt",
    help="value for encrypt flag in conn string",
    type=str,
    default=None,
)

parser.add_argument(
    "-t",
    "--trust",
    help="value for trust flag in conn string",
    type=str,
    default=None,
)

parser.add_argument(
    "-p",
    "--procedure",
    help="name of stored procedure to be run",
    type=str,
    default=None,
)


def execute_sql(conn_str: str, script: str):
    if ";" in script or "(" in script or ")" in script:
        print("disallowed character in stored procedure name")
        return False
    print(f"sending to server:\n{script}...", end="")
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute(script)
        cursor.close()
        conn.commit()
    print(f"complete")


def main():
    args = parser.parse_args()
    try:
        FMCUSAGL = SQLDB("fmcusa_gl_cfg.toml")
    except:
        print("failure to import settings from fmcusa_gl_cfg.toml")
        sys.exit(1)

    execute_sql(FMCUSAGL.pyodbc_conn(), f"exec {args.procedure}")


if __name__ == "__main__":
    main()
