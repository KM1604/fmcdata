import polars as pl
import pyodbc
import tomllib

from fmccfg import read_toml
from fmccsv import csv_to_df
from fmcsql import return_query
from SQLDB import SQLDB

def execute_sql(conn_str: str, script: str):
    '''
    execute command on sql server with provided connection string

    Keyword Arguments:
    conn_str -- connection string for server
    script -- command to be run (avoid disallowed characters)
    '''
    if ";" in script or "(" in script or ")" in script:
        print("disallowed character in stored procedure name")
        return False
    print(f"sending to server:\n{script}...", end="")
    conn = pyodbc.connect(conn_str)
    conn.close()
    with pyodbc.connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute(script)
        cursor.close()
        conn.commit()
    print(f"complete")


def main():
    FMCUSAGL = SQLDB("fmcusa_gl_cfg.toml")
    cfg = read_toml('re_notes_cfg.toml')
    # TODO: update re toml cfg file to include notes info
    re_comments = csv_to_df('km0032_notes_comments.csv', encoding=cfg.get('km0032').get('src_encoding'))
    re_comments = re_comments.rename(cfg.get('km0032').get('col_rename')).with_columns(pl.lit('comment').alias('note_type'))
    print(re_comments)
    
    re_memo = csv_to_df('km0031_notes_memo.csv', encoding=cfg.get('km0031').get('src_encoding'))
    re_memo = re_memo.rename(cfg.get('km0031').get('col_rename')).with_columns(pl.lit('memo').alias('note_type'))
    print(re_memo)
    
    re_notes = pl.concat([re_memo, re_comments], how='vertical_relaxed')
    print(re_notes)

    re_notes.write_database(
        connection='sqlite:///re_notes.db',
        table_name=cfg.get('notes_upload').get('s_table'),
        if_table_exists='replace',
        engine='sqlalchemy'
    )
    re_notes.write_database(
        connection=FMCUSAGL.polars_conn(),
        table_name=cfg.get('notes_upload').get('s_table'),
        if_table_exists='replace',
        engine='sqlalchemy'
    )
    t_re_notes = return_query(
        FMCUSAGL.polars_conn(encrypt='true', trust='false'),
        'SELECT * FROM re_notes'
    )
    t_re_notes.write_database(
        connection='sqlite:///re_notes.db',
        table_name='t_re_notes',
        if_table_exists='replace',
        engine='sqlalchemy'
    )
    print(FMCUSAGL.pyodbc_conn())
    execute_sql(
        FMCUSAGL.pyodbc_conn(),
        'exec u_re_notes'
    )
    execute_sql(
        FMCUSAGL.pyodbc_conn(),
        'DROP TABLE s_re_notes'
    )


if __name__ == '__main__':
    main()