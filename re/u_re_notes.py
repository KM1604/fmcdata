import polars as pl
import tomllib

from fmccfg import read_toml
from fmccsv import csv_to_df
from SQLDB import SQLDB

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
    # run stored procedure to merge s_re_notes to re_notes
    # DROP TABLE of s_re_notes

if __name__ == '__main__':
    main()