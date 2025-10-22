from fmctools import fmccfg, fmccog, fmccsv, fmcsql

cfg = fmccfg.read_toml("cognito_cfg.toml")
megmac = cfg["megmacactions"]

SQL = fmcsql.SQLServer(cfg["sql"])

query = megmac["query"]
df = fmcsql.return_query(SQL.polars_conn, query)
filename = megmac["src_filename"]
fmccsv.writecsv_from_frame(df, filename)
cog_api = cfg["cognito"]["api_key"]

r = fmccog.post_csv_to_form(
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
