from fmctools import fmccfg, fmccog, fmccsv, fmcsql

cfg = fmccfg.read_toml("cognito_cfg.toml")
ordi = cfg["ordination"]

SQL = fmcsql.SQLServer(cfg["sql"])

query = ordi["query"]
df = fmcsql.return_query(SQL.polars_conn, query)
filename = ordi["src_filename"]
fmccsv.writecsv_from_frame(df, filename)
cog_api = cfg["cognito"]["api_key"]

r = fmccog.post_csv_to_form(
    cog_api,
    filename,
    ordi["form_id"],
    ordi["import_mode"],
    ordi["email"],
    ordi["match_on"],
)
print(r)
print(r.headers)
print(r.text)
