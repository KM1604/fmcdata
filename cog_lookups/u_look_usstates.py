from fmctools import fmccfg, fmccog, fmccsv, fmcsql
import polars as pl

cfg = fmccfg.read_toml("cognito_cfg.toml")
cfgval = cfg["usstates"]

SQL = fmcsql.SQLServer(cfg["sql"])

query = cfgval["query"]
df = fmcsql.return_query(SQL.polars_conn, query)
filename = cfgval["src_filename"]
fmccsv.writecsv_from_frame(df, filename)
cog_api = cfg["cognito"]["api_key"]

r = fmccog.post_csv_to_form(
    cog_api,
    filename,
    cfgval["form_id"],
    cfgval["import_mode"],
    cfgval["email"],
    cfgval["match_on"],
)
print(r)
print(r.headers)
print(r.text)

