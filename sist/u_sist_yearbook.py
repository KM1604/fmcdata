import hashlib
import hmac
import polars as pl
import requests
import tomllib

from email.utils import formatdate

from fmccfg import read_toml
from sist_tools import get_views, sist_auth


def main():
    sist_cfg = read_toml("sist_cfg.toml")
    usr = sist_cfg.get("api").get("usr")
    key = sist_cfg.get("api").get("key")
    r = requests.get(
        "https://fmcusa.sitestacker.net/api/reports/views",
        headers=sist_auth(usr, key),
        timeout=30,
    )
    print(r.status_code)
    print(r.headers)
    print(r.text)


if __name__ == "__main__":
    main()
