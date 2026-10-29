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

    reports = get_views(sist_auth(usr, key))


if __name__ == "__main__":
    main()
