import hashlib
import hmac
import polars as pl
import requests

from email.utils import formatdate

def get_views(usr: str, pw: str):
    """given api usr and pw, returns json response"""
    r = requests.get("https://fmcusa.sitestacker.net/api/reports/views", auth=(usr, pw))
    print(r.status_code)
    print(r.encoding)
    print(r.text)
    views = r.json
    print(views)
    return views


'''
Code received on 2026-10-09 from JB (employed at sist)
Refactored slightly by KM on 2026-10-09

def ss_auth_headers(api_id: str, api_secret: str, method: str = "GET", content_type: str = "") -> dict:
    """Site Stacker HMAC headers. Build fresh for every request (the date must be within 5 minutes)."""
    date = formatdate(usegmt=True)  # e.g. "Fri, 09 Oct 2026 17:19:44 GMT"
    string_to_sign = f"{method.upper()}\n{content_type}\n{date}"
    signature = hmac.new(api_secret.encode(), string_to_sign.encode(), hashlib.sha256).hexdigest()
    headers = {"ss-date": date, "Authorization": f"HMAC {api_id}:{signature}"}
    if content_type:
        headers["Content-Type"] = content_type
    return headers
'''


def sist_auth(usr: str, key: str, method: str = "GET", content_type: str = "") -> dict:
    '''
    Generates Site Stacker HMAC headers. Time sensitive within 300s of generation.

    Keyword Arguments:
    usr -- API id
    key -- API secret
    method -- defaults to GET, use uppercase
    content_type -- defaults to empty string - stolen from sist rep
    '''
    date = formatdate(usegmt=True) #not ISO "Fri, 09 Oct 2026 17:19:44 GMT"
    string_to_sign = f'{method}\n{content_type}\n{date}'
    signature = hmac.new(key.encode(), string_to_sign.encode(), hashlib.sha256).hexdigest()
    headers = {"ss-date": date, "Authorization": f"HMAC {usr}:{signature}"}
    if content_type:
        headers['Content-Type'] = content_type
    return headers

def main():
    print("success")


if __name__ == "__main__":
    main()
