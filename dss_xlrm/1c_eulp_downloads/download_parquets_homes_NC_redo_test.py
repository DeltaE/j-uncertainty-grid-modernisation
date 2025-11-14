# -*- coding: utf-8 -*-
"""
Created on Sun Nov  9 21:51:16 2025

@author: luisfernando
"""

import requests, xml.etree.ElementTree as ET
from urllib.parse import quote

def list_prefixes(bucket: str, prefix: str):
    # delimiter=/ makes S3 return folder-like prefixes in <CommonPrefixes>
    url = f"https://{bucket}.s3.amazonaws.com/?list-type=2&delimiter=%2F&prefix={quote(prefix)}"
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    root = ET.fromstring(r.text)
    return [cp.find("s3:Prefix", ns).text for cp in root.findall("s3:CommonPrefixes", ns)]

bucket = "oedi-data-lake"
prefix = "nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/resstock_tmy3_release_2/timeseries_individual_buildings/by_state/upgrade=0/"

states = list_prefixes(bucket, prefix)
print(states[:10])  # e.g., 'AK/', 'AL/', ...
