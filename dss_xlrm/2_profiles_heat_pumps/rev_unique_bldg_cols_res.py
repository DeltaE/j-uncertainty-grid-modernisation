# -*- coding: utf-8 -*-
"""
Created on Sun Nov  9 21:02:38 2025

@author: luisfernando
"""

# check_bldg_ids_commercial.py
import sys
import pandas as pd

# CSV path (override by passing a path as the first CLI arg)
csv_path = sys.argv[1] if len(sys.argv) > 1 else "NC_final_residential.csv"

try:
    # Read only the needed column
    df = pd.read_csv(csv_path, usecols=["bldg_id"])
except ValueError:
    # Column not found: read all and fail clearly
    df = pd.read_csv(csv_path)
    raise KeyError(f"'bldg_id' column not found. Columns present: {list(df.columns)}")

# Clean/standardize: coerce to numeric, drop NaNs, cast to int
s = pd.to_numeric(df["bldg_id"], errors="coerce").dropna().astype(int)

total_rows = len(s)
unique_count = s.nunique()
dup_count = total_rows - unique_count

# Also compute via set (your request)
as_set = set(s.tolist())

print(f"File: {csv_path}")
print(f"Rows in bldg_id (after cleaning): {total_rows}")
print(f"Unique bldg_id count: {unique_count}")
print(f"len(set(bldg_id))        : {len(as_set)}")
print(f"Duplicate rows (total - unique): {dup_count}")

if dup_count > 0:
    dup_series = s[s.duplicated(keep=False)]
    counts = dup_series.value_counts().sort_index()
    print("\nDuplicate values and their counts:")
    # Show all duplicates with how many times each appears
    for val, cnt in counts.items():
        print(f"{val}: {cnt} times")

# If you want the order-preserving unique list as well:
unique_ordered = list(dict.fromkeys(s.tolist()))  # preserves first occurrence order
# print("\nFirst 10 unique (ordered):", unique_ordered[:10])
