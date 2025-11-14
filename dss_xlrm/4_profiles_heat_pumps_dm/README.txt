The process of "4_profiles_heat_pumps" is repeated here.

The folder needs as inputs:
parquet_commercial_20250330_comm_NC
parquet_residential_short_20250330_NC


------

Inside "daily_csvs", only run "parquet_to_csv.py" AND generate_kvar_csvs.py (the input kvar_ratios.pkl IS NEEDED FOR THE LATTER). These .py and .pkl files are in 4_profiles_heat_pumps\daily_csvs.

Importantly, the file needs to have the correct name of the .csv that is opened as df_NDC. E.g., "NC_parquet_and_bldgs_dm.csv" instead of "NC_parquet_and_bldgs.csv".

------

Any scenario folder starts with the "profiles_heat_pumps" substring in the name needs to repeat the process.



