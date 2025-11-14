PARQUET FOLDERS COME FROM:

6_Paper_3_Expander\smartds_connect_download_parquet

and these may also come / is related to:
6_Paper_1_Simulator\imp_20240601\GSO\urban-suburban

--------------------

PLEASE REMEMBER TO CHECK INSIDE THE FOLDER "daily_csvs". These CSVs ate the ones you need for the proper simulation. A proper README folder is placed in there.

--------------------

SEE upgrades_lookup.json FOR THE MEANING OF THE UPGRADES. THE download_parquets_homes_NC.py HAS THE PRINT: "QUERY_YEAR = 2024" AND "resstock_tmy3_release_2" TO NAVIGATE THE OEDI DATABASE.

--------------------

To generate the curves for the updates, run get_scenario_csv_controls.py (the input folder is "plot_parquet_differences", the output folder "plot_parquet_differences").

The CSVs generated inside "get_scenario_csv_controls" must be used to generate profiles in independent folders where this workflow must be repeated.

--------------------

When this workflow is repeated, follow this order of file execution:

1) scale_feeder_curves_NC.py

2) find_max_day_curve_NC.py

3) plot_parquet_differences.py

4) get_scenario_csv_controls.py



