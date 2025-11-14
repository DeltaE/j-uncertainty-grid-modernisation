1) Run save_needed_sd_parquets.py to generate needed_parquets.pkl

2) Copy needed_parquets.pkl into folder *5_kvar_kw_prep* to generate the necessary files to convert kw to kvar according to smart ds.

3) Run the parquet_to_csv.py adjusted for the circuits running here. This generates "folder_timestamps.pkl", which needs to go to  too. 

4) Go get *5_kvar_kw_prep* after following the instructions hosted there. That results in kvar_ratios.pkl. Please copy it here.

5) Run the generate_kvar_csvs.py to pass from pickle to CSV

