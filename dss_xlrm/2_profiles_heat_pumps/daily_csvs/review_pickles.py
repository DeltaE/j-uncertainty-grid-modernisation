# -*- coding: utf-8 -*-
"""
Created on Sun Apr  6 13:30:48 2025

@author: luisfernando
"""

import pickle

# Replace with your actual file path
# filename = 'needed_parquets.pkl'
# filename = 'folder_timestamps.pkl'
filename = 'kvar_ratios.pkl'

# Open and load the pickle file
with open(filename, 'rb') as file:
    data = pickle.load(file)

# Optional: print the contents or type
# print(type(data))
# print(data)
