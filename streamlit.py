"""
# My first app
Here's our first attempt at using data to create a table:
"""

import streamlit as st
import pandas as pd

from transfermarkt_datasets.transfermarkt_datasets import TransfermarktDatasets

td = TransfermarktDatasets()

df = td.assets["clubs"].prep_df

df
