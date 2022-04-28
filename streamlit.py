"""
# My first app
Here's our first attempt at using data to create a table:
"""

import streamlit as st
import pandas as pd

from transfermarkt_datasets.transfermarkt_datasets import TransfermarktDatasets

st.title("Transfermartk Datasets")

@st.cache
def load_data():
  td = TransfermarktDatasets()
  df = td.assets["games"].prep_df
  return df

df = load_data()
df
