import streamlit as st
import pandas as pd

from utils import (
    read_file_contents,
    load_td,
    draw_asset,
    draw_dataset_er_diagram
)

td = load_td()

st.title("Explore :mag_right:")

# i'm not convinced by this
# draw_dataset_er_diagram(td)

st.markdown(
    read_file_contents("streamlit/markdown_blocks/explore/intro.md")
)

st.info("""
There is no support yet for downloading assets from the streamlit app.
For downloading the data, may use the equivalent [data.world](https://data.world/dcereijo/player-scores) 
or [Kaggle](https://www.kaggle.com/datasets/davidcariboo/player-scores) 
datasets for now.
""", 
icon="ℹ️")

st.markdown("""
-----
""")

for asset_name, asset in td.assets.items():
    if not asset.public:
        continue
    
    draw_asset(asset)
