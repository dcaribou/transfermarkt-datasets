import streamlit as st
import pandas as pd

from utils import read_file_contents, load_td, draw_asset
from inflection import dasherize

td = load_td()

st.title("Explore :mag_right:")
st.markdown("""
> :warning: There is no support yet for downloading assets from the streamlit app.
> For downloading the data, use the [data.world](https://data.world/dcereijo/player-scores) or [Kaggle](https://www.kaggle.com/datasets/davidcariboo/player-scores) datasets for now.

-----
"""
)
st.image("resources/diagram.svg", use_column_width=True)
st.markdown(
    read_file_contents("streamlit/markdown_blocks/explore/intro.md")
)

st.markdown("""
-----
""")

for asset_name, asset in td.assets.items():
    if not asset.public:
        continue
    
    draw_asset(asset)
