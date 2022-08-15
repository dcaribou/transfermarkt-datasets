import streamlit as st
import pandas as pd

from utils import read_file_contents, load_td, draw_asset

td = load_td()

st.title("Explore :mag_right:")
st.markdown(
    read_file_contents("markdown_blocks/explore__intro.md")
)

for asset_name, asset in td.assets.items():
    if not asset.public:
        continue
    
    draw_asset(asset)
