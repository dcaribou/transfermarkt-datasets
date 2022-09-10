import streamlit as st
import pandas as pd

from utils import read_file_contents, load_td, draw_asset
from inflection import dasherize

td = load_td()

st.title("Dictionary :mag_right:")
st.image("resources/diagram.svg")
st.markdown(
    read_file_contents("streamlit/markdown_blocks/dictionary/intro.md")
)


for asset_name, asset in td.assets.items():
    if not asset.public:
        continue
    
    draw_asset(asset)
