import streamlit as st
import pandas as pd

from utils import read_file_contents, load_td, draw_asset
from inflection import dasherize

td = load_td()

st.title("Dictionary :mag_right:")
st.markdown(
    read_file_contents("markdown_blocks/explore__intro.md")
)
# st.image("resources/diagram.png")

# asset_list_md = []
# for asset_name, asset in td.assets.items():
#     if not asset.public:
#         continue
    
#     asset_list_md.append(f"* [{asset_name}](#{dasherize(asset_name)})")

# st.markdown("\n".join(asset_list_md), unsafe_allow_html=True)

players = td.assets["cur_players"].prep_df

st.download_button(
     label="Download player data as CSV",
     data=players.to_csv(),
     file_name='large_df.csv',
     mime='text/csv',
 )


for asset_name, asset in td.assets.items():
    if not asset.public:
        continue
    
    draw_asset(asset)
