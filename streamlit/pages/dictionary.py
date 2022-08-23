import streamlit as st
import pandas as pd

from utils import read_file_contents, load_td, draw_asset
from inflection import dasherize

td = load_td()

st.title("Dictionary :mag_right:")
st.markdown("""
The dataset is composed of multiple CSV files with information on competitions, games, clubs, players and appearances that is automatically updated **once a week**.
Each file contains the attributes of the entity and the IDs that can be used to join them together.
"""
)
st.image("resources/diagram.svg")
st.markdown("""
For example, the `appearances` file contains **one row per player appearance**, i.e. one row per player per game played.
For each appearance you will find attributes such as `goals`, `assists` or `yellow_cards` and IDs referencing other entities within the dataset, such as `player_id` and `game_id`.
"""
)

st.markdown("""

""")

st.markdown("----")


for asset_name, asset in td.assets.items():
    if not asset.public:
        continue
    
    draw_asset(asset)
