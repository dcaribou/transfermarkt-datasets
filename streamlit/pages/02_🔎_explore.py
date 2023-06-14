import streamlit as st
import pandas as pd

from utils import (
    load_td,
    draw_asset,
    draw_dataset_er_diagram,
    draw_dataset_index
)

td = load_td()

st.title("Explore :mag_right:")

st.markdown("""
The dataset is composed of multiple assets that are automatically updated **once a week**.
Each file contains the attributes of the entity and the IDs that can be used to join them together.""")

draw_dataset_er_diagram(
    image="resources/diagram.svg",
    caption="Data model."
)

st.markdown("""
For example, the `appearances` file contains **one row per player appearance**, i.e. one row per player per game played.
For each appearance you will find attributes such as `goals`, `assists` or `yellow_cards` and IDs referencing other entities within the dataset, such as `player_id` and `game_id`.
""")

st.info("""
There is no support yet for downloading assets from the streamlit app.
For downloading the data, may use one of the other dataset [frontends](https://github.com/dcaribou/transfermarkt-datasets#frontends).
""", 
icon="ℹ️")

st.markdown("""
-----
""")

for asset_name, asset in td.assets.items():
    if not asset.public:
        continue
    
    draw_asset(asset)


draw_dataset_index(td)
