import streamlit as st
import pandas as pd

from utils import read_file_contents, load_td, draw_asset
from inflection import dasherize

td = load_td()

st.title("Analysis: Player Value")

st.markdown(
    read_file_contents("streamlit/markdown_blocks/analysis/analysis_player_value.md")
)
st.markdown("""
-----
""")

players = td.assets["cur_players"].prep_df

most_valued_players = (
    players
        .sort_values(by="market_value_in_gbp", ascending=False)
        .head(6)
)[["pretty_name", "market_value_in_gbp"]]

st.header("Most valued Players")
most_valued_players
