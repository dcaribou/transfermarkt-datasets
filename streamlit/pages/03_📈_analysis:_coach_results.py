import streamlit as st
import pandas as pd

from utils import read_file_contents, load_td, draw_asset
from inflection import dasherize

td = load_td()

st.title("Analysis: Coach Results")

st.markdown(
    read_file_contents("streamlit/markdown_blocks/analysis/analysis_player_value.md")
)
st.markdown("""
-----
""")

games = td.assets["cur_games"].prep_df

games[(~games["home_manager_name"].isna()) & (games["round"] == "Final")]
