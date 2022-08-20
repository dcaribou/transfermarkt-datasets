import streamlit as st
import pandas as pd

from utils import load_td, draw_asset_explore

td = load_td()

st.title("td-tools")

st.header("Games explorer")

draw_asset_explore(
    asset=td.assets["cur_games"],
    columns=["competition_code", "club_home_pretty_name", "season"]
)

st.header("Players explorer")
draw_asset_explore(
    asset=td.assets["cur_players"],
    columns=["domestic_competition_id", "club_pretty_name", "pretty_name"]
)
