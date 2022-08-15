import streamlit as st
from utils import load_asset, st_td_asset_summary

def appearances_tab():

    st_td_asset_summary("cur_appearances")

    appearances = load_asset("cur_appearances").copy()
    latest_appearances = appearances.sort_values(
        by="date",
        ascending=False
    ).head(3)

    st.table(
        latest_appearances[["date", "player_pretty_name", "minutes_played"]]
    )
