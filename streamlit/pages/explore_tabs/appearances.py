import streamlit as st
from utils import load_asset

def appearances_tab():
    appearances = load_asset("cur_appearances").copy()
    latest_appearances = appearances.sort_values(
        by="date",
        ascending=False
    ).head(3)
    st.header("Appearances")

    st.table(
        latest_appearances[["date", "player_pretty_name", "minutes_played"]]
    )
