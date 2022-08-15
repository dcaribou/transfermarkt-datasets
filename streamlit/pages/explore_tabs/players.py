import streamlit as st
from utils import load_asset

def players_tab():
    st.header("Players")
    players = load_asset("cur_players").copy()
    most_valued_players = (
        players
            .sort_values(by="market_value_in_gbp", ascending=False)
            .head(6)
    )[["pretty_name", "market_value_in_gbp"]]
    st.table(most_valued_players)
