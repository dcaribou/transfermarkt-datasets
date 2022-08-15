import streamlit as st

from utils import load_asset, st_td_asset_summary

def players_tab():

    asset_name = "cur_players"

    st_td_asset_summary(asset_name)
    players = load_asset(asset_name).copy()

    most_valued_players = (
        players
            .sort_values(by="market_value_in_gbp", ascending=False)
            .head(6)
    )[["pretty_name", "market_value_in_gbp"]]
    
    st.table(most_valued_players)
