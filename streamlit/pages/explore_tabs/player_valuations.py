import streamlit as st
import pandas as pd

from utils import load_asset, st_td_asset_summary

def player_valuations_tab():

    asset_name = "cur_player_valuations"

    st_td_asset_summary(asset_name)
    player_valuations = load_asset(asset_name).copy()
    player_valuations["dateweek"] = pd.to_datetime(player_valuations["dateweek"])

    player_valuation_stock = (
        player_valuations[["dateweek", "market_value"]]
            .groupby(["dateweek"])
            .mean()
    )

    st.line_chart(player_valuation_stock)
