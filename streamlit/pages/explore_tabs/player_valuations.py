import streamlit as st
import pandas as pd

from utils import load_asset

def player_valuations_tab():

    st.header("Player Valuations")

    player_valuations = load_asset("cur_player_valuations").copy()
    player_valuations["dateweek"] = pd.to_datetime(player_valuations["dateweek"])

    player_valuation_stock = (
        player_valuations[["dateweek", "market_value"]]
            .groupby(["dateweek"])
            .mean()
    )

    st.line_chart(player_valuation_stock)
