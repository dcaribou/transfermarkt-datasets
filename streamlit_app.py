"""
# My first app
Here's our first attempt at using data to create a table:
"""

import streamlit as st
import pandas as pd
import os

from transfermarkt_datasets.core.dataset import Dataset

st.set_page_config(layout="wide")

@st.cache
def load_asset(name : str) -> pd.DataFrame:
    if os.environ["STREAMLIT"] == "cloud":
        os.system("dvc pull")

    td = Dataset()
    td.discover_assets()
    td.load_assets()
    df = td.assets[name].prep_df
    return df

main_leagues = [
    "GB1", "ES1", "IT1", "L1", "FR1"
]

games = load_asset("cur_games").copy()
clubs = load_asset("base_clubs").copy()
appearances = load_asset("cur_appearances").copy()
players = load_asset("base_players").copy()
player_valuations = load_asset("cur_player_valuations").copy()
player_valuations["dateweek"] = pd.to_datetime(player_valuations["dateweek"])

# Beginning of page definition

# CSS to inject contained in a string
hide_dataframe_row_index = """
            <style>
            .row_heading.level0 {display:none}
            .blank {display:none}
            </style>
            """

# Inject CSS with Markdown
st.markdown(hide_dataframe_row_index, unsafe_allow_html=True)

st.title("Transfermartk Datasets :soccer:")

st.header("Match Data")
left_col, right_col = st.columns(2)

latest_games = games.sort_values(
    by="date",
    ascending=False
).head(3)

latest_games["aggregate"] = \
    latest_games["home_club_goals"].astype("string") + " - " + \
    latest_games["away_club_goals"].astype("string")

latest_appearances = appearances.sort_values(
    by="date",
    ascending=False
).head(3)

left_col.subheader("Latest Games")
left_col.table(
    latest_games[["date", "club_home_pretty_name", "club_away_pretty_name", "aggregate"]],

)
right_col.subheader("Latest Appearances")
right_col.table(
    latest_appearances[["date", "player_pretty_name", "minutes_played"]]
)

st.header("Market Value Data")
left_col, right_col = st.columns(2)

player_valuation_stock = (
    player_valuations[["dateweek", "market_value"]]
        .groupby(["dateweek"])
        .mean()
)

most_valued_players = (
    players
        .sort_values(by="market_value_in_gbp", ascending=False)
        .head(6)
)[["pretty_name", "market_value_in_gbp"]]

left_col.subheader("Avg Player Value")
left_col.line_chart(player_valuation_stock)
right_col.subheader("Most Valuable Players")
right_col.table(most_valued_players)
