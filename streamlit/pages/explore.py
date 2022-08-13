import streamlit as st
import pandas as pd

from utils import load_asset

games = load_asset("cur_games").copy()
clubs = load_asset("base_clubs").copy()
appearances = load_asset("cur_appearances").copy()
players = load_asset("cur_players").copy()
player_valuations = load_asset("cur_player_valuations").copy()
player_valuations["dateweek"] = pd.to_datetime(player_valuations["dateweek"])

st.header("Match Data")

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

st.table(
    latest_games[["date", "club_home_pretty_name", "club_away_pretty_name", "aggregate"]],

)

st.subheader("Latest Appearances")
st.table(
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
