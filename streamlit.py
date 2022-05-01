"""
# My first app
Here's our first attempt at using data to create a table:
"""

import streamlit as st
import pandas as pd
import numpy as np

from inflection import dasherize

from transfermarkt_datasets.transfermarkt_datasets import TransfermarktDatasets

st.set_page_config(layout="wide")


@st.cache
def load_asset(name : str) -> pd.DataFrame:
  td = TransfermarktDatasets()
  df = td.assets[name].prep_df
  return df

def sidebar_header(header_text: str):
    h1 = st.header(header_text)
    dasherized = dasherize(header_text)
    st.sidebar.markdown(f"[{header_text}](#{dasherized})", unsafe_allow_html=True)
    return h1

main_leagues = [
    "GB1", "ES1", "IT1", "L1", "FR1"
]

games = load_asset("games").copy()
games["date_dt"] = pd.to_datetime(games["date"])
games["date"] = games["date_dt"].dt.date
games["date_wk"] = games["date"] - pd.to_timedelta(games["date_dt"].dt.dayofweek, unit='d')
# games = games[games["competition_code"].isin(main_leagues)]
clubs = load_asset("clubs").copy()
appearances = load_asset("appearances").copy()
players = load_asset("players").copy()
player_valuations = load_asset("player_valuations").copy()
player_valuations["date_dt"] = pd.to_datetime(games["date_dt"])
player_valuations["date_wk"] = player_valuations["date_dt"] - pd.to_timedelta(player_valuations["date_dt"].dt.dayofweek, unit='d')

games_dn = games.merge(
    clubs,
    how="left",
    left_on="home_club_id",
    right_on="club_id",
    suffixes=[None, "_home"]
).merge(
    clubs,
    how="left",
    left_on="away_club_id",
    right_on="club_id",
    suffixes=[None, "_away"]
)

appearances_dn = appearances.merge(
    players,
    how="left",
    on="player_id"
).merge(
    games,
    how="left",
    on="game_id"
)

player_valuations_dn = player_valuations.merge(
    players,
    how="left",
    on="player_id"
).merge(
    clubs,
    how="left",
    left_on="current_club_id",
    right_on="club_id",
    suffixes=[None, "_current_club"]
)

players_dn = players.merge(
    clubs,
    how="left",
    left_on="current_club_id",
    right_on="club_id",
    suffixes=[None, "_current_club"]
)

assert len(games) == len(games_dn)
assert len(appearances) == len(appearances_dn)
assert len(player_valuations) == len(player_valuations_dn)
assert len(players) == len(players_dn)

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

sidebar_header("Match Data")

left_col, right_col = st.columns(2)

latest_games = games_dn.sort_values(
    by="date",
    ascending=False
).head(3)
latest_games["aggregate"] = \
    latest_games["home_club_goals"].astype("string") + " - " + \
    latest_games["away_club_goals"].astype("string")

latest_appearances = appearances_dn.sort_values(
    by="date",
    ascending=False
).head(3)

left_col.subheader("Latest Games")
left_col.table(
    latest_games[["date", "pretty_name", "pretty_name_away", "aggregate"]],

)
right_col.subheader("Latest Appearances")
right_col.table(
    latest_appearances[["date", "pretty_name", "minutes_played"]]
)

player_valuations_dn
p =( player_valuations_dn[["date_wk", "domestic_competition_id", "market_value"]]
        .groupby("date_wk", "domestic_competition_id")
        .mean())
p


sidebar_header("Market Value Data")
player_valuation_stock = (
    player_valuations_dn[["date_wk", "domestic_competition_id", "domestic_competition_id"]]
        .groupby("date_wk", "domestic_competition_id")
        .mean()
)
most_valued_players = (
    players_dn
        .sort_values(by="market_value_in_gbp", ascending=False)
        .head(6)
)[["pretty_name", "pretty_name_current_club", "market_value_in_gbp"]]

left_col.subheader("Avg Player Value")
left_col.line_chart(player_valuation_stock)
right_col.subheader("Most Valuable Players")
right_col.table(most_valued_players)
