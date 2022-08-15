import streamlit as st

from utils import load_asset,st_td_asset_summary

def games_tab():

    asset_name = "cur_games"

    st_td_asset_summary(asset_name)
    games = load_asset(asset_name).copy()


    latest_games = games.sort_values(
        by="date",
        ascending=False
    ).head(3)

    latest_games["aggregate"] = \
        latest_games["home_club_goals"].astype("string") + " - " + \
        latest_games["away_club_goals"].astype("string")
        
    st.table(
        latest_games[["date", "club_home_pretty_name", "club_away_pretty_name", "aggregate"]],

    )
