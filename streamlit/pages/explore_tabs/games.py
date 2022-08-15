import streamlit as st
from utils import load_asset



def games_tab():
    st.header("Games")

    games = load_asset("cur_games").copy()

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
