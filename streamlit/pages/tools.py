import streamlit as st
import pandas as pd

from utils import load_td

td = load_td()

st.title("td-tools")

st.header("Games explorer")

games: pd.DataFrame = td.assets["cur_games"].prep_df
competitions: pd.DataFrame = td.assets["cur_competitions"].prep_df

competitions_options = competitions["competition_id"].unique()
competition_selected = st.selectbox(label="Competition", options=competitions_options)
games_on_competition = games[games["competition_code"] == competition_selected]

club_options = games_on_competition["club_home_pretty_name"].unique()
club_selected = st.selectbox(label="Club", options=club_options)
games_on_club = games_on_competition[games_on_competition["club_home_pretty_name"] == club_selected]

season_options = games_on_club["season"].unique()
season_selected = st.selectbox(label="Season", options=season_options)
games_on_season = games_on_club[games_on_club["season"] == season_selected]

st.dataframe(games_on_season)
