import streamlit as st
import pandas as pd

from utils import read_file_contents
from pages.explore_tabs.competitions import competitions_tab
from pages.explore_tabs.games import games_tab
from pages.explore_tabs.players import players_tab
from pages.explore_tabs.player_valuations import player_valuations_tab
from pages.explore_tabs.appearances import appearances_tab
from pages.explore_tabs.clubs import clubs_tab

st.title("Explore :mag_right:")
st.markdown(
    read_file_contents("markdown_blocks/explore__intro.md")
)

competitions_t, clubs_t, players_t, player_valuations_t, games_t, appearances_t = (
    st.tabs([
        "Competitions", "Clubs", "Players", "Player Valuations", "Games", "Appearances"
    ])
)

with competitions_t:
    competitions_tab()

with clubs_t:
    clubs_tab()

with players_t:
    players_tab()

with player_valuations_t:
    player_valuations_tab()

with games_t:
    games_tab()

with appearances_t:
    appearances_tab()
