import streamlit as st
import pandas as pd

from utils import load_td

import altair as alt
import plotly.express as px

td = load_td()

st.title("Matchday Overview")

st.markdown("""
Show match results by date or round.
""")
st.markdown("""
-----
""")

# pull up assets to be used in the calculations
games = td.assets["cur_games"].prep_df.copy()
competitions = td.assets["cur_competitions"].prep_df
club_games = td.assets["cur_club_games"].prep_df

# TODO: it's kind of annoying that this does not come in with the correct type already
games["season"] = games["season"].astype(int)

# create initial mart
mart = games

with st.expander("Baseline Filters"):

    col1, col2 = st.columns(2)

    competition_id = col1.selectbox(
            "Domestic competition ID",
            options=competitions["competition_id"].unique(),
            index=9
        )

    competition_type = col1.selectbox(
            "Competition type",
            options=competitions["type"].unique(),
            index=2
    )

    season = col2.selectbox(
            "Season",
            options=games["season"].unique(),
            index=len(games["season"].unique()) - 1
        )

baselined_mart = mart[
    (mart["competition_id"] == competition_id) &
    (mart["competition_type"] == competition_type) &
    (mart["season"] == season)
]


matchday = st.selectbox(
        "Round",
        options=baselined_mart["round"].unique(),
        index=0
    )

# create a data mart with all required measures and dimensions
# for the base mart we use a "club game" granularity

baselined_mart = baselined_mart[
    (baselined_mart["round"] == matchday)
].sort_values("date")

baselined_mart_club_games = club_games.merge(baselined_mart, on="game_id")

# stats
st.header("Stats")
col1, col2, col3 = st.columns(3)

col1.altair_chart(
    altair_chart=alt.Chart(baselined_mart_club_games).mark_bar().encode(
        x='sum(own_goals)',
        y='hosting',
        color='hosting'
    ),
    use_container_width=True
)

col2.altair_chart(
    altair_chart=alt.Chart(baselined_mart_club_games).mark_boxplot().encode(
        x='attendance'
    ),
    use_container_width=True
)

# aggregates
st.header("Aggregates")
st.dataframe(
    baselined_mart[
        [
            "date",
            "home_club_name",
            "aggregate",
            "away_club_name"
        ]
    ]
)
