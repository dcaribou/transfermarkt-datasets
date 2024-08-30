import streamlit as st
import pandas as pd

from utils import load_td

import altair as alt
import plotly.express as px

td = load_td()

st.title("Manager Performance")

st.markdown("""
This app amis to show which managers exhibit the best performance in their careers. The peformance indicator in this case is the win percentage (`pct_win`).

The win percentage is calculated looking at the team performance, mostly based on the `club_games` asset, and it's displayed across seasons and competition types (domestic or international).
""")
st.markdown("""
-----
""")

# pull up assets to be used in the calculations

competitions = td.assets["cur_competitions"].prep_df
club_games = td.assets["cur_club_games"].prep_df
games = td.assets["cur_games"].prep_df.copy()
clubs = td.assets["cur_clubs"].prep_df

# TODO: it's kind of annoying that this does not come in with the correct type already
games["season"] = pd.to_datetime(games["season"], format="%Y")

# define the set of leagues to be used in the app

DEFAULT_COMPETITIONS = ["GB1", "L1", "ES1", "IT1"]
DEFAULT_SEASONS = [2021, 2024]
DEFAULT_MANAGERS = ["Jürgen Klopp", "Pep Guardiola"]
DEFAULT_COMPETITION_TYPES = [
    "domestic_cup", "domestic_league", "international_cup"
]
DEFAULT_MIN_GAMES = 1

with st.expander("Baseline Filters"):

    col1, col2 = st.columns(2)

    competition_ids = col1.multiselect(
        "Domestic competition IDs",
        options=competitions["competition_id"].unique(),
        default=DEFAULT_COMPETITIONS
    )

    all_seasons = games["season"].dt.year.unique()
    seasons_limits = col2.slider(
        label="Seasons",
        min_value=int(min(all_seasons)),
        max_value=int(max(all_seasons)),
        step=1,
        value=DEFAULT_SEASONS
    )
    seasons = list(range(seasons_limits[0], seasons_limits[1], 1))

    managers = col1.multiselect(
        label="Managers",
        options=club_games["own_manager_name"].unique(),
        default=DEFAULT_MANAGERS
    )

    minimun_number_of_games_played = (
        col2.number_input('Minimum number of games played in a season', value=DEFAULT_MIN_GAMES)
    )

# create a data mart with all required measures and dimensions
# for the base mart we use a "club game" granularity

mart = club_games.merge(
        games,
        how="left",
        on="game_id"
    ).merge(
        clubs[["club_id", "name", "domestic_competition_id"]].rename(
            columns={
                "name": "club_name",
                "domestic_competition_id": "club_domestic_competition_id"
            }
        ),
        how="left",
        on="club_id"
    )

baselined_mart = mart[
    (mart["season"].dt.year.isin(seasons)) &
    (mart["club_domestic_competition_id"].isin(competition_ids)) & 
    (mart["own_manager_name"].isin(managers)) &
    (mart["competition_type"].isin(DEFAULT_COMPETITION_TYPES))
]

# manager perfomance is evaluated on its win percentage
# we want to calculate manages win percentage by season and competition type

managers_win_pct_per_season = (
    baselined_mart
        .groupby(by=[
                "club_name", "season", "own_manager_name", "competition_type"
            ])["is_win"]
        .agg(func=["count", "sum"])
        .reset_index()
)
managers_win_pct_per_season.rename(
    columns={
        "count": "total_games",
        "sum": "total_wins",
    },
    inplace=True
)
managers_win_pct_per_season["pct_win"] = (
    managers_win_pct_per_season["total_wins"] / managers_win_pct_per_season["total_games"]
)

# now we only want to consider managers who had a significative number of games during a season

managers_win_pct_per_season = managers_win_pct_per_season[
    (managers_win_pct_per_season["total_games"] > minimun_number_of_games_played)
]

# the comparative view allows visualizing manager win percentage by competition side by side

managers_win_pct_comparative = (
    managers_win_pct_per_season[
        managers_win_pct_per_season["season"].dt.year.isin(seasons) &
        managers_win_pct_per_season["own_manager_name"].isin(managers)
    ]
        .groupby(["own_manager_name", "competition_type"])["pct_win"]
        .mean()
        .reset_index()
)

st.subheader("Comparative performance")

# https://plotly.com/python/polar-chart/

fig = px.line_polar(
    managers_win_pct_comparative,
    r="pct_win",
    theta="competition_type",
    color="own_manager_name",
    line_close=True,
    color_discrete_sequence=px.colors.sequential.Plasma_r,
    template="plotly_dark"
)
st.plotly_chart(fig, use_container_width=True)


# finally, diplay results in chart

st.subheader(
    f"Performance by season"
)

managers_win_pct_perf_by_season = (
    managers_win_pct_per_season.groupby(
        ["season", "own_manager_name", "club_name"]
    )["total_games", "total_wins"].sum()
    .reset_index()
)

managers_win_pct_perf_by_season["pct_win"] = (
    managers_win_pct_perf_by_season["total_wins"] / managers_win_pct_perf_by_season["total_games"]
)

st.altair_chart(
    altair_chart=alt.Chart(managers_win_pct_perf_by_season).mark_bar().encode(
        x="year(season):T",
        y="own_manager_name:N",
        color=alt.Color(
            shorthand="pct_win",
            scale=alt.Scale(range=["red", "green"])
        ),
        tooltip="club_name:N"
    ),
    use_container_width=True
)
