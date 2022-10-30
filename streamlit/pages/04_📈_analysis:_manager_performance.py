from altair.vegalite.v4.api import value
import streamlit as st
import pandas as pd

from utils import read_file_contents, load_td

import altair as alt

td = load_td()

st.title("Analysis: Manager Performance")

st.markdown(
    read_file_contents("streamlit/markdown_blocks/analysis/analysis_manager_performance.md")
)
st.markdown("""
-----
""")

# pull up assets to be used in the calculations

competitions = td.assets["cur_competitions"].prep_df
club_games = td.assets["cur_club_games"].prep_df
games = td.assets["cur_games"].prep_df
clubs = td.assets["cur_clubs"].prep_df

# define the set of leagues to be used in the analysis

st.subheader("Filters")

col1, col2 = st.columns(2)

focus_on_leagues = col1.multiselect(
    "Competition codes to be included",
    options=competitions["competition_id"].unique(),
    default=["GB1", "L1", "ES1", "IT1"]
)

minimun_number_of_games_played = (
    col2.number_input('Minimum number of games played in a season', value=5)
)
top_n = col2.number_input("Top N performers", value=20)

# create a data mart with all required measures and dimensions
# for the base mart we use a "club game" granularity

mart = club_games.merge(
        games,
        how="left",
        on="game_id"
    ).merge(
        clubs[["club_id", "pretty_name", "domestic_competition_id"]].rename(
            columns={
                "pretty_name": "club_pretty_name",
                "domestic_competition_id": "club_domestic_competition_id"
            }
        ),
        how="left",
        on="club_id"
    )

# TODO: it's kind of annoying that this does not come in with the correct type already
mart["season"] = pd.to_datetime(mart["season"], format="%Y")

# limit the analysis to the selected set of competitions only
mart = mart[mart["club_domestic_competition_id"].isin(focus_on_leagues)]

# manager perfomance is evaluated on its win percentage
# we want to calculate manages win percentage by season and competition type

managers_win_pct_per_season = (
    mart
        .groupby(by=[
                "club_pretty_name", "season", "own_manager_name", "competition_type"
            ])["is_win"]
        .agg(func=["count", "sum"])
        .reset_index()
)
managers_win_pct_per_season.rename(columns={"count": "total_games"}, inplace=True)
managers_win_pct_per_season["pct_win"] = managers_win_pct_per_season["sum"] / managers_win_pct_per_season["total_games"]
del managers_win_pct_per_season["sum"]

# now we only want to consider managers who had a significative number of games during a season

managers_win_pct_per_season = managers_win_pct_per_season[
    managers_win_pct_per_season["total_games"] > minimun_number_of_games_played
]

# we want to further focus the analysis on a small set of high performance managers
top_n_managers = managers_win_pct_per_season.nlargest(n=top_n, columns=["pct_win"])[
    "own_manager_name"
].values

managers_win_pct_per_season = managers_win_pct_per_season[
    managers_win_pct_per_season["own_manager_name"].isin(top_n_managers)]

# finally, diplay results in chart

st.subheader(
    f"Managers performance by season in national competitions"
)

managers_win_pct_per_season_in_domestic_league = managers_win_pct_per_season[
    managers_win_pct_per_season["competition_type"] == "domestic_league"
]


st.altair_chart(
    altair_chart=alt.Chart(managers_win_pct_per_season_in_domestic_league).mark_bar().encode(
        x="year(season):T",
        y="own_manager_name:N",
        color=alt.Color(
            shorthand="pct_win",
            scale=alt.Scale(range=["red", "green"])
        ),
        tooltip="club_pretty_name"
    ),
    use_container_width=True
)

st.subheader(
    f"Managers performance by season in international competitions"
)

managers_win_pct_per_season_in_international_competitions = managers_win_pct_per_season[
    managers_win_pct_per_season["competition_type"] == "international_cup"
]

st.altair_chart(
    altair_chart=alt.Chart(managers_win_pct_per_season_in_international_competitions).mark_bar().encode(
        x="year(season):T",
        y="own_manager_name:N",
        color=alt.Color(
            shorthand="pct_win",
            scale=alt.Scale(range=["red", "green"])
        ),
        tooltip="club_pretty_name"
    ),
    use_container_width=True
)


