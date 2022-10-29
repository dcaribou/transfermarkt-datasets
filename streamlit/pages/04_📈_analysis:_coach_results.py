import streamlit as st
import pandas as pd

from utils import read_file_contents, load_td, draw_asset
from inflection import dasherize

def compute_manager_performance(
    cur_club_games: pd.DataFrame,
    cur_games: pd.DataFrame):

    joined = cur_club_games.merge(
        cur_games,
        how="left",
        on="game_id"
    )

    club_season_results = (
      joined
        .groupby(by=["club_id", "season", "own_manager_name", "competition_type"])["is_win"]
        .agg(func=["count", "sum"])
        .reset_index()
    )

    club_season_results["pct_won"] = club_season_results["sum"] / club_season_results["count"]
    del club_season_results["sum"]
    del club_season_results["count"]

    pivoted = club_season_results.pivot(
      index=["club_id", "season", "own_manager_name"],
      columns="competition_type",
      values="pct_won"
    ).reset_index()

    return pivoted

td = load_td()

st.title("Analysis: Coach Results")

st.markdown(
    read_file_contents("streamlit/markdown_blocks/analysis/analysis_player_value.md")
)
st.markdown("""
-----
""")

club_games = td.assets["cur_club_games"].prep_df
games = td.assets["cur_games"].prep_df

manager_performance = compute_manager_performance(
    club_games,
    games
)

st.dataframe(
    manager_performance.nlargest(n=100, columns=["domestic_league"])
)
