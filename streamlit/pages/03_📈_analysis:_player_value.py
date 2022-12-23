from typing import List

import streamlit as st
import pandas as pd

from utils import read_file_contents, load_td

import plotly.express as px

def top_n_players(df: pd.DataFrame, n: int) -> List[str]:
    return (df
        .sort_values(by="market_value_in_gbp", ascending=False)
        .head(n)["pretty_name"]
        .values
    )

td = load_td()

st.title("Analysis: Player Value")

st.markdown("""
Market value for players is captured in the `players` and `player_valuations` assets. Find out more about this assets in the [explore](explore) section.

* [x] Who are top N valued players today? How did their value behave across time?
* [ ] How does value correlate with performance?
""")
st.markdown("""
-----
""")

# pull up assets to be used in the calculations

players: pd.DataFrame = td.assets["cur_players"].prep_df
player_valuations: pd.DataFrame = td.assets["cur_player_valuations"].prep_df

# define define values for script arguments

DEFAULT_PLAYER_PRETTY_NAMES = ["Kylian Mbappe"]

with st.expander("Baseline Filters"):

    top_n = st.number_input(
        label="Top N",
        min_value=1,
        max_value=15,
        value=10
    )

    player_pretty_names = st.multiselect(
        label="Player name",
        options=players["pretty_name"].unique(),
        default=top_n_players(players, top_n)
    )

# create a data mart with all required measures and dimensions
# for the base mart we use a "player valuation" granularity

mart = player_valuations.merge(
    players[["player_id", "pretty_name"]],
    how="left",
    on="player_id"
)

baselined_mart = mart[
    (mart["pretty_name"].isin(player_pretty_names))
]

# most valuesd players

st.header("Most valued Players")


most_valued_players = (
    baselined_mart
        .sort_values(by="date")
        .groupby("pretty_name")
        .tail(1)
        .sort_values(by="market_value", ascending=False)
        .head(top_n)
)[["pretty_name", "market_value"]]

fig = px.bar(
    most_valued_players,
    x="pretty_name",
    y="market_value",
    color="pretty_name"
)
st.plotly_chart(
    figure_or_data=fig,
    use_container_width=True
)

# value across time

st.header("Time progression")

fig = px.line(
    mart[mart["pretty_name"].isin(player_pretty_names)],
    x="date",
    y="market_value",
    color="pretty_name"
)
st.plotly_chart(
    figure_or_data=fig,
    use_container_width=True
)
