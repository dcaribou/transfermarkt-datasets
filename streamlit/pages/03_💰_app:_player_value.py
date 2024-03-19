from typing import List

import streamlit as st
import pandas as pd

from utils import read_file_contents, load_td

import plotly.express as px

def top_n_players(df: pd.DataFrame, n: int) -> List[str]:
    return (df
        .sort_values(by="market_value_in_eur", ascending=False)
        .head(n)["name"]
        .values
    )

td = load_td()

st.title("Player Value")

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

with st.expander("Baseline Filters"):

    top_n = st.number_input(
        label="Top N",
        min_value=1,
        max_value=15,
        value=10
    )

    player_names = st.multiselect(
        label="Player name",
        options=players["name"].unique(),
        default=top_n_players(players, top_n)
    )

# create a data mart with all required measures and dimensions
# for the base mart we use a "player valuation" granularity

mart = player_valuations.merge(
    players[["player_id", "name"]],
    how="left",
    on="player_id"
)

baselined_mart = mart[
    (mart["name"].isin(player_names))
]

# most valuesd players

st.header("Most valued Players")


most_valued_players = (
    baselined_mart
        .sort_values(by="date")
        .groupby("name")
        .tail(1)
        .sort_values(by="market_value_in_eur", ascending=False)
        .head(top_n)
)[["name", "market_value_in_eur"]]

fig = px.bar(
    most_valued_players,
    x="name",
    y="market_value_in_eur",
    color="name"
)
st.plotly_chart(
    figure_or_data=fig,
    use_container_width=True
)

# value across time

st.header("Time progression")

fig = px.line(
    mart[mart["name"].isin(player_names)],
    x="date",
    y="market_value_in_eur",
    color="name"
)
st.plotly_chart(
    figure_or_data=fig,
    use_container_width=True
)
