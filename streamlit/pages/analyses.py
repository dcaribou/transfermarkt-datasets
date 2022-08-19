import streamlit as st
import pandas as pd
import altair as alt

from utils import read_file_contents, load_td, draw_asset
from inflection import dasherize

td = load_td()

games = td.assets["cur_games"].prep_df.copy()
players = td.assets["cur_players"].prep_df.copy()

left_col, right_col = st.columns(2)

left_col.subheader(
    "Average stadium attendance by period"
)
# ht

# https://pandas.pydata.org/docs/user_guide/timeseries.html#offset-aliases
option_date = left_col.selectbox(
    "Period",
    ("M", "Y", "W")
)
games["date_agg"] = pd.to_datetime(games["date"]).dt.to_period(option_date).dt.to_timestamp()
left_col.altair_chart(
    altair_chart=alt.Chart(games).mark_line().encode(
        x="date_agg",
        y="mean(attendance)"
    ),
    use_container_width=True
)

right_col.subheader(
    "Total player value by"
)
option_agg = right_col.selectbox(
    "By",
    ("M", "Y", "W")
)
right_col.altair_chart(
    altair_chart=alt.Chart(players).mark_bar().encode(
        x=alt.X("domestic_competition_id", sort="-y"),
        y=alt.Y("sum(market_value_in_gbp)")
    ), 
    use_container_width=True
)
