"""
# My first app
Here's our first attempt at using data to create a table:
"""

import streamlit as st
import pandas as pd
import altair as alt

from utils import (
    load_asset,
    read_file_contents
)

st.set_page_config(layout="wide")

# Beginning of page definition

# CSS to inject contained in a string
hide_dataframe_row_index = read_file_contents("markdown_blocks/css.html")
st.markdown(hide_dataframe_row_index, unsafe_allow_html=True)

st.image("streamlit/images/cover.jpg")

st.title("Transfermartk Datasets :soccer:")

players = load_asset("cur_players").copy()
games = load_asset("cur_games").copy()

st.download_button(
     label="Download player data as CSV",
     data=players.to_csv(),
     file_name='large_df.csv',
     mime='text/csv',
 )

st.header("About")
st.markdown(
    read_file_contents("markdown_blocks/about.md")
)


left_col, right_col = st.columns(2)

left_col.subheader(
    "Average attendance series"
)
# https://pandas.pydata.org/docs/user_guide/timeseries.html#offset-aliases
option_date = left_col.selectbox(
    "By period",
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
    "National leagues by total player value"
)
option_agg = right_col.selectbox(
    "By agg",
    ("M", "Y", "W")
)
right_col.altair_chart(
    altair_chart=alt.Chart(players).mark_bar().encode(
        x=alt.X("domestic_competition_id", sort="-y"),
        y=alt.Y("sum(market_value_in_gbp)")
    ), 
    use_container_width=True
)

st.header("How to use")
st.subheader("As a data consumer")
st.markdown(
    read_file_contents("markdown_blocks/how_to_use__consumer.md")
)
st.subheader("As a data contributor")
st.markdown(
    read_file_contents("markdown_blocks/how_to_use__contributor.md")
)

st.header("Explore")
