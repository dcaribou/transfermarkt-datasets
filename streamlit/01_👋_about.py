import streamlit as st
import pandas as pd
import altair as alt

from utils import (
    load_td,
    read_file_contents
)

st.set_page_config(
    layout="wide",
    page_title="About",
    page_icon="👋"
)

td = load_td()
games = td.assets["cur_games"].prep_df.copy()
players = td.assets["cur_players"].prep_df.copy()

left_col, right_col = st.columns([2,1])

right_col.altair_chart(
    altair_chart=alt.Chart(games).mark_line().encode(
        x="yearmonth(date)",
        y=alt.Y("mean(attendance)", axis=alt.Axis(format='~s', title='Average stadium attendance'))
    ),
    use_container_width=True
)
right_col.altair_chart(
    altair_chart=alt.Chart(players).mark_bar().encode(
        x=alt.X("domestic_competition_id", sort="-y"),
        y=alt.Y("sum(market_value_in_gbp)", axis=alt.Axis(format='~s', title='Total player market value in GBP'))
    ), 
    use_container_width=True
)

left_col.markdown(
    read_file_contents("streamlit/markdown_blocks/about/intro.md")
)
