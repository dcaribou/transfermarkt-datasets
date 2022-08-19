"""
# My first app
Here's our first attempt at using data to create a table:
"""

import streamlit as st
import pandas as pd
import altair as alt

from utils import (
    load_td,
    read_file_contents
)

st.set_page_config(layout="wide")

# Beginning of page definition

# CSS to inject contained in a string
hide_dataframe_row_index = read_file_contents("markdown_blocks/css.html")
st.markdown(hide_dataframe_row_index, unsafe_allow_html=True)

set_background_image = read_file_contents("images/css.html")
st.markdown(set_background_image, unsafe_allow_html=True)

st.title("Transfermartk Datasets :soccer:")

td = load_td()
players = td.assets["cur_players"].prep_df.copy()
games = td.assets["cur_games"].prep_df.copy()

st.header("About")
st.markdown(
    read_file_contents("markdown_blocks/about.md")
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
