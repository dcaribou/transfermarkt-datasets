"""
# My first app
Here's our first attempt at using data to create a table:
"""

import streamlit as st
import pandas as pd

from utils import load_asset, read_file_contents

st.set_page_config(layout="wide")

# Beginning of page definition

# CSS to inject contained in a string
hide_dataframe_row_index = read_file_contents("markdown_blocks/css.html")
st.markdown(hide_dataframe_row_index, unsafe_allow_html=True)

st.image("streamlit/images/cover.jpg")

st.title("Transfermartk Datasets :soccer:")

players = load_asset("base_players")
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
