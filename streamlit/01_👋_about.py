import streamlit as st
import altair as alt
import plotly.express as px
import pandas as pd

from utils import load_td

st.set_page_config(
    layout="wide",
    page_title="About",
    page_icon="👋"
)

col1, col2 = st.columns([2, 1])
col2.markdown("""
[![GitHub Repo stars](https://img.shields.io/github/stars/dcaribou/transfermarkt-datasets?style=social)](https://github.com/dcaribou/transfermarkt-datasets)
[![Kaggle](https://kaggle.com/static/images/open-in-kaggle.svg)](https://www.kaggle.com/datasets/davidcariboo/player-scores)
[<img src="https://assets.data.world/assets/logo-sparkle-noscircle.befdc9e044ded0c2756c24b3bff43b1c.png" alt="drawing" width="19"/>
[data.world](https://data.world/dcereijo/player-scores)]
""",
unsafe_allow_html=True
)

st.title("""transfermarkt-datasets""")
st.markdown("""
[transfermarkt-datasets](https://github.com/dcaribou/transfermarkt-datasets) is a clean, structured and **automatically updated** football dataset scraped from [Transfermarkt](https://www.transfermarkt.co.uk/).
The dataset is composed of multiple files (also called "assets") with a bunch of useful information on professional football competitions.
""",

)

st.info(
    """For a thorough list of assets in this dataset and their contents head out to the [explore](explore) page.""",
    icon="🔎"
)

st.subheader("How do I use it?")
st.markdown("""
Access to this dataset is provided through some of its various [frontends](https://github.com/dcaribou/transfermarkt-datasets#%EF%B8%8F-frontends), you can use any of those to start playing with the data. 

For advance users, the source code for the project is available [in Github](https://github.com/dcaribou/transfermarkt-datasets), together with instructions for setting up you local environment to run it. 
""")

st.subheader("What can I do with it?")
st.markdown("""
That very much depends on you, but below are a few simple examples that may inspire you.
""")
st.info(
    """Checkout the different analyses from the left sidebar for more advanced examples.""",
    icon="📈"
)

td = load_td()
games: pd.DataFrame = td.assets["cur_games"].prep_df
players: pd.DataFrame = td.assets["cur_players"].prep_df

left_col, right_col = st.columns(2)

fig = px.line(
    data_frame=games.groupby("date")["attendance"].mean().reset_index(),
    x="date",
    y="attendance",
    title="Average stadium attendance"
)
left_col.plotly_chart(
    figure_or_data=fig,
    use_container_width=True
)

fig = px.bar(
    data_frame=(
        players.groupby("current_club_domestic_competition_id")["market_value_in_eur"]
        .sum()
        .reset_index()
        .sort_values(["market_value_in_eur"], ascending=False)
    ),
    x="current_club_domestic_competition_id",
    y="market_value_in_eur",
    color="current_club_domestic_competition_id",
    title="Total player market value in EUR"
)
fig.update_layout(showlegend=False)
right_col.plotly_chart(
    figure_or_data=fig,
    use_container_width=True
)
