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




st.title("transfermarkt-datasets")
st.markdown("""
[transfermarkt-datasets](https://github.com/dcaribou/transfermarkt-datasets) is a clean, structured and **automatically updated** football dataset scraped from [Transfermarkt](https://www.transfermarkt.co.uk/).
The dataset is composed of multiple files (also called "assets") with a bunch of useful information on professional football competitions.
""")

st.info(
    """For a thorough list of assets in this dataset and their contents head out to the [explore](explore) page.""",
    icon="🔎"
)

st.header("How do I use it?")
st.markdown("""
Access to this dataset is provided through some of its various [frontends](https://github.com/dcaribou/transfermarkt-datasets#data-publication).

Additionally, the source code for the project, including this streamlit app, is available [in Github](https://github.com/dcaribou/transfermarkt-datasets).
You are most welcome to contribute by opening a new issue or picking an existing one from the [`Issues`](https://github.com/dcaribou/transfermarkt-datasets/issues) section.
""")

st.header("What can I do with it?")
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
        players.groupby("domestic_competition_id")["market_value_in_gbp"]
        .sum()
        .reset_index()
        .sort_values(["market_value_in_gbp"], ascending=False)
    ),
    x="domestic_competition_id",
    y="market_value_in_gbp",
    color="domestic_competition_id",
    title="Total player market value in GBP"
)
fig.update_layout(showlegend=False)
right_col.plotly_chart(
    figure_or_data=fig,
    use_container_width=True
)
