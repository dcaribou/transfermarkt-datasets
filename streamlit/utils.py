import streamlit as st
import os
from pathlib import Path
import pandas as pd

from transfermarkt_datasets.core.dataset import Dataset
from transfermarkt_datasets.core.asset import Asset

@st.cache
def load_td() -> Dataset:

    if os.environ["STREAMLIT"] == "cloud":
        os.system("dvc pull")

    td = Dataset()
    td.discover_assets()
    td.load_assets()

    return td

def read_file_contents(markdown_file: str):
    """Read a markdown file in disk as a string.

    Args:
        markdown_file (str): The path of the file to be read.

    Returns:
        str: The contents of the file as a string.
    """
    return Path("streamlit/" + markdown_file).read_text()

def draw_asset(asset: Asset) -> None:
    """Draw a transfermarkt-dataset asset summary

    Args:
        asset_name (str): Name of the asset
    """

    st.header(asset.name)

    info_tab, schema_tab, data_tab = (
        st.tabs([
            "Info", "Schema", "Data"
        ])
    )

    with info_tab:
        st.markdown(asset.description)

    with schema_tab:
        st.dataframe(asset.schema_as_dataframe())

    with data_tab:
        st.dataframe(asset.prep_df.head(10))
