import streamlit as st
import os
from pathlib import Path
import pandas as pd

from transfermarkt_datasets.core.dataset import Dataset

@st.cache
def load_asset(name : str) -> pd.DataFrame:
    """Load a transfermarkt dataset from disk.

    Args:
        name (str): The name of the asset to be loaded.

    Returns:
        pd.DataFrame: The asset as a dataframe.
    """

    if os.environ["STREAMLIT"] == "cloud":
        os.system("dvc pull")

    td = Dataset()
    td.discover_assets()
    td.load_assets()
    df = td.assets[name].prep_df
    return df

def read_file_contents(markdown_file: str):
    """Read a markdown file in disk as a string.

    Args:
        markdown_file (str): The path of the file to be read.

    Returns:
        str: The contents of the file as a string.
    """
    return Path("streamlit/" + markdown_file).read_text()
