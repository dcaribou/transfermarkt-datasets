from typing import List
import streamlit as st
import os
from pathlib import Path
import pandas as pd
from inflection import titleize
from datetime import datetime, timedelta

import sys
cwd = os.getcwd()
print(cwd)
sys.path.insert(0, cwd)

from transfermarkt_datasets.core.dataset import Dataset
from transfermarkt_datasets.core.asset import Asset


@st.cache
def load_td() -> Dataset:

    if os.environ["STREAMLIT"] == "cloud":
        os.system("dvc pull data/prep")

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

    left_col, right_col = st.columns([5,1])

    left_col.subheader(titleize(asset.frictionless_resource_name))

    left_col.markdown(asset.description)
    delta = get_records_delta(asset)
    right_col.metric(
        label="# of records",
        value=len(asset.prep_df),
        delta=delta,
        help="Total number of records in the asset / New records in the past week"
    )

    with st.expander("Explore"):
        draw_asset_explore(asset)

    st.markdown("---")

def draw_asset_explore(asset: Asset, columns: List[str] = None) -> None:
    """Draw dataframe together with dynamic filters for exploration.

    Args:
        asset (Asset): The asset to draw the explore for.
        columns (List[str]): The list of columns to create a filter on.
    """
    if columns is None:
        columns = asset.prep_df.columns[:4]

    st_cols = st.columns(len(columns))
    df = asset.prep_df.copy()

    for st_col, at_col in zip(st_cols, columns):
        options = df[at_col].unique()
        selected = st_col.selectbox(label=at_col, options=options)
        df = df[df[at_col] == selected]

    st.dataframe(df)

def get_records_delta(asset: Asset, offset: int = 7) -> int:
    """Get an asset records' delta (number of new records in last n days).

    Args:
        asset (Asset): The asset to be calculating the delta from.
        offset (int, optional): Number in days to be consider for the delta calculation. Defaults to 7.

    Returns:
        int: Number of records.
    """
    df = asset.prep_df

    if "date" in df.columns:
        dt = pd.to_datetime(df["date"])
        delta = len(df[dt > (datetime.now() - timedelta(days=offset))])
        return delta
    else:
        return None
