from cProfile import label
from typing import List
import streamlit as st
import os
from pathlib import Path
import pandas as pd
from inflection import titleize
from datetime import datetime, timedelta

import sys
cwd = os.getcwd()
sys.path.insert(0, cwd)

from transfermarkt_datasets.core.dataset import Dataset
from transfermarkt_datasets.core.asset import Asset

# community components
from streamlit_agraph import agraph, Node, Edge, Config
from st_aggrid import AgGrid

@st.cache
def load_td() -> Dataset:
    """Instantiate and initialise a Dataset, so it can be used in the app.

    Returns:
        Dataset: A transfermark_datasets.core.Dataset that is initialised and ready to be used.
    """

    if os.environ["STREAMLIT"] == "cloud":
        os.system("dvc pull data/prep")

    td = Dataset()
    td.discover_assets()
    td.load_assets()

    return td

def read_file_contents(file_path: str):
    """Read a markdown file in disk as a string.

    Args:
        markdown_file (str): The path of the file to be read.

    Returns:
        str: The contents of the file as a string.
    """
    return Path(file_path).read_text()

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
        selected = st_col.selectbox(label=at_col, options=options, key=(asset.name + "-" + at_col))
        df = df[df[at_col] == selected]
    
    st.dataframe(df)

def draw_dataset_er_diagram(td: Dataset) -> None:
    nodes = []
    edges = []

    for asset_name, asset in td.assets.items():
        if asset.public:
            nodes.append(
                Node(
                    id=asset_name,
                    label=asset_name,
                    shape="box"
                )
            )
    
    for relationship in td.get_relationships():
        edges.append(
            Edge(
                source=relationship["from"],
                target=relationship["to"],
                label=relationship["on"]["source"][0]
            )
        )

    config = Config(
        width=1000, 
        height=500
    )

    agraph(nodes=nodes, edges=edges, config=config)

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
