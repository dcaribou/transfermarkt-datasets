from cProfile import label
from typing import List
import streamlit as st
import os
from pathlib import Path
import pandas as pd
from inflection import dasherize, titleize
from datetime import datetime, timedelta

import base64

import sys
cwd = os.getcwd()
sys.path.insert(0, cwd)

from transfermarkt_datasets.core.dataset import Dataset
from transfermarkt_datasets.core.asset import Asset

@st.cache_data
def load_td() -> Dataset:
    """Instantiate and initialise a Dataset, so it can be used in the app.

    Returns:
        Dataset: A transfermark_datasets.core.Dataset that is initialised and ready to be used.
    """

    if os.environ["STREAMLIT"] == "cloud":
        os.system("dvc pull data/prep")

    td = Dataset()
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

def draw_dataset_index(td: Dataset) -> None:

    md_index_lines = []

    for asset_name, asset in td.assets.items():
        if asset.public:
            titelized_asset_name = titleize(asset.frictionless_resource_name).lower()
            asset_anchor = dasherize(asset.frictionless_resource_name).lower()
            md_index_line = f"* [{titelized_asset_name}](#{asset_anchor})"
            md_index_lines.append(
                md_index_line
            )

    st.sidebar.markdown(
        "\n".join(md_index_lines)
    )

def draw_asset(asset: Asset) -> None:
    """Draw a transfermarkt-dataset asset summary

    Args:
        asset_name (str): Name of the asset
    """

    left_col, right_col = st.columns([5,1])

    title = titleize(asset.frictionless_resource_name).lower()
    left_col.subheader(title)

    left_col.markdown(asset.description)
    delta = get_records_delta(asset)
    right_col.metric(
        label="# of records",
        value=len(asset.prep_df),
        delta=delta,
        help="Total number of records in the asset / New records in the past week"
    )

    with st.expander("Attributes"):
        draw_asset_schema(asset)

    with st.expander("Explore"):
        draw_asset_explore(asset)

    st.markdown("---")

def draw_asset_explore(asset: Asset) -> None:
    """Draw dataframe together with dynamic filters for exploration.

    Args:
        asset (Asset): The asset to draw the explore for.
    """
    
    tagged_columns = [
        field.name
        for field in asset.schema.get_fields_by_tag("explore")
    ]
    default_columns = list(asset.prep_df.columns[:4].values)

    if len(tagged_columns) > 0:
        columns = tagged_columns
    else:
        columns = default_columns

    filter_columns = st.multiselect(
        label="Search by",
        options=asset.prep_df.columns,
        default=columns
    )
    if len(filter_columns) == 0:
        filter_columns = columns

    st_cols = st.columns(len(filter_columns))

    df = asset.prep_df.copy()

    for st_col, at_col in zip(st_cols, filter_columns):

        options = list(df[at_col].unique())
 
        selected = st_col.selectbox(
            label=at_col,
            options=options,
            key=(asset.name + "-" + at_col)
        )
        if selected:
            df = df[df[at_col] == selected]

    MAX_DF_LENGTH = 20

    df_length = len(df)
    if df_length > MAX_DF_LENGTH:
        st.dataframe(
            df.head(MAX_DF_LENGTH)
        )
        st.warning(f"""
        The dataframe size ({df_length}) exceeded the maximum and has been truncated. 
        """)

    else:
        st.dataframe(df)


def draw_asset_schema(asset: Asset) -> None:
    st.dataframe(
        asset.schema_as_dataframe().astype(str),
        use_container_width=True
    )

# https://gist.github.com/treuille/8b9cbfec270f7cda44c5fc398361b3b1#file-render_svg-py-L12
def render_svg(svg, caption):
    """Renders the given svg string."""
    b64 = base64.b64encode(svg.encode('utf-8')).decode("utf-8")
    html_style = """
    <style>
        figure {
            border: 1px #cccccc solid;
            padding: 4px;
            margin: auto;
        }
        figcaption {
            background-color: black;
            color: white;
            font-style: italic;
            padding: 2px;
            text-align: center;
        }
    </style>
    """
    html_image = r'<img src="data:image/svg+xml;base64,%s"/>' % b64
    html_caption = f"<figcaption>{caption}</figcaption>"
    html_figure = f"""
    {html_style}
    <figure>
    {html_image}
    {html_caption}
    </figure>
    
    &nbsp;
    """
    st.write(html_figure, unsafe_allow_html=True)

def draw_dataset_er_diagram(image, caption) -> None:
    with open(image) as image:
        svg_string = "".join(image.readlines())

    render_svg(svg_string, caption)

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
