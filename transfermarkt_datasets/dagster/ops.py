from dagster import InputDefinition, OpDefinition, Output, OutputDefinition, SourceAsset, In, asset, op, repository, Out
from pandas import DataFrame

from transfermarkt_datasets.core.dataset import Dataset
from transfermarkt_datasets.core.asset import Asset
from transfermarkt_datasets.dagster.io_managers import RawIOManager


# read raw data

raw_assets = ["competitions", "games", "players", "clubs", "appearances"]

def read_raw_fn(context, inputs):
    io = RawIOManager()
    data = io.load_input(context)
    yield Output(data)

read_raw_ops = {
    asset: OpDefinition(
        name=f"read_{asset}",
        input_defs=[],
        output_defs=[OutputDefinition(DataFrame)],
        compute_fn=read_raw_fn,
        required_resource_keys={"settings"}
    )
    for asset in raw_assets
}

# base assets

base_assets = ["competitions", "games", "players", "player_valuations", "clubs", "appearances"]

def build_base_fn(context, inputs):

    if context.op_config and context.op_config.get("asset_name"):
        asset_name = context.op_config["asset_name"]
    else:
        asset_name = context.solid_def.name.split("_")[-1]

    asset = Dataset().get_asset_def("base_" + asset_name)()
    asset.build(context, inputs["raw"])
    yield Output(asset)

build_base_ops = {
    asset: OpDefinition(
        name=f"build_base_{asset}",
        input_defs=[InputDefinition("raw", DataFrame)],
        output_defs=[OutputDefinition(dagster_type=Asset, io_manager_key="prep_io_manager")],
        compute_fn=build_base_fn,
        required_resource_keys={"settings"},

    )
    for asset in base_assets
}

# validation tasks

def validate_fn(context, inputs):
    asset = inputs["base"]
    asset.validate()

validate_base_ops = {
    asset: OpDefinition(
        name=f"validate_base_{asset}",
        input_defs=[InputDefinition("base", Asset)],
        output_defs=[],
        compute_fn=validate_fn,
        required_resource_keys={"settings"},

    )
    for asset in base_assets
}

validate_cur_ops = {
    asset: OpDefinition(
        name=f"validate_cur_{asset}",
        input_defs=[InputDefinition("base", Asset)],
        output_defs=[],
        compute_fn=validate_fn,
        required_resource_keys={"settings"},

    )
    for asset in base_assets
}

# curated assets

@op(out=Out(io_manager_key="prep_io_manager"))
def build_cur_games(context, base_games, base_clubs):
    asset = Dataset().get_asset_def("cur_games")()
    asset.build(context, base_games, base_clubs)
    return asset
