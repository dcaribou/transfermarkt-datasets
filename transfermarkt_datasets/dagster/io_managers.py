from dagster import IOManager, InputContext, OutputContext, io_manager
import pandas as pd

from transfermarkt_datasets.core.dataset import Dataset
from transfermarkt_datasets.core.asset import Asset

class RawIOManager(IOManager):
    def handle_output(self, context, obj):
        pass

    def load_input(self, context):

        raw_files_path = context.resources.settings["source_path"]
        seasons = context.resources.settings["seasons"]

        asset_name = context.op_config["asset_name"]

        raw_dfs = []

        if asset_name == "competitions":
            df = pd.read_json(
            f"data/competitions.json",
            lines=True,
            convert_dates=True,
            orient={'index', 'date'}
            )
            raw_dfs.append(df)
        else:
            file_name = asset_name + ".json"
            for season in seasons:

                season_file = f"{raw_files_path}/{season}/{file_name}"

                context.log.debug("Reading raw data from %s", season_file)
                df = pd.read_json(
                season_file,
                lines=True,
                convert_dates=True,
                orient={'index', 'date'}
                )
                df["season"] = season
                df["season_file"] = season_file
                if len(df) > 0:
                    raw_dfs.append(df)

        return pd.concat(raw_dfs, axis=0)


class PrepIOManager(IOManager):
    def handle_output(self, context: OutputContext, obj: Asset) -> None:
        obj.save_to_stage()

    def load_input(self, context: InputContext, asset_name=None) -> Asset:

        _asset_name = asset_name or context.upstream_output.solid_def.name.replace("build_", "")

        asset: Asset = Dataset().get_asset_def(_asset_name)()
        asset.load_from_stage()

        return asset


class AssetIOManager(IOManager):
    def handle_output(self, context, obj: Asset):
        obj.save_to_stage()

    def load_input(self, context):

        asset_name = context.upstream_output.solid_def.name.replace("build_", "")

        td = Dataset()
        td.discover_assets()
        
        at = td.assets[asset_name]
        at.load_from_stage()

        return at

@io_manager
def raw_io_manager(init_context):
    return RawIOManager()

@io_manager
def prep_io_manager(init_context):
    return PrepIOManager()

@io_manager
def asset_io_manager(init_context):
    return AssetIOManager()
