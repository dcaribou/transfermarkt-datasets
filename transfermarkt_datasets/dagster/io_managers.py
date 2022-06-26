from dagster import IOManager, OutputContext, io_manager
import pandas as pd

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
    def handle_output(self, context: OutputContext, obj: Asset):
        _asset_name = context.solid_def.name.split("_")[-1]
        path = "transfermarkt_datasets/stage" # TODO read from config

        obj.to_csv(
            f"{path}/{_asset_name}.csv",
            index=False
        )

    def load_input(self, context: OutputContext, asset_name=None):

        _asset_name = asset_name or context.solid_def.name.split("_")[-1]
        path = "transfermarkt_datasets/stage" # TODO read from config

        return pd.read_csv(f"{path}/{_asset_name}.csv")

@io_manager
def raw_io_manager(init_context):
    return RawIOManager()

@io_manager
def prep_io_manager(init_context):
    return PrepIOManager()
