from dagster import IOManager, io_manager

from transfermarkt_datasets.core.dataset import Dataset
from transfermarkt_datasets.core.asset import Asset

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
def asset_io_manager(init_context):
    return AssetIOManager()
