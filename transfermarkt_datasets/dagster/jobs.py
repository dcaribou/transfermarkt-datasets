from transfermarkt_datasets.dagster.io_managers import asset_io_manager
from transfermarkt_datasets.core.dataset import Dataset, read_config

config = read_config()

td = Dataset()
td.discover_assets()

build_job = td.as_dagster_job(
    resource_defs={"asset_io_manager": asset_io_manager}
)
