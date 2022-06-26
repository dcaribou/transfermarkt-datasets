from dagster import make_values_resource, job
from transfermarkt_datasets.dagster.ops import (
    read_raw_ops
)
from transfermarkt_datasets.dagster.ops import (
    build_base_ops,
    validate_base_ops
)
from transfermarkt_datasets.dagster.ops import (
    build_cur_games
)
from transfermarkt_datasets.dagster.io_managers import prep_io_manager
from transfermarkt_datasets.core.dataset import read_config

# build_transfermarkt_datasets

config = read_config()

@job(resource_defs={"prep_io_manager": prep_io_manager, "settings": make_values_resource()}, config=config)
def build_transfermarkt_datasets():

    build_base_competitions = build_base_ops["competitions"](
        read_raw_ops["competitions"]()
    )
    build_base_games = build_base_ops["games"](
        read_raw_ops["games"]()
    )
    build_base_clubs = build_base_ops["clubs"](
        read_raw_ops["clubs"]()
    )

    read_raw_players = read_raw_ops["players"]()
    build_base_players = build_base_ops["players"](
        read_raw_players
    )
    build_base_player_valuations = build_base_ops["player_valuations"](
        read_raw_players
    )
    build_base_appearances = build_base_ops["appearances"](
        read_raw_ops["appearances"]()
    )

    validate_base_games = validate_base_ops["games"](
        build_base_games
    )
    validate_base_players = validate_base_ops["players"](
        build_base_players
    )
    validate_base_appearances = validate_base_ops["appearances"](
        build_base_appearances
    )
    validate_base_competitions = validate_base_ops["competitions"](
        build_base_competitions
    )
    validate_base_clubs = validate_base_ops["clubs"](
        build_base_clubs
    )
    validate_base_player_valuations = validate_base_ops["player_valuations"](
        build_base_player_valuations
    )

    build_cur_games(
        build_base_games,
        build_base_clubs
    )

