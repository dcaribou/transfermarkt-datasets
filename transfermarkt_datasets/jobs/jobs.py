from dagster import job, op, get_dagster_logger
from transfermarkt_datasets.ops.ops import hello_cereal, build_games

# @job
# def hello_cereal_job():
#     hello_cereal()

@job
def transfermarkt_datasets():
    build_games()
