"""A generic set of util functions used across the project.
"""
import pathlib
from pandas import DataFrame
import yaml
from typing import Dict, List

import boto3
from time import sleep

# Raw data layout convention: data/raw/<source>/<season>/<asset>.<ext>
RAW_DATA_ROOT = pathlib.Path("data/raw")


def raw_data_path(source: str, season: int = None, asset: str = None, ext: str = "json.gz") -> pathlib.Path:
	"""Build a raw data path following the source-agnostic convention.

	Convention: data/raw/<source>/<season>/<asset>.<ext>

	Args:
		source: Source identifier (e.g. 'transfermarkt-scraper', 'openfootball').
		season: Season year. If None, returns the source root.
		asset: Asset name (e.g. 'games', 'clubs'). If None, returns the season dir.
		ext: File extension. Defaults to 'json.gz'.

	Returns:
		pathlib.Path to the requested location.
	"""
	path = RAW_DATA_ROOT / source
	if season is not None:
		path = path / str(season)
	if asset is not None:
		path = path / f"{asset}.{ext}"
	return path


def read_config(config_file="config.yml") -> Dict:
	"""Read project configuraiton from a yaml file.

	Args:
			config_file (str, optional): Path to the config file. Defaults to "config.yml".

	Returns:
			Dict: The parsed config in a python dict
	"""
	with open(config_file) as config_file:
		config = yaml.load(config_file, yaml.Loader)
		return config

def seasons_list(seasons: str) -> List[str]:
	"""Generate a list of seasons to acquire based on the "seasons" string. For example,
	for "2012-2014", it should return [2012, 2013, 2014].

	Args:
		seasons (str): A string representing a date or range of dates to acquire.

	Returns:
		List[str]: The expanded list of seasons to acquire.
	"""
	parts = seasons.split("-")

	if len(parts) == 0:
		raise Exception("Empty string provided for seasons")

	elif len(parts) == 1: # single season string
		return [int(seasons)]

	elif len(parts) == 2: # range of seasons
		start, end = parts
		season_range = list(range(int(start), int(end) + 1))

		if len(season_range) > 20:
			raise Exception("The range is too high")
		else:
			return season_range

	else:
		raise Exception(f"Invalid string: {seasons}")


def submit_batch_job_and_wait(
	job_name: str,
	job_queue: str,
	job_definition: str,
	cmd: List[str],
	vcpus: float,
	memory: int,
	timeout: int = 4  # in hours
	) -> None:
	"""Launch a job in AWS Batch and wait for completion.
	"""
	
	client = boto3.client("batch", region_name="eu-west-1")

	job_definitions = client.describe_job_definitions(
		jobDefinitionName=job_definition
	)
	revisions = []
	for jd in job_definitions["jobDefinitions"]:
		if jd["jobDefinitionName"] == job_definition:
			revisions.append(jd["revision"])

	if not len(revisions) > 0:
		raise Exception(f"Unable to extract job definition revisions")

	revision = max(revisions)

	submited_job = client.submit_job(
		jobName=job_name,
		jobQueue=job_queue,
		jobDefinition=f"{job_definition}:{revision}",
		containerOverrides={
			'command': cmd,
			'resourceRequirements': [
				{'value': str(vcpus), 'type': 'VCPU'},
				{'value': str(memory), 'type': 'MEMORY'}
			]
		},
		timeout={
			'attemptDurationSeconds': timeout*60*60
		}
	)

	job_id = submited_job["jobId"]
	print(f"Submitted job {job_id}")

	def get_job_status(client, job_id: str) -> str:
		jobs = client.describe_jobs(
			jobs=[job_id]
		)
		if not jobs.get("jobs") or len(jobs["jobs"]) != 1 or jobs["jobs"][0]["jobId"] != job_id:
			raise Exception("Unexpected 'describe_jobs' reponse")
		else:
			return jobs["jobs"][0]["status"]

	job_status = get_job_status(client, job_id)
	while job_status not in ["SUCCEEDED", "FAILED"]:
		sleep(5)
		job_status = get_job_status(client, job_id)

	if job_status == "FAILED":
		raise Exception(f"Job {job_id} has failed")


def get_sample_values(df: DataFrame, column: str, n: int) -> List[object]:
	return list(df[column].unique())[:3]
