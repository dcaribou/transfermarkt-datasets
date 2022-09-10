"""A generic set of util functions used across the project.
"""

import re
import yaml
from typing import Dict, Tuple

import boto3
from time import sleep

from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter


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


def submit_batch_job_and_wait(
	job_name: str,
	job_queue: str,
	job_definition: str,
	branch: str,
	message: str,
	script: str,
	args: str,
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

	if len(args) > 0:
		additional_args = args.split(" ")
	else:
		additional_args = []

	submited_job = client.submit_job(
		jobName=job_name,
		jobQueue=job_queue,
		jobDefinition=f"{job_definition}:{revision}",
		containerOverrides={
			'command': [branch, message, script, "local"] + additional_args,
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


def parse_market_value(market_value):
	"""Parse a "market value" string into an integer number representing a GBP (british pounds) amount,
	such as "£240Th." or "£34.3m".

	:market_value: "Market value" string
	:return: An integer number representing a GBP amount
	"""

	if market_value is not None:
		match = re.search('£([0-9\.]+)(Th|m)', str(market_value))
		if match:
			factor = match.group(2)
			if factor == 'Th':
				numeric_factor = 1000
			elif factor == 'm':
				numeric_factor = 1000000
			else:
				return None

			value = match.group(1)
			return int(float(value)*numeric_factor)
		else:
			return None
	else:
		return None


def cast_metric(metric):
	if len(metric) == 0:
		return 0
	else:
		return int(metric)


def cast_minutes_played(minutes_played):
	if len(minutes_played) > 0:
		numeric = minutes_played[:-1]
		return int(numeric)
	else:
		return 0


def geocode(place: str) -> Tuple:
	"""Get coordinates from a place's name.

	Args:
			place (str): A string that describes the place.

	Returns:
			Tuple: A tuple with the coordinates (latitude, longitude)
	"""

	geolocator = Nominatim(user_agent="transfermarkt-datasets")
	geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

	output = geocode(place)

	return (output.latitude, output.longitude)
