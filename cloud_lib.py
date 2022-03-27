from time import sleep
from typing import List
import boto3

def submit_batch_job_and_wait(
    job_name: str,
    job_queue: str,
    job_definition: str,
    branch: str,
    script: str,
    args: List[str],
    vcpus: float,
    memory: int
):

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
        'command': [ branch, script, "local" ] + args,
        'resourceRequirements': [
            {'value': str(vcpus), 'type': 'VCPU'},
            {'value': str(memory), 'type': 'MEMORY'}
        ]
    },
    timeout={
        'attemptDurationSeconds': 4*60*60
    }
  )

  job_id = submited_job["jobId"]
  print(f"Submitted job {job_id}")

  def get_job_status(client, job_id : str) -> str:
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
