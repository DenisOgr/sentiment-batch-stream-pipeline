import base64
import csv
import json
import os
import random
from dataclasses import dataclass

import collections
import re
from datetime import datetime
from google.cloud import storage
from io import StringIO
from typing import Union, Dict
from google.api_core.exceptions import NotFound

storage_client = storage.Client()

regex = r"^[a-zA-Z\d!\s@#$^&*()_+\"|}{?><~]*$"
pattern = re.compile(regex)

STREAM_JOB_TYPE = 'stream'
BATCH_JOB_TYPE = 'batch'


def get_bucket_for_job_type(job_type: str) -> str:
    return {
        BATCH_JOB_TYPE: os.environ['BATCH_RESULTS_BUCKET'],
        STREAM_JOB_TYPE: os.environ['STREAM_RESULTS_BUCKET'],
    }[job_type]

class Job:
    pass


class BatchJob:

    def __init__(self, query: str) -> None:
        if not list(storage_client.bucket(get_bucket_for_job_type(BATCH_JOB_TYPE)).list_blobs(prefix=f'jobs/{query}')):
            raise ValueError(f'There is no job for query: {query}')
        self.query = query


    def error(self):
        return self._error_path

    def meta(self):
        if not self._meta:
            self._meta = json.loads(
                storage_client.bucket(self.bucket).blob(self.meta_path).download_as_string().decode('utf-8'))
        return self._meta


@dataclass
class JobResult:
    data: list
    labels: list


def get_all_jobs():
    results = set()
    for blob in storage_client.list_blobs(bucket_or_name=os.environ['BATCH_RESULTS_BUCKET'], prefix=f'jobs'):
        job_id = blob.name.split('/')[1]
        job_id = get_string_from_job_id(job_id)
        results.add((job_id, 'b'))

    for blob in storage_client.list_blobs(bucket_or_name=os.environ['STREAM_RESULTS_BUCKET'], prefix=f'jobs'):
        if blob.name.endswith(".json"):
            job = json.loads(blob.download_as_string())
            results.add((job['query'], 's'))

    return list(results)


def get_random_style():
    styles = [
        'primary',
        'secondary',
        'success',
        'danger',
        'warning',
        'info',
        'light',
        'dark',
    ]
    return random.choice(styles)


def validate_query(query: str):
    validators = {
        'Number of words should be less than 100.': lambda v: len(v.split()) < 100,
        'Word should have more than 4 characters.': lambda v: len(v) > 3,
        'String includes invalid characters.': lambda v: pattern.match(v)
    }
    for message, validator in validators.items():
        if not validator(query):
            raise ValueError(message)


def validate_job_type(job_type: str):
    if job_type not in [BATCH_JOB_TYPE, STREAM_JOB_TYPE]:
        raise ValueError("Job type is invalid value.")


def get_string_from_job_id(job_id: str):
    # TODO: remove using base64
    prefix = 'base64:'
    if job_id.startswith(prefix):
        job_id = base64.b64decode(str.encode(job_id[len(prefix):])).decode()
    return job_id


def get_job_by_query(query: str, job_type: str) -> Union[None, Job]:
    blobs = storage_client.bucket(get_bucket_for_job_type(job_type)).list_blobs(prefix=f'jobs/{query}')
    return Job.build_me([blob.name for blob in blobs])


def store_job(job):
    # TODO add param for choosing job_type
    # TODO remove assert
    assert type(job) == dict, 'Job should be represented as dictionary.'
    storage_client.bucket(os.environ['BATCH_RESULTS_BUCKET']).blob(
        f'jobs/{job["job_id"]}/meta.json').upload_from_string(
        json.dumps(job), content_type='application/json')
    storage_client.bucket(os.environ['TWEETS_SCRAP_BUCKET']).blob(f'jobs/{job["job_id"]}/meta.json').upload_from_string(
        json.dumps(job), content_type='application/json')


def get_results(job: Job) -> JobResult:
    # TODO add param for choosing job_type
    # TODO: add two sub methods

    if not job.results_path:
        raise FileNotFoundError
    results = {}
    content = storage_client.bucket(job.bucket).blob(job.results_path).download_as_string().decode('utf-8')
    f = StringIO(content)
    reader = csv.reader(f, delimiter=',')
    for row in reader:
        if row[0] != 'None':
            val = float(row[1])
            val = -1 if val == 0.0 else 1
            results[datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")] = val
    od_results = collections.OrderedDict(sorted(results.items()))

    return JobResult(list(od_results.values()), list(od_results.keys()))


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def apply_window(job_result: JobResult, size=5):
    result = {}
    for chunk in chunks(range(len(job_result.data)), size):
        key = [job_result.labels[idx] for idx in chunk][int(size / 2) if len(chunk) == size else 0]
        value = sum([job_result.data[idx] for idx in chunk]) / len(chunk)
        result[key] = float(value)

    return JobResult(list(result.values()), list(result.keys()))

if __name__ == '__main__':
    BatchJob('test_8')