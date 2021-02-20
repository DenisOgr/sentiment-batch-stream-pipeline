import base64
import json
import os
import random
import re
from dataclasses import dataclass
from typing import Union, Dict
import csv
from io import StringIO
from google.cloud import storage
import collections
from datetime import datetime
storage_client = storage.Client()

regex = r"^[a-zA-Z\d!\s@#$^&*()_+\"|}{?><~]*$"
pattern = re.compile(regex)


@dataclass
class Job:
    bucket: str
    meta_path: Dict
    tweets_path: str
    results_path: str
    _error_path: str
    _meta=None

    @staticmethod
    def build_me(sources: list):
        if not sources:
            return None
        attrs = {s.split('/')[-1].split('.')[0]: s for s in sources}
        return Job(os.environ['RESULTS_BUCKET'], attrs.get('meta'), attrs.get('tweets'), attrs.get('results'), attrs.get('_error'))

    def get_error_path(self):
        return self._error_path

    def get_meta(self):
        if not self._meta:
            self._meta = json.loads(storage_client.bucket(self.bucket).blob(self.meta_path).download_as_string().decode('utf-8'))
        return self._meta

@dataclass
class JobResult:
    data: list
    labels: list


def get_all_jobs():
    results = set()
    for blob in storage_client.bucket(os.environ['RESULTS_BUCKET']).list_blobs(prefix=f'jobs'):
        job_id = blob.name.split('/')[1]
        job_id = get_string_from_job_id(job_id)
        results.add(job_id)
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


def validate(string: str):
    assert len(string.split()) < 100, 'Number of words should be less than 100'
    assert len(string) > 3, 'Word should have more than 4 characters.'
    assert pattern.match(string), 'String includes invalid characters.'


def get_job_from_string(string: str):
    return f"base64:{base64.b64encode(str.encode(string)).decode()}"

def get_string_from_job_id(job_id: str):
    prefix='base64:'
    if job_id.startswith(prefix):
        job_id = base64.b64decode(str.encode(job_id[len(prefix):])).decode()
    return job_id



def get_job(job_id: str) -> Union[None, Job]:
    blobs = storage_client.bucket(os.environ['RESULTS_BUCKET']).list_blobs(prefix=f'jobs/{job_id}')
    return Job.build_me([blob.name for blob in blobs])


def store_job(job):
    assert type(job) == dict, 'Job should be represented as dictionary.'
    storage_client.bucket(os.environ['RESULTS_BUCKET']).blob(f'jobs/{job["job_id"]}/meta.json').upload_from_string(
        json.dumps(job), content_type='application/json')
    storage_client.bucket(os.environ['TWEETS_SCRAP_BUCKET']).blob(f'jobs/{job["job_id"]}/meta.json').upload_from_string(
        json.dumps(job), content_type='application/json')


def get_results(job: Job) -> JobResult:
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
        key = [job_result.labels[idx] for idx in chunk][int(size/2) if len(chunk) == size else 0]
        value = sum([job_result.data[idx] for idx in chunk])/len(chunk)
        result[key] = float(value)

    return JobResult(list(result.values()), list(result.keys()))


if __name__ == '__main__':
    a=get_job('test_8')
    a.get_meta()
