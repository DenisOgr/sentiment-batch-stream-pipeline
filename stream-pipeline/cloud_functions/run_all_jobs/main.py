import os
import json
import logging
from google.cloud import storage
from google.cloud import pubsub_v1

publisher = pubsub_v1.PublisherClient()
bucket = storage.Client().get_bucket(os.environ['ANALYTICS_BUCKET_STREAM'])
topic = f"projects/{os.environ['PROJECT_ID']}/topics/{os.environ['PARSE_JOB_TOPIC']}"
def main(data, context):
    """
    Run all jobs from the storage.
    """

    jobs = [json.loads(blob.download_as_string()) for blob in bucket.list_blobs(prefix='jobs/') if blob.name.endswith(".json")]
    for job in jobs:
        print(f'Send job with ID: {job["job_id"]} to PubSub.')
        publisher.publish(topic, json.dumps(job).encode('utf-8')).result()
