import logging
import os
import tempfile
from shutil import copyfile

import twint
from google.cloud import storage

logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


def run(word: str, dst: str, limit: int = 100):
    with tempfile.TemporaryDirectory() as tmp:
        src = os.path.join(tmp, 'results.csv')

        c = twint.Config()
        c.Search = word
        c.Limit = limit
        c.Custom["tweet"] = ['tweet', 'created_at', 'replies_count', 'retweets_count', 'likes_count']
        c.Output = src
        c.Store_csv = True
        c.Lang = 'en'
        c.Debug = False
        # Run
        twint.run.Search(c)

        if dst.startswith("gs://"):
            logging.info("Storing to Google Cloud Storage")
            client = storage.Client()
            bucket, *blob_path = dst[len('gs://'):].split('/')
            bucket = client.get_bucket(bucket)
            blob = bucket.blob('/'.join(blob_path))

            blob.upload_from_filename(src, content_type='text/csv')
        else:
            logging.info(f"Storing to locally to {dst}")
            copyfile(src, dst)


run("freedom", 'gs://sentiment-twitter-analys/analytics_jobs/test/twits.csv', limit=200)
