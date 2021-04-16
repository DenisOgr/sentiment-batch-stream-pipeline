import csv
import json
import os
import tempfile

import logging
import re
import twint
from datetime import datetime, timezone
from dateutil import parser
from google.cloud import storage
from shutil import copyfile
from time import sleep
from twint_custom import custom_run
from typing import List

pattern = r"[^A-Za-z0-9_!@#$%^&*()<>,.\s/:]+"

logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

ENV_ANALYTICS_BUCKET = 'ANALYTICS_BUCKET'
ENV_TWEETS_LIMIT = 'TWEETS_LIMIT'

client = storage.Client()

twint.run.run = custom_run


def run(word: str, dst: str, limit: int = 100):
    logger.info(f"Query: {word} will store in: {dst} with limit: {limit}")
    fields = ['tweet', 'datetime', 'replies_count', 'retweets_count', 'likes_count']
    with tempfile.TemporaryDirectory() as tmp:
        src = os.path.join(tmp, 'results.csv')

        c = twint.Config()
        c.Search = word
        c.Limit = limit
        c.Store_object = True
        c.Lang = 'en'
        c.Debug = False
        # Run
        twint.run.Search(c)

        tweets_list = filter_tweets(twint.output.tweets_list)
        with open(src, 'w') as f:
            write = csv.writer(f)
            write.writerow(fields)
            write.writerows(([[getattr(tweet, field) for field in fields] for tweet in tweets_list]))

        if dst.startswith("gs://"):
            logging.info("Storing to Google Cloud Storage")
            bucket, *blob_path = dst[len('gs://'):].split('/')
            bucket = client.get_bucket(bucket)
            blob = bucket.blob('/'.join(blob_path))

            blob.upload_from_filename(src, content_type='text/csv')
        else:
            logging.info(f"Storing to locally to {dst}")
            copyfile(src, dst)


def filter_tweets(tweets: List) -> List:
    results = []
    for tweet in tweets:
        text = tweet.tweet
        after_text = re.sub(pattern, '', text, 0, re.MULTILINE)
        after_words = [word for word in after_text.split() if len(word) > 2]
        if len(after_words) < 5:
            logger.warning(f"Reject tweets: {text}")
        else:
            tweet.tweet = " ".join(after_words)
            results.append(tweet)
    logger.warning(f"Before filtering: {len(tweets)}, After filtering: {len(results)}")
    return results


def avoid_infinite_retries(context):
    timestamp = context.timestamp
    max_age_s = int(os.environ.get('MAX_AGE_EVENT_SEC', 60))
    event_time = parser.parse(timestamp)
    cur_time = datetime.now(timezone.utc)
    event_age_s = (cur_time - event_time).total_seconds()
    logger.info(
        f"Current time: {cur_time}, event time: {event_time}, age event: {event_age_s} sec. Max age: {max_age_s}")

    # Ignore events that are too old

    if event_age_s > max_age_s:
        message = f'Dropped {context.event_id} (age {event_age_s}s)'
        logger.warning(message)
        raise TimeoutError(message)


def main(data, context):
    try:
        avoid_infinite_retries(context)
    except TimeoutError:
        return
    if data['name'].endswith("/meta.json"):
        logger.info("Sleeping fo 10 sec")
        sleep(10)  # This is feature of Google Storage. It takes time to sync for all regions.
        content = client.bucket(data['bucket']).blob(data['name']).download_as_string().decode('utf-8')
        logger.info(f"This is content: {content}")
        meta = json.loads(content)
        dst = f"gs://{os.environ[ENV_ANALYTICS_BUCKET]}/jobs/{meta['job_id']}/tweets.csv"
        run(meta['query'], dst, limit=os.environ[ENV_TWEETS_LIMIT])
        client.bucket(os.environ[ENV_ANALYTICS_BUCKET]).blob(f"jobs/{meta['job_id']}/meta.json").upload_from_string(
            content)
    else:
        logger.warning(f"Triggered for non JSON file. File name: {data['name']}")
