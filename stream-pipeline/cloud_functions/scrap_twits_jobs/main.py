import csv
import json
import os
import tempfile
import base64

import logging
import re
import twint
from datetime import datetime, timezone
from dateutil import parser
from google.cloud import pubsub_v1
from twint_custom import custom_run
from typing import List

pattern = r"[^A-Za-z0-9_!@#$%^&*()<>,.\s/:]+"

logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

batch_settings = pubsub_v1.types.BatchSettings()
publisher = pubsub_v1.PublisherClient(batch_settings)
topic = f"projects/{os.environ['PROJECT_ID']}/topics/{os.environ['SENTIMENT_PREDICT_TOPIC']}"

twint.run.run = custom_run


def pubsub_callback(future):
    message_id = future.result()
    print(message_id)

def get_tweets(word: str, limit: int = 100):
    logger.info(f"Query: {word}  with limit: {limit}")
    fields = ['tweet', 'datetime', 'replies_count', 'retweets_count', 'likes_count']

    c = twint.Config()
    c.Search = word
    c.Limit = limit
    c.Store_object = True
    c.Lang = 'en'
    c.Debug = False
    # Run
    twint.run.Search(c)

    return[{field:getattr(tweet, field) for field in fields} for tweet in filter_tweets(twint.output.tweets_list)]


def filter_tweets(tweets: List) -> List:
    results = []
    for tweet in tweets:
        text = tweet.tweet
        after_text = re.sub(pattern, '', text, 0, re.MULTILINE)
        after_words = [word for word in after_text.split() if len(word) > 2]
        if len(after_words) > 5:
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
    # try:
    #     avoid_infinite_retries(context)
    # except TimeoutError:
    #     return
    job_config = json.loads(base64.b64decode(data['data']))
    for tweet in get_tweets(job_config['query'], os.environ['TWEETS_LIMIT']):
        message = {
            "tweet": {
                "text": str(tweet['tweet']),
                "created_at": datetime.strptime(tweet['datetime'], '%Y-%m-%d %H:%M:%S %Z').strftime('%Y-%m-%d %H:%M:%S'),
            },
            "job_config": job_config
        }
        future = publisher.publish(topic,
                                   json.dumps(message).encode('utf-8'))
        future.add_done_callback(pubsub_callback)

