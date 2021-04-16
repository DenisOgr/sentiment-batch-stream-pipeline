#!/bin/bash
# Deploy Google Cloud function from the local machine. Required pre-configured gcloud SDK.
echo "Deploy Google Cloud function."
echo "Using SA account: $(gcloud config get-value account) and project ID: $(gcloud config get-value project)"

cd ../path/with/cloud/function/source

RESULT_TOPIC=''
SOURCE_TOPIC=''
PROJECT_ID=''
ROOT_BUCKET=''

gcloud functions deploy stream-scrap_tweets \
--entry-point main \
--runtime python37 \
--trigger-topic $RESULT_TOPIC \
--set-env-vars=PROJECT_ID=$PROJECT_ID,MAX_AGE_EVENT_SEC=600,SENTIMENT_PREDICT_TOPIC=$RESULT_TOPIC,ANALYTICS_BUCKET_STREAM=$ROOT_BUCKET,TWEETS_LIMIT=100