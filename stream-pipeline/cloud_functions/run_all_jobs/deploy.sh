#!/bin/bash
# Deploy Google Cloud function from the local machine. Required pre-configured gcloud SDK.
echo "Deploy Google Cloud function."
echo "Using SA account: $(gcloud config get-value account) and project ID: $(gcloud config get-value project)"

gcloud functions deploy run_all_jobs --entry-point main --runtime python37 --trigger-topic run_all_jobs \
--set-env-vars=PROJECT_ID=sentiment-twitter-analys,RUN_ALL_TOPIC='run_all_jobs',PARSE_JOB_TOPIC='parse_tweetts_for_job',ANALYTICS_BUCKET_STREAM='sentiment-twitter-analys-stream'