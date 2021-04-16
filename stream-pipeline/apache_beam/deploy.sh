#!/bin/bash
PROJECT_ID=''
REGION=''
TOPIC=''
BIG_QUERY_DB=''
BIG_QUERY_TABLE=''
AI_PLATFORM_MODEL=''
TEMP_GCS_LOCATION=''

python apache_beam/pipeline.py \
projects/$PROJECT_ID/topics/$TOPIC \
$PROJECT_ID:$BIG_QUERY_DB.$BIG_QUERY_TABLE \
--model_project $PROJECT_ID \
--model_region $REGION \
--model_name $AI_PLATFORM_MODEL \
--stream --runner DataflowRunner \
--project $PROJECT_ID \
--temp_location $TEMP_GCS_LOCATION \
--region $REGION
