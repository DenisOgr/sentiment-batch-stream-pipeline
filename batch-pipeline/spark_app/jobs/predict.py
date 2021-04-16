import argparse
from google.cloud import storage
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

from utils import build_logger

spark = SparkSession.builder.appName('Twitter sentiment analytics predict app').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

logger = build_logger()
bucket = 'sentiment-twitter-analys'
default_model_version = 'v5'


def main():
    parser = argparse.ArgumentParser(description='Twitter sentiment analytics predict.')
    parser.add_argument('path_to_source', type=str,
                        help='Full path to folder with twitters. Started with gs://')
    parser.add_argument('path_to_results', type=str,
                        help='Full path to folder where store results. Started with gs://')
    parser.add_argument('--model_version', type=str, default=default_model_version,
                        help=f'Model version. By default: {default_model_version}')

    args = parser.parse_args()

    assert args.path_to_source.startswith('gs://')
    assert args.path_to_source.endswith('.csv')
    assert args.path_to_results.startswith('gs://')
    assert args.path_to_results.endswith('.csv')
    assert args.model_version.startswith('v')

    bucket_to_store, *blob_path = args.path_to_results[len('gs://'):].split('/')
    client_storage = storage.Client()
    storage_bucket = client_storage.get_bucket(bucket_to_store)

    logger.info(f"Getting dataset from {args.path_to_source}...")

    schema = StructType([
        StructField("tweet", StringType(), False),
        StructField("datetime", StringType(), False),
        StructField("replies_count", IntegerType(), False),
        StructField("retweets_count", IntegerType(), False),
        StructField("likes_count", IntegerType(), False),
    ])
    data = (spark.read.format("csv")
            .option("header", "true")
            .load(args.path_to_source, schema=schema)
            .select(["replies_count", "retweets_count", "likes_count", F.col("tweet").alias("text"),
                     F.to_timestamp('datetime', 'yyyy-MM-dd HH:mm:ss').alias('dt')])
            .where(F.col("tweet").isNotNull())
            )

    path_to_pipeline = f"gs://{bucket}/models/{args.model_version}/pipeline"
    logger.info(f"Getting pipeline from: {path_to_pipeline}")
    pipeline = PipelineModel.load(path_to_pipeline)

    logger.info("Computing predictions...")
    predicted_data = pipeline.transform(data)

    logger.info(f"Storing results...")
    pred_sentiments = [str(f"{r.dt}, {r.prediction}") for r in predicted_data.select(['dt', 'prediction']).collect()]
    storage.Blob("/".join(blob_path), storage_bucket).upload_from_string("\n".join(pred_sentiments))


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.exception(e)
        raise
