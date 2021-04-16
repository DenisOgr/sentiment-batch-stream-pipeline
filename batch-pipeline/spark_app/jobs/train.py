from google.cloud import storage
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import NGram, Tokenizer, CountVectorizer, SQLTransformer
from pyspark.sql import SparkSession
from pyspark.sql.types import *

from utils import build_logger, PreprocessTransformer

spark = SparkSession.builder.appName('Twitter sentiment analytics train app').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

logger = build_logger()
bucket = 'sentiment-twitter-analys'
model_version = 'v5'
path_to_train_dataset = f"gs://{bucket}/data/trainingandtestdata/training.1600000.processed.noemoticon.csv"


def get_dataset():
    schema = StructType([
        StructField("sentiment", IntegerType(), False),
        StructField("id", IntegerType(), False),
        StructField("date", StringType(), False),
        StructField("query_string", StringType(), False),
        StructField("user", StringType(), False),
        StructField("text", StringType(), False),
    ])
    return spark.read.format("csv").load(path_to_train_dataset, schema=schema)


def main():
    logger.info(f"Getting dataset from {path_to_train_dataset}...")
    client_storage = storage.Client()
    storage_bucket = client_storage.get_bucket(bucket)

    data = get_dataset().select(['sentiment', 'text'])
    logger.info(f"Current number of partitions: {data.rdd.getNumPartitions()}")
    data = data.repartition(10)
    logger.info(f"After repartition, number of partitions: {data.rdd.getNumPartitions()}")


    logger.info(f"Creating pre processing transformers...")
    pt = PreprocessTransformer(inputCol='text', outputCol='text_clean')

    logger.info(f"Creating feature engineering transformers...")
    # statement = """
    # SELECT
    #     *
    # FROM
    #     __THIS__
    # WHERE
    #    text_clean != ''
    #
    # """
    # flt = SQLTransformer(statement=statement)
    tk = Tokenizer(inputCol='text', outputCol='words')
    ng1 = NGram(n=1, inputCol='words', outputCol='1_gr_words')
    ng2 = NGram(n=2, inputCol='words', outputCol='2_gr_words')
    ng3 = NGram(n=3, inputCol='words', outputCol='3_gr_words')
    statement = """
    SELECT
        *, concat(1_gr_words, 2_gr_words, 3_gr_words) c_words
    FROM
        __THIS__
    """
    cnt = SQLTransformer(statement=statement)
    cv = CountVectorizer(inputCol='c_words', vocabSize=80000, outputCol='features')

    logger.info(f"Split dataset...")
    df_train, df_test = data.randomSplit([0.8, 0.2], seed=100500)
    logger.info(f"Size of train dataset: {df_train.count()} and test dataset: {df_test.count()}")

    logger.info(f"Building  and fitting model...")
    lr = LogisticRegression(featuresCol='features', labelCol='sentiment', maxIter=5000)
    pipeline_model = Pipeline(stages=[pt, tk, ng1, ng2, ng3, cnt, cv, lr]).fit(df_train)

    logger.info(f"Evaluating model...")
    ev = MulticlassClassificationEvaluator(labelCol='sentiment', metricName="accuracy", predictionCol='prediction')
    df_predict = pipeline_model.transform(df_test).cache()
    accuracy = ev.evaluate(df_predict)
    logger.info(f"Model accuracy: {accuracy}")

    logger.info(f"Storing model...")
    storage.Blob(f'models/{model_version}/scores', storage_bucket).upload_from_string(f'"accuracy":{accuracy}')
    pipeline_model.save(f"gs://{bucket}/models/{model_version}/pipeline")


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.exception(e)
        raise
