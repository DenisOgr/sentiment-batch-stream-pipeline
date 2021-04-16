"""
This pipeline for pre-processing and feature engineering, making inferensing and storing results to BQ table.
This is a stream pipeline. This pipeline does not required lesser window function, that GlobalWindow.
This pipeline will be run on local and GCP platform as well.
Steps in workflow:
* Using BeautifulSoap library gets text from tweets (Map)
* Remove mentions and links from text (Map)
* Lowercase (Map)
* Remove all negotiations (Map)
* Remove all except letters. Remove words  are less than 1 (Map)
* Call Google AI Platform API for make inference (DoFn)
* Store source tweet and prediction into BQ (Transformer)

Arguments:
    - input Pub/Sub topic
    - output BigQuery table

"""
from __future__ import absolute_import

import argparse
import json
import logging
import os
import re
from typing import Dict, Iterable

from apache_beam import Pipeline, WindowInto
from apache_beam.transforms import window
from apache_beam.io import ReadFromPubSub, WriteToBigQuery, BigQueryDisposition
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions
from apache_beam.transforms import PTransform, Map, DoFn, ParDo
from bs4 import BeautifulSoup
from google.api_core.client_options import ClientOptions
from googleapiclient.discovery import build
from nltk.tokenize import WordPunctTokenizer


def build_bq_schema():
    table_schema = bigquery.TableSchema()

    text_field = bigquery.TableFieldSchema()
    text_field.name = 'text'
    text_field.type = 'string'
    text_field.mode = 'nullable'
    table_schema.fields.append(text_field)

    created_at_field = bigquery.TableFieldSchema()
    created_at_field.name = 'created_at'
    created_at_field.type = 'datetime'
    created_at_field.mode = 'nullable'
    table_schema.fields.append(created_at_field)

    sentiment_field = bigquery.TableFieldSchema()
    sentiment_field.name = 'sentiment'
    sentiment_field.type = 'integer'
    sentiment_field.mode = 'nullable'
    table_schema.fields.append(sentiment_field)

    # nested field
    job_field = bigquery.TableFieldSchema()
    job_field.name = 'job'
    job_field.type = 'record'
    job_field.mode = 'nullable'

    job_id_field = bigquery.TableFieldSchema()
    job_id_field.name = 'job_id'
    job_id_field.type = 'string'
    job_id_field.mode = 'nullable'
    job_field.fields.append(job_id_field)

    query_field = bigquery.TableFieldSchema()
    query_field.name = 'query'
    query_field.type = 'string'
    query_field.mode = 'nullable'
    job_field.fields.append(query_field)

    created_at_job_field = bigquery.TableFieldSchema()
    created_at_job_field.name = 'created_at'
    created_at_job_field.type = 'datetime'
    created_at_job_field.mode = 'nullable'
    job_field.fields.append(created_at_job_field)

    table_schema.fields.append(job_field)

    return table_schema


class PreProcessing(PTransform):

    def __init__(self, label=None):
        super().__init__(label)
        self.tok = WordPunctTokenizer()

    @staticmethod
    def clean_html(message: Dict) -> Dict:
        text = message['tweet']['text']
        logging.info(f"clean_html: {text}")

        soup = BeautifulSoup(text, 'lxml')
        souped = soup.get_text()
        try:
            bom_removed = souped.decode("utf-8-sig").replace(u"\ufffd", "?")
        except:
            bom_removed = souped

        message['tweet']['text'] = bom_removed
        return message

    @staticmethod
    def remove_mentions_and_links(message: Dict) -> Dict:
        text = message['tweet']['text']

        pat1 = r'@[A-Za-z0-9_]+'
        pat2 = r'https?://[^ ]+'
        combined_pat = r'|'.join((pat1, pat2))
        www_pat = r'www.[^ ]+'

        stripped = re.sub(combined_pat, '', text)
        message['tweet']['text'] = re.sub(www_pat, '', stripped)

        return message

    @staticmethod
    def remove_negations(message: Dict) -> Dict:
        text = message['tweet']['text']

        negations_dic = {
            "isn't": "is not", "aren't": "are not", "wasn't": "was not", "weren't": "were not",
            "haven't": "have not", "hasn't": "has not", "hadn't": "had not", "won't": "will not",
            "wouldn't": "would not", "don't": "do not", "doesn't": "does not", "didn't": "did not",
            "can't": "can not", "couldn't": "could not", "shouldn't": "should not",
            "mightn't": "might not",
            "mustn't": "must not"
        }
        neg_pattern = re.compile(r'\b(' + '|'.join(negations_dic.keys()) + r')\b')

        message['tweet']['text'] = neg_pattern.sub(lambda x: negations_dic[x.group()], text)

        return message

    @staticmethod
    def make_lower(message: Dict) -> Dict:
        text = message['tweet']['text']
        message['tweet']['text'] = text.lower()

        return message

    @staticmethod
    def letter_only(message: Dict) -> Dict:
        text = message['tweet']['text']
        message['tweet']['text'] = re.sub("[^a-zA-Z]", " ", text)

        return message

    @staticmethod
    def remove_small_words(message: Dict, tok: WordPunctTokenizer) -> Dict:
        text = message['tweet']['text']
        message['tweet']['text'] = (" ".join([x for x in tok.tokenize(text) if len(x) > 1])).strip()

        return message

    def expand(self, p):
        return (
                p |
                'clean from HTML' >> Map(PreProcessing.clean_html) |
                'remove mentions and links' >> Map(PreProcessing.remove_mentions_and_links) |
                'lowercase' >> Map(PreProcessing.make_lower) |
                'remove negations' >> Map(PreProcessing.remove_negations) |
                'letter only' >> Map(PreProcessing.letter_only) |
                'remove small words' >> Map(PreProcessing.remove_small_words, self.tok)
        )


class MakeRemoteInferenceDoFn(DoFn):

    def __init__(self, model_project, model_name, model_region=None, model_version=None):

        self.model_project = model_project
        self.model_name = model_name
        self.model_region = model_region
        self.model_version = model_version

        self.service = None
        self.name = None

    def setup(self):
        super().setup()
        prefix = "{}-ml".format(self.model_region) if self.model_region else "ml"
        api_endpoint = "https://{}.googleapis.com".format(prefix)
        client_options = ClientOptions(api_endpoint=api_endpoint)

        self.service = build('ml', 'v1', client_options=client_options)

        self.name = 'projects/{}/models/{}'.format(self.model_project, self.model_name)

        if self.model_version is not None:
            self.name += '/versions/{}'.format(self.model_version)

    def process(self, message, *args, **kwargs) -> Iterable[Dict]:
        """
        Make inference.

        Returns:
            Dictionary with results.
        """
        response = self.service.projects().predict(
            name=self.name,
            body={'instances': [message['tweet']['text']]}
        ).execute()

        if 'error' in response:
            raise RuntimeError(response['error'])

        message['sentiment'] = int(response['predictions'][0])
        yield message


def formatter(message: Dict) -> Dict:
    """
     Return in following format:
            {
                “text": string,
                "created_at": date,
                “sentiment": integer,
                “job":
                     {
                         “job_id": string,
                         “query": string,
                         “created_at": date
                     }

            }
    Args:
        message: Required. Input message.

    Returns:
        Formatted message.
    """
    a=1
    return {
        "text": message['tweet']['text'],
        "created_at": message['tweet']['created_at'],
        "sentiment": int(message['sentiment']),
        "job": {
            "job_id": message['job_config']['job_id'],
            "query": message['job_config']['query'],
            "created_at": message['job_config']['created_at']
        }
    }


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('input_topic', type=str, help="Input Pub/Sub topic name.")
    parser.add_argument('output_table', type=str, help="Output BigQuery table name. Example: project.db.name")
    parser.add_argument('--model_project', type=str, help="Google Project ID with model.")
    parser.add_argument('--model_name', type=str, help="Name of the Google AI Platform model name.")
    parser.add_argument('--model_region', type=str, help="AI Platform region name.")
    parser.add_argument('--model_version', type=str, help="AI Platform model version.")

    known_args, pipeline_args = parser.parse_known_args(argv)

    _topic_comp = known_args.input_topic.split('/')
    if len(_topic_comp) != 4 or _topic_comp[0] != 'projects' or _topic_comp[2] != 'topics':
        raise ValueError("Table topic name has inappropriate format.")

    if len(known_args.output_table.split('.')) != 2:
        raise ValueError("Table name has inappropriate format.")

    inf_args = [known_args.model_project, known_args.model_name, known_args.model_region, known_args.model_version]
    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).streaming = True

    p = Pipeline(options=options)
    _ = (
            p |
            'read from pub/sub' >> ReadFromPubSub(known_args.input_topic).with_output_types(bytes) |
            'windowing' >> WindowInto(window.FixedWindows(10, 0)) |
            'convert to dict' >> Map(json.loads) |
            'pre processing' >> PreProcessing() |
            'make inference' >> ParDo(MakeRemoteInferenceDoFn(*inf_args)) |
            'format message' >> Map(formatter) |
            'write to BQ' >> WriteToBigQuery(table=known_args.output_table, schema=build_bq_schema(),
                                             create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                                             write_disposition=BigQueryDisposition.WRITE_APPEND)

    )
    if os.environ.get('DEPLOY'):
        p.run()  # I use p.run() instead of "opening context `with Pipeline() as p`" because it need to exit after running.
    else:
        p.run().wait_until_finish()
