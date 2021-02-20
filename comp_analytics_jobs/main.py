import json
import logging
import os
import time

from google.cloud import dataproc, storage

ENV_RESULTS_BUCKET = 'RESULTS_BUCKET'
ENV_PROJECT_ID = 'PROJECT_ID'
ENV_REGION = 'REGION'
ENV_WF_TEMPLATE = 'WF_TEMPLATE'
ENV_MAX_AGE_EVENT_SEC = 'MAX_AGE_EVENT_SEC'

logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

# noinspection PyTypeChecker
client_wf = dataproc.WorkflowTemplateServiceClient(
    client_options={'api_endpoint': f'{os.environ[ENV_REGION]}-dataproc.googleapis.com:443'})
workflow_template_path = f'projects/{os.environ[ENV_PROJECT_ID]}/regions/{os.environ[ENV_REGION]}/workflowTemplates/{os.environ[ENV_WF_TEMPLATE]}'

client_st = storage.Client()


def run(path_to_source: str, path_to_results: str, model_version: str):
    """
    Run Apache Spark job for predict.
    :param path_to_source: Required. Path to stored source.
    :param path_to_results: Required. Path to store results.
    :param model_version: Required. Version of model
    :return:
    """
    assert path_to_source.startswith("gs://")
    assert path_to_source.endswith("/tweets.csv")
    assert path_to_results.startswith("gs://")
    assert path_to_results.endswith("/results.csv")
    assert model_version.startswith("--model_version=v")

    logger.info(f"path_to_source: {path_to_source}")
    logger.info(f"path_to_results: {path_to_results}")
    logger.info(f"model_version: {model_version}")

    parameters = {'MODEL_VERSION': model_version,
                  'PATH_TO_SOURCE': path_to_source,
                  'PATH_TO_RESULTS': path_to_results
                  }

    result = client_wf.instantiate_workflow_template(name=workflow_template_path,
                                                  parameters=parameters)

    logger.info("Waiting 10 sec...")
    time.sleep(10)
    response = client_wf.transport.operations_client.get_operation(result.operation.name)

    if response.error.code != 0:
        raise RuntimeError(str(response.error))

    logger.info(f"Started operation: {result.operation.name}")


def main(data, context):
    """
    Wrapper for Google Cloud Function.
    Run Apache Spark job for predict using  Google Cloud Function arguments
    :param data: Required. Information about changed blob.
    :param context: Required. Information about event and context.
    :return:
    """

    if data['name'].endswith("/tweets.csv"):
        logger.info("Sleeping fo 10 sec")
        time.sleep(10)  # This is feature of Google Storage. It takes time to sync for all regions.
        # copy meta.json
        path_to_meta = ("/".join(data['name'].split("/")[0:-1])) + "/meta.json"
        meta = client_st.bucket(data['bucket']).blob(path_to_meta).download_as_string().decode('utf-8')
        logger.info(f"This is meta content: {meta}")
        meta = json.loads(meta)
        client_st.bucket(os.environ[ENV_RESULTS_BUCKET]).blob(f"jobs/{meta['job_id']}/meta.json").upload_from_string(
            json.dumps(meta))
        # copy tweets.csv
        tweets = client_st.bucket(data['bucket']).blob(data['name']).download_as_string().decode('utf-8')
        client_st.bucket(os.environ[ENV_RESULTS_BUCKET]).blob(f"jobs/{meta['job_id']}/tweets.csv").upload_from_string(
            tweets)
        del tweets

        try:
            path_to_source = f"gs://{data['bucket']}/{data['name']}"
            path_to_results = f"gs://{os.environ[ENV_RESULTS_BUCKET]}/jobs/{meta['job_id']}/results.csv"
            model_version = "--model_version=v5"
            logger.warning(f"Using default version of the model: {model_version}")
            run(path_to_source, path_to_results, model_version)
        except Exception as e:
            client_st.bucket(os.environ[ENV_RESULTS_BUCKET]).blob(
                f"jobs/{meta['job_id']}/_error").upload_from_string(
                str(e))
            raise e
    else:
        logger.warning(f"Triggered for non tweets.csv. File name: {data['name']}")