import logging
from sentiment_pipeline.pipeline import run
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()