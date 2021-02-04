import logging
import re
import sys

from bs4 import BeautifulSoup
from nltk.tokenize import WordPunctTokenizer
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol
from pyspark.ml.util import DefaultParamsWritable, DefaultParamsReadable
from pyspark.sql.functions import udf
from pyspark.sql.types import *
from pyspark import keyword_only

class PreprocessTransformer(Transformer, HasInputCol, HasOutputCol, DefaultParamsWritable, DefaultParamsReadable):
    pat1 = r'@[A-Za-z0-9_]+'
    pat2 = r'https?://[^ ]+'
    combined_pat = r'|'.join((pat1, pat2))
    www_pat = r'www.[^ ]+'
    negations_dic = {"isn't": "is not", "aren't": "are not", "wasn't": "was not", "weren't": "were not",
                     "haven't": "have not", "hasn't": "has not", "hadn't": "had not", "won't": "will not",
                     "wouldn't": "would not", "don't": "do not", "doesn't": "does not", "didn't": "did not",
                     "can't": "can not", "couldn't": "could not", "shouldn't": "should not", "mightn't": "might not",
                     "mustn't": "must not"}
    neg_pattern = re.compile(r'\b(' + '|'.join(negations_dic.keys()) + r')\b')
    tok = WordPunctTokenizer()

    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super(PreprocessTransformer, self).__init__()
        kwargs = self._input_kwargs
        self._set(**kwargs)

    def _transform(self, dataset):
        def f(text):
            try:
                soup = BeautifulSoup(text, 'lxml')
                souped = soup.get_text()
                try:
                    bom_removed = souped.decode("utf-8-sig").replace(u"\ufffd", "?")
                except:
                    bom_removed = souped
                stripped = re.sub(self.combined_pat, '', bom_removed)
                stripped = re.sub(self.www_pat, '', stripped)
                lower_case = stripped.lower()
                neg_handled = self.neg_pattern.sub(lambda x: self.negations_dic[x.group()], lower_case)
                letters_only = re.sub("[^a-zA-Z]", " ", neg_handled)
                words = [x for x in self.tok.tokenize(letters_only) if len(x) > 1]
            except:
                return ""
            return (" ".join(words)).strip()

        return dataset.withColumn(self.getOutputCol(), udf(f, StringType())(dataset[self.getInputCol()]))


def build_logger(level=logging.INFO):
    """
    Build and return logger.
    Args:
        level: Optional. Level of logging.
    Returns:
        Object that represents logger.
    """
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s"))

    logger = logging.getLogger()
    logger.setLevel(level)
    logger.addHandler(console_handler)

    return logger
