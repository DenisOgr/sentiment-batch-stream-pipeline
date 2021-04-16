import setuptools

setuptools.setup(
    name='sentiment_pipeline',
    version='v1',
    install_requires=[
        'nltk==3.5',
        'bs4==0.0.1',
        'google-api-python-client==2.0.2',
        'google-cloud-bigquery==1.28.0',
        'google-cloud-pubsub==1.7.0',
        'lxml==4.6.3',
    ],
    packages=setuptools.find_packages(),
)
