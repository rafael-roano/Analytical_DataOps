from pyspark.sql import SparkSession
import pytest


@pytest.fixture(scope='session')
def spark_session():
    spark_session = SparkSession.builder.master("local[*]").appName('pyspark-pytest-local-testing').getOrCreate()
    yield spark_session
    spark_session.stop()