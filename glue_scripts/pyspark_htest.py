"""PySparkTest is base class to do functional testing on PySpark"""
import unittest
import logging
import os
from pyspark.sql import SparkSession
from pandas.testing import assert_frame_equal

"""
From https://gitlab.com/suekto.andreas/gluebilling/-/blob/master/gluebilling/pyspark_htest.py
"""
class PySparkTest(unittest.TestCase):
    """BaseClass which setup local PySpark"""

    @classmethod
    def suppress_py4j_logging(cls):
        """Supress the logging level into WARN and above"""
        logger = logging.getLogger('py4j')
        logger.setLevel(logging.WARN)

    @classmethod
    def create_testing_pyspark_session(cls):
        """Returns SparkSession connecting to local context
        the extrajava session is to generate
        the metastore_db and derby.log into .tmp/ directory"""
        tmp_dir = os.path.abspath(".tmp/")
        return (SparkSession.builder
                .master('local[1]')
                .appName('local-testing-pyspark-context')
                .config("spark.driver.extraJavaOptions",
                        "-Dderby.system.home="+tmp_dir)
                .config("spark.sql.warehouse.dir", tmp_dir)
                .getOrCreate())

    @classmethod
    def setUpClass(cls):
        """Setup the Spark"""
        cls.suppress_py4j_logging()
        cls.spark = cls.create_testing_pyspark_session()

    @classmethod
    def tearDownClass(cls):
        """Clean up the Class"""
        cls.spark.stop()

    @classmethod
    def assert_dataframe_equal(cls, actual, expected, keycolumns):
        """Helper function to compare small dataframe"""
        exp_pd = expected.toPandas().sort_values(
            by=keycolumns
        ).reset_index(drop=True)

        act_pd = actual.toPandas().sort_values(
            by=keycolumns
        ).reset_index(drop=True)
        return assert_frame_equal(act_pd, exp_pd)
