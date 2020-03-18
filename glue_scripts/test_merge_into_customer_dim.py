import unittest
import logging
import os
from datetime import date
from pyspark.sql import SparkSession
from pandas.testing import assert_frame_equal
from awsglue.context import GlueContext, DynamicFrame
from pyspark.sql.types import StructField, StructType, StringType, DateType
import glue_scripts.merge_into_customer_dim as merge_into_customer_dim
from glue_scripts.pyspark_htest import PySparkTest

class TestMergeIntoCustomerDim(PySparkTest):
    def test_sanity(self):
        assert 1 == 1

    def test_no_existing_output(self):
        glueContext = GlueContext(self.spark)

        input_schema = StructType([
            StructField("id", StringType(), True),
            StructField("firstname", StringType(), True),
            StructField("lastname", StringType(), True),
            StructField("birthdate", StringType(), True),
            StructField("zipcode", StringType(), True),
            StructField("modifieddate", StringType(), True)
        ])
        input_df = self.spark.createDataFrame([
                ["01", "John", "Smith", "1990-01-01", "12345", "2019-01-01T00:40:32Z"]
            ],
            schema=input_schema)

        existing_target_df = self.spark.createDataFrame([], StructType([]))
        actual = merge_into_customer_dim.do_merge_data(
            staging_df=DynamicFrame.fromDF(input_df, glueContext, 'staging_df'),
            existing_target_df=DynamicFrame.fromDF(existing_target_df, glueContext, 'staging_df'))

        expected_schema = StructType([
            StructField("id", StringType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("birth_date", DateType(), True),
            StructField("zipcode", StringType(), True)
        ])
        expected_df = input_df = self.spark.createDataFrame([
                ["01", "John", "Smith", date(1990, 1, 1), "12345"]
            ],
            schema=expected_schema)

        self.assert_dataframe_equal(actual.toDF(), expected_df, ["id"])
