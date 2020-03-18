import unittest
from unittest.mock import MagicMock, patch
import logging
import os
from datetime import date
from pyspark.sql import SparkSession
from pandas.testing import assert_frame_equal
from awsglue.context import GlueContext, DynamicFrame
from pyspark.sql.types import StructField, StructType, StringType, DateType
import glue_scripts.merge_into_customer_dim as merge_into_customer_dim
from glue_scripts.pyspark_htest import PySparkTest
from awsglue.job import Job

class TestMergeIntoCustomerDim(PySparkTest):
    output_schema = StructType([
        StructField("id", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("birth_date", DateType(), True),
        StructField("zipcode", StringType(), True)
    ])

    argv = ["",
        "--JOB_NAME", "ut_job",
        "--database_name", "db_name",
        "--source_table", "ut_source",
        "--target_path", "ut_target_path"]

    def test_sanity(self):
        assert 1 == 1

    @patch('awsglue.job.Job')
    def test_no_existing_output(self, mock_job):
        glue_context = GlueContext(self.spark)

        input_df = self.spark.createDataFrame([
                {
                    "id": "01",
                    "firstname": "John",
                    "lastname": "Smith",
                    "birthdate": "1990-01-01",
                    "zipcode": "12345",
                    "modifieddate": "2019-01-01T00:40:32Z",
                }
            ])
        glue_context.create_dynamic_frame_from_catalog = MagicMock(
            return_value=DynamicFrame.fromDF(input_df, glue_context, 'staging_df'))

        existing_target_df = self.spark.createDataFrame([], StructType([]))
        glue_context.create_dynamic_frame_from_options = MagicMock(
            return_value=DynamicFrame.fromDF(existing_target_df, glue_context, 'staging_df'))

        glue_context.write_dynamic_frame_from_options = MagicMock()

        merge_into_customer_dim.main(self.argv, glue_context, mock_job)

        expected_df = input_df = self.spark.createDataFrame([
                ["01", "John", "Smith", date(1990, 1, 1), "12345"]
            ], schema=self.output_schema)

        write_args, write_kargs = glue_context.write_dynamic_frame_from_options.call_args
        self.assert_dataframe_equal(write_kargs['frame'].toDF(), expected_df, ["id"])

