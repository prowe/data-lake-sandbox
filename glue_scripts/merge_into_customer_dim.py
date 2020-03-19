import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import inspect

def __do_merge_data(staging_df, existing_target_df):
    mapped_source = ApplyMapping.apply(
        frame = staging_df,
        mappings = [
            ("id", "string", "id", "string"),
            ("firstname", "string", "first_name", "string"),
            ("lastname", "string", "last_name", "string"),
            ("birthdate", "string", "birth_date", "date"),
            ("zipcode", "string", "zipcode", "string"),
            ("modifieddate", "string", "modified_date", "timestamp")
        ])

    merged_frame = mapped_source
    if existing_target_df.toDF().take(1):
        merged_frame = existing_target_df.union(mapped_source)

    repartitioned_stream = merged_frame.repartition(2)
    return repartitioned_stream

def main(argv, glueContext, job):
    args = getResolvedOptions(argv, [
        "JOB_NAME",
        "database_name",
        "source_table",
        "target_path"
        ])
    job.init(args['JOB_NAME'], args)

    db_name = args["database_name"]
    source_table = args["source_table"]
    staging_df = glueContext.create_dynamic_frame_from_catalog(
        database=db_name,
        table_name=source_table,
        transformation_ctx="source")

    target_path = args["target_path"]
    exsting_target_df = glueContext.create_dynamic_frame_from_options(
        connection_type = "s3",
        format = "glueparquet",
        connection_options = {"path": target_path})

    merged_result = __do_merge_data(staging_df, exsting_target_df)

    written_data = glueContext.write_dynamic_frame_from_options(
        frame = merged_result,
        connection_type = "s3",
        connection_options = {"path": target_path},
        format = "glueparquet")

    job.commit()

if __name__ == '__main__':
    glueContext = GlueContext(SparkContext.getOrCreate())
    main(sys.argv, glueContext, Job(glueContext))