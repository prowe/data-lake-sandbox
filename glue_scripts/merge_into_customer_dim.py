import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext, DynamicFrame
from awsglue.job import Job
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, row_number, desc
import inspect

def __handle_merging_frames(mapped_source, existing_target_df):
    if not existing_target_df.toDF().take(1):
        return mapped_source.toDF()

    # return existing_target_df.toDF() \
    #     .join(mapped_source.toDF(), "id", "outer")
    return existing_target_df.toDF() \
        .union(mapped_source.toDF())


def __do_merge_data(staging_df, existing_target_df):
    mapped_source = staging_df.apply_mapping([
        ("id", "string", "id", "string"),
        ("firstname", "string", "first_name", "string"),
        ("lastname", "string", "last_name", "string"),
        ("birthdate", "string", "birth_date", "date"),
        ("zipcode", "string", "zipcode", "string"),
        ("modifieddate", "string", "modified_date", "timestamp")
    ])

    merged_frame = __handle_merging_frames(mapped_source, existing_target_df)
    merged_frame.show()

    # merged_frame
    window = Window.partitionBy('id').orderBy(desc('modified_date'))
    deduped = merged_frame.withColumn("rn", row_number().over(window)) \
        .filter(col("rn") == 1) \
        .drop("rn")
    deduped.show()

    return deduped

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

    repartitioned_stream = DynamicFrame.fromDF(merged_result, glueContext, 'merged_df') \
        .repartition(2)
    written_data = glueContext.write_dynamic_frame_from_options(
        frame = repartitioned_stream,
        connection_type = "s3",
        connection_options = {"path": target_path},
        format = "glueparquet")

    job.commit()

if __name__ == '__main__':
    glueContext = GlueContext(SparkContext.getOrCreate())
    main(sys.argv, glueContext, Job(glueContext))