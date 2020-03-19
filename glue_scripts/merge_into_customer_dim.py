import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext, DynamicFrame
from awsglue.job import Job
from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, row_number, desc
import inspect

def __load_staging_data(args, glueContext):
    db_name = args["database_name"]
    source_table = args["source_table"]
    return glueContext.create_dynamic_frame_from_catalog(
        database=db_name,
        table_name=source_table,
        transformation_ctx="source")

def __map_staging_data(staging_df):
    return staging_df.apply_mapping([
        ("id", "string", "id", "string"),
        ("firstname", "string", "first_name", "string"),
        ("lastname", "string", "last_name", "string"),
        ("birthdate", "string", "birth_date", "date"),
        ("zipcode", "string", "zipcode", "string"),
        ("modifieddate", "string", "modified_date", "timestamp")
    ])

def __union_with_existing_data(staging_data, args, glueContext):
    target_path = args["target_path"]
    existing_data = glueContext.create_dynamic_frame_from_options(
        connection_type = "s3",
        format = "glueparquet",
        connection_options = {"path": target_path})

    if existing_data.toDF().take(1):
        return existing_data.toDF() \
            .union(mapped_source.toDF())
    else:
        return mapped_source.toDF()

def __merge_rows(unioned_data):
    window = Window.partitionBy('id').orderBy(desc('modified_date'))
    return unioned_data \
        .withColumn("rn", row_number().over(window)) \
        .filter(col("rn") == 1) \
        .drop("rn")

def __repartition_and_store(result_df, args, glueContext):
    target_path = args["target_path"]
    repartitioned_stream = DynamicFrame.fromDF(merged_result, glueContext, 'result_df') \
        .repartition(2)

    return glueContext.write_dynamic_frame_from_options(
        frame = repartitioned_stream,
        connection_type = "s3",
        connection_options = {"path": target_path},
        format = "glueparquet")

def main(argv, glueContext, job):
    args = getResolvedOptions(argv, [
        "JOB_NAME",
        "database_name",
        "source_table",
        "target_path"
        ])
    job.init(args['JOB_NAME'], args)

    staging_df = __load_staging_data(args, glueContext)
    mapped_staging = __map_staging_data(staging_df)
    unioned_data = __union_with_existing_data(mapped_staging, args, glueContext)
    merged_result = __merge_rows(unioned_data)

    __repartition_and_store(merged_result, args, glueContext)
    job.commit()

if __name__ == '__main__':
    glueContext = GlueContext(SparkContext.getOrCreate())
    main(sys.argv, glueContext, Job(glueContext))