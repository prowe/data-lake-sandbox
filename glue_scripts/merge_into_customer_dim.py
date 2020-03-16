import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "database-name",
    "target-path"])

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

db_name = args["database-name"]
target_path = args["target-path"]

source = glueContext.create_dynamic_frame.from_catalog(
    database=db_name,
    table_name="raw_customersraw_customer_events"
    transformation_ctx="source")

mapped_source = ApplyMapping.apply(
    frame = source,
    mappings = [
        ("id", "string", "id", "string"),
        ("firstname", "string", "firstname", "string"),
        ("lastname", "string", "lastname", "string"),
        ("birthdate", "string", "birthdate", "date"),
        ("zipcode", "string", "zipcode", "string")
    ],
    transformation_ctx = "mapped_source")

existing_target = glueContext.create_dynamic_frame_from_options(
    connection_type = "s3",
    format = "glueparquet",
    connection_options = {"path": target_path}
    transformation_ctx = "existing_target")

merged_frame = mapped_source
if existing_target.toDF().take(1):
    print("merging")

    merged_frame = table_exists_df.mergeDynamicFrame(
        stage_dynamic_frame = mapped_source,
        primary_keys = ["id"]
        transformation_ctx = "merged_frame"
    )

repartitioned_stream = merged_frame.repartition(2)
written_data = glueContext.write_dynamic_frame.from_options(
    frame = repartitioned_stream,
    connection_type = "s3",
    connection_options = {"path": target_path},
    format = "glueparquet",
    transformation_ctx = "written_data")

job.commit()
