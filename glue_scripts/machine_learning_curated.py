import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. Read from Glue Data Catalog
step_trainer_trusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_node"
)

accelerometer_trusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node"
)

#2. Register as SQL views
step_trainer_trusted_node.toDF().createOrReplaceTempView("step_trainer_trusted")
accelerometer_trusted_node.toDF().createOrReplaceTempView("accelerometer_trusted")

# 3. Inner join on timestamp (sensorReadingTime = timestamp)
#    Aggregate Step Trainer readings with matching accelerometer readings for the same moment in time only for research-consenting customers.
ml_curated_df = spark.sql("""
    SELECT  s.sensorreadingtime,
            s.serialnumber,
            s.distancefromobject,
            a.user,
            a.timestamp,
            a.x,
            a.y,
            a.z
    FROM    step_trainer_trusted  s
    INNER JOIN accelerometer_trusted a
           ON  s.sensorreadingtime = a.timestamp
""")

machine_learning_curated_dynamic = DynamicFrame.fromDF(
    ml_curated_df, glueContext, "machine_learning_curated_dynamic"
)

# 4. Write to S3 and update Glue catalog
sink = glueContext.getSink(
    connection_type="s3",
    path="s3://satyam-stedi-lakehouse/machine_learning_curated/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[]
)
sink.setFormat("json")
sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="machine_learning_curated")
sink.writeFrame(machine_learning_curated_dynamic)

job.commit()
