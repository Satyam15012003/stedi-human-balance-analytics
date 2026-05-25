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

#1. Read source tables
#    Used Data Catalog nodes
step_trainer_landing_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="step_trainer_landing_node"
)

customers_curated_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customers_curated",
    transformation_ctx="customers_curated_node"
)

# 2. Register as SQL views 
step_trainer_landing_node.toDF().createOrReplaceTempView("step_trainer_landing")
customers_curated_node.toDF().createOrReplaceTempView("customers_curated")

# 3. Inner join on serial number 
#    Only Step Trainer records whose serialNumber matches a curated customer
step_trainer_trusted_df = spark.sql("""
    SELECT  s.sensorreadingtime,
            s.serialnumber,
            s.distancefromobject
    FROM    step_trainer_landing s
    INNER JOIN customers_curated c
           ON  s.serialnumber = c.serialnumber
""")

step_trainer_trusted_dynamic = DynamicFrame.fromDF(
    step_trainer_trusted_df, glueContext, "step_trainer_trusted_dynamic"
)

# 4. Write to S3 and update Glue catalog
sink = glueContext.getSink(
    connection_type="s3",
    path="s3://satyam-stedi-lakehouse/step_trainer_trusted/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[]
)
sink.setFormat("json")
sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="step_trainer_trusted")
sink.writeFrame(step_trainer_trusted_dynamic)

job.commit()
