import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as F

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

#1. Read customer_landing from Glue Data Catalog
customer_landing_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing",
    transformation_ctx="customer_landing_node"
)

#2. Filter: keep only rows where shareWithResearchAsOfDate is NOT NULL
#    Using a SQL Query node pattern for consistency.
customer_landing_node.toDF().createOrReplaceTempView("customer_landing")

trusted_df = spark.sql("""
    SELECT *
    FROM   customer_landing
    WHERE  shareWithResearchAsOfDate IS NOT NULL
""")

customer_trusted_dynamic = DynamicFrame.fromDF(
    trusted_df, glueContext, "customer_trusted_dynamic"
)

#3. Write to S3 and register / update Glue catalog table
sink = glueContext.getSink(
    connection_type="s3",
    path="s3://satyam-stedi-lakehouse/customer_trusted/",
    enableUpdateCatalog=True,         
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[]
)
sink.setFormat("json")
sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="customer_trusted")
sink.writeFrame(customer_trusted_dynamic)

job.commit()
