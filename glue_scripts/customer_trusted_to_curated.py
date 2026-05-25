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
customer_trusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node"
)
accelerometer_trusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node"
)
# 2. Register as SQL views
customer_trusted_node.toDF().createOrReplaceTempView("customer_trusted")
accelerometer_trusted_node.toDF().createOrReplaceTempView("accelerometer_trusted")

# 3. Inner join: customers who HAVE accelerometer data + agreed to share
#    Output keeps only customer columns.
curated_df = spark.sql("""
    SELECT DISTINCT
            c.customername,
            c.email,
            c.phone,
            c.birthday,
            c.serialnumber,
            c.registrationdate,
            c.lastupdatedate,
            c.sharewithresearchasofdate,
            c.sharewithpublicasofdate,
            c.sharewithfriendsasofdate
    FROM    customer_trusted   c
    INNER JOIN accelerometer_trusted a
           ON  c.email = a.user
""")

customers_curated_dynamic = DynamicFrame.fromDF(
    curated_df, glueContext, "customers_curated_dynamic"
)

# 4. Write to S3 and update Glue catalog
sink = glueContext.getSink(
    connection_type="s3",
    path="s3://satyam-stedi-lakehouse/customer_curated/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[]
)
sink.setFormat("json")
sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="customers_curated")
sink.writeFrame(customers_curated_dynamic)
job.commit()