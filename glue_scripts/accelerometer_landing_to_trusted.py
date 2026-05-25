import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ── 1. Read source tables from Glue Data Catalog ──────────────────────────────
accelerometer_landing_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometer_landing_node"
)

customer_trusted_node = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node"
)

# ── 2. Register as SQL views ──────────────────────────────────────────────────
accelerometer_landing_node.toDF().createOrReplaceTempView("accelerometer_landing")
customer_trusted_node.toDF().createOrReplaceTempView("customer_trusted")

# ── 3. Inner join on email – keep ONLY accelerometer columns ─────────────────
#    (rubric requires output table has only accelerometer columns)
trusted_df = spark.sql("""
    SELECT  a.user,
            a.timestamp,
            a.x,
            a.y,
            a.z
    FROM    accelerometer_landing a
    INNER JOIN customer_trusted c
           ON  a.user = c.email
""")

accelerometer_trusted_dynamic = DynamicFrame.fromDF(
    trusted_df, glueContext, "accelerometer_trusted_dynamic"
)

# ── 4. Write to S3 and update Glue catalog ────────────────────────────────────
sink = glueContext.getSink(
    connection_type="s3",
    path="s3://satyam-stedi-lakehouse/accelerometer_trusted/",
    enableUpdateCatalog=True,
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[]
)
sink.setFormat("json")
sink.setCatalogInfo(catalogDatabase="stedi", catalogTableName="accelerometer_trusted")
sink.writeFrame(accelerometer_trusted_dynamic)

job.commit()
