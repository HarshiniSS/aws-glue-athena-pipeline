import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1790481566795 = glueContext.create_dynamic_frame.from_catalog(database="jaffle_db", table_name="customers", transformation_ctx="AWSGlueDataCatalog_node1790481566795")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1790481570576 = glueContext.create_dynamic_frame.from_catalog(database="jaffle_db", table_name="orders", transformation_ctx="AWSGlueDataCatalog_node1790481570576")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1790481571115 = glueContext.create_dynamic_frame.from_catalog(database="jaffle_db", table_name="payments", transformation_ctx="AWSGlueDataCatalog_node1790481571115")

# Script generated for node SQL Query
SqlQuery2432 = '''
SELECT /*+ COALESCE(1) */ *
FROM (
  SELECT o.id                  AS order_id,
         o.order_date,
         o.status,
         c.id                  AS customer_id,
         c.first_name,
         c.last_name,
         p.id                  AS payment_id,
         p.payment_method,
         p.amount / 100.0      AS amount_usd
  FROM orders o
  JOIN customers c      ON o.user_id  = c.id
  LEFT JOIN payments p  ON p.order_id = o.id
  ORDER BY order_id, payment_id
) t

'''
SQLQuery_node1790481665214 = sparkSqlQuery(glueContext, query = SqlQuery2432, mapping = {"customers":AWSGlueDataCatalog_node1790481566795, "orders":AWSGlueDataCatalog_node1790481570576, "payments":AWSGlueDataCatalog_node1790481571115}, transformation_ctx = "SQLQuery_node1790481665214")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1790481665214, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1790467126110", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1790481775575 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1790481665214, connection_type="s3", format="csv", connection_options={"path": "s3://jaffle-curated-harsh/combined/", "partitionKeys": []}, transformation_ctx="AmazonS3_node1790481775575")

job.commit()