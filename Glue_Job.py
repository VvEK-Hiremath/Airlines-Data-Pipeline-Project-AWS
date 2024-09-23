import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Daily_Raw_Flight_Data
Daily_Raw_Flight_Data_node1727089661948 = glueContext.create_dynamic_frame.from_catalog(database="vk-airlines", table_name="daily_raw", transformation_ctx="Daily_Raw_Flight_Data_node1727089661948")

# Script generated for node Dim_Airport
Dim_Airport_node1727089664168 = glueContext.create_dynamic_frame.from_catalog(database="vk-airlines", table_name="dev_airlines_airports_dim", redshift_tmp_dir="s3://temp-vk", transformation_ctx="Dim_Airport_node1727089664168")

# Script generated for node InnerJoin_OriginAirport
InnerJoin_OriginAirport_node1727089823224 = Join.apply(frame1=Daily_Raw_Flight_Data_node1727089661948, frame2=Dim_Airport_node1727089664168, keys1=["originairportid"], keys2=["airport_id"], transformation_ctx="InnerJoin_OriginAirport_node1727089823224")

# Script generated for node Origin Change Schema
OriginChangeSchema_node1727090281352 = ApplyMapping.apply(frame=InnerJoin_OriginAirport_node1727089823224, mappings=[("carrier", "string", "carrier", "string"), ("destairportid", "long", "destairportid", "long"), ("depdelay", "long", "dep_delay", "bigint"), ("arrdelay", "long", "arr_delay", "bigint"), ("city", "string", "des_city", "string"), ("name", "string", "des_airport", "string"), ("state", "string", "des_state", "string")], transformation_ctx="OriginChangeSchema_node1727090281352")

# Script generated for node InnerJoin_DestAirport
InnerJoin_DestAirport_node1727090703037 = Join.apply(frame1=OriginChangeSchema_node1727090281352, frame2=Dim_Airport_node1727089664168, keys1=["destairportid"], keys2=["airport_id"], transformation_ctx="InnerJoin_DestAirport_node1727090703037")

# Script generated for node Dest Change Schema
DestChangeSchema_node1727090741432 = ApplyMapping.apply(frame=InnerJoin_DestAirport_node1727090703037, mappings=[("carrier", "string", "carrier", "string"), ("state", "string", "arr_state", "string"), ("arr_delay", "bigint", "arr_delay", "long"), ("city", "string", "arr_city", "string"), ("name", "string", "arr_airport", "string"), ("dep_delay", "bigint", "dep_delay", "long")], transformation_ctx="DestChangeSchema_node1727090741432")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1727090974688 = glueContext.write_dynamic_frame.from_catalog(frame=DestChangeSchema_node1727090741432, database="vk-airlines", table_name="dev_airlines_daily_flights_fact", redshift_tmp_dir="s3://temp-vk",additional_options={"aws_iam_role": "arn:aws:iam::025066246764:role/redshift_vk"}, transformation_ctx="AWSGlueDataCatalog_node1727090974688")

job.commit()