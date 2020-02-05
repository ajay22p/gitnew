from __future__ import print_function
from pyspark.sql import SparkSession
from pyspark import SparkContext as sc
from pyspark.streaming.kafka import KafkaUtils


from pyspark.sql import SQLContext as sc

spark = SparkSession \
    .builder \
    .appName("pysparkAssignmnet") \
    .enableHiveSupport()\
    .getOrCreate()
# df = spark \
#   .readStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "localhost:9092,localhost:2181") \
#   .option("subscribe", "test1") \
#   .load()
# # df.selectExpr("CAST(key AS STRING)", "CAST(value A)
# # df.show()
df = spark \
     .read \
     .format("kafka") \
     .option("kafka.bootstrap.servers", "localhost:9092,localhost:2181") \
     .option("subscribePattern", "test2") \
     .option("startingOffsets", "earliest") \
     .option("endingOffsets", "latest") \
     .load()
df = spark.read.format("kafka").option("kafka.bootstrap.servers", "localhost:9092,localhost:2181").option("subscribe", "test2").load()
df1=df.selectExpr("CAST(value AS STRING)")
df1.show()
for row in df1.take(11):
    print(row)
df1.printSchema()

# #df = spark.read.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "test").load()
# df1.registerTempTable("test2")
# temp_df = sc.sql("select * from test2")
# print(temp_df)
# df2 = lines.map(lambda x: (x.split(" ")[0], x))
# df1.show()
from pyspark.sql.types import *
#
templeSchema = StructType([ \
  StructField("Name", StringType()), \
  StructField("Latitude", FloatType()), \
  StructField("Longitude", FloatType()), \
  StructField("Distance", FloatType())])
def parse_data_from_kafka_message(sdf, schema):
 from pyspark.sql.functions import split
 assert sdf.isStreaming == False
 col = split(sdf['value'], ',')
 for idx, field in enumerate(schema):
     sdf = sdf.withColumn(field.name, col.getItem(idx).cast(field.dataType))
 return sdf.select([field.name for field in schema])
df2 = parse_data_from_kafka_message(df,templeSchema)
df2.show()

