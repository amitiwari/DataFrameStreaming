from pyspark.sql import SQLContext
from pyspark import SparkContext 
from pyspark import SparkConf
import sys, operator
import re, string
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('temp range')
sc = SparkContext()
sqlContext = SQLContext(sc)

myschema = StructType([
    StructField('station', StringType(), False),
    StructField('date', StringType(), False),
    StructField('element', StringType(), False),
    StructField('value', IntegerType(), False),
    StructField('M-FLAG', StringType(), True),
    StructField('Q-FLAG', StringType(), True),
    StructField('S-FLAG', StringType(), True),
    StructField('OBS-TIME', StringType(), True)
])

text = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").load(inputs, schema=myschema)

text_filtered = text.filter(text['Q-FLAG'] == "").cache()

select_tmax=text_filtered.where(text_filtered['element']=='TMAX').alias("temp_max")

select_tmin=text_filtered.where(text_filtered['element']=='TMIN').alias("temp_min")

select_range = select_tmax.join(select_tmin, ["station", "date"]) \
    .select("date", "station", (col("temp_max.value")-col("temp_min.value")).alias("range"))
    
select_max=select_range.select("date", "range").groupby("date").agg(max("range").alias("range"))

find_stat = select_range.join(select_max, ["date", "range"]).sort("date")

save_row = find_stat.rdd.map(lambda eachrow : (eachrow.date+ " " + eachrow.station+ " " +str(eachrow.range))).coalesce(1)

save_row.saveAsTextFile(output)