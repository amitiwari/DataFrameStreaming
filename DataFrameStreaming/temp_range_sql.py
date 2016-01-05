from pyspark.sql import SQLContext
from pyspark import SparkContext 
from pyspark import SparkConf
import sys, operator
import re, string
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

text_filtered.registerTempTable('weather')

getmax_temp = sqlContext.sql("""
    SELECT date, station, value
    FROM weather    
    where element = 'TMAX'    
""")
getmin_temp = sqlContext.sql("""
    SELECT date, station, value
    FROM weather    
    where element = 'TMIN'       
""")

getmax_temp.registerTempTable('getmax_temp')
getmin_temp.registerTempTable('getmin_temp')

do_join = sqlContext.sql("""
    SELECT getmax_temp.date, getmax_temp.station, (getmax_temp.value-getmin_temp.value) as range 
    FROM getmax_temp,getmin_temp
    WHERE getmax_temp.date= getmin_temp.date
    AND getmax_temp.station = getmin_temp.station       
""")

do_join.registerTempTable('do_join')

find_max = sqlContext.sql("""
    SELECT date, MAX(range) as range 
    FROM do_join
    GROUP BY date            
""")

find_max.registerTempTable('find_max')

find_station = sqlContext.sql("""
    SELECT do_join.date, do_join.station, do_join.range 
    FROM do_join, find_max
    WHERE do_join.date = find_max.date
    AND do_join.range = find_max.range
    ORDER BY do_join.date           
""")

save_row = find_station.rdd.map(lambda eachrow : (eachrow.date+ " " + eachrow.station+ " " +str(eachrow.range))).coalesce(1)

save_row.saveAsTextFile(output)