from pyspark import SparkContext 
from pyspark import SparkConf
import sys, operator
import re, string
from pyspark.streaming import StreamingContext
import datetime


inputs = int(sys.argv[1])
output = sys.argv[2]

conf = SparkConf().setAppName('read stream')
sc = SparkContext()

ssc = StreamingContext(sc, 5)

def add_tuples(a, b):
    return tuple(sum(p) for p in zip(a,b))

def calc_values(sumx,sumy,sumxy,sumxsquare,n):
    xmean=sumx/n
    ymean=sumy/n
    xymean=sumxy/n
    xsquaremean=sumxsquare/n
    beta=(xymean-(xmean*ymean))/(xsquaremean-(xmean*xmean))
    alpha=ymean-(beta*xmean)    
    return (alpha,beta)

def stream_function(points):
    points_float=points.map(lambda (x,y):(float(x),float(y)))    
    points_mean = points_float.map(lambda (x,y): (x, y, (x*y), (x*x), 1)).reduce(add_tuples)
    meanvalues=calc_values(*points_mean)
    alpha=meanvalues[0]
    beta=meanvalues[1]
    rdd = sc.parallelize([(alpha,beta)], numSlices=1)
    rdd.saveAsTextFile(output + '/' + datetime.datetime.now().isoformat().replace(':', '-'))
    

lines = ssc.socketTextStream("cmpt732.csil.sfu.ca", inputs)

points = lines.map(lambda line: line.split())
points.foreachRDD(stream_function)
ssc.start()
ssc.awaitTermination(timeout=300)