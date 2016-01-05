from pyspark.sql import SQLContext
from pyspark import SparkContext 
from pyspark import SparkConf
import sys, operator
import re, string


inputs = sys.argv[1]
output = sys.argv[2]
sourceNode= int(sys.argv[3])
destinationNode=int(sys.argv[4])

def myfun(x): return x

def correct_dist((node, (key,(src,dist)))):
    return (key,(node, (dist+1)))

def find_min_dist((key1,value1),(key2,value2)):
    if(value1<=value2):
        return (key1,value1)
    else:
        return (key2,value2)
    

conf = SparkConf().setAppName('shortest path')
sc = SparkContext()

nodes = sc.textFile(inputs + '/links-simple-sorted.txt')

structure = nodes.map(lambda line : line.split(":")).map(lambda (w,x) : (int(w), map(int,x.split())))

graph_edges= structure.flatMapValues(myfun).cache()

initial_known_path = sc.parallelize([(1,('',0))])

for loop in range(6):
    joined_paths = graph_edges.join(initial_known_path.filter(lambda (node,(src,dist)) : dist==loop))
    intermediate_result = joined_paths.map(correct_dist).cache()
    initial_known_path = initial_known_path.union(intermediate_result).cache()
    reduced_path=initial_known_path.reduceByKey(find_min_dist)
    break_out=reduced_path.filter(lambda (node,(src,dist)) : node==destinationNode).count()
    reduced_path.coalesce(1).saveAsTextFile(output+ '/iter-' + str(loop))
    if (break_out>0):
        break
    
outputpath=[destinationNode]

while (destinationNode!=sourceNode and destinationNode!=''):    
    dest = reduced_path.lookup(destinationNode)
    if(dest[0][0]==''):
        break
    else:
        outputpath.append(dest[0][0])
        destinationNode= dest[0][0]
outputpath=outputpath[::-1]
finaloutput = sc.parallelize(outputpath).coalesce(1)
finaloutput.saveAsTextFile(output + '/path')