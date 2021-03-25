import os
import sys
# os.environ["PYSPARK_PYTHON"] = 'C:/Users/hinna\AppData/Roaming/Microsoft/Windows/Start Menu/Programs/Anaconda3 (64-bit)'
# os.environ["JAVA_HOME"] = 'C:\Program Files\Java 8'
# os.environ["SPARK_HOME"] = 'C:/Program Files/spark-3.1.1-bin-hadoop3.2'

# os.environ["PYLIB"] = os.environ["SPARK_HOME"] + "/python/lib"
# sys.path.insert(0, os.environ["PYLIB"] +"/py4j-0.9-src.zip")
# sys.path.insert(0, os.environ["PYLIB"] +"/pyspark.zip")

#Initialize SparkSession and SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from py4j.java_gateway import JavaGateway
import findspark
findspark.init()


#Create a Spark Session
#Create a Spark Session
SpSession = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("hinna_spark") \
    .config("spark.executor.memory", "1g") \
    .config("spark.cores.max","2") \
    .config("spark.sql.warehouse.dir", "/Users/jlyang/Spark/spark-warehouse")\
    .getOrCreate()
    
#Get the Spark Context from Spark Session    
SpContext = SpSession.sparkContext

testData = SpContext.parallelize([3,6,4,2])
testData.count()

print( "show number", testData.count() )
