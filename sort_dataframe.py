from pyspark.sql import *
from pyspark import SparkContext
import sys 

'''
 _______________________________________________________
|														|
| CS494 Cloud Data Center Systems - Homework 1, ex 1	|
|_______________________________________________________|

The following program takes as input:
	- Absolute path to csv input file in HDFS
	- Absolute path to csv output file in HDFS 
	- Public IP address of MAster node 

And do the following:
	- Set up a Spark Session using the appropriate config values
	- Load the data from HDFS into a dataframe
	- Sort the data using the "cca2" and "timestamp" columns
	- Store back the data into HDFS

OBS: The appropriate public IP address of the spark master has to be provided!
	 Also, it is supposed that the HDFS Namenode has private network IP = 10.10.1.1 

EX: spark../bin/spark-submit.sh load_csv_test.py /data/export.csv /data/export_sorted.csv
'''

if __name__ == "__main__":

	if len(sys.argv) != 4:
		print("Usage: load_csv_test.py <inputFile> <outputFile> <master_IP>")
		sys.exit(-1)


	# Set the configuration properties required by the homework:
	# Spark driver memory 			= 32GB
	# Executor memory  				= 32GB
	# Executor cores  				= 10
	# Number of cpus per task  		= 1 

	master_IP = sys.argv[3]

	spark = SparkSession.builder\
	.master("spark://"+master_IP+":7077")\
	.appName("homework 1 part 1")\
	.config("spark.submit.deployMode", "cluster")\
	.config("spark.eventLog.enabled","true") \
	.config("spark.driver.memory", "32g")\
	.config("spark.executor.memory", "32g")\
	.config("spark.executor.cores", "10")\
	.config("spark.task.cpus", "1")\
	.getOrCreate()

	# Input/Output path - hdfs://namenode_IP:9000/path/to/file/in/hdfs/filename
	# OBS: take namenode_IP from hadoop-2.7.6/etc/hadoop/core-site.html  
	input_path = "hdfs://10.10.1.1:9000" + sys.argv[1]

	output_path = "hdfs://10.10.1.1:9000" + sys.argv[2]

	# Load data in RDD 
	logData = spark.read.csv(input_path, header=True)

	print('data type: ' + str(type(logData)))

	logData.printSchema()
	logData.show(10)

	# Sort the data
	ordered = logData.orderBy(["cca2", "timestamp"], ascending=[1,1])

	ordered.show(10)

	# Store data 
	ordered.write.csv(output_path, header=True)
