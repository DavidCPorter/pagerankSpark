from pyspark.sql import *
from pyspark import SparkContext
import sys
import re 
from operator import add

'''
 _______________________________________________________
|														|
| CS494 Cloud Data Center Systems - Homework 1, ex 2.2	|
|_______________________________________________________|

The following program takes as input:
	- Absolute path to graph input file in HDFS (separator = tab)
	- Absolute path to ranks output file in HDFS 
	- Number of partitions in Spark
	- Public IP Address of the Spark Master
	- number of iterations for Page-Rank Algorithm

And do the following:
	- Set up a Spark Session using the appropriate config values
	- Load the graph from HDFS into an RDD (using the input partitions) and initialize the ranks to 1
	- Repeat Page-Rank iteration until done:
			- Compute the neighbors contribution with d = 0.15
			- Update the ranks RDD 
	- Store the ranks into HDFS

OBS: The appropriate IP address of the spark master has to be provided!
	 Also, it is supposed that the HDFS Namenode has private network IP = 10.10.1.1 

EX: spark../bin/spark-submit.sh page_rank_wiki_partitions.py /data/wiki/* /data/ranks_wiki 10 30 128.72.1.1
'''


def parseNeighborsWiki(ids):
	"""Parses a nodeID pair string into nodeID pair 
		filtering lines with ':' unless they are of the type 'Category:'
	"""
	match = re.search(r':', ids)
	if match:
		match2 = re.search(r'Category:', ids)
		if match2:
			parts = re.split(r'\t+', ids.lower())
			return parts[0], parts[1]
		else:
			return
	else:
		parts = re.split(r'\t+', ids.lower())
		return parts[0], parts[1]

def computeContribs(ids, rank):
	"""Calculates nodeID contributions to the rank of other nodeIDs."""
	num_ids = len(ids)
	for id in ids:
		yield (id, rank / num_ids)

if __name__ == "__main__":

	if len(sys.argv) != 6:
		print("Usage: load_csv_test.py <inputFile> <outputFile> <iterations> <Master IP> <partitions>")
		sys.exit(-1)


	# Set the configuration properties required by the homework:
	# Spark driver memory 			= 32GB
	# Executor memory  				= 32GB
	# Executor cores  				= 10
	# Number of cpus per task  		= 1 

	masterIP = sys.argv[4]
	iterations = int (sys.argv[3])
	partitions = int (sys.argv[5])

	spark = SparkSession.builder\
	.master("spark://"+ masterIP +":7077")\
	.appName("homework 1 part 2 - wiki with " + str(partitions) + " partitions")\
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


	# Loads in input file. It should be in format of:
	#     nodeID         neighbor nodeID
	#     nodeID         neighbor nodeID
	# 		...	
	lines = spark.read.text(input_path).rdd.map(lambda r: r[0])
	
	# Loads all IDs from input file and initialize their neighbors.
	links = lines.map(lambda ids: parseNeighborsWiki(ids)).filter(lambda x: x).distinct().groupByKey().partitionBy(partitions)
	#links = links_basic.partitionBy(new HashPartitioner(8))

	# Loads all IDs with other ID(s) link to from input file and initialize ranks of them to one.
	ranks = links.map(lambda id_neighbors: (id_neighbors[0], 1.0)).partitionBy(partitions)

	print('ranks data type: ' + str(type(ranks)))

	# Calculates and updates nodeIDs ranks continuously using PageRank algorithm.
	for iteration in range(iterations):

		# Calculates nodeID contributions to the rank of other nodeIDs.
		contribs = links.join(ranks).flatMap( lambda id_ids_rank: computeContribs(id_ids_rank[1][0], id_ids_rank[1][1]))

		# Re-calculates nodeID ranks based on neighbor contributions.
		ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15).partitionBy(partitions)


	# Store data 
	ranks.saveAsTextFile(output_path);

	#mean = ranks.map(lambda x: x[1]).mean()
	#print('AVG ranking: ' + str(mean))  		
