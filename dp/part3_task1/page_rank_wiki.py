from pyspark.sql import *
from pyspark import SparkContext
import sys
import re
from operator import add


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

"""
class DomainPartitioner extends Partitioner {
	def numPartitions = 20

	def getPartition(key: Any): Int = parseDomain(key.toString).hashCode % numPartitions

	def equals(other: Any): Boolean = other.isInstanceOf[DomainPartitioner]
}
"""

if __name__ == "__main__":

	if len(sys.argv) != 4:
		print("Usage: load_csv_test.py <inputFile> <outputFile> <iterations> ")
		sys.exit(-1)


	# Set the configuration properties required by the homework:
	# Spark driver memory 			= 32GB
	# Executor memory  				= 32GB
	# Executor cores  				= 10
	# Number of cpus per task  		= 1

	spark = SparkSession.builder\
	.config("spark.eventLog.enabled","true") \
	.appName("test")\
	.config("spark.submit.deployMode", "cluster")\
	.config("spark.driver.memory", "32g")\
	.config("spark.executor.memory", "32g")\
	.config("spark.executor.cores", "10")\
	.config("spark.task.cpus", "1")\
	.getOrCreate()

	# Input/Output path - hdfs://namenode_IP:9000/path/to/file/in/hdfs/filename
	# OBS: take namenode_IP from hadoop-2.7.6/etc/hadoop/core-site.html
	input_path = "hdfs://10.10.1.1:9000" + sys.argv[1]

	output_path = "hdfs://10.10.1.1:9000" + sys.argv[2]

	iterations = int(sys.argv[3])

	# Loads in input file. It should be in format of:
	#     nodeID         neighbor nodeID
	#     nodeID         neighbor nodeID
	# 		...
	lines = spark.read.text(input_path).rdd.map(lambda r: r[0])

	# Loads all IDs from input file and initialize their neighbors.
	links = lines.map(lambda ids: parseNeighborsWiki(ids)).filter(lambda x: x).distinct().groupByKey()
	#links = links_basic.partitionBy(new HashPartitioner(8))
	print("links"+str(links.getNumPartitions()))

	# Loads all IDs with other ID(s) link to from input file and initialize ranks of them to one.
	ranks = links.map(lambda id_neighbors: (id_neighbors[0], 1.0))

	# print('ranks data type: ' + str(type(ranks)))
	print("ranks"+str(ranks.getNumPartitions()))

	# Calculates and updates nodeIDs ranks continuously using PageRank algorithm.
	for iteration in range(iterations):

		# Calculates nodeID contributions to the rank of other nodeIDs.
		contribs = links.join(ranks).flatMap( lambda id_ids_rank: computeContribs(id_ids_rank[1][0], id_ids_rank[1][1]))
		print("contribs"+str(iteration)+'->'+str(contribs.getNumPartitions()))


		# Re-calculates nodeID ranks based on neighbor contributions.
		ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

		print("ranks"+str(iteration)+'->'+str(ranks.getNumPartitions()))

	# Store data
	ranks.saveAsTextFile(output_path)
	spark.stop()
