from pyspark.sql import *
from pyspark import SparkContext
import sys
import re
from operator import add


def parseNeighbors(ids):
	"""Parses a nodeID pair string into nodeID pair."""
	parts = re.split(r'\s+', ids)
	return parts[0], parts[1]

def parseNeighborsWiki(ids):
	"""Parses a nodeID pair string into nodeID pair
		filtering lines with ':' unless they are of the type 'Category:'
	"""
	parts = re.split(r'\s+', ids)
	return parts[0], parts[1]

def computeContribs(ids, rank):
	"""Calculates nodeID contributions to the rank of other nodeIDs."""
	num_ids = len(ids)
	for id in ids:
		yield (id, rank / num_ids)


if __name__ == "__main__":

	if len(sys.argv) != 4:
		print("Usage: load_csv_test.py <inputFile> <outputFile> <iterations>")
		sys.exit(-1)

		##testing

	# Set the configuration properties required by the homework:
	# Spark driver memory 			= 32GB
	# Executor memory  				= 32GB
	# Executor cores  				= 10
	# Number of cpus per task  		= 1

	spark = SparkSession.builder              \
	.appName("homework 1 part 2")             \
	.config("spark.eventLog.enabled","true") \
	.config("spark.submit.deployMode", "cluster")             \
	.config("spark.driver.memory", "32g")             \
	.config("spark.executor.memory", "32g")             \
	.config("spark.executor.cores", "10")             \
	.config("spark.task.cpus", "1")             \
	.getOrCreate()



	# Input/Output path - hdfs://namenode_IP:9000/path/to/file/in/hdfs/filename
	# OBS: take namenode_IP from hadoop-2.7.6/etc/hadoop/core-site.html
	input_path = "hdfs://10.10.1.1:9000" + sys.argv[1]

	output_path = "hdfs://10.10.1.1:9000" + sys.argv[2]

	iterations = int (sys.argv[3])

	# Loads in input file. It should be in format of:
	#     nodeID         neighbor nodeID
	#     nodeID         neighbor nodeID
	# 		...
	lines = spark.read.text(input_path).rdd.map(lambda r: r[0])
	# Loads all IDs from input file and initialize their neighbors.
	links = lines.map(lambda ids: parseNeighbors(ids)).distinct().groupByKey().repartition(30)
	print("links"+str(links.getNumPartitions()))
	#links contain an RDD of (url, set(urls))
	ranks = links.map(lambda website: (website[0], 1.0))
	#ranks contain a list of urls with their rank == 1self.
	#Note, we got rid of many of the urls for enwiki data set since our limited data set will not contain the full graph
	print("ranks"+str(ranks.getNumPartitions()))

	print('ranks data type: ' + str(type(ranks)))

	# Calculates and updates nodeIDs ranks continuously using PageRank algorithm.
	for iteration in range(iterations):
		#if iteration % 100 == 0:
		#	print('iteration ' + str(iteration))
		# Calculates nodeID contributions to the rank of other nodeIDs.
		# links.join(ranks) will create a tuple (url, (urls, float))
		# flatmap function takes the urls and updates their Ranks and maps them to the contribs RDD.
		contribs = links.join(ranks).flatMap( lambda id_ids_rank: computeContribs(id_ids_rank[1][0], id_ids_rank[1][1])).repartition(200).cache()
		print("contribs"+str(iteration)+'->'+str(contribs.getNumPartitions()))

		# Re-calculates nodeID ranks based on neighbor contributions.
		ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
		print("ranks"+str(iteration)+'->'+str(ranks.getNumPartitions()))

	# Store data
	ranks.saveAsTextFile (output_path);
