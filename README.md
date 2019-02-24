# CS 494 - Cloud Data Center Systems

## Homework 1

### Setup Script

In order to properly set up the HDFS and Spark frameworks for the given network configuration ( 1 leader, 3 followers) do what follows:

- Set up parallel-ssh on the cluster:
	- Do `ssh-keygen -t rsa` on the leader node
	- Copy the public key into __~/.ssh/authorized_keys__ for each machine in the cluster
	- On the master add into a file called slaves the hostname of the nodes in the cluster 

- Run __init_script.sh__ with root privileges 

To check that the installation was successful execute on the master node `parallel-ssh -h slaves -P jps` and expect to have a similar output:

```
follower-1: 7959 DataNode
follower-1: 8142 Worker
follower-1: 8453 Jps
follower-2: 7996 DataNode
follower-2: 8152 Worker
follower-2: 8464 Jps
follower-3: 7953 DataNode
follower-3: 8107 Worker
follower-3: 8416 Jps
leader: 8710 NameNode
leader: 8977 SecondaryNameNode
leader: 9178 Master
leader: 9534 Jps
```

### Spark sorting application

Simple application that sort an input .csv file from HDFS and saves it back sorted into HDFS. The sparkSession used is the one suggested in the homework description, thus:

- Spark driver memory = 32GB
- Executor memory = 32GB
- Executor cores = 10
- Number of CPUs per task = 1 

Before running the application the correct IP of the master node has to be inserted in the code at __line 41__:

	`.master("spark://master_public_IP:7077")\`
ex: 
	`.master("spark://128.110.153.141:7077")\`

__Execution example:__

- Load the __export__ file into HDFS using the following command `hdfs dfs -put /data/export /data/export.csv`
- Execute the application using: 
```spark-2.2.0-bin-hadoop2.7/bin/spark-submit sort_dataframe.py <abs path to input file> <abs path to output file>```

In our case: 
`spark-2.2.0-bin-hadoop2.7/bin/spark-submit sort_dataframe.py /data/export.csv /data/export_sorted`


### PageRank using Spark 

