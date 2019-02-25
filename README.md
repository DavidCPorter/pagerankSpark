# CS 494 - Cloud Data Center Systems

## Homework 1

### Setup Script

In order to properly set up the HDFS and Spark frameworks for the given network configuration ( 1 leader, 3 followers) do what follows:

- Set up parallel-ssh on the cluster:
	- Do `ssh-keygen -t rsa` on the leader node
	- Copy the public key into __~/.ssh/authorized_keys__ for each machine in the cluster
	- On the master add into a file called slaves the hostname of the nodes in the cluster 

- Run __init_script.sh__ with root privileges (don't use  sudo you just have to have root permissions on the cluster)

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

__OBS:__ In order to have an always working environment you have to add the path to `/users/[username]/hadoop-2.7.6/bin/` and to `/users/[username]/hadoop-2.7.6/sbin/ ` in the `$PATH` variable. To permanently do that, update `$PATH` in /etc/environment using `sudo`.

### Spark sorting application

Simple application that sort an input .csv file from HDFS and saves it back sorted into HDFS. The sparkSession used is the one suggested in the homework description, thus:

- Spark driver memory = 32GB
- Executor memory = 32GB
- Executor cores = 10
- Number of CPUs per task = 1 

__Execution example:__

- Load the __export__ file into HDFS using the following command `hdfs dfs -put /data/export /data/export.csv`
- Execute the application using: 
```spark-2.2.0-bin-hadoop2.7/bin/spark-submit sort_dataframe.py <abs path to input file> <abs path to output file> <master IP>```

To replicate the required task just execute the script __sort_dataframe.sh__ which automatically downloads the data, load them into HDFS and run the application.


### PageRank using Spark 

This application execute the __PageRank__ algorithm on a given input file in HDFS. Also in this case we used the above mentioned set up configuration for the sparkSession. 

Before running the application the correct IP address of the master node has to be inserted in the code as in the previous example. Also the input files should be already loaded into HDFS.

The output file and the number of iterations should be provided as input parameters by the user. 

There are more flavors of the same application which try to explore how the performance are affected when making little changes to the code:

- __page_rank.py__  is used to test the algorithm with the __web-StanBerk.txt__ input graph with a standard configuration (no in-memory persistence or custom partitioning).

- __page\_rank\_wiki.py__ is used to test pagerank on the __Wikipedia__ dataset with a standard configuration (no in-memory persistence or custom partitioning).

- __page\_rank\_wiki\_paritioning.py__ is used to test pagerank on the __Wikipedia__ dataset with a custom partitioning but without in-memory persistence.

- __page\_rank\_wiki\persistence.py__ is used to test pagerank on the __Wikipedia__ dataset with a custom partitioning and in-memory persistence.

To run the applications that doesn't have custom partitioning use the following:

```spark-2.2.0-bin-hadoop2.7/bin/spark-submit <app name> <abs path to input file> <abs path to output file> <num of iterations> <master IP>```

__For example:__
```spark-2.2.0-bin-hadoop2.7/bin/spark-submit page_rank.py /data/web-BerkStan /data/ranks-BerkStan 15 128.110.154.121```

To run applications with custom partitioning use the following:

```spark-2.2.0-bin-hadoop2.7/bin/spark-submit <app name> <abs path to input file> <abs path to output file> <num of iterations> <num of partitions> <master IP>```

__For example:__
```spark-2.2.0-bin-hadoop2.7/bin/spark-submit page_rank_wiki_partitioning.py /data/wiki/* /data/ranks-wiki 15 30 128.110.154.121```

To replicate the required task just run the scripts inside `/Scripts/` the names should be pretty self explanatory for the task of the script. They automatically downloads the data, upload them into HDFS and run the given application.

For the last part of the Homework instead the scripts inside the `/dp/` directory specifically replicate each of tasks required. Look in that directory for an explanation on how to execute them and what they do exactly. 