Below are the commands for running the page_rank with the wiki data set.
Be sure you are in the directory with the scripts.sh. For task4, please have a slave file in the parent directory with all of the slave IPs.
Also, you will need to create a folder /tmp/spark-events in order to save the log from each job. Then you can access the job history at masternodeIP:18080.

```
run_task1_wiki.sh <masterIP:port> <HDFS inputFile> <HDFS outputFile> <#iterations>

run_task2_wiki.sh <masterIP:port> <HDFS inputFile> <HDFS outputFile> <#iterations>

run_task3_wiki.sh <masterIP:port> <HDFS inputFile> <HDFS outputFile> <#iterations>

run_task4_wiki.sh <masterIP:port> <HDFS inputFile> <HDFS outputFile> <#iterations> <processID to kill>

example:
./run_task1_wiki.sh spark://128.104.222.40:7077 /bigdata /output-store/ 10

```
