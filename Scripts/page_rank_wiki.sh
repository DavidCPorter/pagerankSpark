#! /bin/bash

EXE=$1											# Path to the executable that should be run

MASTER_IP="$(ifconfig | awk '/inet / {print $2; exit}')"
INPUT_HDFS='/data/wiki'
OUTPUT_HDFS='/data/ranks-wiki/'
INPUT_DATA='../Data/wiki/'
DATA_LINK='https://www.dropbox.com/sh/9uvvugxuq9ekqeh/AABITMBbNqLux_ZkW9yt2vdJa?dl=0'
ITERATIONS=10
PARTITIONS=30

if [ ! -d $INPUT_DATA ]; then
    echo "Data not found! Downloading them..."
    wget $DATA_LINK
    mkdir ../Data/wiki/
    unzip 'AABITMBbNqLux_ZkW9yt2vdJa?dl=0' -d ../Data/wiki/
    rm 'AABITMBbNqLux_ZkW9yt2vdJa?dl=0'
    rm ../Data/wiki/README*
fi

echo 'Uploading the files in HDFS...'

hdfs dfs -mkdir /data/wiki/
hdfs dfs -rm -r $INPUT_HDFS 
hdfs dfs -put $INPUT_DATA $INPUT_HDFS

hdfs dfs -rm -r $OUTPUT_HDFS

echo 'Cleaning the cache of the nodes...'
parallel-ssh -i -h ../../slaves -P "sudo sh -c \"sync; echo 3 > /proc/sys/vm/drop_caches\""

echo 'Starting the application...'
../../spark-2.2.0-bin-hadoop2.7/bin/spark-submit $EXE $INPUT_HDFS'/link*' $OUTPUT_HDFS $ITERATIONS $MASTER_IP $PARTITIONS 

echo 'Execution completed!'

exit

