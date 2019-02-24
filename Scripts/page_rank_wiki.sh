#! /bin/bash

MASTER_IP="$(ifconfig | awk '/inet / {print $2; exit}')"
INPUT_HDFS='/data/wiki/*'
OUTPUT_HDFS='/data/ranks-wiki/'
INPUT_DATA='../Data/wiki/'
DATA_LINK='https://www.dropbox.com/sh/9uvvugxuq9ekqeh/AABITMBbNqLux_ZkW9yt2vdJa?dl=0'
ITERATIONS=15

if [ ! -d $INPUT_DATA ]; then
    echo "Data not found! Downloading them..."
    wget $DATA_LINK
    mkdir ../Data/wiki/
    mv AABIT* ../Data/wiki/
    unzip ../Data/wiki/AABIT*
fi

echo 'Uploading the files in HDFS...'

hdfs dfs -rm -r $INPUT_HDFS 
hdfs dfs -put ../Data/wiki/links* $INPUT_HDFS

hdfs dfs -rm -r $OUTPUT_HDFS


echo 'Starting the application...'

../../spark-2.2.0-bin-hadoop2.7/bin/spark-submit ../page_rank.py $INPUT_HDFS $OUTPUT_HDFS $ITERATIONS $MASTER_IP

echo 'Execution completed!'

exit

