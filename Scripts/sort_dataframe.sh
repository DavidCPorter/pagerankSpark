#! /bin/bash

MASTER_IP="$(ifconfig | awk '/inet / {print $2; exit}')"
INPUT_HDFS='/data/export.csv'
OUTPUT_HDFS='/data/export_sorted'
INPUT_DATA='../Data/export.csv'
DATA_LINK='https://www2.cs.uic.edu/~brents/cs494-cdcs/data/export'

if [ ! -f $INPUT_DATA ]; then
    echo "Data not found! Downloading them..."
    wget $DATA_LINK
    mv export ../Data/export.csv
fi

echo 'Uploading the files in HDFS...'

hdfs dfs -rm -r $INPUT_HDFS 
hdfs dfs -put ../Data/export.csv $INPUT_HDFS

hdfs dfs -rm -r $OUTPUT_HDFS


echo 'Starting the application...'

../../spark-2.2.0-bin-hadoop2.7/bin/spark-submit ../sort_dataframe.py $INPUT_HDFS $OUTPUT_HDFS $MASTER_IP

echo 'Execution completed!'

exit

