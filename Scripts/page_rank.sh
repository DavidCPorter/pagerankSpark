#! /bin/bash

MASTER_IP="$(ifconfig | awk '/inet / {print $2; exit}')"
INPUT_HDFS='/data/web-BerkStan.txt'
OUTPUT_HDFS='/data/ranks-BerkStan'
INPUT_DATA='../Data/web-BerkStan.txt'
DATA_LINK='https://snap.stanford.edu/data/web-BerkStan.txt.gz'
ITERATIONS=15

if [ ! -f $INPUT_DATA ]; then
    echo "Data not found! Downloading them..."
    wget $DATA_LINK
    mv web-BerkStan.txt.gz ../Data/
    gunzip ../Data/web-BerkStan.txt.gz
fi

echo 'Uploading the files in HDFS...'

hdfs dfs -rm -r $INPUT_HDFS 
hdfs dfs -put ../Data/web-BerkStan.txt $INPUT_HDFS

hdfs dfs -rm -r $OUTPUT_HDFS


echo 'Starting the application...'

../../spark-2.2.0-bin-hadoop2.7/bin/spark-submit ../page_rank.py $INPUT_HDFS $OUTPUT_HDFS $ITERATIONS $MASTER_IP

echo 'Execution completed!'

exit

