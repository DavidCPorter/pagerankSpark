#!/bin/bash

spark-submit --master $1 ./part3_task4/page_rank_wiki_partitioned_killed.py $2 $3 $4 &
echo "***********************"
sleep 20
echo "***********************"
sudo sh -c "sync; echo 3 > /proc/sys/vm/drop_caches"
echo "***********************"
parallel-ssh -h ../slaves -P kill -9 $5
exit 1
