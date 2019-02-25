#!/bin/bash

PID_TO_KILL="$(parallel-ssh -H follower-1 -P jps | awk '/Worker/ {print $2}')"

spark-submit --master $1 ./part3_task4/page_rank_wiki_partitioned_killed.py $2 $3 $4 &
echo "***********************"
sleep 40
echo "***********************"
parallel-ssh -H follower-1 -P "sudo sh -c \"sync; echo 3 > /proc/sys/vm/drop_caches\""
echo "***********************"
parallel-ssh -H follower-1 -P sudo kill $PID_TO_KILL
exit 1
