#!/bin/bash

spark-submit --master $1 ./part3_task3/page_rank_wiki_partitioned_cached.py $2 $3 $4
exit 1
