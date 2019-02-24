#!/bin/bash

spark-submit --master $1 ./part3_task2/page_rank_wiki_partitioned.py $2 $3 $4
exit 1
