#!/bin/bash

spark-submit --master $1 ./part3_task1/page_rank_wiki.py $2 $3 $4
exit 1
