#!/bin/bash


for month in {6..10}
do
   for day in {1..31}
   do
       python mrr_pro_ingest.py 2023 $month $day /data/datastream/neiu/neiu-mrrpro-a1
   done
done   
