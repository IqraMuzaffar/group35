#!/bin/bash

DATE=$1
YEAR=$(echo $DATE | cut -d'-' -f1)
MONTH=$(echo $DATE | cut -d'-' -f2)
DAY=$(echo $DATE | cut -d'-' -f3)

# Create HDFS directories
hdfs dfs -mkdir -p /raw/logs/$YEAR/$MONTH/$DAY
hdfs dfs -mkdir -p /raw/metadata

# Copy log file to HDFS
hdfs dfs -put raw_data/$DATE.csv /raw/logs/$YEAR/$MONTH/$DAY/

# Copy metadata file to HDFS (only once)
if [ ! "$(hdfs dfs -ls /raw/metadata/content_metadata.csv)" ]; then
    hdfs dfs -put raw_data/content_metadata.csv /raw/metadata/
fi

echo "Data ingestion complete for $DATE"
