#!/bin/bash

ROOT=$(git rev-parse --show-toplevel)
PYSPARK_SCRIPT=workflows/spark-job.py

function upload_pyspark_script_to_s3 {
  # Uploads the pyspark script into S3
  s3cmd -c ~/.aws/.s3cfg put "$ROOT/$PYSPARK_SCRIPT" \
        s3://customerintelligence/argo/scripts/python/spark-job.py
}


function help {
    echo "$0 <task> [args]"
    echo "Tasks:"
    compgen -A function | cat -n
}

TIMEFORMAT="Task completed in %3lR"
time ${@:-help}
