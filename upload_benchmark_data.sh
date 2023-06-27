#!/bin/bash

set -e -x -u -o pipefail

if [ "$#" -ne 4 ]; then
	echo "Usage: $0 DATASET_LABEL OBSERVATIONS_FILE ORBIT_FILE CONFIG_FILE" >&2
	exit 1
fi

DATASET_LABEL=$1
OBSERVATIONS_FILE=$2
ORBIT_FILE=$3
CONFIG_FILE=$4

gsutil cp $OBSERVATIONS_FILE gs://thor-benchmark-data/$DATASET_LABEL/observations.csv
gsutil cp $ORBIT_FILE gs://thor-benchmark-data/$DATASET_LABEL/orbits.csv
gsutil cp $CONFIG_FILE gs://thor-benchmark-data/$DATASET_LABEL/config.yaml
