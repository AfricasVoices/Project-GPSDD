#!/usr/bin/env bash

set -e

if [[ $# -ne 3 ]]; then
    echo "Usage: ./1_bungoma_coda_get.sh <coda-auth-file> <coda-v2-root> <data-root>"
    echo "Downloads coded messages datasets from Coda to '<data-root>/Coded Coda Files'"
    exit
fi

AUTH=$1
CODA_V2_ROOT=$2
DATA_ROOT=$3

./checkout_coda_v2.sh "$CODA_V2_ROOT"

DATASETS=(
    "GPSDD_BUNGOMA_s01e01"
    "GPSDD_BUNGOMA_s01e02"
    "GPSDD_BUNGOMA_s01e03"
    "GPSDD_BUNGOMA_s01e04"
    "GPSDD_BUNGOMA_s01e05"
    "GPSDD_BUNGOMA_s01e06"
    "GPSDD_BUNGOMA_s01e07"
    "GPSDD_BUNGOMA_s01e08"
    "GPSDD_BUNGOMA_s01e09"
    "GPSDD_BUNGOMA_s01e10"
    "GPSDD_BUNGOMA_s01_closeout"

    "GPSDD_BUNGOMA_age"
    "GPSDD_BUNGOMA_gender"
    "GPSDD_BUNGOMA_location"
    "GPSDD_BUNGOMA_disabled"

    "GPSDD_BUNGOMA_baseline_community_awareness"
    "GPSDD_BUNGOMA_baseline_government_role"

    "GPSDD_BUNGOMA_endline_community_awareness"
    "GPSDD_BUNGOMA_endline_government_role"
)

cd "$CODA_V2_ROOT/data_tools"
git checkout "c47977d03f96ba3e97c704c967c755f0f8b666cb"  # (master which supports incremental add)

mkdir -p "$DATA_ROOT/Coded Coda Files"

for DATASET in ${DATASETS[@]}
do
    FILE="$DATA_ROOT/Coded Coda Files/$DATASET.json"

    if [ -e "$FILE" ]; then
        echo "Getting messages data from ${DATASET} (incremental update)..."
        MESSAGES=$(pipenv run python get.py --previous-export-file-path "$FILE" "$AUTH" "${DATASET}" messages)
        echo "$MESSAGES" >"$FILE"
    else
        echo "Getting messages data from ${DATASET} (full download)..."
        pipenv run python get.py "$AUTH" "${DATASET}" messages >"$FILE"
    fi

done
