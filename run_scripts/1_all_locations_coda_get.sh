#!/usr/bin/env bash

set -e

if [[ $# -ne 3 ]]; then
    echo "Usage: ./1_all_locations_coda_get.sh <coda-auth-file> <coda-v2-root> <data-root>"
    echo "Downloads coded messages datasets from Coda to '<data-root>/Coded Coda Files'"
    exit
fi

AUTH=$1
CODA_V2_ROOT=$2
DATA_ROOT=$3

./checkout_coda_v2.sh "$CODA_V2_ROOT"

DATASETS=(
    "GPSDD_KILIFI_s01e01"
    "GPSDD_KILIFI_s01e02"
    "GPSDD_KILIFI_s01e03"
    "GPSDD_KILIFI_s01e04"
    "GPSDD_KILIFI_s01e05"

    "GPSDD_KILIFI_age"
    "GPSDD_KILIFI_gender"
    "GPSDD_KILIFI_location"
    "GPSDD_KILIFI_disabled"

    "GPSDD_KIAMBU_s01e01"
    "GPSDD_KIAMBU_s01e02"
    "GPSDD_KIAMBU_s01e03"
    "GPSDD_KIAMBU_s01e04"
    "GPSDD_KIAMBU_s01e05"

    "GPSDD_KIAMBU_age"
    "GPSDD_KIAMBU_gender"
    "GPSDD_KIAMBU_location"
    "GPSDD_KIAMBU_disabled"

    "GPSDD_BUNGOMA_s01e01"
    "GPSDD_BUNGOMA_s01e02"
    "GPSDD_BUNGOMA_s01e03"
    "GPSDD_BUNGOMA_s01e04"
    "GPSDD_BUNGOMA_s01e05"

    "GPSDD_BUNGOMA_age"
    "GPSDD_BUNGOMA_gender"
    "GPSDD_BUNGOMA_location"
    "GPSDD_BUNGOMA_disabled"
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
