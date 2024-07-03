#!/usr/bin/env bash

if [[ -z "${GITHUB_ACTIONS}" ]]; then
  cd "$(dirname "$0")"
fi

export PREDICTION_YEAR=2023
export PREDICTION_MONTH=1
if [ "${LOCAL_IMAGE_NAME}" == "" ]; then 
    export LOCAL_IMAGE_NAME="batch-model-duration:${PREDICTION_YEAR}-${PREDICTION_MONTH}"
    echo "LOCAL_IMAGE_NAME is not set, building a new image with tag ${LOCAL_IMAGE_NAME}"
    docker build -t ${LOCAL_IMAGE_NAME} ..
else
    echo "no need to build image ${LOCAL_IMAGE_NAME}"
fi

export S3_ENDPOINT_URL="http://localhost:4566"

docker compose up -d

sleep 5

aws --endpoint-url=${S3_ENDPOINT_URL} \
    s3 mb s3://nyc-duration

pipenv run python integration_test.py

ERROR_CODE=$?

if [ ${ERROR_CODE} != 0 ]; then
 docker compose logs
 docker compose down
 exit ${ERROR_CODE}
fi

docker compose down
