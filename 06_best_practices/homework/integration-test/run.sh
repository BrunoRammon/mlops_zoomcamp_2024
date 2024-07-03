#!/usr/bin/env bash

if [[ -z "${GITHUB_ACTIONS}" ]]; then
  cd "$(dirname "$0")"
fi

export INPUT_FILE_PATTERN="s3://nyc-duration/in/{year:04d}-{month:02d}.parquet"
export OUTPUT_FILE_PATTERN="s3://nyc-duration/out/{year:04d}-{month:02d}.parquet"

docker compose up -d

sleep 5

aws --endpoint-url=http://localhost:4566 \
    s3 mb s3://nyc-duration

pipenv run python integration_test.py

# ERROR_CODE=$?

# if [ ${ERROR_CODE} != 0 ]; then
#     docker compose logs
#     docker compose down
#     exit ${ERROR_CODE}
# fi

# docker compose down