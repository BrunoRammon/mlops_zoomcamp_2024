# pylint: disable=duplicate-code

from datetime import datetime
import pandas as pd
import os

def get_s3_storage_options():
    # pylint: disable=C0116
    s3_endpoint_url = os.getenv('S3_ENDPOINT_URL')
    if s3_endpoint_url is not None:
        options = {
            'client_kwargs': {
                'endpoint_url': s3_endpoint_url
            }
        }
    else:
        options = None

    return options

def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)

data = [
    (None, None, dt(1, 1), dt(1, 10)),
    (1, 1, dt(1, 2), dt(1, 10)),
    (1, None, dt(1, 2, 0), dt(1, 2, 59)),
    (3, 4, dt(1, 2, 0), dt(2, 2, 1)),
]
columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime',
               'tpep_dropoff_datetime']
input_data = pd.DataFrame(data, columns=columns)
def get_date_prediction():
    year = os.getenv('PREDICTION_YEAR', 2023)
    month = os.getenv('PREDICTION_MONTH', 1)
    return year, month

year,month = get_date_prediction()
input_data.to_parquet(
    f's3://nyc-duration/in/{year:04d}-{month:02d}.parquet',
    engine='pyarrow',
    compression=None,
    index=False,
    storage_options=get_s3_storage_options()
)

#os.system(f'pipenv run python ../batch.py {year} {month}')
