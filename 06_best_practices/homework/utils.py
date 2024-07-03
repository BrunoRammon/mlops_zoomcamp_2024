import os

def get_input_path(year, month, taxi_type):
    # pylint: disable=C0116
    default_input_pattern = (
        'https://d37ci6vzurychx.cloudfront.net/trip-data/'
        '{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet'
    )
    input_pattern = os.getenv('INPUT_FILE_PATTERN', default_input_pattern)
    return input_pattern.format(year=year, month=month, taxi_type=taxi_type)

def get_output_path(year, month, taxi_type):
    # pylint: disable=C0116
    default_output_pattern = (
        's3://nyc-duration/taxi_type={taxi_type}/'
        'year={year:04d}/month={month:02d}/predictions.parquet'
    )
    output_pattern = os.getenv('OUTPUT_FILE_PATTERN', default_output_pattern)
    return output_pattern.format(year=year, month=month, taxi_type=taxi_type)

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
