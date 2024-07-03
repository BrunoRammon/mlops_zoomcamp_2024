#!/usr/bin/env python
# coding: utf-8

import sys
import pickle
import pandas as pd
import utils


def load_model(model_filename):
    # pylint: disable=C0116
    with open(model_filename, 'rb') as f_in:
        dict_vev, log_reg = pickle.load(f_in)

    return dict_vev, log_reg

def read_data(filename,options):
    # pylint: disable=C0116
    df = pd.read_parquet(filename, storage_options=options)
    return df

def prepare_data(df, categorical_cols):
    # pylint: disable=C0116

    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical_cols] = df[categorical_cols].fillna(-1).astype('int').astype('str')

    return df

def prediction(df, dict_vec, log_reg,categorical_cols):
    # pylint: disable=C0116
    dicts = df[categorical_cols].to_dict(orient='records')
    X_val = dict_vec.transform(dicts)
    y_pred = log_reg.predict(X_val)

    print('predicted sum duration:', y_pred.sum())
    return y_pred

def save_results(df, y_pred, output_filename, year, month, storage_options):
    # pylint: disable=C0116
    df_result = pd.DataFrame()
    df_result['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    df_result['predicted_duration'] = y_pred
    df_result.to_parquet(output_filename, engine='pyarrow', index=False,
                         storage_options=storage_options)

def main(year,month, taxi_type='yellow', model_filename='model.bin'):
    # pylint: disable=C0116
    input_file = utils.get_input_path(year, month, taxi_type)
    output_file =   utils.get_output_path(year, month, taxi_type)
    storage_options = utils.get_s3_storage_options()
    data = read_data(input_file, storage_options)
    categorical = ['PULocationID', 'DOLocationID']
    data = prepare_data(data, categorical)
    dv,lr = load_model(model_filename)
    y_inference = prediction(data,dv,lr,categorical)
    save_results(data, y_inference, output_file, year, month,storage_options)

if __name__ == "__main__":
    y = int(sys.argv[1])
    m = int(sys.argv[2])
    main(y,m)
