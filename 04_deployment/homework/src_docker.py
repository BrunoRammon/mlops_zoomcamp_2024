import pickle
import pandas as pd
import argparse

categorical = ['PULocationID', 'DOLocationID']



def read_data(filename):
    df = pd.read_parquet(filename)

    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')

    return df
def load_model(file_name):
    with open(file_name, 'rb') as f_in:
        dv, model = pickle.load(f_in)
    return dv, model

def score(df, dv, model):

    dicts = df[categorical].to_dict(orient='records')
    X_val = dv.transform(dicts)
    y_pred = model.predict(X_val)

    return y_pred

def output(year, month, predictions, path):
    df = pd.DataFrame({'prediction': predictions})
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    df.to_parquet(f'{path}/prediction_{year}{month:02d}.parquet',
                  engine='pyarrow',
                  compression=None,
                  index=False)

if __name__ == "__main__":
    descp = 'Predicts ride duration for yellow taxi trip.'
    parser = argparse.ArgumentParser(description=descp)
    parser.add_argument('--year', metavar='-y', type=int,
                        help='Year of the data')
    parser.add_argument('--month', metavar='-m', type=int,
                        help='Month of the data')
    args = parser.parse_args()
    year = args.year
    month = args.month
    url = (
        'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_'
        f'{year}-{month:02d}.parquet'
    )

    df = read_data(url)
    dv, model = load_model('./model.bin')
    predictions = score(df,dv,model)
    print(f'Mean: {predictions.mean()}')
    print(f'Std: {predictions.std()}')
