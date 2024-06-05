import pandas  as pd
from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
import mlflow 

def ingest():
    df = pd.read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-03.parquet')
    return df

def preprocess(df):

    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df.duration = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)]

    categorical = ['PULocationID', 'DOLocationID']
    df[categorical] = df[categorical].astype(str)
    
    return df

def _get_X_train(df):
    categorical = ['PULocationID', 'DOLocationID']
    train_dicts = df[categorical].to_dict(orient='records')
    dv = DictVectorizer()
    X_train = dv.fit_transform(train_dicts)
    return X_train

def train_model(df):
    target = 'duration'
    y_train = df[target].values
    X_train = _get_X_train(df)
    lr = LinearRegression()
    lr.fit(X_train, y_train)

    y_pred = lr.predict(X_train)

    print(f'Train RMSE: {mean_squared_error(y_train, y_pred, squared=False)}')
    print(f'Intercept: {lr.intercept_}')

    return lr

def log_model(lr):
    mlflow.start_run()
    mlflow.sklearn.log_model(lr,"sk_models")
    mlflow.end_run()

if __name__ == "__main__":
    EXPERIMENT_NAME = "lasso-regression"

    mlflow.set_tracking_uri("http://127.0.0.1:5000")
    mlflow.set_experiment(EXPERIMENT_NAME)
    df = ingest()
    df = preprocess(df)

    lr = train_model(df)

    log_model(lr)

