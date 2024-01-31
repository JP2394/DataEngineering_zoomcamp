import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    quarter = ['2020-10','2020-11','2020-12']
    list_dataframes = []
    
    for i in quarter:
        url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_{i}.csv.gz'
        parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']
        list_dataframes.append(pd.read_csv(url, sep=',',compression='gzip',parse_dates=parse_dates))

    # Concatenating DataFrames
    quarter_df = pd.concat(list_dataframes, ignore_index=True)

    taxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'RatecodeID': pd.Int64Dtype(),
        'store_and_fwd_flag': str,
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'payment_type': pd.Int64Dtype(),
        'fare_amount': float,
        'extra': float,
        'mta_tax': float,
        'tip_amount': float,
        'tolls_amount': float,
        'improvement_surcharge': float,
        'total_amount': float,
        'congestion_surcharge': float 
    }
    
    # Change the data types of columns in the DataFrame
    quarter_df= quarter_df.astype(taxi_dtypes)
    print(quarter_df.shape)

    return quarter_df
