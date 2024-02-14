import pandas as pd
from google.cloud import storage
import pyarrow as pa
import pyarrow.parquet as pq
import io
import os

green_trip_data_dfs = []
bucket_name = 'mage-zoomcamp-daniel-3'
destination_blob_name = 'green_taxi_data_22.parquet'
credentials_file = "/home/daniel/mage-zoomcamp/ny-rides-daniel-291132867b40.json"
destination_folder = 'green_taxi_data_22'


def load_taxi_data():
    for i in range(1,13):
        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-{i:02d}.parquet'
        green_trip_data_dfs.append(pd.read_parquet(url))
        #response = requests.get(url)
        #green_trip_data_dfs.append(pd.read_parquet(io.StringIO(response.text)))

    return  pd.concat(green_trip_data_dfs, ignore_index=True)


def transform_taxi_data(df):
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
    df= df.astype(taxi_dtypes)
    

    df['lpep_pickup_date'] = df['lpep_pickup_datetime'].dt.date
    df['lpep_dropoff_datetime'] = df['lpep_dropoff_datetime'].dt.date
    
    return df    


def upload_dataframe_to_gcs(bucket_name, dataframe, destination_blob_name, credentials_file):
    """Uploads a Pandas DataFrame as a Parquet file to Google Cloud Storage."""
    # Convert DataFrame to Parquet format
    table = pa.Table.from_pandas(dataframe)
    output = io.BytesIO()
    pq.write_table(table, output)
    output.seek(0)

    # Initialize the Google Cloud Storage client with the credentials
    storage_client = storage.Client.from_service_account_json(credentials_file)

    # Get the target bucket
    bucket = storage_client.bucket(bucket_name)

    # Upload the Parquet data to the bucket
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_file(output, content_type='application/octet-stream')

    print(f'DataFrame uploaded to gs://{bucket_name}/{destination_blob_name}')


if __name__ == "__main__":
   load_df =load_taxi_data()
   transformed_df =  transform_taxi_data(load_df)
   upload_dataframe_to_gcs(bucket_name,  transformed_df,destination_blob_name, credentials_file)