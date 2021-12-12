from datetime import datetime
import redshift_connector

from data_getter import FireDataGetter
from s3 import S3Handler 
from data_processing import DataProcessor
from connector import  DwConnector
from redshift_boto3 import RedshiftBotoUploader
from config import *

# import json


if __name__ == "__main__":

    # Retrieve data
    now = datetime.now()
    print(f"Process initiated: {now}")
    api_token = api_config["app_token"]
    dataset_id = api_config["sf_fires_dataset_id"]
    data_url = api_config["data_url"]
    retriever = FireDataGetter(data_url, dataset_id, api_token)
    data = retriever.get_all_data()
    print("Data Retrieved")

    # Data Processing
    df = DataProcessor(data).df
    print("Data processed")

    # Upload to S3
    ACCESS_KEY = s3_config["access_key"]
    SECRET_KEY = s3_config["secret_key"]
    BUCKET = s3_config["bucket"]
    handler = S3Handler(ACCESS_KEY, SECRET_KEY, BUCKET)
    filepath = "data/sf_fires.csv"
    backup_path = "data/sf_fires_"
    backup_path += f"{now.year}-{now.month}-{now.day}:{now.hour}H:{now.minute}M.csv" 
    
    print(f"Handler instantiated, backup filepath {filepath}")
    
    handler.upload_dataframe(df, filepath, backup_path=backup_path)

    print("File uploaded")

    # Load to Redshift
    print("Updating Data Warehouse")
    
    region = redshift_config["region"]
    cluster = redshift_config["cluster"]
    rs_database = redshift_config["database"]
    secret = redshift_config["secret"]
    uploader = RedshiftBotoUploader(cluster, region, ACCESS_KEY, 
                                    SECRET_KEY, rs_database, secret)
    
    table_prod = redshift_config["db.table_prod"]
    table_dev = redshift_config["db.table_dev"]
    uploader.update(table_prod, table_dev, BUCKET, filepath)

    print("Redshift table updated")








