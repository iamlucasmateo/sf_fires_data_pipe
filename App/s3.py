import boto3
import io
import pandas as pd

class S3Handler:
    def __init__(self, access_key, secret_access_key, bucket):
        self.access_key = access_key
        self.secret_access_key = secret_access_key
        self.client = boto3.client(
            "s3",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_access_key,
        )
        self.bucket = bucket
    
    def upload_dataframe(self, 
                         df:pd.DataFrame, 
                         filepath: str, 
                         backup_path:str=None):
        with io.StringIO() as csv_buffer:
            df.to_csv(csv_buffer, index=False, header=False)
            self.client.put_object(
                Bucket=self.bucket, Key=filepath, Body=csv_buffer.getvalue()
            )
        if backup_path:
            copy_source = {
                'Bucket': self.bucket,
                'Key': filepath
            }
            self.client.copy(copy_source, self.bucket, backup_path)
    
    def copy_to_redshift(self, connector, table, filepath):
        query = f"""
        COPY {table} FROM 's3://{self.bucket}/{filepath}'
        access_key_id '{self.access_key}'
        secret_access_key '{self.secret_access_key}'
        CSV QUOTE '\"' DELIMITER ','
        TIMEFORMAT 'auto'
        acceptinvchars
        """
        try:
            connector.cursor.execute(f"TRUNCATE TABLE {table}")
            connector.cursor.execute(query)
            connector.connection.commit()
            print("Commited")
        except:
            connector.connection.rollback()
            print("Error in copy job")
