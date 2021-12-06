import boto3

class RedshiftBotoUploader:
    """Uploads data to Redshift Data API through Boto3 client
    """
    def __init__(self, 
                 cluster:str,
                 region:str,
                 access_key:str, 
                 secret_key:str,
                 database:str,
                 secret:str):
        self.client = boto3.client(
            "redshift-data", 
            region_name=region,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key
        )
        self.cluster = cluster
        self.access_key = access_key
        self.secret_key = secret_key
        self.database = database
        self.secret = secret

    def _execute_sql(self, sql, name=None):
        with_event = True if name else False
        self.client.execute_statement(
            ClusterIdentifier=self.cluster,
            Database=self.database,
            SecretArn=self.secret,
            Sql=sql,
            StatementName=name,
            WithEvent=with_event
        )

    def truncate(self, table):
        sql = f"TRUNCATE {table}"
        self._execute_sql(sql, name="sf_fires table reinitialized")

    def upload(self, 
               table:str, 
               bucket:str, 
               filepath:str):
        sql = f"""
        COPY {table} FROM 's3://{bucket}/{filepath}'
        access_key_id '{self.access_key}'
        secret_access_key '{self.secret_key}'
        CSV QUOTE '\"' DELIMITER ','
        TIMEFORMAT 'auto'
        acceptinvchars
        """
        self._execute_sql(sql, name="sf_fires data uploaded")
    
    def update(self,
               table: str,
               bucket: str,
               filepath:str):
        self.truncate(table)
        self.upload(table, bucket, filepath)
