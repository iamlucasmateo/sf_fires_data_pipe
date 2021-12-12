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

    def _execute_sql(self, sql: str, name=""):
        with_event = True if name else False
        result = self.client.execute_statement(
            ClusterIdentifier=self.cluster,
            Database=self.database,
            SecretArn=self.secret,
            Sql=sql,
            StatementName=name,
            WithEvent=with_event
        )
        return result
    
    def _batch_execute_sql(self, sqls: list, name=""):
        with_event = True if name else False
        result = self.client.batch_execute_statement(
            ClusterIdentifier=self.cluster,
            Database=self.database,
            SecretArn=self.secret,
            Sqls=sqls,
            StatementName=name,
            WithEvent=with_event
        )
        return result

    def _get_truncate_sql(self, table):
        return f"TRUNCATE {table}"
    
    def truncate(self, table):
        sql = self._get_truncate_sql(table)
        self._execute_sql(sql, name=f"{table} table reinitialized")
    
    def _get_copy_sql(self, new_table, original_table):
        return f"INSERT INTO {new_table} SELECT * FROM {original_table}"
    
    def copy(self, new_table, original_table):
        sql = self._get_copy_sql(new_table, original_table)
        self._execute_sql(sql, name=f"copies {new_table} from {original_table}")
    
    def _get_upload_sql(self, 
                       table:str, 
                       bucket:str, 
                       filepath:str):
        return f"""
        COPY {table} FROM 's3://{bucket}/{filepath}'
        access_key_id '{self.access_key}'
        secret_access_key '{self.secret_key}'
        CSV QUOTE '\"' DELIMITER ','
        TIMEFORMAT 'auto'
        acceptinvchars
        """

    def upload_from_s3(self, 
                       table:str, 
                       bucket:str, 
                       filepath:str):
        sql = self._get_upload_sql(table, bucket, filepath)
        self._execute_sql(sql, name="sf_fires data uploaded")
    
    def update(self,
               table_prod: str,
               table_dev: str,
               bucket: str,
               filepath:str):
        truncate_dev_sql = self._get_truncate_sql(table_dev)
        upload_dev_sql = self._get_upload_sql(table_dev, bucket, filepath)
        truncate_prod_sql = self._get_truncate_sql(table_prod)
        copy_sql = self._get_copy_sql(table_prod, table_dev)
        
        sqls = [truncate_dev_sql, upload_dev_sql, 
                    truncate_prod_sql, copy_sql]

        self._batch_execute_sql(sqls, name="updated sf_fires tables")
