from data_getter import FireDataGetter
from table_creator import SfFiresTableCreator
from connector import MySQLConnector
from data_uploader import RDSUploader
from col2type_map import rs_col2type
import json


if __name__ == "__main__":

    # Retrieve data
    api_token = "C0VXHS8THSI6qUXuuopy2FqCP"
    dataset_id = "wr8u-xric"
    data_url = "https://data.sfgov.org/resource"
    retriever = FireDataGetter(data_url, dataset_id, api_token)
    data = retriever.get_all_data()

    # Connect to database
    user = "admin"
    password = "adminadmin"
    host = "sf-fires-1.cmbkjcuzgzbs.us-east-2.rds.amazonaws.com"
    connector = MySQLConnector(user, password, host)
    
    # Create table
    db_name = "sf_fires_testing"
    table_name = "sf_fires"
    columns = col2type
    
    table_creator = SfFiresTableCreator(db_name, table_name, connector, columns)
    table_creator.reinit_table()

    # Upload data to RDS database
    uploader = RDSUploader(connector, data, columns)
    NR_OF_BATCHES = 70
    uploader.update_table(NR_OF_BATCHES)

    # Close connection
    connector.connection.close()








