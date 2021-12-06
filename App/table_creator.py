from abc import abstractmethod, ABC 
from connector import Connector
import mysql.connector

class TableCreator:
    def __init__(self, 
                db_name:str, 
                table_name:str, 
                connector:Connector, 
                columns:dict):
        self.db_name = db_name
        self.table_name = table_name
        self.connector = connector
        self.connection = connector.connection
        self.cursor = connector.cursor
        self.columns = columns

    @abstractmethod
    def get_create_str(self):
        pass

    @abstractmethod
    def create_table(self):
        pass

    @abstractmethod
    def drop_table(self):
        pass


    
class SfFiresTableCreator(TableCreator):
    TABLE_NAME = "sf_fires"
    CREATE_TABLE_QUERY = f"CREATE TABLE IF NOT EXISTS {TABLE_NAME}"
    INDEX_QUERY = "INDEX(incident_date, battalion, neighborhood_district)"
    KEY_QUERY = "PRIMARY KEY (id)"
    ENGINE_QUERY = "ENGINE=InnoDB"

    def _get_create_query(self):
        create_query = self.CREATE_TABLE_QUERY + " ("
        for k, v in self.columns.items():
            create_query += f"`{k.lower()}` {v},"
        create_query += f"{self.INDEX_QUERY}, {self.KEY_QUERY}) {self.ENGINE_QUERY}"
        return create_query
    
    def drop_table(self):
        drop_table_query = f"DROP TABLE IF EXISTS {self.table_name}"
        self.cursor.execute(drop_table_query)
        self.connection.commit()

    def create_table(self):
        create_str = self._get_create_query()
        try:
            self.cursor.execute(create_str)
            self.connection.commit()
        except mysql.connector.Error as err:
            self.connection.rollback()
    
    def use_db(self):
        query = f"USE {self.db_name}"
        self.cursor.execute(query)

    def reinit_table(self):
        self.use_db()
        self.drop_table()
        self.create_table()
    

