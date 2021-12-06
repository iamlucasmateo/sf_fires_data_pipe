from abc import abstractmethod, ABC

class Connector:
    """Connects to database
    """
    def __init__(self, 
                 user:str, 
                 password:str,
                 connector, 
                 host,  
                 database: str=None):
        self.user = user
        self.password = password
        self.host = host
        self.connector = connector
        self.database = database
        self.connection = self._get_connection()
        self.cursor = self._get_cursor()

    @abstractmethod
    def _get_connection(self):
        return
    
    def _get_cursor(self):
        cursor = self.connection.cursor()
        return cursor


class DbConnector(Connector):    
    def _get_connection(self):
        connection = self.connector.connect(
            user=self.user, 
            password=self.password,
            host=self.host
        )
        return connection


class DwConnector(DbConnector):
    def _get_connection(self):
        connection = self.connector.connect(
            user=self.user, 
            password=self.password,
            host=self.host,
            database=self.database
        )
        return connection




