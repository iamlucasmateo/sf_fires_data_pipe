from connector import Connector
from query_generator import InsertQueryGenerator
from abc import abstractmethod, ABC

class DataUploader(ABC):
    """Uploads data to database
    """
    def __init__(self, 
                 connector:Connector, 
                 data:list, 
                 columns:dict=None, 
                 query_gen: InsertQueryGenerator=None):
        self.connector = connector
        self.connection = connector.connection
        self.cursor = connector.cursor
        self.data = data
        self.columns = columns
        self.query_gen = query_gen
    
    @abstractmethod
    def insert_table(self, batches: int):
        pass
    

class DbUploader(DataUploader):
    def insert_table(self, batches: int):
        try:
            self._insert_in_batches(batches)
            self.connection.commit()
        except:
            self.connection.rollback()
            # Nice to have: error monitoring
    
    def _insert_batch(self, batch_data: list):
        insert_query = self.query_gen.get_init_insert_query()
        values = []
        
        for item in batch_data:
            row = [ self.query_gen.get_cell_value(item, col) for col in self.columns ]
            values.append(tuple(row))
        
        self.cursor.executemany(insert_query, values)

    def _insert_in_batches(self, batches: int):
        batch_size = int(len(self.data) / batches) + 1
        total_rows = len(self.data)
        from_index, to_index = 0, batch_size
        for i in range(batches):
            batch_data = self.data[from_index:to_index]
            self._insert_batch(batch_data)
            
            from_index = to_index
            to_index = min(to_index + batch_size, total_rows)
