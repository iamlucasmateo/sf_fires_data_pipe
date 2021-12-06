from abc import abstractmethod, ABC

class InsertQueryGenerator(ABC):
    """Generates statements for SQL insertions
    """
    def __init__(self, columns: dict):
        self.columns = columns
    
    @abstractmethod
    def get_init_insert_query(self):
        pass
    
    @abstractmethod
    def get_cell_value(self):
        pass

    @staticmethod
    def get_latlon(item, col):
        coords = item["point"].get("coordinates")
        if not coords:
            return None
        latitude, longitude = coords
        if col == "latitude":
            return latitude
        return longitude


class MySqlInsertQueryGenerator(InsertQueryGenerator):
    def get_init_insert_query(self):
        insert_query = "INSERT INTO sf_fires ("
        for col in self.columns:
            insert_query += f"{col}, "

        insert_query = f"{insert_query[:-2]}) VALUES ("
        insert_query += "%s, " * len(self.columns)
        insert_query = insert_query[:-2] + ")"

        return insert_query

    def get_cell_value(self, item, col):
        if col not in item.keys():
            return None
        if col in ["latitude", "longitude"]:
            return self.get_latlon(item, col)
        if "INT" in self.columns[col]:
            return int(item[col])
        if "FLOAT" in self.columns[col]:
            return float(item[col])
        return item[col]