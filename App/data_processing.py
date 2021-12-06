import pandas as pd

from col2type_map import column_order

class DataProcessor:
    def __init__(self, data):
        self.df = pd.DataFrame.from_records(data)
        self._process_df()
    
    def _process_df(self):
        self.df["latitude"] = self.df["point"].apply(self.get_coord("latitude"))
        self.df["longitude"] = self.df["point"].apply(self.get_coord("longitude"))
        self.df = self.df.drop(columns=["point"])
        self.df = self.df[column_order]

    @staticmethod
    def get_coord(coord_type):
        def get_item_coord(item):
            try:
                latitude, longitude = item["coordinates"]
                if coord_type == "latitude":
                    return latitude
                return longitude
            except:
                return None
        return get_item_coord