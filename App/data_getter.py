import requests

class FireDataGetter:
    def __init__(self, data_url, dataset_id, api_token):
        self.dataset_id = dataset_id
        self.data_url = self.get_data_url(data_url, dataset_id)
        self.api_token = api_token
        self.headers = {
            "X-App-Token": api_token,
            "Content-Type": "application/json"
        }
    
    @staticmethod
    def get_data_url(data_url, dataset_id):
        return f"{data_url}/{dataset_id}.json"
    
    def get_row_qty(self):
        req_count = requests.get(
            f"{self.data_url}?$select=count(ID)",
            headers=self.headers
        )

        total_rows = req_count.json()[0]["count_ID"]
        
        return total_rows
    
    def get_all_data(self, return_pandas=False):
        total_rows = self.get_row_qty()
        LIMIT_QUERY = f"?$limit={total_rows}"
        req = requests.get(
            f"{self.data_url}{LIMIT_QUERY}",
            headers=self.headers
        )
        data = req.json()
        return data
    
