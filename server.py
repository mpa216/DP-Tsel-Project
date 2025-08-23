import socket
import pandas as pd
from pydp.algorithms.laplacian import BoundedSum, Count
import json

class DataServer:
    """
    The Server holds the raw data, applies differential privacy, and listens
    for network requests from clients.
    """
    def __init__(self, data_path):
        """Initializes the server and loads the dataset."""
        try:
            self._raw_data = pd.read_csv(data_path)
            # Define key columns for analytics
            self._rev_col = 'cust_profile_bba_wl72k_v3.total_rev'
            self._region_col = 'cust_profile_bba_wl72k_v3.package_service'
            self._category_col = 'cust_profile_bba_wl72k_v3.package_category'
            self._act_date_col = 'cust_profile_bba_wl72k_v3.act_date'
            self._los_col = 'cust_profile_bba_wl72k_v3.los_segment'
            self._channel_col = 'cust_profile_bba_wl72k_v3.channel_new'

            # Clean and prepare data
            self._raw_data.dropna(subset=[self._rev_col, self._region_col, self._category_col, self._act_date_col, self._los_col, self._channel_col], inplace=True)
            # Convert act_date to datetime objects for filtering
            self._raw_data[self._act_date_col] = pd.to_datetime(self._raw_data[self._act_date_col])
            self._lower_bound = float(self._raw_data[self._rev_col].min())
            self._upper_bound = float(self._raw_data[self._rev_col].max())
            print("✅ Server initialized and data loaded successfully.")
        except Exception as e:
            print(f"❌ Server failed to initialize: {e}")
            self._raw_data = None

    def _get_private_sum(self, data, epsilon):
        """Calculates the differentially private sum of a list of numbers."""
        if not data: return 0
        return BoundedSum(epsilon=epsilon, lower_bound=self._lower_bound, upper_bound=self._upper_bound, dtype='float').quick_result(data)

    def _get_private_count(self, data, epsilon):
        """Calculates the differentially private count of items in a list."""
        return Count(epsilon=epsilon, dtype='int').quick_result(data)

    def process_query(self, query):
        """Processes a query and returns a result, applying DP where requested."""
        if self._raw_data is None: return {"error": "Server data not loaded."}

        query_type = query.get("type")
        use_dp = query.get("use_dp", False)
        epsilon = query.get("epsilon", 0.5)

        if query_type == "revenue_by_region":
            grouped_data = self._raw_data.groupby(self._region_col)[self._rev_col]
            if use_dp:
                results = {region: self._get_private_sum(revenues.tolist(), epsilon) for region, revenues in grouped_data}
            else:
                results = grouped_data.sum().astype(float).to_dict()
            return {"result": results}

        elif query_type == "count_by_category":
            grouped_data = self._raw_data.groupby(self._category_col).size()
            if use_dp:
                results = {category: self._get_private_count([1] * count, epsilon) for category, count in grouped_data.items()}
            else:
                results = grouped_data.astype(int).to_dict()
            return {"result": results}

        elif query_type == "count_by_fingerprint":
            # Filter data based on the attacker's criteria
            params = query.get("params", {})
            year = params.get("year")
            month = params.get("month")
            los = params.get("los")
            channel = params.get("channel")
            
            filtered_df = self._raw_data[
                (self._raw_data[self._act_date_col].dt.year == year) &
                (self._raw_data[self._act_date_col].dt.month == month) &
                (self._raw_data[self._los_col] == los) &
                (self._raw_data[self._channel_col] == channel)
            ]
            count = len(filtered_df)

            if use_dp:
                print(f"🔒 Processing private fingerprint query (ε={epsilon})...")
                result = self._get_private_count([1] * count, epsilon)
            else:
                print("⚠️ Processing NON-private fingerprint query...")
                result = count
            return {"result": result}
        else:
            return {"error": "Unsupported query type."}

    def start(self, host='localhost', port=9999):
        """Starts the server to listen for incoming connections."""
        if self._raw_data is None: return
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((host, port))
            s.listen()
            print(f"✅ Server is listening for connections on {host}:{port}...")
            while True:
                conn, addr = s.accept()
                with conn:
                    print(f"🤝 Connected by {addr}")
                    data = conn.recv(4096)
                    if not data: break
                    query = json.loads(data.decode('utf-8'))
                    response_data = self.process_query(query)
                    conn.sendall(json.dumps(response_data).encode('utf-8'))
                    print(f"✅ Sent response to {addr}")

if __name__ == "__main__":
    server = DataServer(data_path='BBA_Cleaned.csv')
    server.start()