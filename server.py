import socket
import pandas as pd
from pydp.algorithms.laplacian import BoundedSum
import json
import pickle

class DataServer:
    """
    The Server holds the raw data, applies differential privacy, and listens
    for network requests from clients.
    """
    def __init__(self, data_path):
        """Initializes the server and loads the dataset."""
        try:
            self._raw_data = pd.read_csv(data_path)
            self._rev_data = self._raw_data['cust_profile_bba_wl72k_v3.total_rev'].dropna()
            self._lower_bound = float(self._rev_data.min())
            self._upper_bound = float(self._rev_data.max())
            print("✅ Server initialized and data loaded successfully.")
        except Exception as e:
            print(f"❌ Server failed to initialize: {e}")
            self._raw_data = None

    def _get_private_total_revenue(self, data, epsilon):
        """Calculates the differentially private sum of revenue."""
        private_sum_calculator = BoundedSum(
            epsilon=epsilon,
            lower_bound=self._lower_bound,
            upper_bound=self._upper_bound,
            dtype='float'
        )
        return private_sum_calculator.quick_result(list(data))

    def process_query(self, query):
        """
        Processes a query dictionary and returns a result dictionary.
        This is the core logic that protects the data.
        """
        if self._raw_data is None:
            return {"error": "Server data not loaded."}

        query_type = query.get("type")
        use_dp = query.get("use_dp", False)
        user_id_to_remove = query.get("user_id_to_remove")

        # Create a temporary dataset for the query
        query_data = self._rev_data.copy()
        if user_id_to_remove is not None and user_id_to_remove < len(query_data):
            query_data = query_data.drop(query_data.index[user_id_to_remove]).reset_index(drop=True)

        if query_type == "total_revenue":
            if use_dp:
                print(f"🔒 Processing private query (ε={query.get('epsilon')})...")
                result = self._get_private_total_revenue(query_data, query.get("epsilon", 0.5))
            else:
                print("⚠️ Processing NON-private query...")
                # FIX: Convert the numpy.int64 result from .sum() to a standard Python float.
                result = float(query_data.sum())
            return {"result": result}
        else:
            return {"error": "Unsupported query type."}

    def start(self, host='localhost', port=9999):
        """Starts the server to listen for incoming connections."""
        if self._raw_data is None:
            return

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((host, port))
            s.listen()
            print(f"✅ Server is listening for connections on {host}:{port}...")
            while True:
                conn, addr = s.accept()
                with conn:
                    print(f"🤝 Connected by {addr}")
                    # Receive data from the client
                    data = conn.recv(1024)
                    if not data:
                        break
                    
                    # Decode the query from bytes to a dictionary
                    query = json.loads(data.decode('utf-8'))
                    
                    # Process the query to get the result
                    response_data = self.process_query(query)
                    
                    # Encode the response dictionary to bytes and send it back
                    conn.sendall(json.dumps(response_data).encode('utf-8'))
                    print(f"✅ Sent response to {addr}")


if __name__ == "__main__":
    server = DataServer(data_path='BBA_Cleaned.csv')
    server.start()