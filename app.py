from flask import Flask, request, jsonify
import pandas as pd
from pydp.algorithms.laplacian import BoundedSum, Count
import threading

class PrivacyEngine:
    """
    A class to hold the data and DP logic. This ensures data is loaded only once
    when the server starts.
    """
    def __init__(self, data_path):
        self._raw_data = None
        try:
            self._raw_data = pd.read_csv(data_path)
            # Define key columns
            self._rev_col = 'cust_profile_bba_wl72k_v3.total_rev'
            self._region_col = 'cust_profile_bba_wl72k_v3.package_service'
            self._category_col = 'cust_profile_bba_wl72k_v3.package_category'
            self._act_date_col = 'cust_profile_bba_wl72k_v3.act_date'
            self._los_col = 'cust_profile_bba_wl72k_v3.los_segment'
            self._channel_col = 'cust_profile_bba_wl72k_v3.channel_new'

            # Clean and prepare data
            self._raw_data.dropna(subset=[self._rev_col, self._region_col, self._category_col, self._act_date_col, self._los_col, self._channel_col], inplace=True)
            self._raw_data[self._act_date_col] = pd.to_datetime(self._raw_data[self._act_date_col])
            self._lower_bound = float(self._raw_data[self._rev_col].min())
            self._upper_bound = float(self._raw_data[self._rev_col].max())
            print("✅ Privacy Engine initialized and data loaded successfully.")
        except Exception as e:
            print(f"❌ Privacy Engine failed to initialize: {e}")

    def _get_private_sum(self, data, epsilon):
        if not data: return 0
        return BoundedSum(epsilon=epsilon, lower_bound=self._lower_bound, upper_bound=self._upper_bound, dtype='float').quick_result(data)

    def _get_private_count(self, data, epsilon):
        return Count(epsilon=epsilon, dtype='int').quick_result(data)

    def get_revenue_by_region(self, use_dp, epsilon):
        grouped_data = self._raw_data.groupby(self._region_col)[self._rev_col]
        if use_dp:
            return {region: self._get_private_sum(revenues.tolist(), epsilon) for region, revenues in grouped_data}
        else:
            return grouped_data.sum().astype(float).to_dict()

    def get_count_by_category(self, use_dp, epsilon):
        grouped_data = self._raw_data.groupby(self._category_col).size()
        if use_dp:
            return {category: self._get_private_count([1] * count, epsilon) for category, count in grouped_data.items()}
        else:
            return grouped_data.astype(int).to_dict()

    def get_count_by_fingerprint(self, use_dp, epsilon, params):
        year, month, los, channel = params.get("year"), params.get("month"), params.get("los"), params.get("channel")
        filtered_df = self._raw_data[
            (self._raw_data[self._act_date_col].dt.year == year) &
            (self._raw_data[self._act_date_col].dt.month == month) &
            (self._raw_data[self._los_col] == los) &
            (self._raw_data[self._channel_col] == channel)
        ]
        count = len(filtered_df)
        if use_dp:
            return self._get_private_count([1] * count, epsilon)
        else:
            return count

# --- Flask App Setup ---
app = Flask(__name__)
# Initialize the privacy engine once when the app starts
privacy_engine = PrivacyEngine(data_path='BBA_Cleaned.csv')

# --- SERVER-SIDE PRIVACY POLICY ---
# The server defines the epsilon (privacy budget) for each query type.
# The client cannot change these values.
SERVER_EPSILON_POLICY = {
    "revenue_by_region": 4.0,
    "count_by_category": 2.5,
    "count_by_fingerprint": 0.2
}

@app.route('/api/query', methods=['POST'])
def handle_query():
    """A single endpoint to handle all types of queries."""
    if not privacy_engine._raw_data is not None:
        return jsonify({"error": "Server data not loaded."}), 500

    data = request.get_json()
    query_type = data.get("type")
    use_dp = data.get("use_dp", False)
    params = data.get("params", {})

    # The server now retrieves the epsilon from its internal policy.
    # It IGNORES any epsilon value sent by the client.
    epsilon = SERVER_EPSILON_POLICY.get(query_type, 1.0) # Default to 1.0 if not in policy

    print(f"Received query: {query_type} (DP={'On' if use_dp else 'Off'}, Server Epsilon={epsilon if use_dp else 'N/A'})")

    if query_type == "revenue_by_region":
        result = privacy_engine.get_revenue_by_region(use_dp, epsilon)
    elif query_type == "count_by_category":
        result = privacy_engine.get_count_by_category(use_dp, epsilon)
    elif query_type == "count_by_fingerprint":
        result = privacy_engine.get_count_by_fingerprint(use_dp, epsilon, params)
    else:
        return jsonify({"error": "Unsupported query type."}), 400

    return jsonify({"result": result})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)