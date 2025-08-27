import requests
import pandas as pd

class DataGenerator:
    """
    This client queries the server and saves the results to CSV files
    formatted for easy import into Power BI.
    """
    def __init__(self, base_url='http://127.0.0.1:5000'):
        self._base_url = base_url
        print(f"✅ Data Generator initialized. Server URL: {self._base_url}")

    def _send_query(self, query_payload):
        """Sends a POST request to the server's API."""
        try:
            response = requests.post(f"{self._base_url}/api/query", json=query_payload)
            response.raise_for_status()
            return response.json().get("result")
        except requests.exceptions.RequestException as e:
            print(f"❌ Connection Error: Could not connect to the server. {e}")
            return None

    def generate_data_files(self):
        """Queries all data types and saves them to CSV files."""
        print("\n--- Generating data files for Power BI ---")

        # --- 1. Get Revenue Data ---
        print("Querying revenue by region...")
        actual_revenue = self._send_query({"type": "revenue_by_region", "use_dp": False})
        private_revenue = self._send_query({"type": "revenue_by_region", "use_dp": True})

        # --- 2. Get Count Data ---
        print("Querying count by category...")
        actual_counts = self._send_query({"type": "count_by_category", "use_dp": False})
        private_counts = self._send_query({"type": "count_by_category", "use_dp": True})

        if not all([actual_revenue, private_revenue, actual_counts, private_counts]):
            print("❌ Failed to retrieve data from server. Aborting.")
            return

        # --- 3. Format Data into DataFrames ---
        # Actual Data
        df_actual_rev = pd.DataFrame(list(actual_revenue.items()), columns=['Category', 'Revenue'])
        df_actual_rev['AnalysisType'] = 'Revenue by Region'
        
        df_actual_count = pd.DataFrame(list(actual_counts.items()), columns=['Category', 'Count'])
        df_actual_count['AnalysisType'] = 'Count by Category'

        # Private Data
        df_private_rev = pd.DataFrame(list(private_revenue.items()), columns=['Category', 'Revenue'])
        df_private_rev['AnalysisType'] = 'Revenue by Region'

        df_private_count = pd.DataFrame(list(private_counts.items()), columns=['Category', 'Count'])
        df_private_count['AnalysisType'] = 'Count by Category'

        # --- 4. Save to CSV ---
        df_actual_rev.to_csv('actual_revenue.csv', index=False)
        df_private_rev.to_csv('private_revenue.csv', index=False)
        df_actual_count.to_csv('actual_counts.csv', index=False)
        df_private_count.to_csv('private_counts.csv', index=False)

        print("\n✅ Successfully created four CSV files:")
        print("   - actual_revenue.csv")
        print("   - private_revenue.csv")
        print("   - actual_counts.csv")
        print("   - private_counts.csv")
        print("\nYou can now import these files into Power BI.")


if __name__ == "__main__":
    generator = DataGenerator()
    generator.generate_data_files()
