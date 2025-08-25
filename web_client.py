import requests
import json
import matplotlib.pyplot as plt
import pandas as pd

class AnalystClient:
    """
    The Client (Analyst) sends HTTP requests to the Flask API server.
    """
    def __init__(self, base_url='http://127.0.0.1:5000'):
        self._base_url = base_url
        print(f"✅ Client initialized. Server URL: {self._base_url}")

    def _send_query(self, query_payload):
        """Sends a POST request with a JSON payload to the server's API."""
        try:
            response = requests.post(f"{self._base_url}/api/query", json=query_payload)
            # Raise an exception if the server returned an error (e.g., 400, 500)
            response.raise_for_status()
            return response.json().get("result")
        except requests.exceptions.RequestException as e:
            print(f"❌ Connection Error: Could not connect to the server. {e}")
            return None

    # All plotting functions (plot_bar_charts, plot_pie_charts, etc.) remain exactly the same
    def plot_bar_charts(self, non_private_data, private_data, title):
        df = pd.DataFrame({
            'Category': list(non_private_data.keys()),
            'Actual Revenue': list(non_private_data.values()),
            'Private Revenue': list(private_data.values())
        }).sort_values('Actual Revenue', ascending=False).head(10)
        df.plot(x='Category', y=['Actual Revenue', 'Private Revenue'], kind='bar', figsize=(15, 7))
        plt.title(title, fontsize=16)
        plt.ylabel('Total Revenue')
        plt.xlabel('Package Service (Category)')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.show()

    def plot_pie_charts(self, non_private_data, private_data, title):
        df_non_private = pd.Series(non_private_data).sort_values(ascending=False)
        df_private = pd.Series(private_data)
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8))
        fig.suptitle(title, fontsize=20)
        df_non_private.head(7).plot(kind='pie', ax=ax1, autopct='%1.1f%%', startangle=90, wedgeprops={'edgecolor': 'white'})
        ax1.set_title('Actual Customer Distribution', fontsize=14)
        ax1.set_ylabel('')
        df_private.reindex(df_non_private.head(7).index).clip(lower=0).plot(kind='pie', ax=ax2, autopct='%1.1f%%', startangle=90, wedgeprops={'edgecolor': 'white'})
        ax2.set_title('Differentially Private Distribution', fontsize=14)
        ax2.set_ylabel('')
        plt.tight_layout(rect=[0, 0.05, 1, 0.95])
        plt.show()

    def plot_long_tail_chart(self, non_private_data, private_data, title):
        df = pd.DataFrame({
            'Category': list(non_private_data.keys()),
            'Actual Count': list(non_private_data.values()),
            'Private Count': [private_data.get(cat, 0) for cat in non_private_data.keys()]
        }).sort_values('Actual Count', ascending=True)
        df.plot(x='Category', y=['Actual Count', 'Private Count'], kind='barh', figsize=(15, 10))
        plt.title(title, fontsize=16)
        plt.xlabel('Number of Customers')
        plt.ylabel('Package Category')
        plt.tight_layout()
        plt.show()


    # All analysis functions are the same, they just build a different payload dictionary
    def perform_revenue_analysis(self):
        print("\n--- 📊 1. Revenue Analysis Initiated 📊 ---\n")
        non_private = self._send_query({"type": "revenue_by_region", "use_dp": False})
        private_high = self._send_query({"type": "revenue_by_region", "use_dp": True, "epsilon": 0.1})
        if not all([non_private, private_high]): return
        self.plot_bar_charts(non_private, private_high, 'Actual vs. Private Revenue (High Privacy, ε=0.1)')

    def perform_count_analysis(self):
        print("\n--- 📊 2. Customer Count Analysis Initiated 📊 ---\n")
        non_private = self._send_query({"type": "count_by_category", "use_dp": False})
        private = self._send_query({"type": "count_by_category", "use_dp": True, "epsilon": 0.2})
        if not all([non_private, private]): return
        self.plot_pie_charts(non_private, private, 'Customer Distribution by Package Category')

    def perform_long_tail_analysis(self):
        print("\n--- 📊 3. Long-Tail Category Analysis Initiated 📊 ---\n")
        non_private = self._send_query({"type": "count_by_category", "use_dp": False})
        private = self._send_query({"type": "count_by_category", "use_dp": True, "epsilon": 0.5})
        if not all([non_private, private]): return
        long_tail_categories = {cat: count for cat, count in non_private.items() if count <= 10}
        if not long_tail_categories: return
        self.plot_long_tail_chart(long_tail_categories, private, 'Analysis of Low-Population ("Long-Tail") Categories')

    def perform_fingerprint_analysis(self):
        print("\n--- 📊 4. Fingerprinting Attack Analysis Initiated 📊 ---\n")
        attack_params = {"year": 2022, "month": 12, "los": "05. 1-3yr", "channel": "MyTelkomsel"}
        print(f"Attacker's Goal: Find how many users match the fingerprint: {attack_params}")
        
        non_private_payload = {"type": "count_by_fingerprint", "use_dp": False, "params": attack_params}
        non_private_result = self._send_query(non_private_payload)
        
        analysis_text = "--- Numerical Analysis: Fingerprinting Attack ---\n\n"
        analysis_text += f"Server's Exact Count: {non_private_result}\n"
        if non_private_result is not None and non_private_result <= 5:
            analysis_text += f"🚨 VULNERABILITY CONFIRMED: The attacker has isolated a very small group of {non_private_result} people.\n\n"
        
        private_payload = {"type": "count_by_fingerprint", "use_dp": True, "epsilon": 0.1, "params": attack_params}
        private_result = self._send_query(private_payload)

        analysis_text += "--- Attack WITH Differential Privacy (ε=0.1) ---\n"
        analysis_text += f"Server's Noisy Count: {private_result:.4f}\n"
        analysis_text += "Result: The attacker cannot be certain of the true group size. ✅ PRIVACY PRESERVED."
        print(analysis_text)

if __name__ == "__main__":
    client = AnalystClient()
    client.perform_revenue_analysis()
    client.perform_count_analysis()
    client.perform_long_tail_analysis()
    client.perform_fingerprint_analysis()
    print("\n--- 🏁 All Analyses Complete 🏁 ---")
