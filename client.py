import socket
import json
import time
import matplotlib.pyplot as plt
import pandas as pd

class AnalystClient:
    """
    The Client (Analyst) sends queries to the server and analyzes the results.
    """
    def __init__(self, host='localhost', port=9999):
        self._host = host
        self._port = port
        print("✅ Client initialized.")

    def _send_query(self, query_dict):
        """Sends a query to the server and returns the response."""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((self._host, self._port))
                s.sendall(json.dumps(query_dict).encode('utf-8'))
                response_chunks = [chunk for chunk in iter(lambda: s.recv(4096), b'')]
                response = json.loads(b''.join(response_chunks).decode('utf-8'))
                return response.get("result")
        except ConnectionRefusedError:
            print("❌ Connection Error: Could not connect to the server. Is it running?")
            return None
        except Exception as e:
            print(f"❌ An error occurred: {e}")
            return None

    def plot_bar_charts(self, non_private_data, private_data, title):
        """Generates a bar chart comparing non-private and private revenue results."""
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
        """Generates pie charts to compare distributions."""
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
        """Generates a horizontal bar chart for low-population categories."""
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

    def perform_revenue_analysis(self):
        """Performs revenue analysis and generates bar charts."""
        print("\n--- 📊 1. Revenue Analysis Initiated 📊 ---\n")
        # ... (This function remains unchanged)
        non_private = self._send_query({"type": "revenue_by_region", "use_dp": False})
        private_high = self._send_query({"type": "revenue_by_region", "use_dp": True, "epsilon": 0.1})
        if not all([non_private, private_high]): return
        self.plot_bar_charts(non_private, private_high, 'Actual vs. Private Revenue (High Privacy, ε=0.1)')

    def perform_count_analysis(self):
        """Performs count analysis, demonstrating threshold attacks and plotting pie charts."""
        print("\n--- 📊 2. Customer Count Analysis Initiated 📊 ---\n")
        # ... (This function remains unchanged)
        non_private = self._send_query({"type": "count_by_category", "use_dp": False})
        private = self._send_query({"type": "count_by_category", "use_dp": True, "epsilon": 0.2})
        if not all([non_private, private]): return
        self.plot_pie_charts(non_private, private, 'Customer Distribution by Package Category')

    def perform_long_tail_analysis(self):
        """Analyzes and visualizes categories with a small number of customers."""
        print("\n--- 📊 3. Long-Tail Category Analysis Initiated 📊 ---\n")
        # ... (This function remains unchanged)
        non_private = self._send_query({"type": "count_by_category", "use_dp": False})
        private = self._send_query({"type": "count_by_category", "use_dp": True, "epsilon": 0.5})
        if not all([non_private, private]): return
        long_tail_categories = {cat: count for cat, count in non_private.items() if count <= 10}
        if not long_tail_categories: return
        self.plot_long_tail_chart(long_tail_categories, private, 'Analysis of Low-Population ("Long-Tail") Categories')

    def perform_fingerprint_analysis(self):
        """Simulates a fingerprinting attack using multiple quasi-identifiers."""
        print("\n--- 📊 4. Fingerprinting Attack Analysis Initiated 📊 ---\n")
        
        # Define the attacker's knowledge to create a "fingerprint"
        attack_params = {
            "year": 2020,
            "month": 9,
            "los": "06. 3-7yr",
            "channel": "MyTelkomsel"
        }
        print(f"Attacker's Goal: Find how many users signed up in {attack_params['month']}/{attack_params['year']}, have a '{attack_params['los']}' tenure, and use the '{attack_params['channel']}' channel.")

        # --- Attack without DP ---
        non_private_query = {"type": "count_by_fingerprint", "use_dp": False, "params": attack_params}
        non_private_result = self._send_query(non_private_query)
        
        analysis_text = "--- Numerical Analysis: Fingerprinting Attack ---\n\n"
        analysis_text += f"Attacker's Query Parameters: {attack_params}\n\n"
        analysis_text += "--- Attack WITHOUT Differential Privacy ---\n"
        analysis_text += f"Server's Exact Count: {non_private_result}\n"
        if non_private_result is not None and non_private_result <= 5:
            analysis_text += f"🚨 VULNERABILITY CONFIRMED: The attacker has isolated a very small group of {non_private_result} people, making them easy to identify.\n\n"
        else:
            analysis_text += "Result: The attacker found a group, but it may not be small enough to identify individuals.\n\n"

        # --- Attack with DP ---
        private_query = {"type": "count_by_fingerprint", "use_dp": True, "epsilon": 0.1, "params": attack_params}
        private_result = self._send_query(private_query)

        analysis_text += "--- Attack WITH Differential Privacy (ε=0.1) ---\n"
        analysis_text += f"Server's Noisy Count: {private_result:.4f}\n"
        analysis_text += "Result: The attacker cannot be certain of the true group size.\n"
        analysis_text += "If the true count was 1, the noisy answer prevents them from confirming it. ✅ PRIVACY PRESERVED."

        print(analysis_text)

if __name__ == "__main__":
    client = AnalystClient()
    client.perform_revenue_analysis()
    client.perform_count_analysis()
    client.perform_long_tail_analysis()
    client.perform_fingerprint_analysis() # New analysis added here
    print("\n--- 🏁 All Analyses Complete 🏁 ---")