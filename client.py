import socket
import json
import random
import time

class AnalystClient:
    """
    The Client (Analyst) sends queries to the server and analyzes the results.
    It has no direct access to the database.
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
                # Encode query dictionary to a JSON string, then to bytes
                s.sendall(json.dumps(query_dict).encode('utf-8'))
                # Receive the response
                data = s.recv(1024)
                # Decode the response from bytes to a JSON string, then to a dictionary
                response = json.loads(data.decode('utf-8'))
                return response.get("result")
        except ConnectionRefusedError:
            print("❌ Connection Error: Could not connect to the server. Is it running?")
            return None
        except Exception as e:
            print(f"❌ An error occurred: {e}")
            return None

    def perform_analysis(self):
        """
        Performs the analysis by sending queries to the remote server.
        """
        print("\n--- 📊 Client Analysis Initiated 📊 ---\n")
        time.sleep(1) # Pause for dramatic effect

        # 1. Simple query for total revenue
        print("1. Client asks for the total revenue of all customers.")
        non_private_total_rev = self._send_query({"type": "total_revenue", "use_dp": False})
        private_total_rev = self._send_query({"type": "total_revenue", "use_dp": True, "epsilon": 0.1})

        if non_private_total_rev is None or private_total_rev is None: return

        print(f"\n   - Result WITHOUT DP: {non_private_total_rev:,.2f}")
        print(f"   - Result WITH DP (ε=0.1):    {private_total_rev:,.2f}")
        print("   - Observation: The DP result is close to the actual total but has added noise for privacy.")
        time.sleep(2)

        # 2. Highlighting the vulnerability (Differencing Attack)
        print("\n\n2. Client attempts a 'Differencing Attack' to find a specific user's revenue.")
        target_user_id = random.randint(0, 100)
        print(f"\n   - Attacker's Goal: Find the revenue of User #{target_user_id}.")
        
        # Query 1: Get total revenue of EVERYONE (non-private)
        total_rev_all_users = self._send_query({"type": "total_revenue", "use_dp": False})
        print(f"   - Attacker Query 1 (All Users): {total_rev_all_users:,.2f}")

        # Query 2: Get total revenue with the target user removed (non-private)
        total_rev_minus_one = self._send_query({
            "type": "total_revenue", "use_dp": False, "user_id_to_remove": target_user_id
        })
        print(f"   - Attacker Query 2 (All Users except Target): {total_rev_minus_one:,.2f}")

        inferred_revenue = total_rev_all_users - total_rev_minus_one
        print(f"   - Attacker's Inference: {inferred_revenue:,.2f}")
        print("   - 🚨 VULNERABILITY CONFIRMED: The attacker successfully re-identified a user's exact revenue contribution.")
        time.sleep(2)

        # 3. Showing how Differential Privacy prevents this attack
        print("\n\n3. Client tries the same attack, but this time the server uses DP.")
        
        # Query 1 (DP): Get total revenue of EVERYONE
        private_total_rev_all = self._send_query({"type": "total_revenue", "use_dp": True, "epsilon": 0.1})
        print(f"   - Attacker DP Query 1 (All Users): {private_total_rev_all:,.2f}")

        # Query 2 (DP): Get total revenue with the target user removed
        private_total_rev_minus_one = self._send_query({
            "type": "total_revenue", "use_dp": True, "epsilon": 0.1, "user_id_to_remove": target_user_id
        })
        print(f"   - Attacker DP Query 2 (All Users except Target): {private_total_rev_minus_one:,.2f}")

        inferred_private_revenue = private_total_rev_all - private_total_rev_minus_one
        print(f"   - Attacker's DP Inference: {inferred_private_revenue:,.2f}")
        print("   - ✅ PRIVACY PRESERVED: The inference is wrong. The noise makes it impossible to determine the true value.")
        print("\n--- 🏁 Analysis Complete 🏁 ---")


if __name__ == "__main__":
    client = AnalystClient()
    client.perform_analysis()