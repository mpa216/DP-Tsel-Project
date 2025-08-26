cara jalaninnya (lokal):
1. Pastiin semua file ada di folder yg sama
2. Buka 2 terminal
3. di Terminal 1 tulis: python app.py
4. di Terminal 2 tulis : python web_client.py 

cara jalanin secara cross device:
1. Find Your Server's Local IP Address
The computer running the app.py server needs to tell the other devices its address on the local network.

On the computer where the server is running, open the Command Prompt (you can search for cmd).

Type the following command and press Enter:

ipconfig
Look for the "Wireless LAN adapter Wi-Fi" or "Ethernet adapter" section. You will find an "IPv4 Address" that looks something like 192.168.1.15 or 10.0.0.5. This is the server's local IP address.

2. Modify the Client Code on the Other Devices
On each of the other devices where you want to run web_client.py, you need to make one small change to the code:

Open web_client.py.

Find this line near the top:

def __init__(self, base_url='http://127.0.0.1:5000'):
Replace 127.0.0.1 with the server's IP address you found in Step 1. For example:

def __init__(self, base_url='http://192.168.1.15:5000'):
3. Run Everything
Make sure all the devices (the server and all the clients) are connected to the same Wi-Fi or local network.

Start the server on the main computer by running python app.py.

On each of the other devices, run the modified python web_client.py.
