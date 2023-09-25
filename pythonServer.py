import sqlite3
import json
import socket

# Initialize the SQLite database
db = sqlite3.connect('data.db')
cursor = db.cursor()

# Create a table to store sensor data
cursor.execute('''
    CREATE TABLE IF NOT EXISTS sensor_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        label TEXT,
        PM1_0 REAL,
        PM2_5 REAL,
        PM4_0 REAL,
        PM10 REAL,
        RH REAL,
        F REAL,
        VOC REAL,
        NOx REAL
    )
''')
db.commit()

# Create a UDP socket
udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
udp_socket.bind(('0.0.0.0', 11420))  # Replace 12345 with the desired port

print("UDP server is listening on port 12345")

while True:
    data, addr = udp_socket.recvfrom(1024)

    try:
        contents = json.loads(data.decode('utf-8'))

        # Insert data into the database
        insert_sql = '''
            INSERT INTO sensor_data (label, PM1_0, PM2_5, PM4_0, PM10, RH, F, VOC, NOx)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        values = (
            contents['label'],
            contents['PM1_0'],
            contents['PM2_5'],
            contents['PM4_0'],
            contents['PM10'],
            contents['RH'],
            contents['F'],
            contents['VOC'],
            contents['NOx']
        )
        cursor.execute(insert_sql, values)
        db.commit()

        # Print the received JSON data
        print("Received JSON data:", contents)

    except json.JSONDecodeError as e:
        print("Error decoding JSON:", e)
    except KeyError as e:
        print(f"Key not found in JSON data: {e}")

# Close the database connection when done
db.close()
