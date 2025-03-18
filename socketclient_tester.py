import socketio

# Create a Socket.IO client instance
sio = socketio.Client()

# Event handler for connection success
@sio.event
def connect():
    print("Successfully connected to the server")

# Event handler for disconnection
@sio.event
def disconnect():
    print("Disconnected from the server")

# Event handler for receiving kafka_data from the server
@sio.on('kafka_data')
def on_kafka_data(data):
    print(f"Received data: {data}")

# Connect to the server
if __name__ == "__main__":
    try:
        sio.connect('http://172.22.19.237:5000', wait_timeout=10)  # Connect to localhost on port 5000, with a 10-second timeout
        sio.wait()  # Keep the connection alive to receive data
    except Exception as e:
        print(f"Connection failed: {e}")