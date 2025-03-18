import eventlet
eventlet.monkey_patch()

from confluent_kafka import Consumer, KafkaError
from flask import Flask
from flask_socketio import SocketIO
from flask_cors import CORS
import json

# Initialize Flask app
app = Flask(__name__)
CORS(app)  # Enable CORS for all domains

# Initialize SocketIO with Flask app, adjusted ping timeout and interval
socketio = SocketIO(app, cors_allowed_origins="*", ping_timeout=60, ping_interval=25, async_mode='eventlet')

# Kafka consumer configuration
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',  # Kafka broker address
    'group.id': 'websocket_consumer_group',  # Consumer group ID
    'auto.offset.reset': 'earliest'          # Start consuming from the earliest message
})

# Subscribe to Kafka topics
consumer.subscribe(['environmental_data', 'sound_pollution_data'])

@app.route('/')
def index():
    return "WebSocket server is running"

# Kafka listener function (no async for eventlet)
def kafka_listener():
    while True:
        try:
            msg = consumer.poll(1.0)  # Poll Kafka every second

            if msg is None:
                eventlet.sleep(1)  # Yield to other greenlets
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Kafka error: {msg.error()}")
                    continue

            # Decode and parse the message
            data = json.loads(msg.value().decode('utf-8'))
            print(f"Received data from Kafka: {data}")

            # Emit the data to all connected WebSocket clients
            try:
                socketio.emit('kafka_data', data)
                print(f"Emitted data to WebSocket clients: {data}")
            except Exception as e:
                print(f"Error emitting data to WebSocket clients: {e}")

        except Exception as e:
            print(f"Error processing message: {e}")

        # Yield to allow other greenlets to run
        eventlet.sleep(1)

# Heartbeat function to keep WebSocket connections alive (no async)
def heartbeat():
    while True:
        socketio.emit('heartbeat', {'status': 'alive'})
        eventlet.sleep(30)  # Send heartbeat every 30 seconds

# Handle client connections
@socketio.on('connect')
def handle_connect():
    print('Client connected')

# Handle client disconnections
@socketio.on('disconnect')
def handle_disconnect():
    print('Client disconnected')

if __name__ == '__main__':
    try:
        # Start Kafka listener in a background thread
        socketio.start_background_task(kafka_listener)

        # Start heartbeat task in the background to keep connections alive
        socketio.start_background_task(heartbeat)

        # Run the Flask-SocketIO server
        socketio.run(app, host="0.0.0.0", port=5000)
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        # Clean up Kafka consumer on exit
        consumer.close()

