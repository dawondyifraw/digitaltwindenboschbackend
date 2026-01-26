# Digital Twin Den Bosch Backend

A comprehensive backend system for the Digital Twin of Den Bosch, providing real-time sensor data management, AI-driven natural language querying, and dashboard APIs for urban monitoring and analytics.

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [API Documentation](#api-documentation)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## Overview

This project implements a smart city digital twin backend for Den Bosch, integrating IoT sensor data with AI-powered query capabilities. The system collects environmental sensor data (CO₂, NO₂, PM2.5, noise levels) through Kafka streams, stores it in InfluxDB, and provides multiple interfaces for data access:

- RESTful APIs for dashboard visualization
- Natural language query interface using Large Language Models (LLMs)
- Real-time data streaming and anomaly detection

## 🔒 Security Notice

**IMPORTANT**: This project handles sensitive data including API keys and database credentials. Never commit sensitive information to version control.

- The `.env` file is automatically ignored by Git
- All sensitive values are loaded from environment variables
- Use strong, unique API keys and tokens
- Rotate credentials regularly
- Never share `.env` files or commit them to repositories

## Features

- **Real-time Sensor Data Ingestion**: Kafka-based data pipeline for collecting sensor readings from 50+ environmental sensors
- **Time-series Data Storage**: InfluxDB for efficient storage and querying of sensor data
- **AI-Powered Querying**: Natural language to InfluxDB query translation using LLMs
- **Dashboard APIs**: RESTful endpoints providing aggregated data for frontend visualization
- **Anomaly Detection**: Real-time monitoring and alerting for sensor anomalies
- **WebSocket Support**: Live data streaming for real-time dashboards
- **Docker Containerization**: Complete containerized deployment with Docker Compose
- **Monitoring and Metrics**: Telegraf integration for system monitoring

## Architecture

### Core Components

- **apis/dashboard_api.py**: Flask-based REST API serving aggregated sensor data for dashboards
- **apis/llm_influx_query_engine.py**: AI-powered natural language query processor using Hyperbolic API
- **apis/explainer.py**: LLM integration component for query translation
- **producers/kafka_producer_simulator.py**: Kafka producer for simulated sensor data
- **consumers/kafka_consumer_influx.py**: Kafka consumer writing data to InfluxDB
- **detectors/anomaly_detector_websocket.py**: Real-time anomaly detection with WebSocket support
- **utils/**: Utility scripts for metrics reading, socket testing, etc.

### Data Flow

1. **Data Ingestion**: Sensors → Kafka Producers → Kafka Topics
2. **Data Storage**: Kafka Consumers → InfluxDB
3. **Data Access**:
   - Dashboard API → InfluxDB (aggregated data)
   - LLM Query Engine → InfluxDB (natural language queries)
   - WebSocket Server → Real-time streaming

### Technology Stack

- **Backend**: Python 3, Flask
- **Database**: InfluxDB (time-series)
- **Message Queue**: Apache Kafka
- **AI/ML**: Hyperbolic API (DeepSeek-V3 model)
- **Monitoring**: Telegraf
- **Containerization**: Docker, Docker Compose
- **WebSockets**: For real-time data

## Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Git
- Valid API keys for LLM services (Hyperbolic)

## Installation

### Step-by-Step Installation Guide

#### Step 1: Clone the Repository

```bash
git clone <repository-url>
cd digitaltwindenboschbackend
```

#### Step 2: Set Up Environment Variables

Create a `.env` file in the root directory:

```bash
# Copy the example environment file
cp .env.example .env

# Edit the .env file with your actual values
nano .env
```

Required environment variables:

```env
INFLUX_TOKEN=your_influx_token_here
HYPERBOLIC_API_KEY=your_hyperbolic_api_key_here
INFLUX_URL=http://localhost:8086
INFLUX_ORG=DenBosch
BUCKET=sensors_db
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

#### Step 3: Docker Deployment (Recommended)

```bash
# Build and start all services
docker-compose -f config/docker-compose.yml up --build -d

# Check service status
docker-compose -f config/docker-compose.yml ps

# View logs
docker-compose -f config/docker-compose.yml logs -f
```

#### Step 4: Verify Services are Running

```bash
# Check InfluxDB health
curl http://localhost:8086/health

# Check Kafka
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# Test Dashboard API
curl http://localhost:5001/health

# Test LLM Query API
curl http://localhost:5050/health
```

### Manual Installation (Alternative)

If you prefer not to use Docker:

#### Step 1: Install Dependencies

```bash
# Install Python packages
pip install flask flask-cors requests influxdb-client kafka-python

# Install InfluxDB
# Follow: https://docs.influxdata.com/influxdb/v2/install/

# Install Kafka
# Follow: https://kafka.apache.org/quickstart
```

#### Step 2: Configure InfluxDB

```bash
# Create organization and bucket
influx org create -n DenBosch
influx bucket create -n sensors_db -o DenBosch

# Generate API token
influx auth create --org DenBosch --all-access
```

#### Step 3: Configure Kafka

```bash
# Create topics
kafka-topics.sh --create --topic sensor-data --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
kafka-topics.sh --create --topic anomalies --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
```

#### Step 4: Start Services Manually

```bash
# Terminal 1: Start Dashboard API
cd apis
python dashboard_api.py

# Terminal 2: Start LLM Query Engine
python llm_influx_query_engine.py

# Terminal 3: Start Kafka Producer
cd ../producers
python kafka_producer_simulator.py

# Terminal 4: Start Kafka Consumer
cd ../consumers
python kafka_consumer_influx.py
```

1. **Start services manually**:

   ```bash
   # Start Dashboard API
   python DashboardAPI.py

   # Start LLM Query Engine
   python LLMInfluxQuerieEngine.py

   # Start Kafka producer
   python kafka-producer-simulator-50-sensors.py
   ```

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `INFLUX_URL` | InfluxDB server URL | `http://localhost:8086` |
| `INFLUX_TOKEN` | InfluxDB authentication token | Required |
| `INFLUX_ORG` | InfluxDB organization | `DenBosch` |
| `BUCKET` | InfluxDB bucket name | `sensors_db` |
| `HYPERBOLIC_API_KEY` | Hyperbolic API key | Required |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka broker addresses | `localhost:9092` |

### Sensor Configuration

The system monitors the following environmental metrics:

- **CO₂ (ppm)**: Carbon dioxide levels
- **NO₂ (ppb)**: Nitrogen dioxide concentration
- **PM2.5 (µg/m³)**: Particulate matter 2.5 microns
- **Noise (dB)**: Sound levels
- **Location Data**: Latitude/Longitude coordinates

## Usage

### Quick Start

1. **Start the system**:

   ```bash
   docker-compose -f config/docker-compose.yml up -d
   ```

2. **Run data ingestion**:

   ```bash
   docker exec -it digitaltwindenboschbackend_kafka-producer_1 python producers/kafka_producer_simulator.py
   ```

3. **Test API endpoints**:

   ```bash
   # Dashboard data
   curl http://localhost:5001/api/influx/dashboard

   # Natural language query
   curl -X POST http://localhost:5050/query \
     -H "Content-Type: application/json" \
     -d '{"query": "What is the current CO2 level?"}'
   ```

### Testing the System

#### Run Quick Tests

```bash
# Run the quick test script
cd tests
python quick_test.py
```

#### Manual Testing

```bash
# Test data ingestion
python producers/kafka_producer_simulator.py

# Check data in InfluxDB
python utils/metrics_reader.py

# Test WebSocket connection
python utils/socket_client_tester.py
```

#### API Testing

```bash
# Health checks
curl http://localhost:5001/health
curl http://localhost:5050/health

# Get dashboard data
curl http://localhost:5001/api/influx/dashboard

# Query with natural language
curl -X POST http://localhost:5050/query \
  -H "Content-Type: application/json" \
  -d '{"query": "Show noise levels for sensor S-I1"}'
```

### WebSocket Streaming

Connect to `ws://localhost:8080` for real-time sensor data updates.

## API Documentation

### Dashboard API (`/api/influx/dashboard`)

**Endpoint**: `GET /api/influx/dashboard`

**Description**: Returns aggregated sensor data for dashboard visualization.

**Response**:

```json
{
  "ok": true,
  "timestamp": "2024-01-26T10:00:00Z",
  "windowMinutes": 720,
  "latestSensors": [
    {
      "sensor_id": "S-001",
      "location": "construction",
      "metric": "co2_ppm",
      "metric_label": "CO₂",
      "value": 450.5,
      "unit": "ppm",
      "time": "2024-01-26T09:45:00Z"
    }
  ],
  "sensorData": {
    "labels": ["CO₂", "PM2.5", "NO₂", "Noise"],
    "values": [425.3, 15.2, 25.1, 65.4]
  },
  "populationGrowth": {...},
  "ageDemographics": {...},
  "housing": {...},
  "pollution": {...},
  "emissions": {...},
  "energyUsage": {...},
  "trafficDensity": {...}
}
```

### LLM Query API (`/query`)

**Endpoint**: `POST /query`

**Description**: Processes natural language queries about sensor data.

**Request**:

```json
{
  "query": "Show noise levels for sensor S-I1"
}
```

**Response**:

```json
{
  "response": "Current noise level at sensor S-I1 is 62.3 dB (measured at 2024-01-26T10:00:00Z)",
  "data": [
    {
      "sensor_id": "S-I1",
      "value": 62.3,
      "unit": "dB",
      "time": "2024-01-26T10:00:00Z"
    }
  ]
}
```

### Health Check (`/health`)

**Endpoint**: `GET /health`

**Description**: Service health status.

**Response**:

```json
{
  "status": "ok",
  "service": "SmartCityDashboardAPI",
  "timestamp": "2024-01-26T10:00:00Z"
}
```

## Development

### Project Structure

```
digitaltwindenboschbackend/
├── apis/                    # API endpoints
│   ├── dashboard_api.py
│   ├── llm_influx_query_engine.py
│   └── explainer.py
├── producers/               # Kafka producers
│   ├── kafka_producer_simulator.py
│   └── kafka_simulator_correlation.py
├── consumers/               # Kafka consumers
│   ├── kafka_consumer_influx.py
│   └── kafka_consumer_anomalies.py
├── detectors/               # Anomaly detection
│   ├── anomaly_detector_websocket.py
│   └── detector_evaluation.py
├── utils/                   # Utility scripts
│   ├── metrics_reader.py
│   ├── socket_client_tester.py
│   ├── websocket_server_emitter.py
│   ├── odin_metrics.py
│   └── odin_brain.py
├── config/                  # Configuration files
│   ├── docker-compose.yml
│   ├── docker-compose-new.yml
│   ├── telegraf.conf
│   └── telegraf_new.conf
├── data/                    # Data files
│   ├── *.csv
│   └── *.html
├── tests/                   # Test scripts
│   └── quick_test.py
├── docs/                    # Documentation
│   ├── README.md
│   └── LICENSE
├── scripts/                 # Miscellaneous scripts
│   ├── python_script.py
│   └── untitled_script.py
├── outdatedscripts/         # Legacy code
└── .env                     # Environment variables
```

### Running Tests

```bash
# Run the quick test script
python tests/quick_test.py

# Run evaluation scripts
python detectors/detector_evaluation.py
```

### Adding New Sensors

1. Update `apis/dashboard_api.py` Config.METRICS
2. Modify producer scripts to include new fields
3. Update InfluxDB measurement schema if needed

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Follow PEP 8 style guidelines
- Add docstrings to all functions
- Write tests for new features
- Update documentation for API changes
- Use meaningful commit messages

## Troubleshooting

### Common Issues and Solutions

#### Docker Issues

**Problem**: `docker-compose up` fails with permission errors

```bash
# Solution: Ensure Docker daemon is running and user has permissions
sudo systemctl start docker
sudo usermod -aG docker $USER
# Log out and back in, or run: newgrp docker
```

**Problem**: Port already in use

```bash
# Find what's using the port
lsof -i :5001
# Kill the process or change ports in docker-compose.yml
```

#### InfluxDB Issues

**Problem**: Cannot connect to InfluxDB

```bash
# Check if InfluxDB is running
curl http://localhost:8086/health

# Check InfluxDB logs
docker-compose -f config/docker-compose.yml logs influxdb

# Verify token and organization
influx auth list --org DenBosch
```

**Problem**: No data in InfluxDB

```bash
# Check if Kafka producer is running
docker-compose -f config/docker-compose.yml ps

# Check Kafka topics
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# Run manual data ingestion
python producers/kafka_producer_simulator.py
```

#### API Issues

**Problem**: API returns 500 errors

```bash
# Check API logs
docker-compose -f config/docker-compose.yml logs dashboard-api

# Verify environment variables
docker exec dashboard-api env | grep INFLUX

# Test InfluxDB connection manually
python -c "from influxdb_client import InfluxDBClient; client = InfluxDBClient(url='http://localhost:8086', token='your-token', org='DenBosch'); print('Connection OK' if client.ping() else 'Connection Failed')"
```

**Problem**: LLM queries not working

```bash
# Check API key
curl -X POST http://localhost:5050/query \
  -H "Content-Type: application/json" \
  -d '{"query": "test"}'

# Verify Hyperbolic API key is valid
# Check logs for API errors
docker-compose -f config/docker-compose.yml logs llm-query-engine
```

#### Kafka Issues

**Problem**: Kafka connection refused

```bash
# Check if Kafka is running
docker-compose -f config/docker-compose.yml ps

# Check Kafka logs
docker-compose -f config/docker-compose.yml logs kafka

# Test Kafka manually
docker exec kafka kafka-topics.sh --create --topic test --bootstrap-server localhost:9092
```

### Performance Tuning

- **InfluxDB**: Increase memory limits in docker-compose.yml for large datasets
- **Kafka**: Adjust partition count and replication factor for high throughput
- **API**: Use gunicorn for production deployment instead of Flask dev server

### Logs and Monitoring

```bash
# View all logs
docker-compose -f config/docker-compose.yml logs -f

# View specific service logs
docker-compose -f config/docker-compose.yml logs -f dashboard-api

# Check resource usage
docker stats

# Monitor InfluxDB performance
curl http://localhost:8086/metrics
```

### Data Validation

```bash
# Check data in InfluxDB
python utils/metrics_reader.py

# Validate API responses
curl http://localhost:5001/api/influx/dashboard | jq .

# Test with different queries
python tests/quick_test.py
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For support and questions:

- Create an issue in the repository
- Contact the development team
- Check the documentation for common solutions
- Review the troubleshooting section above

## Changelog

### Version 1.0.0

- Initial release with core functionality
- AI-powered natural language querying
- Real-time dashboard APIs
- Docker containerization
- Kafka data pipeline
- Anomaly detection system
