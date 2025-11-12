# StreamDAQ Experiments Docker

Docker-based evaluation framework for Stream-DaQ stable and burst performance testing. Provides containerized environment for running streaming data acquisition experiments with Kafka integration.

## Features

• **Dockerized Setup** - Complete containerized environment with Docker Compose
• **Kafka Integration** - Built-in Kafka streaming infrastructure for data flow
• **Multiple Experiment Types** - Support for stable and burst evaluation scenarios
• **ATLANTIS Integration** - Specialized streaming experiments for ATLANTIS data
• **Output Consumer** - Dedicated consumer service for experiment results
• **Custom Streams** - Configurable streaming data sources

## Installation

```bash
git clone https://github.com/Gatmatz/streamdaq-experiments-docker.git
cd streamdaq-experiments-docker
```

## Usage

```bash
# Start the complete experiment environment
./run.sh

# Or use Docker Compose directly
docker-compose up
```

## License

MIT License