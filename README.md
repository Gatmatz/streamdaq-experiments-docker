# StreamDAQ Experiments Docker

Docker-based evaluation framework for Stream-DaQ performance testing. Provides containerized environment for running streaming data acquisition experiments with Kafka integration.

## Features

• **Dockerized Setup** - Complete containerized environment with Docker Compose
• **Kafka Integration** - Built-in Kafka streaming infrastructure for data flow
• **Multiple Experiment Types** - Support for basic/stable and more advanced evaluation scenarios
• **ATLANTIS Integration** - Specialized streaming experiments for ATLANTIS dummy data
• **Output Consumer** - Dedicated consumer service for experiment results
• **Custom Streams** - Configurable streaming data sources

## Installation

```bash
git clone https://github.com/Gatmatz/streamdaq-experiments-docker.git
cd streamdaq-experiments-docker
```

## Usage

Use the `.env` file to switch between the **stable** and **Atlantis** environments.  
You can also adjust other parameters in the `.env` file, such as StreamDAQ, Kafka, and Postgres configurations.

```bash
# Start the full experiment environment
./run.sh

# Or use Docker Compose directly
docker-compose up
```

---

## Kafka UI

To access the Kafka UI, connect to:  
**`kafka:29092`**

---

## PGAdmin

To view the experiment results, log in to the PGAdmin page using:  
- **Username:** `admin@admin.com`  
- **Password:** `admin`

To connect to the database, use the following credentials:  
- **Host:** `postgres-dq`  
- **User:** `dq_user`  
- **Password:** `dq_pass`

## License

MIT License
