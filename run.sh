#!/bin/bash

run_experiment() {
  local description="$2"

  echo "Starting experiment: $description"

  # Ensure core services are running (Kafka, Kafka UI, and Postgres)
  docker compose up -d kafka kafka-ui postgres-dq pgadmin
  echo "Waiting for Kafka and Postgres to be ready..."
  sleep 10

  # Initialize Kafka topics if needed
  docker exec kafka bash /app/scripts/kafka-entrypoint.sh

  # Start the experiment-specific services (stream + daq or similar)
  docker compose up -d daq
  sleep 5
  docker compose up -d stream
}


# Experiment configurations
memory_limits=("8G")
cpu_limits=("4")
tumbling_windows=("1 minutes")

# Set window type
sed -i "s/^WINDOW_TYPE=.*/WINDOW_TYPE=tumbling/" .env

# Loop through parameter combinations
for cpus in "${cpu_limits[@]}"; do
  for memory in "${memory_limits[@]}"; do
    for tumbling_window in "${tumbling_windows[@]}"; do
      echo "Configuring .env for MEMORY_LIMIT=$memory and SPARK_NUM_CORES=$cpus"

      sed -i "s/^MEMORY_LIMIT=.*/MEMORY_LIMIT=$memory/" .env
      sed -i "s/^SPARK_NUM_CORES=.*/SPARK_NUM_CORES=$cpus/" .env
      sed -i "s/^WINDOW_DURATION=.*/WINDOW_DURATION=$tumbling_window/" .env

      description="Running: tumbling $tumbling_window with MEMORY_LIMIT=$memory and SPARK_NUM_CORES=$cpus"
      run_experiment "$description"
    done
  done
done
