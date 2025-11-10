import os
import json
from kafka import KafkaConsumer
import psycopg2
from psycopg2 import sql

# Get environment variables
OUTPUT_TOPIC = os.getenv('OUTPUT_TOPIC', 'output_topic')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
WINDOW_DURATION_STR = os.getenv('WINDOW_DURATION', '10 seconds')
SLIDE_DURATION_STR = os.getenv('SLIDE_DURATION', '10 seconds')
GAP_DURATION_STR = os.getenv('GAP_DURATION', '5 seconds')
WINDOW_TYPE = os.getenv('WINDOW_TYPE', 'tumbling').lower()
NUM_CORES = os.getenv('SPARK_NUM_CORES', '20')
NUM_THREADS = os.getenv('NUM_THREADS', '1')
EXPERIMENT_TYPE = os.getenv('EXPERIMENT_TYPE', 'kafka')
MEMORY_LIMIT = os.getenv('MEMORY_LIMIT', '8G')

# Postgres environment variables
PG_HOST = os.getenv('POSTGRES_HOST', 'postgres')
PG_PORT = int(os.getenv('POSTGRES_PORT', '5432'))
PG_DB = os.getenv('POSTGRES_DB', 'dq_db')
PG_USER = os.getenv('POSTGRES_USER', 'dq_user')
PG_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'dq_pass')
PERFORMANCE_TABLE_NAME = os.getenv('PERFORMANCE_TABLE', 'final_performance')
STREAMDAQ_TABLE = os.getenv('STREAMDAQ_TABLE', 'streamdaq_output')

# Kafka keys for different message types
STREAM_DAQ_KEY = os.getenv('STREAM_DAQ_KEY', 'stream-daq')
BLAST_KEY: str = os.getenv('BLAST_KEY', 'blast')

def get_window_short_name(window_type: str, window_duration: str, slide_duration: str | None,
                         gap_duration: str | None) -> str:
    window_type = str(window_type).lower()
    match window_type:
        case "tumbling":
            return f"{window_type}_{window_duration}"
        case "sliding":
            return f"{window_type}_{window_duration}_{slide_duration}"
        case "session":
            return f"{window_type}_{window_duration}_{gap_duration}"

print(f"Starting output consumer. Listening to topic: {OUTPUT_TOPIC}")
print(f"Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")

def initialize_or_reset_performance_state() -> dict:
    """
    Returns a dictionary holding performance metrics for each tool (stream-daq, deequ).
    We track:
      - number_of_windows (int)
      - max_timestamp (int -> the largest broker timestamp we've seen for that tool)
    """
    return {
        STREAM_DAQ_KEY: {
            'number_of_windows': 0,
            'max_timestamp': -1,
            'processing_time': 0
        }
    }

def update_performance_metrics_state(performance_state: dict, new_result: dict, tool_key: str) -> None:
    """
    Updates the in-memory state (performance_state) based on a new message from 'stream-daq' or 'deequ'.
    We do two things:
      1) Increment number_of_windows by one.
      2) Update max_timestamp if the new message's 'timestamp' is greater than the current stored one.
    """
    print(f"Just received new message: {new_result} for tool: {tool_key}")
    print()
    if tool_key not in performance_state:
        # Unknown tool -> ignore
        print(f"Warning: Received unknown tool key = {tool_key}, ignoring.")
        return

    performance_state[tool_key]['number_of_windows'] += 1
    new_timestamp = new_result.get('timestamp', -1)
    if new_timestamp > performance_state[tool_key]['max_timestamp']:
        performance_state[tool_key]['max_timestamp'] = new_timestamp

    new_processing_time = new_result.get('checkingTimeMs', new_result['timestamp'] - new_result['time'])
    performance_state[tool_key]['processing_time'] += new_processing_time

def create_performance_table_if_not_exists(conn):
    create_sql = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {table} (
        id SERIAL PRIMARY KEY,
        tool TEXT NOT NULL,
        blast_id BIGINT,
        blast_size BIGINT,
        blast_start_time BIGINT,
        blast_end_time BIGINT,
        number_of_windows INTEGER,
        max_timestamp BIGINT,
        processing_time BIGINT,
        window_setting TEXT,
        number_of_cores TEXT,
        number_of_threads TEXT,
        memory_limit TEXT,
        experiment_type TEXT,
        created_at TIMESTAMPTZ DEFAULT now()
    );
    """).format(table=sql.Identifier(PERFORMANCE_TABLE_NAME))
    with conn.cursor() as cur:
        cur.execute(create_sql)
    conn.commit()

def create_dq_table_if_not_exists(conn):
    create_sql = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {table} (
        id SERIAL PRIMARY KEY,
        window_start BIGINT,
        window_end BIGINT,
        has_events BIGINT,
        event_variety REAL,
        reasonable_amounts INTEGER,
        avg_amount_range REAL,
        diff INTEGER,
        time BIGINT,
        timestamp BIGINT,
        created_at TIMESTAMPTZ DEFAULT now()
    );
    """).format(table=sql.Identifier(STREAMDAQ_TABLE))
    with conn.cursor() as cur:
        cur.execute(create_sql)
    conn.commit()

def write_results_to_performance(state: dict, blast_info: dict, conn) -> None:
    """
    Inserts one row per tool into Postgres for the provided blast_info.
    """
    blast_id = blast_info.get("blast_id", -1)
    blast_size = blast_info.get("blast_size", -1)
    blast_start = blast_info.get("blast_start_time", -1)
    blast_end = blast_info.get("blast_end_time", -1)
    window_setting = get_window_short_name(WINDOW_TYPE, WINDOW_DURATION_STR, SLIDE_DURATION_STR, GAP_DURATION_STR)
    number_of_cores = NUM_CORES
    number_of_threads = NUM_THREADS

    insert_sql = sql.SQL("""
    INSERT INTO {table} (
        tool, blast_id, blast_size, blast_start_time, blast_end_time,
        number_of_windows, max_timestamp, processing_time,
        window_setting, number_of_cores, number_of_threads,
        memory_limit, experiment_type
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """).format(table=sql.Identifier(PERFORMANCE_TABLE_NAME))

    with conn.cursor() as cur:
        for tool_key, tool_state in state.items():
            if tool_state.get("max_timestamp", -1) > 0:
                params = (
                    tool_key,
                    blast_id,
                    blast_size,
                    blast_start,
                    blast_end,
                    tool_state.get('number_of_windows', 0),
                    tool_state.get('max_timestamp', -1),
                    tool_state.get('processing_time', 0),
                    window_setting,
                    number_of_cores,
                    number_of_threads,
                    MEMORY_LIMIT,
                    EXPERIMENT_TYPE
                )
                cur.execute(insert_sql, params)
    conn.commit()
    print(f"Inserted final performance rows for blast_id={blast_id} into table {PERFORMANCE_TABLE_NAME}")


def write_results_to_dq(message_value: dict, conn) -> None:
    """
    Inserts StreamDAQ data quality results into the streamdaq_output table.
    """
    insert_sql = sql.SQL("""
    INSERT INTO {table} (
        window_start, window_end, has_events, event_variety, 
        reasonable_amounts, avg_amount_range, 
        diff, time, timestamp
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """).format(table=sql.Identifier(STREAMDAQ_TABLE))

    with conn.cursor() as cur:
        params = (
            message_value.get('window_start'),
            message_value.get('window_end'),
            message_value.get('has_events'),
            message_value.get('event_variety'),
            message_value.get('reasonable_amounts'),
            message_value.get('avg_amount_range'),
            message_value.get('diff'),
            message_value.get('time'),
            message_value.get('timestamp')
        )
        cur.execute(insert_sql, params)

    conn.commit()
    print(f"Inserted StreamDAQ result into table {STREAMDAQ_TABLE}")


def main():
    # Create Kafka consumer
    consumer = KafkaConsumer(
        OUTPUT_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    # Connect to Postgres
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )
    create_performance_table_if_not_exists(conn)
    create_dq_table_if_not_exists(conn)

    # Initialize in-memory performance state
    state = initialize_or_reset_performance_state()

    try:
        for message in consumer:
            message_value = message.value  # Dictionary representing the message

            try:
                message_key = message.key.decode('utf-8') if message.key else "unknown"
            except Exception as e:
                message_key = "unknown"

            print(message_key)
            # Add the broker timestamp field to the dict
            message_value['timestamp'] = message.timestamp # when kafka broker received the message; in ms

            # todo: Fix performance evaluation
            if message_key == BLAST_KEY:
                # We finalize the metrics for the previous blast
                # by writing them to final CSV, then reset state
                write_results_to_performance(state, message_value, conn)
                state = initialize_or_reset_performance_state()
            else:
                write_results_to_dq(message_value, conn)
                update_performance_metrics_state(state, message_value, message_key)

    except KeyboardInterrupt:
        print("Stopping output consumer…")
    finally:
        print("Reached the finally block. Closing consumer and DB connection...")
        try:
            consumer.close()
        except Exception:
            pass

        try:
            conn.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()