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

def create_performance_table_if_not_exists(conn):
    create_sql = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {table} (
        id SERIAL PRIMARY KEY,
        window_start BIGINT,
        window_end BIGINT,
        window_setting TEXT,
        number_of_cores TEXT,
        number_of_threads TEXT,
        latency REAL,
        throughput REAL,
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
        kafka_time BIGINT,
        created_at TIMESTAMPTZ DEFAULT now()
    );
    """).format(table=sql.Identifier(STREAMDAQ_TABLE))
    with conn.cursor() as cur:
        cur.execute(create_sql)
    conn.commit()

def write_results_to_performance(message_value: dict, conn) -> None:
    """
    Inserts one row per tool into Postgres for the provided blast_info.
    """
    window_start = message_value.get('window_start', -1) # in seconds
    window_end = message_value.get('window_end', -1)    # in seconds
    window_setting = get_window_short_name(WINDOW_TYPE, WINDOW_DURATION_STR, SLIDE_DURATION_STR, GAP_DURATION_STR)
    number_of_cores = NUM_CORES
    number_of_threads = NUM_THREADS
    latency = (message_value.get('kafka_time') - message_value.get('time')) / 1000  # in seconds
    # Throughput is calculated as number of events divided by latency
    throughput = message_value.get('has_events') / latency  # in seconds

    insert_sql = sql.SQL("""
    INSERT INTO {table} (
        window_start, window_end, window_setting, number_of_cores, number_of_threads,
        latency, throughput
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s
    )
    """).format(table=sql.Identifier(PERFORMANCE_TABLE_NAME))

    with conn.cursor() as cur:
        params = (
            window_start,
            window_end,
            window_setting,
            number_of_cores,
            number_of_threads,
            latency,
            throughput
        )
        cur.execute(insert_sql, params)
    conn.commit()
    print(f"Inserted performance rows for window={window_start}-{window_end} into table {PERFORMANCE_TABLE_NAME}")


def write_results_to_dq(message_value: dict, conn) -> None:
    """
    Inserts StreamDAQ data quality results into the streamdaq_output table.
    """
    insert_sql = sql.SQL("""
    INSERT INTO {table} (
        window_start, window_end, has_events, event_variety, 
        reasonable_amounts, avg_amount_range, 
        diff, time, kafka_time
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
            message_value.get('kafka_time')
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

    try:
        for message in consumer:
            message_value = message.value  # Dictionary representing the message

            try:
                message_key = message.key.decode('utf-8') if message.key else "unknown"
            except Exception as e:
                message_key = "unknown"

            # Add the broker timestamp field to the dict
            message_value['kafka_time'] = message.timestamp # when kafka broker received the message; in ms

            # Write window results to DQ table
            write_results_to_dq(message_value, conn)

            # Compute performance metrics and write to performance table
            write_results_to_performance(message_value, conn)

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