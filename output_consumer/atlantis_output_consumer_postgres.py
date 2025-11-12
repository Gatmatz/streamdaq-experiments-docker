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
    # build angle column definitions programmatically
    angle_columns = []
    for i in range(500):
        if i in [314, 315, 316, 482, 483]:
            angle_columns.append(f"angle_{i} BOOLEAN")
        else:
            angle_columns.append(f"angle_{i} REAL")
    cols_sql = ",\n".join(angle_columns)

    create_sql = sql.SQL(f"""
    CREATE TABLE IF NOT EXISTS {{table}} (
        id SERIAL PRIMARY KEY,
        window_start BIGINT,
        window_end BIGINT,
        {cols_sql},
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
    throughput = message_value.get('angle_0') / latency  # events per second

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
    """Write data quality results to PostgreSQL database."""

    # Extract values from message_value dict
    values = [
        message_value.get('window_start'),
        message_value.get('window_end'),
        message_value.get('angle_0'),
        message_value.get('angle_1'),
        message_value.get('angle_2'),
        message_value.get('angle_3'),
        message_value.get('angle_4'),
        message_value.get('angle_5'),
        message_value.get('angle_6'),
        message_value.get('angle_7'),
        message_value.get('angle_8'),
        message_value.get('angle_9'),
        message_value.get('angle_10'),
        message_value.get('angle_11'),
        message_value.get('angle_12'),
        message_value.get('angle_13'),
        message_value.get('angle_14'),
        message_value.get('angle_15'),
        message_value.get('angle_16'),
        message_value.get('angle_17'),
        message_value.get('angle_18'),
        message_value.get('angle_19'),
        message_value.get('angle_20'),
        message_value.get('angle_21'),
        message_value.get('angle_22'),
        message_value.get('angle_23'),
        message_value.get('angle_24'),
        message_value.get('angle_25'),
        message_value.get('angle_26'),
        message_value.get('angle_27'),
        message_value.get('angle_28'),
        message_value.get('angle_29'),
        message_value.get('angle_30'),
        message_value.get('angle_31'),
        message_value.get('angle_32'),
        message_value.get('angle_33'),
        message_value.get('angle_34'),
        message_value.get('angle_35'),
        message_value.get('angle_36'),
        message_value.get('angle_37'),
        message_value.get('angle_38'),
        message_value.get('angle_39'),
        message_value.get('angle_40'),
        message_value.get('angle_41'),
        message_value.get('angle_42'),
        message_value.get('angle_43'),
        message_value.get('angle_44'),
        message_value.get('angle_45'),
        message_value.get('angle_46'),
        message_value.get('angle_47'),
        message_value.get('angle_48'),
        message_value.get('angle_49'),
        message_value.get('angle_50'),
        message_value.get('angle_51'),
        message_value.get('angle_52'),
        message_value.get('angle_53'),
        message_value.get('angle_54'),
        message_value.get('angle_55'),
        message_value.get('angle_56'),
        message_value.get('angle_57'),
        message_value.get('angle_58'),
        message_value.get('angle_59'),
        message_value.get('angle_60'),
        message_value.get('angle_61'),
        message_value.get('angle_62'),
        message_value.get('angle_63'),
        message_value.get('angle_64'),
        message_value.get('angle_65'),
        message_value.get('angle_66'),
        message_value.get('angle_67'),
        message_value.get('angle_68'),
        message_value.get('angle_69'),
        message_value.get('angle_70'),
        message_value.get('angle_71'),
        message_value.get('angle_72'),
        message_value.get('angle_73'),
        message_value.get('angle_74'),
        message_value.get('angle_75'),
        message_value.get('angle_76'),
        message_value.get('angle_77'),
        message_value.get('angle_78'),
        message_value.get('angle_79'),
        message_value.get('angle_80'),
        message_value.get('angle_81'),
        message_value.get('angle_82'),
        message_value.get('angle_83'),
        message_value.get('angle_84'),
        message_value.get('angle_85'),
        message_value.get('angle_86'),
        message_value.get('angle_87'),
        message_value.get('angle_88'),
        message_value.get('angle_89'),
        message_value.get('angle_90'),
        message_value.get('angle_91'),
        message_value.get('angle_92'),
        message_value.get('angle_93'),
        message_value.get('angle_94'),
        message_value.get('angle_95'),
        message_value.get('angle_96'),
        message_value.get('angle_97'),
        message_value.get('angle_98'),
        message_value.get('angle_99'),
        message_value.get('angle_100'),
        message_value.get('angle_101'),
        message_value.get('angle_102'),
        message_value.get('angle_103'),
        message_value.get('angle_104'),
        message_value.get('angle_105'),
        message_value.get('angle_106'),
        message_value.get('angle_107'),
        message_value.get('angle_108'),
        message_value.get('angle_109'),
        message_value.get('angle_110'),
        message_value.get('angle_111'),
        message_value.get('angle_112'),
        message_value.get('angle_113'),
        message_value.get('angle_114'),
        message_value.get('angle_115'),
        message_value.get('angle_116'),
        message_value.get('angle_117'),
        message_value.get('angle_118'),
        message_value.get('angle_119'),
        message_value.get('angle_120'),
        message_value.get('angle_121'),
        message_value.get('angle_122'),
        message_value.get('angle_123'),
        message_value.get('angle_124'),
        message_value.get('angle_125'),
        message_value.get('angle_126'),
        message_value.get('angle_127'),
        message_value.get('angle_128'),
        message_value.get('angle_129'),
        message_value.get('angle_130'),
        message_value.get('angle_131'),
        message_value.get('angle_132'),
        message_value.get('angle_133'),
        message_value.get('angle_134'),
        message_value.get('angle_135'),
        message_value.get('angle_136'),
        message_value.get('angle_137'),
        message_value.get('angle_138'),
        message_value.get('angle_139'),
        message_value.get('angle_140'),
        message_value.get('angle_141'),
        message_value.get('angle_142'),
        message_value.get('angle_143'),
        message_value.get('angle_144'),
        message_value.get('angle_145'),
        message_value.get('angle_146'),
        message_value.get('angle_147'),
        message_value.get('angle_148'),
        message_value.get('angle_149'),
        message_value.get('angle_150'),
        message_value.get('angle_151'),
        message_value.get('angle_152'),
        message_value.get('angle_153'),
        message_value.get('angle_154'),
        message_value.get('angle_155'),
        message_value.get('angle_156'),
        message_value.get('angle_157'),
        message_value.get('angle_158'),
        message_value.get('angle_159'),
        message_value.get('angle_160'),
        message_value.get('angle_161'),
        message_value.get('angle_162'),
        message_value.get('angle_163'),
        message_value.get('angle_164'),
        message_value.get('angle_165'),
        message_value.get('angle_166'),
        message_value.get('angle_167'),
        message_value.get('angle_168'),
        message_value.get('angle_169'),
        message_value.get('angle_170'),
        message_value.get('angle_171'),
        message_value.get('angle_172'),
        message_value.get('angle_173'),
        message_value.get('angle_174'),
        message_value.get('angle_175'),
        message_value.get('angle_176'),
        message_value.get('angle_177'),
        message_value.get('angle_178'),
        message_value.get('angle_179'),
        message_value.get('angle_180'),
        message_value.get('angle_181'),
        message_value.get('angle_182'),
        message_value.get('angle_183'),
        message_value.get('angle_184'),
        message_value.get('angle_185'),
        message_value.get('angle_186'),
        message_value.get('angle_187'),
        message_value.get('angle_188'),
        message_value.get('angle_189'),
        message_value.get('angle_190'),
        message_value.get('angle_191'),
        message_value.get('angle_192'),
        message_value.get('angle_193'),
        message_value.get('angle_194'),
        message_value.get('angle_195'),
        message_value.get('angle_196'),
        message_value.get('angle_197'),
        message_value.get('angle_198'),
        message_value.get('angle_199'),
        message_value.get('angle_200'),
        message_value.get('angle_201'),
        message_value.get('angle_202'),
        message_value.get('angle_203'),
        message_value.get('angle_204'),
        message_value.get('angle_205'),
        message_value.get('angle_206'),
        message_value.get('angle_207'),
        message_value.get('angle_208'),
        message_value.get('angle_209'),
        message_value.get('angle_210'),
        message_value.get('angle_211'),
        message_value.get('angle_212'),
        message_value.get('angle_213'),
        message_value.get('angle_214'),
        message_value.get('angle_215'),
        message_value.get('angle_216'),
        message_value.get('angle_217'),
        message_value.get('angle_218'),
        message_value.get('angle_219'),
        message_value.get('angle_220'),
        message_value.get('angle_221'),
        message_value.get('angle_222'),
        message_value.get('angle_223'),
        message_value.get('angle_224'),
        message_value.get('angle_225'),
        message_value.get('angle_226'),
        message_value.get('angle_227'),
        message_value.get('angle_228'),
        message_value.get('angle_229'),
        message_value.get('angle_230'),
        message_value.get('angle_231'),
        message_value.get('angle_232'),
        message_value.get('angle_233'),
        message_value.get('angle_234'),
        message_value.get('angle_235'),
        message_value.get('angle_236'),
        message_value.get('angle_237'),
        message_value.get('angle_238'),
        message_value.get('angle_239'),
        message_value.get('angle_240'),
        message_value.get('angle_241'),
        message_value.get('angle_242'),
        message_value.get('angle_243'),
        message_value.get('angle_244'),
        message_value.get('angle_245'),
        message_value.get('angle_246'),
        message_value.get('angle_247'),
        message_value.get('angle_248'),
        message_value.get('angle_249'),
        message_value.get('angle_250'),
        message_value.get('angle_251'),
        message_value.get('angle_252'),
        message_value.get('angle_253'),
        message_value.get('angle_254'),
        message_value.get('angle_255'),
        message_value.get('angle_256'),
        message_value.get('angle_257'),
        message_value.get('angle_258'),
        message_value.get('angle_259'),
        message_value.get('angle_260'),
        message_value.get('angle_261'),
        message_value.get('angle_262'),
        message_value.get('angle_263'),
        message_value.get('angle_264'),
        message_value.get('angle_265'),
        message_value.get('angle_266'),
        message_value.get('angle_267'),
        message_value.get('angle_268'),
        message_value.get('angle_269'),
        message_value.get('angle_270'),
        message_value.get('angle_271'),
        message_value.get('angle_272'),
        message_value.get('angle_273'),
        message_value.get('angle_274'),
        message_value.get('angle_275'),
        message_value.get('angle_276'),
        message_value.get('angle_277'),
        message_value.get('angle_278'),
        message_value.get('angle_279'),
        message_value.get('angle_280'),
        message_value.get('angle_281'),
        message_value.get('angle_282'),
        message_value.get('angle_283'),
        message_value.get('angle_284'),
        message_value.get('angle_285'),
        message_value.get('angle_286'),
        message_value.get('angle_287'),
        message_value.get('angle_288'),
        message_value.get('angle_289'),
        message_value.get('angle_290'),
        message_value.get('angle_291'),
        message_value.get('angle_292'),
        message_value.get('angle_293'),
        message_value.get('angle_294'),
        message_value.get('angle_295'),
        message_value.get('angle_296'),
        message_value.get('angle_297'),
        message_value.get('angle_298'),
        message_value.get('angle_299'),
        message_value.get('angle_300'),
        message_value.get('angle_301'),
        message_value.get('angle_302'),
        message_value.get('angle_303'),
        message_value.get('angle_304'),
        message_value.get('angle_305'),
        message_value.get('angle_306'),
        message_value.get('angle_307'),
        message_value.get('angle_308'),
        message_value.get('angle_309'),
        message_value.get('angle_310'),
        message_value.get('angle_311'),
        message_value.get('angle_312'),
        message_value.get('angle_313'),
        message_value.get('angle_314'),
        message_value.get('angle_315'),
        message_value.get('angle_316'),
        message_value.get('angle_317'),
        message_value.get('angle_318'),
        message_value.get('angle_319'),
        message_value.get('angle_320'),
        message_value.get('angle_321'),
        message_value.get('angle_322'),
        message_value.get('angle_323'),
        message_value.get('angle_324'),
        message_value.get('angle_325'),
        message_value.get('angle_326'),
        message_value.get('angle_327'),
        message_value.get('angle_328'),
        message_value.get('angle_329'),
        message_value.get('angle_330'),
        message_value.get('angle_331'),
        message_value.get('angle_332'),
        message_value.get('angle_333'),
        message_value.get('angle_334'),
        message_value.get('angle_335'),
        message_value.get('angle_336'),
        message_value.get('angle_337'),
        message_value.get('angle_338'),
        message_value.get('angle_339'),
        message_value.get('angle_340'),
        message_value.get('angle_341'),
        message_value.get('angle_342'),
        message_value.get('angle_343'),
        message_value.get('angle_344'),
        message_value.get('angle_345'),
        message_value.get('angle_346'),
        message_value.get('angle_347'),
        message_value.get('angle_348'),
        message_value.get('angle_349'),
        message_value.get('angle_350'),
        message_value.get('angle_351'),
        message_value.get('angle_352'),
        message_value.get('angle_353'),
        message_value.get('angle_354'),
        message_value.get('angle_355'),
        message_value.get('angle_356'),
        message_value.get('angle_357'),
        message_value.get('angle_358'),
        message_value.get('angle_359'),
        message_value.get('angle_360'),
        message_value.get('angle_361'),
        message_value.get('angle_362'),
        message_value.get('angle_363'),
        message_value.get('angle_364'),
        message_value.get('angle_365'),
        message_value.get('angle_366'),
        message_value.get('angle_367'),
        message_value.get('angle_368'),
        message_value.get('angle_369'),
        message_value.get('angle_370'),
        message_value.get('angle_371'),
        message_value.get('angle_372'),
        message_value.get('angle_373'),
        message_value.get('angle_374'),
        message_value.get('angle_375'),
        message_value.get('angle_376'),
        message_value.get('angle_377'),
        message_value.get('angle_378'),
        message_value.get('angle_379'),
        message_value.get('angle_380'),
        message_value.get('angle_381'),
        message_value.get('angle_382'),
        message_value.get('angle_383'),
        message_value.get('angle_384'),
        message_value.get('angle_385'),
        message_value.get('angle_386'),
        message_value.get('angle_387'),
        message_value.get('angle_388'),
        message_value.get('angle_389'),
        message_value.get('angle_390'),
        message_value.get('angle_391'),
        message_value.get('angle_392'),
        message_value.get('angle_393'),
        message_value.get('angle_394'),
        message_value.get('angle_395'),
        message_value.get('angle_396'),
        message_value.get('angle_397'),
        message_value.get('angle_398'),
        message_value.get('angle_399'),
        message_value.get('angle_400'),
        message_value.get('angle_401'),
        message_value.get('angle_402'),
        message_value.get('angle_403'),
        message_value.get('angle_404'),
        message_value.get('angle_405'),
        message_value.get('angle_406'),
        message_value.get('angle_407'),
        message_value.get('angle_408'),
        message_value.get('angle_409'),
        message_value.get('angle_410'),
        message_value.get('angle_411'),
        message_value.get('angle_412'),
        message_value.get('angle_413'),
        message_value.get('angle_414'),
        message_value.get('angle_415'),
        message_value.get('angle_416'),
        message_value.get('angle_417'),
        message_value.get('angle_418'),
        message_value.get('angle_419'),
        message_value.get('angle_420'),
        message_value.get('angle_421'),
        message_value.get('angle_422'),
        message_value.get('angle_423'),
        message_value.get('angle_424'),
        message_value.get('angle_425'),
        message_value.get('angle_426'),
        message_value.get('angle_427'),
        message_value.get('angle_428'),
        message_value.get('angle_429'),
        message_value.get('angle_430'),
        message_value.get('angle_431'),
        message_value.get('angle_432'),
        message_value.get('angle_433'),
        message_value.get('angle_434'),
        message_value.get('angle_435'),
        message_value.get('angle_436'),
        message_value.get('angle_437'),
        message_value.get('angle_438'),
        message_value.get('angle_439'),
        message_value.get('angle_440'),
        message_value.get('angle_441'),
        message_value.get('angle_442'),
        message_value.get('angle_443'),
        message_value.get('angle_444'),
        message_value.get('angle_445'),
        message_value.get('angle_446'),
        message_value.get('angle_447'),
        message_value.get('angle_448'),
        message_value.get('angle_449'),
        message_value.get('angle_450'),
        message_value.get('angle_451'),
        message_value.get('angle_452'),
        message_value.get('angle_453'),
        message_value.get('angle_454'),
        message_value.get('angle_455'),
        message_value.get('angle_456'),
        message_value.get('angle_457'),
        message_value.get('angle_458'),
        message_value.get('angle_459'),
        message_value.get('angle_460'),
        message_value.get('angle_461'),
        message_value.get('angle_462'),
        message_value.get('angle_463'),
        message_value.get('angle_464'),
        message_value.get('angle_465'),
        message_value.get('angle_466'),
        message_value.get('angle_467'),
        message_value.get('angle_468'),
        message_value.get('angle_469'),
        message_value.get('angle_470'),
        message_value.get('angle_471'),
        message_value.get('angle_472'),
        message_value.get('angle_473'),
        message_value.get('angle_474'),
        message_value.get('angle_475'),
        message_value.get('angle_476'),
        message_value.get('angle_477'),
        message_value.get('angle_478'),
        message_value.get('angle_479'),
        message_value.get('angle_480'),
        message_value.get('angle_481'),
        message_value.get('angle_482'),
        message_value.get('angle_483'),
        message_value.get('angle_484'),
        message_value.get('angle_485'),
        message_value.get('angle_486'),
        message_value.get('angle_487'),
        message_value.get('angle_488'),
        message_value.get('angle_489'),
        message_value.get('angle_490'),
        message_value.get('angle_491'),
        message_value.get('angle_492'),
        message_value.get('angle_493'),
        message_value.get('angle_494'),
        message_value.get('angle_495'),
        message_value.get('angle_496'),
        message_value.get('angle_497'),
        message_value.get('angle_498'),
        message_value.get('angle_499'),
        message_value.get('diff'),
        message_value.get('time'),
        message_value.get('kafka_time')
    ]

    # Create column names
    angle_columns = [f"angle_{i}" for i in range(500)]
    all_columns = ["window_start", "window_end"] + angle_columns + ["diff", "time", "kafka_time"]

    # Create placeholders for all values
    placeholders = ", ".join(["%s"] * len(values))

    # Create INSERT query
    insert_query = sql.SQL("""
    INSERT INTO {table} ({columns})
    VALUES ({placeholders})
    """).format(
        table=sql.Identifier(STREAMDAQ_TABLE),
        columns=sql.SQL(", ").join(map(sql.Identifier, all_columns)),
        placeholders=sql.SQL(placeholders)
    )

    # Execute the query
    with conn.cursor() as cursor:
        cursor.execute(insert_query, values)
    conn.commit()


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
        i = 0
        for message in consumer:
            message_value = message.value  # Dictionary representing the message

            try:
                message_key = message.key.decode('utf-8') if message.key else "unknown"
            except Exception as e:
                message_key = "unknown"

            # Add the broker timestamp field to the dict
            message_value['kafka_time'] = message.timestamp # when kafka broker received the message; in ms

            # Write window results to DQ table
            if i % 10 == 0:
                write_results_to_dq(message_value, conn)

            # Compute performance metrics and write to performance table
            write_results_to_performance(message_value, conn)

            i += 1

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