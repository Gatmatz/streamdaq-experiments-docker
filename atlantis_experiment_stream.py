from streamdaq import StreamDaQ, DaQMeasures as dqm, Windows
from Windows import tumbling, sliding, session

import os, time
import pathway as pw

PATHWAY_LICENSE_KEY = os.getenv('PATHWAY_LICENSE_KEY', 'You can get yours for free at https://pathway.com/get-license/')
pw.set_license_key(PATHWAY_LICENSE_KEY)

# Get configuration from environment variables
INPUT_KAFKA_TOPIC = os.getenv('INPUT_TOPIC', 'data_input')
OUTPUT_KAFKA_TOPIC = os.getenv('OUTPUT_TOPIC', 'data_output')
READ_FROM_KAFKA_EVERY_MS = os.getenv('READ_FROM_KAFKA_EVERY_MS', '1000')
KAFKA_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
WINDOW_DURATION_STR = os.getenv('WINDOW_DURATION', '10 seconds')
SLIDE_DURATION_STR = os.getenv('SLIDE_DURATION', '5 seconds')
GAP_DURATION_STR = os.getenv('GAP_DURATION', '5 seconds')
MESSAGES_PER_WINDOW_LIST = os.getenv('MESSAGES_PER_WINDOW', '1000')
WINDOW_TYPE = os.getenv('WINDOW_TYPE', 'tumbling').lower()

print(f"INPUT_KAFKA_TOPIC: {INPUT_KAFKA_TOPIC}")
print(f"OUTPUT_KAFKA_TOPIC: {OUTPUT_KAFKA_TOPIC}")
print(f"READ_FROM_KAFKA_EVERY_MS: {READ_FROM_KAFKA_EVERY_MS}")
print(f"KAFKA_SERVER: {KAFKA_SERVER}")
print(f"WINDOW_DURATION_STR: {WINDOW_DURATION_STR}")
print(f"MESSAGES_PER_WINDOW_LIST: {MESSAGES_PER_WINDOW_LIST}")
print(f"WINDOW_TYPE: {WINDOW_TYPE}")
print(f"GAP_DURATION_STR: {GAP_DURATION_STR}")


def standardize_timestamp_to_milliseconds(_) -> str:
    return str(int(time.time() * 1e3))


def parse_duration(duration_str):
    # Parses '10 seconds' into 10.0
    units = {'s': 1, 'seconds': 1, 'sec': 1, 'secs': 1, 'second': 1,
             'm': 60, 'minutes': 60, 'min': 60, 'mins': 60, 'minute': 60}
    parts = duration_str.strip().split()
    if len(parts) != 2:
        raise ValueError(f"Invalid duration format: {duration_str}")
    value = float(parts[0])
    unit = parts[1].lower()
    if unit not in units:
        raise ValueError(f"Unknown unit in duration: {unit}")
    return value * units[unit]


def get_window_from_string(window_type_string: str):
    window_str = window_type_string.lower()
    match window_str:
        case 'tumbling':
            return tumbling(duration=int(parse_duration(WINDOW_DURATION_STR)),
                            origin=0)   # 'created_utc' is in seconds, so no need for *1000
        case 'sliding':
            return sliding(
                duration=int(parse_duration(WINDOW_DURATION_STR)),
                hop=int(parse_duration(SLIDE_DURATION_STR)),
                origin=0
            )
        case 'session':
            return session(max_gap=parse_duration(GAP_DURATION_STR))
        case _:
            print(f"Unknown window type: {window_str}. Falling back to tumbling.")
            return tumbling(duration=parse_duration(WINDOW_DURATION_STR))


class AtlantisDataSchema(pw.Schema):
    timestamp: int
    angle_0: float
    angle_1: float
    angle_2: float
    angle_3: float
    angle_4: float
    angle_5: float
    angle_6: float
    angle_7: float
    angle_8: float
    angle_9: float
    angle_10: float
    angle_11: float
    angle_12: float
    angle_13: float
    angle_14: float
    angle_15: float
    angle_16: float
    angle_17: float
    angle_18: float
    angle_19: float
    angle_20: float
    angle_21: float
    angle_22: float
    angle_23: float
    angle_24: float
    angle_25: float
    angle_26: float
    angle_27: float
    angle_28: float
    angle_29: float
    angle_30: float
    angle_31: float
    angle_32: float
    angle_33: float
    angle_34: float
    angle_35: float
    angle_36: float
    angle_37: float
    angle_38: float
    angle_39: float
    angle_40: float
    angle_41: float
    angle_42: float
    angle_43: float
    angle_44: float
    angle_45: float
    angle_46: float
    angle_47: float
    angle_48: float
    angle_49: float
    angle_50: float
    angle_51: float
    angle_52: float
    angle_53: float
    angle_54: float
    angle_55: float
    angle_56: float
    angle_57: float
    angle_58: float
    angle_59: float
    angle_60: float
    angle_61: float
    angle_62: float
    angle_63: float
    angle_64: float
    angle_65: float
    angle_66: float
    angle_67: float
    angle_68: float
    angle_69: float
    angle_70: float
    angle_71: float
    angle_72: float
    angle_73: float
    angle_74: float
    angle_75: float
    angle_76: float
    angle_77: float
    angle_78: float
    angle_79: float
    angle_80: float
    angle_81: float
    angle_82: float
    angle_83: float
    angle_84: float
    angle_85: float
    angle_86: float
    angle_87: float
    angle_88: float
    angle_89: float
    angle_90: float
    angle_91: float
    angle_92: float
    angle_93: float
    angle_94: float
    angle_95: float
    angle_96: float
    angle_97: float
    angle_98: float
    angle_99: float
    angle_100: float
    angle_101: float
    angle_102: float
    angle_103: float
    angle_104: float
    angle_105: float
    angle_106: float
    angle_107: float
    angle_108: float
    angle_109: float
    angle_110: float
    angle_111: float
    angle_112: float
    angle_113: float
    angle_114: float
    angle_115: float
    angle_116: float
    angle_117: float
    angle_118: float
    angle_119: float
    angle_120: float
    angle_121: float
    angle_122: float
    angle_123: float
    angle_124: float
    angle_125: float
    angle_126: float
    angle_127: float
    angle_128: float
    angle_129: float
    angle_130: float
    angle_131: float
    angle_132: float
    angle_133: float
    angle_134: float
    angle_135: float
    angle_136: float
    angle_137: float
    angle_138: float
    angle_139: float
    angle_140: float
    angle_141: float
    angle_142: float
    angle_143: float
    angle_144: float
    angle_145: float
    angle_146: float
    angle_147: float
    angle_148: float
    angle_149: float
    angle_150: float
    angle_151: float
    angle_152: float
    angle_153: float
    angle_154: float
    angle_155: float
    angle_156: float
    angle_157: float
    angle_158: float
    angle_159: float
    angle_160: float
    angle_161: float
    angle_162: float
    angle_163: float
    angle_164: float
    angle_165: float
    angle_166: float
    angle_167: float
    angle_168: float
    angle_169: float
    angle_170: float
    angle_171: float
    angle_172: float
    angle_173: float
    angle_174: float
    angle_175: float
    angle_176: float
    angle_177: float
    angle_178: float
    angle_179: float
    angle_180: float
    angle_181: float
    angle_182: float
    angle_183: float
    angle_184: float
    angle_185: float
    angle_186: float
    angle_187: float
    angle_188: float
    angle_189: float
    angle_190: float
    angle_191: float
    angle_192: float
    angle_193: float
    angle_194: float
    angle_195: float
    angle_196: float
    angle_197: float
    angle_198: float
    angle_199: float
    angle_200: float
    angle_201: float
    angle_202: float
    angle_203: float
    angle_204: float
    angle_205: float
    angle_206: float
    angle_207: float
    angle_208: float
    angle_209: float
    angle_210: float
    angle_211: float
    angle_212: float
    angle_213: float
    angle_214: float
    angle_215: float
    angle_216: float
    angle_217: float
    angle_218: float
    angle_219: float
    angle_220: float
    angle_221: float
    angle_222: float
    angle_223: float
    angle_224: float
    angle_225: float
    angle_226: float
    angle_227: float
    angle_228: float
    angle_229: float
    angle_230: float
    angle_231: float
    angle_232: float
    angle_233: float
    angle_234: float
    angle_235: float
    angle_236: float
    angle_237: float
    angle_238: float
    angle_239: float
    angle_240: float
    angle_241: float
    angle_242: float
    angle_243: float
    angle_244: float
    angle_245: float
    angle_246: float
    angle_247: float
    angle_248: float
    angle_249: float
    angle_250: float
    angle_251: float
    angle_252: float
    angle_253: float
    angle_254: float
    angle_255: float
    angle_256: float
    angle_257: float
    angle_258: float
    angle_259: float
    angle_260: float
    angle_261: float
    angle_262: float
    angle_263: float
    angle_264: float
    angle_265: float
    angle_266: float
    angle_267: float
    angle_268: float
    angle_269: float
    angle_270: float
    angle_271: float
    angle_272: float
    angle_273: float
    angle_274: float
    angle_275: float
    angle_276: float
    angle_277: float
    angle_278: float
    angle_279: float
    angle_280: float
    angle_281: float
    angle_282: float
    angle_283: float
    angle_284: float
    angle_285: float
    angle_286: float
    angle_287: float
    angle_288: float
    angle_289: float
    angle_290: float
    angle_291: float
    angle_292: float
    angle_293: float
    angle_294: float
    angle_295: float
    angle_296: float
    angle_297: float
    angle_298: float
    angle_299: float
    angle_300: float
    angle_301: float
    angle_302: float
    angle_303: float
    angle_304: float
    angle_305: float
    angle_306: float
    angle_307: float
    angle_308: float
    angle_309: float
    angle_310: float
    angle_311: float
    angle_312: float
    angle_313: float
    angle_314: float
    angle_315: float
    angle_316: float
    angle_317: float
    angle_318: float
    angle_319: float
    angle_320: float
    angle_321: float
    angle_322: float
    angle_323: float
    angle_324: float
    angle_325: float
    angle_326: float
    angle_327: float
    angle_328: float
    angle_329: float
    angle_330: float
    angle_331: float
    angle_332: float
    angle_333: float
    angle_334: float
    angle_335: float
    angle_336: float
    angle_337: float
    angle_338: float
    angle_339: float
    angle_340: float
    angle_341: float
    angle_342: float
    angle_343: float
    angle_344: float
    angle_345: float
    angle_346: float
    angle_347: float
    angle_348: float
    angle_349: float
    angle_350: float
    angle_351: float
    angle_352: float
    angle_353: float
    angle_354: float
    angle_355: float
    angle_356: float
    angle_357: float
    angle_358: float
    angle_359: float
    angle_360: float
    angle_361: float
    angle_362: float
    angle_363: float
    angle_364: float
    angle_365: float
    angle_366: float
    angle_367: float
    angle_368: float
    angle_369: float
    angle_370: float
    angle_371: float
    angle_372: float
    angle_373: float
    angle_374: float
    angle_375: float
    angle_376: float
    angle_377: float
    angle_378: float
    angle_379: float
    angle_380: float
    angle_381: float
    angle_382: float
    angle_383: float
    angle_384: float
    angle_385: float
    angle_386: float
    angle_387: float
    angle_388: float
    angle_389: float
    angle_390: float
    angle_391: float
    angle_392: float
    angle_393: float
    angle_394: float
    angle_395: float
    angle_396: float
    angle_397: float
    angle_398: float
    angle_399: float
    angle_400: float
    angle_401: float
    angle_402: float
    angle_403: float
    angle_404: float
    angle_405: float
    angle_406: float
    angle_407: float
    angle_408: float
    angle_409: float
    angle_410: float
    angle_411: float
    angle_412: float
    angle_413: float
    angle_414: float
    angle_415: float
    angle_416: float
    angle_417: float
    angle_418: float
    angle_419: float
    angle_420: float
    angle_421: float
    angle_422: float
    angle_423: float
    angle_424: float
    angle_425: float
    angle_426: float
    angle_427: float
    angle_428: float
    angle_429: float
    angle_430: float
    angle_431: float
    angle_432: float
    angle_433: float
    angle_434: float
    angle_435: float
    angle_436: float
    angle_437: float
    angle_438: float
    angle_439: float
    angle_440: float
    angle_441: float
    angle_442: float
    angle_443: float
    angle_444: float
    angle_445: float
    angle_446: float
    angle_447: float
    angle_448: float
    angle_449: float
    angle_450: float
    angle_451: float
    angle_452: float
    angle_453: float
    angle_454: float
    angle_455: float
    angle_456: float
    angle_457: float
    angle_458: float
    angle_459: float
    angle_460: float
    angle_461: float
    angle_462: float
    angle_463: float
    angle_464: float
    angle_465: float
    angle_466: float
    angle_467: float
    angle_468: float
    angle_469: float
    angle_470: float
    angle_471: float
    angle_472: float
    angle_473: float
    angle_474: float
    angle_475: float
    angle_476: float
    angle_477: float
    angle_478: float
    angle_479: float
    angle_480: float
    angle_481: float
    angle_482: float
    angle_483: float
    angle_484: float
    angle_485: float
    angle_486: float
    angle_487: float
    angle_488: float
    angle_489: float
    angle_490: float
    angle_491: float
    angle_492: float
    angle_493: float
    angle_494: float
    angle_495: float
    angle_496: float
    angle_497: float
    angle_498: float
    angle_499: float

rdkafka_settings = {
    "bootstrap.servers": KAFKA_SERVER,
    "security.protocol": "plaintext",
    "group.id": "0",
    "session.timeout.ms": "6000",
    "auto.offset.reset": "earliest",
}

postgres_settings = {
    "host": "postgres-dq",
    "port": "5432",
    "dbname": "dq_db",
    "user": "dq_user",
    "password": "dq_pass",
}


data = (pw.io.kafka.read(
    rdkafka_settings,
    topic=INPUT_KAFKA_TOPIC,
    format="json",
    schema=AtlantisDataSchema,
    autocommit_duration_ms=int(READ_FROM_KAFKA_EVERY_MS),
))

def write_to_kafka(data: pw.internals.Table) -> None:
    pw.io.kafka.write(
        table=data,
        rdkafka_settings=rdkafka_settings,
        topic_name=OUTPUT_KAFKA_TOPIC,
        format="json",
    )

daq = StreamDaQ()
atlantis_task = daq.new_task("atlantis_monitoring", critical=True)

# Step 1: Configure monitoring parameters
atlantis_task.configure(
    window=get_window_from_string(WINDOW_TYPE),
    time_column="timestamp",
    wait_for_late=0,
    show_window_start=True,
    show_window_end=True,
    source=data,
    sink_operation=write_to_kafka
)

# Step 2: Define what Data Quality means for you
atlantis_task.check(dqm.count('angle_0'), name="angle_0_count") \
    .check(dqm.mean('angle_1'), name="angle_1_mean") \
    .check(dqm.count('angle_2'), name="angle_2_count") \
    .check(dqm.count('angle_3'), name="angle_3_count") \
    .check(dqm.count('angle_4'), name="angle_4_count") \
    .check(dqm.count('angle_5'), name="angle_5_count") \
    .check(dqm.mean('angle_6'), name="angle_6_mean") \
    .check(dqm.mean('angle_7'), name="angle_7_mean") \
    .check(dqm.mean('angle_8'), name="angle_8_mean") \
    .check(dqm.min('angle_9'), name="angle_9_min") \
    .check(dqm.min('angle_10'), name="angle_10_min") \
    .check(dqm.min('angle_11'), name="angle_11_min") \
    .check(dqm.min('angle_12'), name="angle_12_min") \
    .check(dqm.min('angle_13'), name="angle_13_min") \
    .check(dqm.max('angle_14'), name="angle_14_max") \
    .check(dqm.max('angle_15'), name="angle_15_max") \
    .check(dqm.count('angle_16'), name="angle_16_count") \
    .check(dqm.count('angle_17'), name="angle_17_count") \
    .check(dqm.count('angle_18'), name="angle_18_count") \
    .check(dqm.count('angle_19'), name="angle_19_count") \
    .check(dqm.count('angle_20'), name="angle_20_count") \
    .check(dqm.count('angle_21'), name="angle_21_count") \
    .check(dqm.count('angle_22'), name="angle_22_count") \
    .check(dqm.count('angle_23'), name="angle_23_count") \
    .check(dqm.count('angle_24'), name="angle_24_count") \
    .check(dqm.count('angle_25'), name="angle_25_count") \
    .check(dqm.count('angle_26'), name="angle_26_count") \
    .check(dqm.count('angle_27'), name="angle_27_count") \
    .check(dqm.count('angle_28'), name="angle_28_count") \
    .check(dqm.count('angle_29'), name="angle_29_count") \
    .check(dqm.count('angle_30'), name="angle_30_count") \
    .check(dqm.count('angle_31'), name="angle_31_count") \
    .check(dqm.count('angle_32'), name="angle_32_count") \
    .check(dqm.max('angle_33'), name="angle_33_max") \
    .check(dqm.max('angle_34'), name="angle_34_max") \
    .check(dqm.max('angle_35'), name="angle_35_max") \
    .check(dqm.mean('angle_36'), name="angle_36_mean") \
    .check(dqm.mean('angle_37'), name="angle_37_mean") \
    .check(dqm.mean('angle_38'), name="angle_38_mean") \
    .check(dqm.mean('angle_39'), name="angle_39_mean") \
    .check(dqm.count('angle_40'), name="angle_40_count") \
    .check(dqm.count('angle_41'), name="angle_41_count") \
    .check(dqm.count('angle_42'), name="angle_42_count") \
    .check(dqm.count('angle_43'), name="angle_43_count") \
    .check(dqm.count('angle_44'), name="angle_44_count") \
    .check(dqm.count('angle_45'), name="angle_45_count") \
    .check(dqm.count('angle_46'), name="angle_46_count") \
    .check(dqm.count('angle_47'), name="angle_47_count") \
    .check(dqm.count('angle_48'), name="angle_48_count") \
    .check(dqm.count('angle_49'), name="angle_49_count") \
    .check(dqm.count('angle_50'), name="angle_50_count") \
    .check(dqm.count('angle_51'), name="angle_51_count") \
    .check(dqm.count('angle_52'), name="angle_52_count") \
    .check(dqm.unique_count('angle_53'), name="angle_53_unique_count") \
    .check(dqm.unique_count('angle_54'), name="angle_54_unique_count") \
    .check(dqm.count('angle_55'), name="angle_55_count") \
    .check(dqm.count('angle_56'), name="angle_56_count") \
    .check(dqm.count('angle_57'), name="angle_57_count") \
    .check(dqm.count('angle_58'), name="angle_58_count") \
    .check(dqm.count('angle_59'), name="angle_59_count") \
    .check(dqm.count('angle_60'), name="angle_60_count") \
    .check(dqm.count('angle_61'), name="angle_61_count") \
    .check(dqm.count('angle_62'), name="angle_62_count") \
    .check(dqm.count('angle_63'), name="angle_63_count") \
    .check(dqm.count('angle_64'), name="angle_64_count") \
    .check(dqm.count('angle_65'), name="angle_65_count") \
    .check(dqm.count('angle_66'), name="angle_66_count") \
    .check(dqm.count('angle_67'), name="angle_67_count") \
    .check(dqm.count('angle_68'), name="angle_68_count") \
    .check(dqm.count('angle_69'), name="angle_69_count") \
    .check(dqm.count('angle_70'), name="angle_70_count") \
    .check(dqm.count('angle_71'), name="angle_71_count") \
    .check(dqm.count('angle_72'), name="angle_72_count") \
    .check(dqm.count('angle_73'), name="angle_73_count") \
    .check(dqm.count('angle_74'), name="angle_74_count") \
    .check(dqm.count('angle_75'), name="angle_75_count") \
    .check(dqm.count('angle_76'), name="angle_76_count") \
    .check(dqm.count('angle_77'), name="angle_77_count") \
    .check(dqm.count('angle_78'), name="angle_78_count") \
    .check(dqm.count('angle_79'), name="angle_79_count") \
    .check(dqm.count('angle_80'), name="angle_80_count") \
    .check(dqm.count('angle_81'), name="angle_81_count") \
    .check(dqm.count('angle_82'), name="angle_82_count") \
    .check(dqm.count('angle_83'), name="angle_83_count") \
    .check(dqm.count('angle_84'), name="angle_84_count") \
    .check(dqm.count('angle_85'), name="angle_85_count") \
    .check(dqm.count('angle_86'), name="angle_86_count") \
    .check(dqm.count('angle_87'), name="angle_87_count") \
    .check(dqm.count('angle_88'), name="angle_88_count") \
    .check(dqm.count('angle_89'), name="angle_89_count") \
    .check(dqm.count('angle_90'), name="angle_90_count") \
    .check(dqm.count('angle_91'), name="angle_91_count") \
    .check(dqm.count('angle_92'), name="angle_92_count") \
    .check(dqm.count('angle_93'), name="angle_93_count") \
    .check(dqm.count('angle_94'), name="angle_94_count") \
    .check(dqm.count('angle_95'), name="angle_95_count") \
    .check(dqm.count('angle_96'), name="angle_96_count") \
    .check(dqm.count('angle_97'), name="angle_97_count") \
    .check(dqm.count('angle_98'), name="angle_98_count") \
    .check(dqm.count('angle_99'), name="angle_99_count") \
    .check(dqm.count('angle_100'), name="angle_100_count") \
    .check(dqm.count('angle_101'), name="angle_101_count") \
    .check(dqm.count('angle_102'), name="angle_102_count") \
    .check(dqm.count('angle_103'), name="angle_103_count") \
    .check(dqm.count('angle_104'), name="angle_104_count") \
    .check(dqm.count('angle_105'), name="angle_105_count") \
    .check(dqm.count('angle_106'), name="angle_106_count") \
    .check(dqm.count('angle_107'), name="angle_107_count") \
    .check(dqm.count('angle_108'), name="angle_108_count") \
    .check(dqm.count('angle_109'), name="angle_109_count") \
    .check(dqm.count('angle_110'), name="angle_110_count") \
    .check(dqm.count('angle_111'), name="angle_111_count") \
    .check(dqm.count('angle_112'), name="angle_112_count") \
    .check(dqm.count('angle_113'), name="angle_113_count") \
    .check(dqm.unique_count('angle_114'), name="angle_114_unique_count") \
    .check(dqm.unique_count('angle_115'), name="angle_115_unique_count") \
    .check(dqm.unique_count('angle_116'), name="angle_116_unique_count") \
    .check(dqm.unique_count('angle_117'), name="angle_117_unique_count") \
    .check(dqm.count('angle_118'), name="angle_118_count") \
    .check(dqm.count('angle_119'), name="angle_119_count") \
    .check(dqm.count('angle_120'), name="angle_120_count") \
    .check(dqm.count('angle_121'), name="angle_121_count") \
    .check(dqm.count('angle_122'), name="angle_122_count") \
    .check(dqm.count('angle_123'), name="angle_123_count") \
    .check(dqm.count('angle_124'), name="angle_124_count") \
    .check(dqm.count('angle_125'), name="angle_125_count") \
    .check(dqm.count('angle_126'), name="angle_126_count") \
    .check(dqm.count('angle_127'), name="angle_127_count") \
    .check(dqm.count('angle_128'), name="angle_128_count") \
    .check(dqm.count('angle_129'), name="angle_129_count") \
    .check(dqm.count('angle_130'), name="angle_130_count") \
    .check(dqm.count('angle_131'), name="angle_131_count") \
    .check(dqm.count('angle_132'), name="angle_132_count") \
    .check(dqm.count('angle_133'), name="angle_133_count") \
    .check(dqm.count('angle_134'), name="angle_134_count") \
    .check(dqm.count('angle_135'), name="angle_135_count") \
    .check(dqm.count('angle_136'), name="angle_136_count") \
    .check(dqm.count('angle_137'), name="angle_137_count") \
    .check(dqm.count('angle_138'), name="angle_138_count") \
    .check(dqm.count('angle_139'), name="angle_139_count") \
    .check(dqm.count('angle_140'), name="angle_140_count") \
    .check(dqm.count('angle_141'), name="angle_141_count") \
    .check(dqm.count('angle_142'), name="angle_142_count") \
    .check(dqm.count('angle_143'), name="angle_143_count") \
    .check(dqm.count('angle_144'), name="angle_144_count") \
    .check(dqm.count('angle_145'), name="angle_145_count") \
    .check(dqm.count('angle_146'), name="angle_146_count") \
    .check(dqm.count('angle_147'), name="angle_147_count") \
    .check(dqm.count('angle_148'), name="angle_148_count") \
    .check(dqm.count('angle_149'), name="angle_149_count") \
    .check(dqm.count('angle_150'), name="angle_150_count") \
    .check(dqm.count('angle_151'), name="angle_151_count") \
    .check(dqm.count('angle_152'), name="angle_152_count") \
    .check(dqm.count('angle_153'), name="angle_153_count") \
    .check(dqm.count('angle_154'), name="angle_154_count") \
    .check(dqm.count('angle_155'), name="angle_155_count") \
    .check(dqm.count('angle_156'), name="angle_156_count") \
    .check(dqm.count('angle_157'), name="angle_157_count") \
    .check(dqm.count('angle_158'), name="angle_158_count") \
    .check(dqm.count('angle_159'), name="angle_159_count") \
    .check(dqm.count('angle_160'), name="angle_160_count") \
    .check(dqm.count('angle_161'), name="angle_161_count") \
    .check(dqm.count('angle_162'), name="angle_162_count") \
    .check(dqm.count('angle_163'), name="angle_163_count") \
    .check(dqm.count('angle_164'), name="angle_164_count") \
    .check(dqm.count('angle_165'), name="angle_165_count") \
    .check(dqm.count('angle_166'), name="angle_166_count") \
    .check(dqm.count('angle_167'), name="angle_167_count") \
    .check(dqm.count('angle_168'), name="angle_168_count") \
    .check(dqm.count('angle_169'), name="angle_169_count") \
    .check(dqm.count('angle_170'), name="angle_170_count") \
    .check(dqm.count('angle_171'), name="angle_171_count") \
    .check(dqm.count('angle_172'), name="angle_172_count") \
    .check(dqm.count('angle_173'), name="angle_173_count") \
    .check(dqm.count('angle_174'), name="angle_174_count") \
    .check(dqm.count('angle_175'), name="angle_175_count") \
    .check(dqm.count('angle_176'), name="angle_176_count") \
    .check(dqm.count('angle_177'), name="angle_177_count") \
    .check(dqm.count('angle_178'), name="angle_178_count") \
    .check(dqm.count('angle_179'), name="angle_179_count") \
    .check(dqm.count('angle_180'), name="angle_180_count") \
    .check(dqm.count('angle_181'), name="angle_181_count") \
    .check(dqm.count('angle_182'), name="angle_182_count") \
    .check(dqm.count('angle_183'), name="angle_183_count") \
    .check(dqm.count('angle_184'), name="angle_184_count") \
    .check(dqm.count('angle_185'), name="angle_185_count") \
    .check(dqm.count('angle_186'), name="angle_186_count") \
    .check(dqm.count('angle_187'), name="angle_187_count") \
    .check(dqm.count('angle_188'), name="angle_188_count") \
    .check(dqm.count('angle_189'), name="angle_189_count") \
    .check(dqm.count('angle_190'), name="angle_190_count") \
    .check(dqm.count('angle_191'), name="angle_191_count") \
    .check(dqm.count('angle_192'), name="angle_192_count") \
    .check(dqm.count('angle_193'), name="angle_193_count") \
    .check(dqm.count('angle_194'), name="angle_194_count") \
    .check(dqm.count('angle_195'), name="angle_195_count") \
    .check(dqm.count('angle_196'), name="angle_196_count") \
    .check(dqm.count('angle_197'), name="angle_197_count") \
    .check(dqm.count('angle_198'), name="angle_198_count") \
    .check(dqm.unique_fraction('angle_199'), name="angle_199_unique_fraction") \
    .check(dqm.unique_fraction('angle_200'), name="angle_200_unique_fraction") \
    .check(dqm.unique_fraction('angle_201'), name="angle_201_unique_fraction") \
    .check(dqm.count('angle_202'), name="angle_202_count") \
    .check(dqm.count('angle_203'), name="angle_203_count") \
    .check(dqm.count('angle_204'), name="angle_204_count") \
    .check(dqm.count('angle_205'), name="angle_205_count") \
    .check(dqm.count('angle_206'), name="angle_206_count") \
    .check(dqm.count('angle_207'), name="angle_207_count") \
    .check(dqm.count('angle_208'), name="angle_208_count") \
    .check(dqm.count('angle_209'), name="angle_209_count") \
    .check(dqm.count('angle_210'), name="angle_210_count") \
    .check(dqm.count('angle_211'), name="angle_211_count") \
    .check(dqm.count('angle_212'), name="angle_212_count") \
    .check(dqm.count('angle_213'), name="angle_213_count") \
    .check(dqm.count('angle_214'), name="angle_214_count") \
    .check(dqm.count('angle_215'), name="angle_215_count") \
    .check(dqm.count('angle_216'), name="angle_216_count") \
    .check(dqm.count('angle_217'), name="angle_217_count") \
    .check(dqm.count('angle_218'), name="angle_218_count") \
    .check(dqm.count('angle_219'), name="angle_219_count") \
    .check(dqm.count('angle_220'), name="angle_220_count") \
    .check(dqm.count('angle_221'), name="angle_221_count") \
    .check(dqm.count('angle_222'), name="angle_222_count") \
    .check(dqm.count('angle_223'), name="angle_223_count") \
    .check(dqm.count('angle_224'), name="angle_224_count") \
    .check(dqm.count('angle_225'), name="angle_225_count") \
    .check(dqm.count('angle_226'), name="angle_226_count") \
    .check(dqm.count('angle_227'), name="angle_227_count") \
    .check(dqm.count('angle_228'), name="angle_228_count") \
    .check(dqm.count('angle_229'), name="angle_229_count") \
    .check(dqm.count('angle_230'), name="angle_230_count") \
    .check(dqm.count('angle_231'), name="angle_231_count") \
    .check(dqm.count('angle_232'), name="angle_232_count") \
    .check(dqm.count('angle_233'), name="angle_233_count") \
    .check(dqm.mean('angle_234'), name="angle_234_mean") \
    .check(dqm.mean('angle_235'), name="angle_235_mean") \
    .check(dqm.mean('angle_236'), name="angle_236_mean") \
    .check(dqm.count('angle_237'), name="angle_237_count") \
    .check(dqm.count('angle_238'), name="angle_238_count") \
    .check(dqm.count('angle_239'), name="angle_239_count") \
    .check(dqm.count('angle_240'), name="angle_240_count") \
    .check(dqm.count('angle_241'), name="angle_241_count") \
    .check(dqm.count('angle_242'), name="angle_242_count") \
    .check(dqm.count('angle_243'), name="angle_243_count") \
    .check(dqm.count('angle_244'), name="angle_244_count") \
    .check(dqm.count('angle_245'), name="angle_245_count") \
    .check(dqm.count('angle_246'), name="angle_246_count") \
    .check(dqm.count('angle_247'), name="angle_247_count") \
    .check(dqm.unique_fraction('angle_248'), name="angle_248_unique_fraction") \
    .check(dqm.unique_fraction('angle_249'), name="angle_249_unique_fraction") \
    .check(dqm.unique_fraction('angle_250'), name="angle_250_unique_fraction") \
    .check(dqm.count('angle_251'), name="angle_251_count") \
    .check(dqm.count('angle_252'), name="angle_252_count") \
    .check(dqm.count('angle_253'), name="angle_253_count") \
    .check(dqm.count('angle_254'), name="angle_254_count") \
    .check(dqm.count('angle_255'), name="angle_255_count") \
    .check(dqm.count('angle_256'), name="angle_256_count") \
    .check(dqm.count('angle_257'), name="angle_257_count") \
    .check(dqm.count('angle_258'), name="angle_258_count") \
    .check(dqm.count('angle_259'), name="angle_259_count") \
    .check(dqm.count('angle_260'), name="angle_260_count") \
    .check(dqm.count('angle_261'), name="angle_261_count") \
    .check(dqm.count('angle_262'), name="angle_262_count") \
    .check(dqm.count('angle_263'), name="angle_263_count") \
    .check(dqm.count('angle_264'), name="angle_264_count") \
    .check(dqm.count('angle_265'), name="angle_265_count") \
    .check(dqm.count('angle_266'), name="angle_266_count") \
    .check(dqm.count('angle_267'), name="angle_267_count") \
    .check(dqm.count('angle_268'), name="angle_268_count") \
    .check(dqm.count('angle_269'), name="angle_269_count") \
    .check(dqm.count('angle_270'), name="angle_270_count") \
    .check(dqm.count('angle_271'), name="angle_271_count") \
    .check(dqm.unique_fraction('angle_272'), name="angle_272_unique_fraction") \
    .check(dqm.unique_fraction('angle_273'), name="angle_273_unique_fraction") \
    .check(dqm.unique_fraction('angle_274'), name="angle_274_unique_fraction") \
    .check(dqm.unique_fraction('angle_275'), name="angle_275_unique_fraction") \
    .check(dqm.count('angle_276'), name="angle_276_count") \
    .check(dqm.count('angle_277'), name="angle_277_count") \
    .check(dqm.count('angle_278'), name="angle_278_count") \
    .check(dqm.count('angle_279'), name="angle_279_count") \
    .check(dqm.count('angle_280'), name="angle_280_count") \
    .check(dqm.count('angle_281'), name="angle_281_count") \
    .check(dqm.count('angle_282'), name="angle_282_count") \
    .check(dqm.count('angle_283'), name="angle_283_count") \
    .check(dqm.count('angle_284'), name="angle_284_count") \
    .check(dqm.count('angle_285'), name="angle_285_count") \
    .check(dqm.count('angle_286'), name="angle_286_count") \
    .check(dqm.count('angle_287'), name="angle_287_count") \
    .check(dqm.count('angle_288'), name="angle_288_count") \
    .check(dqm.count('angle_289'), name="angle_289_count") \
    .check(dqm.count('angle_290'), name="angle_290_count") \
    .check(dqm.count('angle_291'), name="angle_291_count") \
    .check(dqm.count('angle_292'), name="angle_292_count") \
    .check(dqm.count('angle_293'), name="angle_293_count") \
    .check(dqm.count('angle_294'), name="angle_294_count") \
    .check(dqm.count('angle_295'), name="angle_295_count") \
    .check(dqm.count('angle_296'), name="angle_296_count") \
    .check(dqm.count('angle_297'), name="angle_297_count") \
    .check(dqm.count('angle_298'), name="angle_298_count") \
    .check(dqm.count('angle_299'), name="angle_299_count") \
    .check(dqm.count('angle_300'), name="angle_300_count") \
    .check(dqm.count('angle_301'), name="angle_301_count") \
    .check(dqm.count('angle_302'), name="angle_302_count") \
    .check(dqm.count('angle_303'), name="angle_303_count") \
    .check(dqm.count('angle_304'), name="angle_304_count") \
    .check(dqm.min('angle_305'), name="angle_305_min") \
    .check(dqm.min('angle_306'), name="angle_306_min") \
    .check(dqm.min('angle_307'), name="angle_307_min") \
    .check(dqm.count('angle_308'), name="angle_308_count") \
    .check(dqm.count('angle_309'), name="angle_309_count") \
    .check(dqm.count('angle_310'), name="angle_310_count") \
    .check(dqm.count('angle_311'), name="angle_311_count") \
    .check(dqm.count('angle_312'), name="angle_312_count") \
    .check(dqm.count('angle_313'), name="angle_313_count") \
    .check(dqm.count('angle_314'), name="angle_314_count") \
    .check(dqm.count('angle_315'), name="angle_315_count") \
    .check(dqm.count('angle_316'), name="angle_316_count") \
    .check(dqm.count('angle_317'), name="angle_317_count") \
    .check(dqm.count('angle_318'), name="angle_318_count") \
    .check(dqm.count('angle_319'), name="angle_319_count") \
    .check(dqm.count('angle_320'), name="angle_320_count") \
    .check(dqm.count('angle_321'), name="angle_321_count") \
    .check(dqm.count('angle_322'), name="angle_322_count") \
    .check(dqm.count('angle_323'), name="angle_323_count") \
    .check(dqm.count('angle_324'), name="angle_324_count") \
    .check(dqm.count('angle_325'), name="angle_325_count") \
    .check(dqm.count('angle_326'), name="angle_326_count") \
    .check(dqm.count('angle_327'), name="angle_327_count") \
    .check(dqm.count('angle_328'), name="angle_328_count") \
    .check(dqm.count('angle_329'), name="angle_329_count") \
    .check(dqm.count('angle_330'), name="angle_330_count") \
    .check(dqm.sum('angle_331'), name="angle_331_sum") \
    .check(dqm.sum('angle_332'), name="angle_332_sum") \
    .check(dqm.sum('angle_333'), name="angle_333_sum") \
    .check(dqm.median('angle_334'), name="angle_334_median") \
    .check(dqm.median('angle_335'), name="angle_335_median") \
    .check(dqm.median('angle_336'), name="angle_336_median") \
    .check(dqm.median('angle_337'), name="angle_337_median") \
    .check(dqm.missing_fraction('angle_338'), name="angle_338_missing_fraction") \
    .check(dqm.missing_fraction('angle_339'), name="angle_339_missing_fraction") \
    .check(dqm.missing_fraction('angle_340'), name="angle_340_missing_fraction") \
    .check(dqm.missing_fraction('angle_341'), name="angle_341_missing_fraction") \
    .check(dqm.count('angle_342'), name="angle_342_count") \
    .check(dqm.count('angle_343'), name="angle_343_count") \
    .check(dqm.count('angle_344'), name="angle_344_count") \
    .check(dqm.count('angle_345'), name="angle_345_count") \
    .check(dqm.count('angle_346'), name="angle_346_count") \
    .check(dqm.count('angle_347'), name="angle_347_count") \
    .check(dqm.count('angle_348'), name="angle_348_count") \
    .check(dqm.count('angle_349'), name="angle_349_count") \
    .check(dqm.count('angle_350'), name="angle_350_count") \
    .check(dqm.count('angle_351'), name="angle_351_count") \
    .check(dqm.count('angle_352'), name="angle_352_count") \
    .check(dqm.count('angle_353'), name="angle_353_count") \
    .check(dqm.count('angle_354'), name="angle_354_count") \
    .check(dqm.count('angle_355'), name="angle_355_count") \
    .check(dqm.count('angle_356'), name="angle_356_count") \
    .check(dqm.count('angle_357'), name="angle_357_count") \
    .check(dqm.count('angle_358'), name="angle_358_count") \
    .check(dqm.count('angle_359'), name="angle_359_count") \
    .check(dqm.count('angle_360'), name="angle_360_count") \
    .check(dqm.count('angle_361'), name="angle_361_count") \
    .check(dqm.count('angle_362'), name="angle_362_count") \
    .check(dqm.count('angle_363'), name="angle_363_count") \
    .check(dqm.distinct_count('angle_364'), name="angle_364_distinct_count") \
    .check(dqm.distinct_count('angle_365'), name="angle_365_count") \
    .check(dqm.count('angle_366'), name="angle_366_count") \
    .check(dqm.count('angle_367'), name="angle_367_count") \
    .check(dqm.count('angle_368'), name="angle_368_count") \
    .check(dqm.count('angle_369'), name="angle_369_count") \
    .check(dqm.count('angle_370'), name="angle_370_count") \
    .check(dqm.count('angle_371'), name="angle_371_count") \
    .check(dqm.count('angle_372'), name="angle_372_count") \
    .check(dqm.count('angle_373'), name="angle_373_count") \
    .check(dqm.count('angle_374'), name="angle_374_count") \
    .check(dqm.count('angle_375'), name="angle_375_count") \
    .check(dqm.count('angle_376'), name="angle_376_count") \
    .check(dqm.count('angle_377'), name="angle_377_count") \
    .check(dqm.count('angle_378'), name="angle_378_count") \
    .check(dqm.count('angle_379'), name="angle_379_count") \
    .check(dqm.count('angle_380'), name="angle_380_count") \
    .check(dqm.count('angle_381'), name="angle_381_count") \
    .check(dqm.count('angle_382'), name="angle_382_count") \
    .check(dqm.count('angle_383'), name="angle_383_count") \
    .check(dqm.count('angle_384'), name="angle_384_count") \
    .check(dqm.count('angle_385'), name="angle_385_count") \
    .check(dqm.count('angle_386'), name="angle_386_count") \
    .check(dqm.count('angle_387'), name="angle_387_count") \
    .check(dqm.count('angle_388'), name="angle_388_count") \
    .check(dqm.count('angle_389'), name="angle_389_count") \
    .check(dqm.count('angle_390'), name="angle_390_count") \
    .check(dqm.count('angle_391'), name="angle_391_count") \
    .check(dqm.count('angle_392'), name="angle_392_count") \
    .check(dqm.max('angle_393'), name="angle_393_max") \
    .check(dqm.max('angle_394'), name="angle_394_max") \
    .check(dqm.max('angle_395'), name="angle_395_max") \
    .check(dqm.count('angle_396'), name="angle_396_count") \
    .check(dqm.count('angle_397'), name="angle_397_count") \
    .check(dqm.count('angle_398'), name="angle_398_count") \
    .check(dqm.count('angle_399'), name="angle_399_count") \
    .check(dqm.count('angle_400'), name="angle_400_count") \
    .check(dqm.count('angle_401'), name="angle_401_count") \
    .check(dqm.count('angle_402'), name="angle_402_count") \
    .check(dqm.count('angle_403'), name="angle_403_count") \
    .check(dqm.count('angle_404'), name="angle_404_count") \
    .check(dqm.count('angle_405'), name="angle_405_count") \
    .check(dqm.count('angle_406'), name="angle_406_count") \
    .check(dqm.count('angle_407'), name="angle_407_count") \
    .check(dqm.count('angle_408'), name="angle_408_count") \
    .check(dqm.count('angle_409'), name="angle_409_count") \
    .check(dqm.count('angle_410'), name="angle_410_count") \
    .check(dqm.count('angle_411'), name="angle_411_count") \
    .check(dqm.count('angle_412'), name="angle_412_count") \
    .check(dqm.count('angle_413'), name="angle_413_count") \
    .check(dqm.count('angle_414'), name="angle_414_count") \
    .check(dqm.count('angle_415'), name="angle_415_count") \
    .check(dqm.count('angle_416'), name="angle_416_count") \
    .check(dqm.missing_fraction('angle_417'), name="angle_417_missing_fraction") \
    .check(dqm.missing_fraction('angle_418'), name="angle_418_missing_fraction") \
    .check(dqm.missing_fraction('angle_419'), name="angle_419_missing_fraction") \
    .check(dqm.missing_fraction('angle_420'), name="angle_420_missing_fraction") \
    .check(dqm.count('angle_421'), name="angle_421_count") \
    .check(dqm.count('angle_422'), name="angle_422_count") \
    .check(dqm.count('angle_423'), name="angle_423_count") \
    .check(dqm.count('angle_424'), name="angle_424_count") \
    .check(dqm.count('angle_425'), name="angle_425_count") \
    .check(dqm.count('angle_426'), name="angle_426_count") \
    .check(dqm.count('angle_427'), name="angle_427_count") \
    .check(dqm.count('angle_428'), name="angle_428_count") \
    .check(dqm.count('angle_429'), name="angle_429_count") \
    .check(dqm.count('angle_430'), name="angle_430_count") \
    .check(dqm.count('angle_431'), name="angle_431_count") \
    .check(dqm.count('angle_432'), name="angle_432_count") \
    .check(dqm.distinct_count('angle_433'), name="angle_433_distinct_count") \
    .check(dqm.count('angle_434'), name="angle_434_count") \
    .check(dqm.count('angle_435'), name="angle_435_count") \
    .check(dqm.count('angle_436'), name="angle_436_count") \
    .check(dqm.count('angle_437'), name="angle_437_count") \
    .check(dqm.count('angle_438'), name="angle_438_count") \
    .check(dqm.count('angle_439'), name="angle_439_count") \
    .check(dqm.count('angle_440'), name="angle_440_count") \
    .check(dqm.count('angle_441'), name="angle_441_count") \
    .check(dqm.count('angle_442'), name="angle_442_count") \
    .check(dqm.count('angle_443'), name="angle_443_count") \
    .check(dqm.count('angle_444'), name="angle_444_count") \
    .check(dqm.count('angle_445'), name="angle_445_count") \
    .check(dqm.count('angle_446'), name="angle_446_count") \
    .check(dqm.count('angle_447'), name="angle_447_count") \
    .check(dqm.count('angle_448'), name="angle_448_count") \
    .check(dqm.count('angle_449'), name="angle_449_count") \
    .check(dqm.count('angle_450'), name="angle_450_count") \
    .check(dqm.count('angle_451'), name="angle_451_count") \
    .check(dqm.count('angle_452'), name="angle_452_count") \
    .check(dqm.count('angle_453'), name="angle_453_count") \
    .check(dqm.count('angle_454'), name="angle_454_count") \
    .check(dqm.distinct_count('angle_455'), name="angle_455_distinct_count") \
    .check(dqm.distinct_count('angle_456'), name="angle_456_distinct_count") \
    .check(dqm.distinct_count('angle_457'), name="angle_457_distinct_count") \
    .check(dqm.distinct_count('angle_458'), name="angle_458_distinct_count") \
    .check(dqm.distinct_count('angle_459'), name="angle_459_distinct_count") \
    .check(dqm.distinct_count('angle_460'), name="angle_460_distinct_count") \
    .check(dqm.count('angle_461'), name="angle_461_count") \
    .check(dqm.count('angle_462'), name="angle_462_count") \
    .check(dqm.count('angle_463'), name="angle_463_count") \
    .check(dqm.count('angle_464'), name="angle_464_count") \
    .check(dqm.count('angle_465'), name="angle_465_count") \
    .check(dqm.count('angle_466'), name="angle_466_count") \
    .check(dqm.count('angle_467'), name="angle_467_count") \
    .check(dqm.count('angle_468'), name="angle_468_count") \
    .check(dqm.count('angle_469'), name="angle_469_count") \
    .check(dqm.count('angle_470'), name="angle_470_count") \
    .check(dqm.count('angle_471'), name="angle_471_count") \
    .check(dqm.count('angle_472'), name="angle_472_count") \
    .check(dqm.count('angle_473'), name="angle_473_count") \
    .check(dqm.count('angle_474'), name="angle_474_count") \
    .check(dqm.count('angle_475'), name="angle_475_count") \
    .check(dqm.count('angle_476'), name="angle_476_count") \
    .check(dqm.count('angle_477'), name="angle_477_count") \
    .check(dqm.count('angle_478'), name="angle_478_count") \
    .check(dqm.count('angle_479'), name="angle_479_count") \
    .check(dqm.count('angle_480'), name="angle_480_count") \
    .check(dqm.count('angle_481'), name="angle_481_count") \
    .check(dqm.count('angle_482'), name="angle_482_count") \
    .check(dqm.count('angle_483'), name="angle_483_count") \
    .check(dqm.count('angle_484'), name="angle_484_count") \
    .check(dqm.count('angle_485'), name="angle_485_count") \
    .check(dqm.count('angle_486'), name="angle_486_count") \
    .check(dqm.count('angle_487'), name="angle_487_count") \
    .check(dqm.min('angle_488'), name="angle_488_min") \
    .check(dqm.min('angle_489'), name="angle_489_min") \
    .check(dqm.min('angle_490'), name="angle_490_min") \
    .check(dqm.mean('angle_491'), name="angle_491_mean") \
    .check(dqm.mean('angle_492'), name="angle_492_mean") \
    .check(dqm.mean('angle_493'), name="angle_493_mean") \
    .check(dqm.mean('angle_494'), name="angle_494_max") \
    .check(dqm.max('angle_495'), name="angle_495_max") \
    .check(dqm.max('angle_496'), name="angle_496_max") \
    .check(dqm.max('angle_497'), name="angle_497_max") \
    .check(dqm.max('angle_498'), name="angle_498_max") \
    .check(dqm.max('angle_499'), name="angle_499_max")

# Step 3: Kick-off monitoring and let Stream DaQ do the work while you focus on the important
daq.watch_out()