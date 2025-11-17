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

data = data.windowby(
            data['timestamp'],
            window=get_window_from_string(WINDOW_TYPE),
        ).reduce(count=pw.reducers.count('angle_0'))

write_to_kafka(data)

pw.run()