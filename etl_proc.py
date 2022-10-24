import time

from extract.extract_all_tables import ext_all_tables
from load.load_all_tables import load_all_tables
from transform.transform_all_tables import tran_all_tables

try:
    t0 = time.perf_counter()
    ext_all_tables()
    t1 = time.perf_counter()
    print(f"Extraction took: {t1 - t0}")
    t0 = time.perf_counter()
    tran_all_tables()
    t1 = time.perf_counter()
    print(f"Transformation took: {t1 - t0}")
    t0 = time.perf_counter()
    load_all_tables()
    t1 = time.perf_counter()
    print(f"Loading took: {t1 - t0}")
finally:

    print("Ended ETL Process")
