# Testing file
import time
import traceback

from extract.extract_all_tables import extract_all_tables
from load.load_all_tables import load_all_tables
from transform.transform_all_tables import transform_all_tables
from util.get_curr_etl_code import get_curr_etl_code
from util.get_db_sessions import get_db_sessions

try:
    sess = get_db_sessions()
    ses_db_stg = sess["ses_db_stg"]
    ses_db_sor = sess["ses_db_sor"]
    if ses_db_stg is not None and ses_db_sor is not None:
        curr_etl_code = get_curr_etl_code(ses_db_stg=ses_db_stg)
        t0 = time.perf_counter()
        extract_all_tables(ses_db_stg=ses_db_stg)
        t1 = time.perf_counter()
        print(f"Extraction took: {t1 - t0}")
        t0 = time.perf_counter()
        transform_all_tables(curr_etl_code=curr_etl_code, ses_db_stg=ses_db_stg)
        t1 = time.perf_counter()
        print(f"Transformation took: {t1 - t0}")
        t0 = time.perf_counter()
        load_all_tables(curr_etl_code=curr_etl_code, ses_db_stg=ses_db_stg, ses_db_sor=ses_db_sor)
        t1 = time.perf_counter()
        print(f"Loading took: {t1 - t0}")
        ses_db_stg.dispose()
        ses_db_sor.dispose()
except KeyError:
    traceback.print_exc()
finally:
    print("Testing ended")
