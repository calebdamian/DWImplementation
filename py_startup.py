# Testing file
import traceback

from load.load_sales import load_sales
from util.get_curr_etl_code import get_curr_etl_code
from util.get_db_sessions import get_db_sessions

try:
    sess = get_db_sessions()
    ses_db_stg = sess["ses_db_stg"]
    ses_db_sor = sess["ses_db_sor"]
    if ses_db_stg is not None and ses_db_sor is not None:
        curr_etl_code = get_curr_etl_code(ses_db_stg=ses_db_stg)
        load_sales(curr_cod_etl=1, ses_db_stg=ses_db_stg, ses_db_sor=ses_db_sor)
except KeyError:
    traceback.print_exc()
finally:
    print("Testing ended")
