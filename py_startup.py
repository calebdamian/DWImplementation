# Testing file
import traceback
from transform.transform_all_tables import tran_all_tables
from transform.transform_chann import tran_chann
from transform.transform_customers import tran_customers
from transform.transform_sales import tran_sales
from transform.transform_times import tran_times
from transform.transformations import *
from extract.extract_all_tables import ext_all_tables
from extract.extract_times import ext_times

try:
    # ext_all_tables()
    tran_all_tables()
except:
    traceback.print_exc()
finally:
    pass
