# Testing file
import traceback
from transform.transform_all_tables import tran_all_tables
from transform.transform_chann import tran_chann
from transform.transformations import *
from extract.extract_all_tables import ext_all_tables
from extract.extract_times import ext_times

try:
    tran_all_tables()
    # ext_all_tables()
except:
    traceback.print_exc()
finally:
    pass
