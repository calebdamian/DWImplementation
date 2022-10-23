from extract.extract_all_tables import ext_all_tables
from transform.transform_all_tables import tran_all_tables

try:
    ext_all_tables()
    tran_all_tables()
finally:
    print("Ended ETL Process")
