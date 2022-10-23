# Testing file
import traceback
from transform.transform_all_tables import tran_all_tables


try:
    # ext_all_tables()
    tran_all_tables()
except:
    traceback.print_exc()
finally:
    pass
