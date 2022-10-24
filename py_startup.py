# Testing file
import configparser
import time
import traceback

from load.load_all_tables import load_all_tables
from util import db_connection

config = configparser.ConfigParser()
config.read(".properties")
# Creating a new db ses_db_sor object
sectionName = "DatabaseSection"
sor_conn = db_connection.Db_Connection(
    config.get(sectionName, "DB_TYPE"),
    config.get(sectionName, "DB_HOST"),
    config.get(sectionName, "DB_PORT"),
    config.get(sectionName, "DB_USER"),
    config.get(sectionName, "DB_PWD"),
    config.get(sectionName, "SOR_NAME"),
)
stg_conn = db_connection.Db_Connection(
    config.get(sectionName, "DB_TYPE"),
    config.get(sectionName, "DB_HOST"),
    config.get(sectionName, "DB_PORT"),
    config.get(sectionName, "DB_USER"),
    config.get(sectionName, "DB_PWD"),
    config.get(sectionName, "STG_NAME"),
)
cvsSectionName = "CSVSection"

ses_db_sor = sor_conn.start()
ses_db_stg = stg_conn.start()

if ses_db_stg == -1:
    raise Exception(f"The database type {ses_db_stg.type} is not valid")
elif ses_db_stg == -2:
    raise Exception("Error trying to connect to cdnastaging")

if ses_db_sor == -1:
    raise Exception(f"The database type {ses_db_sor.type} is not valid")
elif ses_db_sor == -2:
    raise Exception("Error trying to connect to cdnasor")

try:
    t0 = time.perf_counter()
    load_all_tables(ses_db_stg=ses_db_stg, ses_db_sor=ses_db_sor)
    t1 = time.perf_counter()
    print(f"Loading took: {t1 - t0}")
except Exception:
    traceback.print_exc()
finally:
    ses_db_stg.dispose()
    ses_db_sor.dispose()
