# pruebas de funcionamiento
import traceback
from extract.extract_channels import ext_channels
from util import db_connection
import configparser

"""config = configparser.ConfigParser()
config.read("./.properties")
config.get("DatabaseSection", "DB_TYPE")
# instancia de clase
sectionName = "DatabaseSection"
stg_conn = db_connection.Db_Connection(
    config.get(sectionName, "DB_TYPE"),
    config.get(sectionName, "DB_HOST"),
    config.get(sectionName, "DB_PORT"),
    config.get(sectionName, "DB_USER"),
    config.get(sectionName, "DB_PWD"),
    config.get(sectionName, "STG_NAME"),
)
"""
try:
    """conn = stg_conn.start()
    if conn == -1:
        raise Exception(f"The database type {stg_conn.type} is not valid")
    elif conn == -2:
        raise Exception("Error trying to connect to cdnastaging")"""
    ext_channels()
except:
    traceback.print_exc()
finally:
    pass
