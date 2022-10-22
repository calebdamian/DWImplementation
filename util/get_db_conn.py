from configparser import ConfigParser

from db_connection import Db_Connection


def get_db_conn(db_purpose="stg"):
    config = ConfigParser()
    config.read(".properties")
    # Creating a new db conn object
    sectionName = "DatabaseSection"
    if db_purpose == "stg":
        stg_conn = Db_Connection(
            config.get(sectionName, "DB_TYPE"),
            config.get(sectionName, "DB_HOST"),
            config.get(sectionName, "DB_PORT"),
            config.get(sectionName, "DB_USER"),
            config.get(sectionName, "DB_PWD"),
            config.get(sectionName, "STG_NAME"),
        )
        return stg_conn
    else:
        sor_conn = Db_Connection(
            config.get(sectionName, "DB_TYPE"),
            config.get(sectionName, "DB_HOST"),
            config.get(sectionName, "DB_PORT"),
            config.get(sectionName, "DB_USER"),
            config.get(sectionName, "DB_PWD"),
            config.get(sectionName, "SOR_NAME"),
        )
        return sor_conn
