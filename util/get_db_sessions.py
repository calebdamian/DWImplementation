import configparser

from util import db_connection


def get_db_sessions():
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

    ses_dict = {"ses_db_stg": ses_db_stg, "ses_db_sor": ses_db_sor}

    return ses_dict 
