import traceback
from util import db_connection
import pandas as pd
import configparser


config = configparser.ConfigParser()
config.read(".properties")
config.get("DatabaseSection", "DB_TYPE")
# Creating a new db conn object
sectionName = "DatabaseSection"
stg_conn = db_connection.Db_Connection(
    config.get(sectionName, "DB_TYPE"),
    config.get(sectionName, "DB_HOST"),
    config.get(sectionName, "DB_PORT"),
    config.get(sectionName, "DB_USER"),
    config.get(sectionName, "DB_PWD"),
    config.get(sectionName, "STG_NAME"),
)
cvsSectionName = "CSVSection"


def ext_countries():
    try:
        # Connecting db
        conn = stg_conn.start()
        if conn == -1:
            raise Exception(f"The database type {stg_conn.type} is not valid")
        elif conn == -2:
            raise Exception("Error trying to connect to cdnastaging")

        # Dictionary of values

        country_col_dict = {
            "country_id": [],
            "country_name": [],
            "country_region": [],
            "country_region_id": [],
        }

        # Read CSV
        country_csv = pd.read_csv(config.get(cvsSectionName, "COUNTRIES_PATH"))

        # Processing CSV content
        if not country_csv.empty:
            for id, name, reg, reg_id in zip(
                country_csv["COUNTRY_ID"],
                country_csv["COUNTRY_NAME"],
                country_csv["COUNTRY_REGION"],
                country_csv["COUNTRY_REGION_ID"],
            ):

                country_col_dict["country_id"].append(id)
                country_col_dict["country_name"].append(name)
                country_col_dict["country_region"].append(reg)
                country_col_dict["country_region_id"].append(reg_id)
        if country_col_dict["country_id"]:
            conn.connect().execute("TRUNCATE TABLE countries_ext")
            # Creating Dataframe
            # Persisting into db
            df_countries = pd.DataFrame(country_col_dict)
            df_countries.to_sql("countries_ext", conn, if_exists="append", index=False)
            # Dispose db connection
            conn.dispose()
    except:
        traceback.print_exc()
    finally:
        pass
