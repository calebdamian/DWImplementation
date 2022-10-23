import traceback
from transform.transformations import str_to_int, str_to_str_w_length
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

# Db stays the same
def tran_countries():
    try:

        # Connecting db
        ses_db_stg = stg_conn.start()
        if ses_db_stg == -1:
            raise Exception(f"The database type {stg_conn.type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to cdnastaging")

        # Dictionary of values

        country_col_dict = {
            "country_id": [],
            "country_name": [],
            "country_region": [],
            "country_region_id": [],
        }

        # Read extraction table
        country_ext = pd.read_sql(
            "SELECT COUNTRY_ID,COUNTRY_NAME, COUNTRY_REGION, COUNTRY_REGION_ID FROM countries_ext",
            ses_db_stg,
        )
        # Processing rows
        if not country_ext.empty:
            for id, name, reg, reg_id in zip(
                country_ext["COUNTRY_ID"],
                country_ext["COUNTRY_NAME"],
                country_ext["COUNTRY_REGION"],
                country_ext["COUNTRY_REGION_ID"],
            ):

                country_col_dict["country_id"].append(str_to_int(id))
                country_col_dict["country_name"].append(str_to_str_w_length(name, 40))
                country_col_dict["country_region"].append(str_to_str_w_length(reg, 20))
                country_col_dict["country_region_id"].append(str_to_int(reg_id))
        if country_col_dict["country_id"]:
            # Creating Dataframe
            # Persisting into db
            df_countries = pd.DataFrame(country_col_dict)
            df_countries.to_sql(
                "countries_tra", ses_db_stg, if_exists="append", index=False
            )
            # Dispose db connection
            ses_db_stg.dispose()
    except:
        traceback.print_exc()
    finally:
        pass
