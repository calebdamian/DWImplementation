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


def ext_promotions():
    try:
        # Connecting db
        conn = stg_conn.start()
        if conn == -1:
            raise Exception(f"The database type {stg_conn.type} is not valid")
        elif conn == -2:
            raise Exception("Error trying to connect to cdnastaging")

        # Dictionary of values

        promos_col_dict = {
            "promo_id": [],
            "promo_name": [],
            "promo_cost": [],
            "promo_begin_date": [],
            "promo_end_date": [],
        }

        # Read CSV
        promos_csv = pd.read_csv(config.get(cvsSectionName, "PROMOTIONS_PATH"))

        # Processing CSV content
        if not promos_csv.empty:
            for (id, pr_name, pr_cost, pr_begin, pr_end) in zip(
                promos_csv["PROMO_ID"],
                promos_csv["PROMO_NAME"],
                promos_csv["PROMO_COST"],
                promos_csv["PROMO_BEGIN_DATE"],
                promos_csv["PROMO_END_DATE"],
            ):

                promos_col_dict["promo_id"].append(id)
                promos_col_dict["promo_name"].append(pr_name)
                promos_col_dict["promo_cost"].append(pr_cost)
                promos_col_dict["promo_begin_date"].append(pr_begin)
                promos_col_dict["promo_end_date"].append(pr_end)

        if promos_col_dict["promo_id"]:
            conn.connect().execute("TRUNCATE TABLE promotions")
            # Creating Dataframe
            # Persisting into db
            df_countries = pd.DataFrame(promos_col_dict)
            df_countries.to_sql("promotions", conn, if_exists="append", index=False)
            # Dispose db connection
            conn.dispose()
    except:
        traceback.print_exc()
    finally:
        pass
