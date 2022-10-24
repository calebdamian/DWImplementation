import configparser
import traceback

import pandas as pd

config = configparser.ConfigParser()
config.read(".properties")
config.get("DatabaseSection", "DB_TYPE")
cvsSectionName = "CSVSection"


def ext_countries(ses_db_stg):
    try:

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
            ses_db_stg.connect().execute("TRUNCATE TABLE countries_ext")
            # Creating Dataframe
            # Persisting into db
            df_countries = pd.DataFrame(country_col_dict)
            df_countries.to_sql("countries_ext", ses_db_stg, if_exists="append", index=False)

    except:
        traceback.print_exc()
    finally:
        pass
