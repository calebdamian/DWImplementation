import configparser
import traceback

import pandas as pd

config = configparser.ConfigParser()
config.read(".properties")
config.get("DatabaseSection", "DB_TYPE")
cvsSectionName = "CSVSection"


def ext_promotions(ses_db_stg):
    try:

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
            ses_db_stg.connect().execute("TRUNCATE TABLE promotions_ext")
            # Creating Dataframe
            # Persisting into db
            df_countries = pd.DataFrame(promos_col_dict)
            df_countries.to_sql("promotions_ext", ses_db_stg, if_exists="append", index=False)
    except:
        traceback.print_exc()
    finally:
        pass
