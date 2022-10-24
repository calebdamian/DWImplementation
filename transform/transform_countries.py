import traceback

import pandas as pd

from transform.transformations import str_to_int, str_to_str_w_length


# Db stays the same
def tran_countries(curr_cod_etl, ses_db_stg):
    try:
        # Dictionary of values

        country_col_dict = {
            "country_id": [],
            "country_name": [],
            "country_region": [],
            "country_region_id": [],
            "cod_etl": [],
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
                country_col_dict["cod_etl"].append(curr_cod_etl)
        if country_col_dict["country_id"]:
            # Creating Dataframe
            # Persisting into db
            df_countries = pd.DataFrame(country_col_dict)
            df_countries.to_sql(
                "countries_tra", ses_db_stg, if_exists="append", index=False
            )
    except:
        traceback.print_exc()
    finally:
        pass
