import traceback

import pandas as pd


def load_countries(curr_cod_etl, ses_db_stg, ses_db_sor):
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
            "SELECT country_id,country_name, country_region, country_region_id FROM countries_tra",
            ses_db_stg,
        )
        # Processing rows
        if not country_ext.empty:
            for coun_id, name, reg, reg_id in zip(
                    country_ext["country_id"],
                    country_ext["country_name"],
                    country_ext["country_region"],
                    country_ext["country_region_id"],
            ):
                country_col_dict["country_id"].append(coun_id)
                country_col_dict["country_name"].append(name)
                country_col_dict["country_region"].append(reg)
                country_col_dict["country_region_id"].append(reg_id)
                country_col_dict["cod_etl"].append(curr_cod_etl)
        if country_col_dict["country_id"]:
            # Creating Dataframe
            # Persisting into db
            df_countries = pd.DataFrame(country_col_dict)
            df_countries.to_sql(
                "dim_countries", ses_db_sor, if_exists="append", index=False
            )
    except:
        traceback.print_exc()
    finally:
        pass
