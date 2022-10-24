import traceback

import pandas as pd

from transform.transformations import (
    str_to_date,
    str_to_float,
    str_to_int,
    str_to_str_w_length,
)


# Db stays the same
def tran_promotions(curr_cod_etl, ses_db_stg):
    try:

        # Dictionary of values

        promos_col_dict = {
            "promo_id": [],
            "promo_name": [],
            "promo_cost": [],
            "promo_begin_date": [],
            "promo_end_date": [],
            "cod_etl": [],
        }

        # Read extraction table
        promos_ext = pd.read_sql(
            "SELECT PROMO_ID,PROMO_NAME, PROMO_COST, PROMO_BEGIN_DATE,PROMO_END_DATE FROM promotions_ext",
            ses_db_stg,
        )
        # Processing rows
        if not promos_ext.empty:
            for (id, pr_name, pr_cost, pr_begin, pr_end) in zip(
                    promos_ext["PROMO_ID"],
                    promos_ext["PROMO_NAME"],
                    promos_ext["PROMO_COST"],
                    promos_ext["PROMO_BEGIN_DATE"],
                    promos_ext["PROMO_END_DATE"],
            ):
                promos_col_dict["promo_id"].append(str_to_int(id))
                promos_col_dict["promo_name"].append(str_to_str_w_length(pr_name, 30))
                promos_col_dict["promo_cost"].append(str_to_float(pr_cost))
                promos_col_dict["promo_begin_date"].append(str_to_date(pr_begin))
                promos_col_dict["promo_end_date"].append(str_to_date(pr_end))
                promos_col_dict["cod_etl"].append(curr_cod_etl)

        if promos_col_dict["promo_id"]:
            # Creating Dataframe
            # Persisting into db
            df_promotions = pd.DataFrame(promos_col_dict)
            df_promotions.to_sql(
                "promotions_tra", ses_db_stg, if_exists="append", index=False
            )

    except:
        traceback.print_exc()
    finally:
        pass
