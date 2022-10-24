import traceback

import pandas as pd


def load_promotions(curr_cod_etl, ses_db_stg, ses_db_sor):
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
        promos_tra = pd.read_sql(
            "SELECT promo_id,promo_name, promo_cost, promo_begin_date,promo_end_date FROM promotions_tra",
            ses_db_stg,
        )
        # Processing rows
        if not promos_tra.empty:
            for (id, pr_name, pr_cost, pr_begin, pr_end) in zip(
                    promos_tra["promo_id"],
                    promos_tra["promo_name"],
                    promos_tra["promo_cost"],
                    promos_tra["promo_begin_date"],
                    promos_tra["promo_end_date"],
            ):
                promos_col_dict["promo_id"].append(id)
                promos_col_dict["promo_name"].append(pr_name)
                promos_col_dict["promo_cost"].append(pr_cost)
                promos_col_dict["promo_begin_date"].append(pr_begin)
                promos_col_dict["promo_end_date"].append(pr_end)
                promos_col_dict["cod_etl"].append(curr_cod_etl)

        if promos_col_dict["promo_id"]:
            # Creating Dataframe
            # Persisting into db
            df_promotions = pd.DataFrame(promos_col_dict)
            df_promotions.to_sql(
                "dim_promotions", ses_db_sor, if_exists="append", index=False
            )
    except:
        traceback.print_exc()
    finally:
        pass
