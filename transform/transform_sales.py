import traceback

import pandas as pd

from transform.transformations import (
    str_to_date,
    str_to_float,
    str_to_int,
)


# Db stays the same
def tran_sales(curr_cod_etl, ses_db_stg):
    try:
        # Dictionary of values

        sales_col_dict = {
            "prod_id": [],
            "cust_id": [],
            "time_id": [],
            "channel_id": [],
            "promo_id": [],
            "quantity_sold": [],
            "amount_sold": [],
            "cod_etl": [],
        }

        # Read extraction table
        sales_ext = pd.read_sql(
            "SELECT PROD_ID,CUST_ID, TIME_ID, CHANNEL_ID,PROMO_ID,QUANTITY_SOLD,AMOUNT_SOLD FROM sales_ext",
            ses_db_stg,
        )
        # Processing rows
        if not sales_ext.empty:
            for id, sl_cus_id, sl_ti_id, sl_ch_id, sl_promo_id, sl_q, sl_am, in zip(
                    sales_ext["PROD_ID"],
                    sales_ext["CUST_ID"],
                    sales_ext["TIME_ID"],
                    sales_ext["CHANNEL_ID"],
                    sales_ext["PROMO_ID"],
                    sales_ext["QUANTITY_SOLD"],
                    sales_ext["AMOUNT_SOLD"],
            ):
                sales_col_dict["prod_id"].append(str_to_int(id))
                sales_col_dict["cust_id"].append(str_to_int(sl_cus_id))
                sales_col_dict["time_id"].append(str_to_date(sl_ti_id))
                sales_col_dict["channel_id"].append(str_to_int(sl_ch_id))
                sales_col_dict["promo_id"].append(str_to_int(sl_promo_id))
                sales_col_dict["quantity_sold"].append(str_to_float(sl_q))
                sales_col_dict["amount_sold"].append(str_to_float(sl_am))
                sales_col_dict["cod_etl"].append(curr_cod_etl)
        if sales_col_dict["prod_id"]:
            # Creating Dataframe
            # Persisting into db
            # colummns_dict["cod_etl"].append(curr_etl_code)
            df_sales_tra = pd.DataFrame(sales_col_dict)
            df_sales_tra.to_sql(
                "sales_tra", ses_db_stg, if_exists="append", index=False
            )
    except:
        traceback.print_exc()
    finally:
        pass
