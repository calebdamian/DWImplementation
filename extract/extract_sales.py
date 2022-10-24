import configparser
import traceback

import pandas as pd

config = configparser.ConfigParser()
config.read(".properties")
config.get("DatabaseSection", "DB_TYPE")
cvsSectionName = "CSVSection"


def ext_sales(ses_db_stg):
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
        }

        # Read CSV
        sales_csv = pd.read_csv(config.get(cvsSectionName, "SALES_PATH"))

        # Processing CSV content
        if not sales_csv.empty:
            for (id, sl_cus_id, sl_ti_id, sl_ch_id, sl_promo_id, sl_q, sl_am,) in zip(
                    sales_csv["PROD_ID"],
                    sales_csv["CUST_ID"],
                    sales_csv["TIME_ID"],
                    sales_csv["CHANNEL_ID"],
                    sales_csv["PROMO_ID"],
                    sales_csv["QUANTITY_SOLD"],
                    sales_csv["AMOUNT_SOLD"],
            ):
                sales_col_dict["prod_id"].append(id)
                sales_col_dict["cust_id"].append(sl_cus_id)
                sales_col_dict["time_id"].append(sl_ti_id)
                sales_col_dict["channel_id"].append(sl_ch_id)
                sales_col_dict["promo_id"].append(sl_promo_id)
                sales_col_dict["quantity_sold"].append(sl_q)
                sales_col_dict["amount_sold"].append(sl_am)

        if sales_col_dict["prod_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE sales_ext")
            # Creating Dataframe
            # Persisting into db
            df_countries = pd.DataFrame(sales_col_dict)
            df_countries.to_sql("sales_ext", ses_db_stg, if_exists="append", index=False)
            # Dispose db connection
            ses_db_stg.dispose()
    except:
        traceback.print_exc()
    finally:
        pass
