import configparser
import traceback

import pandas as pd

from util import db_connection

config = configparser.ConfigParser()
config.read(".properties")
# Creating a new db ses_db_sor object
sectionName = "DatabaseSection"
sor_conn = db_connection.Db_Connection(
    config.get(sectionName, "DB_TYPE"),
    config.get(sectionName, "DB_HOST"),
    config.get(sectionName, "DB_PORT"),
    config.get(sectionName, "DB_USER"),
    config.get(sectionName, "DB_PWD"),
    config.get(sectionName, "SOR_NAME"),
)
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
def load_sales(curr_cod_etl, ses_db_stg, ses_db_sor):
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
        sales_tra = pd.read_sql(
            "SELECT prod_id,cust_id, time_id, channel_id,promo_id,quantity_sold,amount_sold FROM sales_tra",
            ses_db_stg,
        )
        # Processing rows
        if not sales_tra.empty:
            for sl_id, sl_cus_id, sl_ti_id, sl_ch_id, sl_promo_id, sl_q, sl_am, in zip(
                    sales_tra["prod_id"],
                    sales_tra["cust_id"],
                    sales_tra["time_id"],
                    sales_tra["channel_id"],
                    sales_tra["promo_id"],
                    sales_tra["quantity_sold"],
                    sales_tra["amount_sold"],
            ):
                sales_col_dict["prod_id"].append(sl_id)
                sales_col_dict["cust_id"].append(sl_cus_id)
                sales_col_dict["time_id"].append(sl_ti_id)
                sales_col_dict["channel_id"].append(sl_ch_id)
                sales_col_dict["promo_id"].append(sl_promo_id)
                sales_col_dict["quantity_sold"].append(sl_q)
                sales_col_dict["amount_sold"].append(sl_am)
                sales_col_dict["cod_etl"].append(curr_cod_etl)
        if sales_col_dict["prod_id"]:
            # Creating Dataframe
            # Persisting into db

            df_sales_tra = pd.DataFrame(sales_col_dict)
            df_sales_tra.to_sql(
                "dim_sales", ses_db_sor, if_exists="append", index=False
            )
    except:
        traceback.print_exc()
    finally:
        pass
