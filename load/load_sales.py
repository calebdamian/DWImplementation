import configparser
import traceback

import pandas as pd

from util.merge_dfs_tables import merge_dfs_tables

config = configparser.ConfigParser()
config.read(".properties")
cvsSectionName = "CSVSection"


def load_sales(curr_cod_etl, ses_db_stg, ses_db_sor):
    try:

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
        sales_tra = pd.read_sql(
            f"SELECT prod_id,cust_id, time_id, channel_id,promo_id,quantity_sold,amount_sold FROM sales_tra WHERE cod_etl = {curr_cod_etl}",
            ses_db_stg,
        )

        # Getting surrogate key from related tables and adding them to the sales dictionary
        surr_key_prod = \
            pd.read_sql_query("SELECT surr_id, prod_id from dim_products", ses_db_sor).set_index("prod_id").to_dict()[
                "surr_id"]

        surr_key_cust = \
            pd.read_sql_query("SELECT surr_id, cust_id from dim_customers", ses_db_sor).set_index("cust_id").to_dict()[
                "surr_id"]

        surr_key_time = \
            pd.read_sql_query("SELECT surr_id, time_id from dim_times", ses_db_sor).set_index("time_id").to_dict()[
                "surr_id"]

        surr_key_chann = \
            pd.read_sql_query("SELECT surr_id, channel_id from dim_channels", ses_db_sor).set_index(
                "channel_id").to_dict()[
                "surr_id"]

        surr_key_prom = \
            pd.read_sql_query("SELECT surr_id, promo_id from dim_promotions", ses_db_sor).set_index(
                "promo_id").to_dict()[
                "surr_id"]
        # Adding surrogate keys to sales dataframe
        sales_tra["prod_id"] = sales_tra["prod_id"].apply(lambda key: surr_key_prod[key])
        sales_tra["cust_id"] = sales_tra["cust_id"].apply(lambda key: surr_key_cust[key])
        sales_tra["time_id"] = sales_tra["time_id"].apply(lambda key: surr_key_time[key])
        sales_tra["channel_id"] = sales_tra["channel_id"].apply(lambda key: surr_key_chann[key])
        sales_tra["promo_id"] = sales_tra["promo_id"].apply(lambda key: surr_key_prom[key])
        # Processing rows
        if not sales_tra.empty:
            for sl_prod_id, sl_cus_id, sl_ti_id, sl_ch_id, sl_promo_id, sl_q, sl_am, in zip(
                    sales_tra["prod_id"],
                    sales_tra["cust_id"],
                    sales_tra["time_id"],
                    sales_tra["channel_id"],
                    sales_tra["promo_id"],
                    sales_tra["quantity_sold"],
                    sales_tra["amount_sold"],
            ):
                sales_col_dict["prod_id"].append(sl_prod_id)
                sales_col_dict["cust_id"].append(sl_cus_id)
                sales_col_dict["time_id"].append(sl_ti_id)
                sales_col_dict["channel_id"].append(sl_ch_id)
                sales_col_dict["promo_id"].append(sl_promo_id)
                sales_col_dict["quantity_sold"].append(sl_q)
                sales_col_dict["amount_sold"].append(sl_am)
                sales_col_dict["cod_etl"].append(curr_cod_etl)

        if sales_col_dict["prod_id"]:
            # Creating Dataframe
            df_dim_sales = pd.DataFrame(sales_col_dict)
            merge_dfs_tables(table_name="dim_sales",
                             business_key_col=["prod_id", "cust_id", "time_id", "channel_id", "promo_id"],
                             dataframe=df_dim_sales, db_context=ses_db_sor)
    except:
        traceback.print_exc()
    finally:
        pass
