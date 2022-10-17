import traceback
from util import db_connection
import pandas as pd
import configparser


config = configparser.ConfigParser()
config.read(".properties")
config.get("DatabaseSection", "DB_TYPE")
# Creating a new db conn object
sectionName = "DatabaseSection"
stg_conn = db_connection.Db_Connection(
    config.get(sectionName, "DB_TYPE"),
    config.get(sectionName, "DB_HOST"),
    config.get(sectionName, "DB_PORT"),
    config.get(sectionName, "DB_USER"),
    config.get(sectionName, "DB_PWD"),
    config.get(sectionName, "STG_NAME"),
)
cvsSectionName = "CSVSection"


def ext_sales():
    try:
        # Connecting db
        conn = stg_conn.start()
        if conn == -1:
            raise Exception(f"The database type {stg_conn.type} is not valid")
        elif conn == -2:
            raise Exception("Error trying to connect to cdnastaging")

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
            conn.connect().execute("TRUNCATE TABLE sales")
            # Creating Dataframe
            # Persisting into db
            df_countries = pd.DataFrame(sales_col_dict)
            df_countries.to_sql("sales", conn, if_exists="append", index=False)
            # Dispose db connection
            conn.dispose()
    except:
        traceback.print_exc()
    finally:
        pass
