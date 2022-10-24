import configparser
import traceback

import pandas as pd

config = configparser.ConfigParser()
config.read(".properties")
config.get("DatabaseSection", "DB_TYPE")
cvsSectionName = "CSVSection"


def ext_products(ses_db_stg):
    try:

        # Dictionary of values

        products_col_dict = {
            "prod_id": [],
            "prod_name": [],
            "prod_desc": [],
            "prod_category": [],
            "prod_category_id": [],
            "prod_category_desc": [],
            "prod_weight_class": [],
            "supplier_id": [],
            "prod_status": [],
            "prod_list_price": [],
            "prod_min_price": [],
        }

        # Read CSV
        products_csv = pd.read_csv(config.get(cvsSectionName, "PRODUCTS_PATH"))

        # Processing CSV content
        if not products_csv.empty:
            for (
                    id,
                    p_name,
                    p_desc,
                    p_cat,
                    p_cat_id,
                    p_cat_desc,
                    p_w_class,
                    supp_id,
                    p_status,
                    p_list,
                    p_min,
            ) in zip(
                products_csv["PROD_ID"],
                products_csv["PROD_NAME"],
                products_csv["PROD_DESC"],
                products_csv["PROD_CATEGORY"],
                products_csv["PROD_CATEGORY_ID"],
                products_csv["PROD_CATEGORY_DESC"],
                products_csv["PROD_WEIGHT_CLASS"],
                products_csv["SUPPLIER_ID"],
                products_csv["PROD_STATUS"],
                products_csv["PROD_LIST_PRICE"],
                products_csv["PROD_MIN_PRICE"],
            ):
                products_col_dict["prod_id"].append(id)
                products_col_dict["prod_name"].append(p_name)
                products_col_dict["prod_desc"].append(p_desc)
                products_col_dict["prod_category"].append(p_cat)
                products_col_dict["prod_category_id"].append(p_cat_id)
                products_col_dict["prod_category_desc"].append(p_cat_desc)
                products_col_dict["prod_weight_class"].append(p_w_class)
                products_col_dict["supplier_id"].append(supp_id)
                products_col_dict["prod_status"].append(p_status)
                products_col_dict["prod_list_price"].append(p_list)
                products_col_dict["prod_min_price"].append(p_min)

        if products_col_dict["prod_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE products_ext")
            # Creating Dataframe
            # Persisting into db
            df_countries = pd.DataFrame(products_col_dict)
            df_countries.to_sql("products_ext", ses_db_stg, if_exists="append", index=False)
    except:
        traceback.print_exc()
    finally:
        pass
