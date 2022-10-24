import traceback

import pandas as pd

from transform.transformations import str_to_float, str_to_int, str_to_str_w_length


# Db stays the same
def tran_products(curr_cod_etl, ses_db_stg):
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
            "cod_etl": [],
        }

        # Read extraction table
        product_ext = pd.read_sql(
            "SELECT PROD_ID,PROD_NAME, PROD_DESC, PROD_CATEGORY,PROD_CATEGORY_ID,PROD_CATEGORY_DESC,PROD_WEIGHT_CLASS,SUPPLIER_ID,PROD_STATUS,PROD_LIST_PRICE,PROD_MIN_PRICE FROM products_ext",
            ses_db_stg,
        )
        # Processing rows
        if not product_ext.empty:
            for (
                    id,
                    p_name,
                    p_desc,
                    p_cat,
                    p_cat_id,
                    p_cat_desc,
                    p_w_class,
                    p_supp_id,
                    p_status,
                    p_list,
                    p_min,
            ) in zip(
                product_ext["PROD_ID"],
                product_ext["PROD_NAME"],
                product_ext["PROD_DESC"],
                product_ext["PROD_CATEGORY"],
                product_ext["PROD_CATEGORY_ID"],
                product_ext["PROD_CATEGORY_DESC"],
                product_ext["PROD_WEIGHT_CLASS"],
                product_ext["SUPPLIER_ID"],
                product_ext["PROD_STATUS"],
                product_ext["PROD_LIST_PRICE"],
                product_ext["PROD_MIN_PRICE"],
            ):
                products_col_dict["prod_id"].append(str_to_int(id))
                products_col_dict["prod_name"].append(str_to_str_w_length(p_name, 50))
                products_col_dict["prod_desc"].append(str_to_str_w_length(p_desc, 4000))
                products_col_dict["prod_category"].append(
                    str_to_str_w_length(p_cat, 50)
                )
                products_col_dict["prod_category_id"].append(str_to_int(p_cat_id))
                products_col_dict["prod_category_desc"].append(
                    str_to_str_w_length(p_cat_desc, 2000)
                )
                products_col_dict["prod_weight_class"].append(str_to_int(p_w_class))
                products_col_dict["supplier_id"].append(str_to_int(p_supp_id))
                products_col_dict["prod_status"].append(
                    str_to_str_w_length(p_status, 20)
                )
                products_col_dict["prod_list_price"].append(str_to_float(p_list))
                products_col_dict["prod_min_price"].append(str_to_float(p_min))
                products_col_dict["cod_etl"].append(curr_cod_etl)

        if products_col_dict["prod_id"]:
            # Creating Dataframe
            # Persisting into db
            df_products = pd.DataFrame(products_col_dict)
            df_products.to_sql(
                "products_tra", ses_db_stg, if_exists="append", index=False
            )
            # Dispose db connection
            ses_db_stg.dispose()
    except:
        traceback.print_exc()
    finally:
        pass
