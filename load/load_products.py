import traceback

import pandas as pd


def load_products(curr_cod_etl, ses_db_stg, ses_db_sor):
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

        product_tra = pd.read_sql(
            "SELECT prod_id,prod_name, prod_desc, prod_category,prod_category_id,prod_category_desc,prod_weight_class,supplier_id,prod_status,prod_list_price,prod_min_price FROM products_tra",
            ses_db_stg,
        )
        if not product_tra.empty:
            for (
                    p_id,
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
                product_tra["prod_id"],
                product_tra["prod_name"],
                product_tra["prod_desc"],
                product_tra["prod_category"],
                product_tra["prod_category_id"],
                product_tra["prod_category_desc"],
                product_tra["prod_weight_class"],
                product_tra["supplier_id"],
                product_tra["prod_status"],
                product_tra["prod_list_price"],
                product_tra["prod_min_price"],
            ):
                products_col_dict["prod_id"].append(p_id)
                products_col_dict["prod_name"].append(p_name)
                products_col_dict["prod_desc"].append(p_desc)
                products_col_dict["prod_category"].append(p_cat)
                products_col_dict["prod_category_id"].append(p_cat_id)
                products_col_dict["prod_category_desc"].append(p_cat_desc)
                products_col_dict["prod_weight_class"].append(p_w_class)
                products_col_dict["supplier_id"].append(p_supp_id)
                products_col_dict["prod_status"].append(p_status)
                products_col_dict["prod_list_price"].append(p_list)
                products_col_dict["prod_min_price"].append(p_min)
                products_col_dict["cod_etl"].append(curr_cod_etl)
        if products_col_dict["prod_id"]:
            # Creating Dataframe
            # Persisting into db
            df_products = pd.DataFrame(products_col_dict)
            df_products.to_sql(
                "dim_products", ses_db_sor, if_exists="append", index=False
            )
    except:
        traceback.print_exc()
    finally:
        pass
