import traceback

import pandas as pd


# Db stays the same
def load_customers(curr_cod_etl, ses_db_stg, ses_db_sor):
    try:

        # Dictionary of values

        customers_col_dict = {
            "cust_id": [],
            "cust_name": [],
            "cust_gender": [],
            "cust_year_of_birth": [],
            "cust_marital_status": [],
            "cust_street_address": [],
            "cust_postal_code": [],
            "cust_city": [],
            "cust_state_province": [],
            "country_id": [],
            "cust_main_phone_number": [],
            "cust_income_level": [],
            "cust_credit_limit": [],
            "cust_email": [],
            "cod_etl": [],
        }

        # Read extraction table
        customers_tra = pd.read_sql(
            "SELECT cust_id,cust_name,cust_gender,cust_year_of_birth,cust_marital_status,cust_street_address,cust_postal_code,cust_city,cust_state_province,country_id,cust_main_phone_number,cust_income_level,cust_credit_limit,cust_email FROM customers_tra",
            ses_db_stg,
        )
        # Processing rows
        if not customers_tra.empty:
            for (
                    cus_id,
                    name,
                    gender,
                    y_birth,
                    marital_status,
                    street,
                    postal,
                    city,
                    state_province,
                    country_id,
                    phone_number,
                    income,
                    credit,
                    email,
            ) in zip(
                customers_tra["cust_id"],
                customers_tra["cust_name"],
                customers_tra["cust_gender"],
                customers_tra["cust_year_of_birth"],
                customers_tra["cust_marital_status"],
                customers_tra["cust_street_address"],
                customers_tra["cust_postal_code"],
                customers_tra["cust_city"],
                customers_tra["cust_state_province"],
                customers_tra["country_id"],
                customers_tra["cust_main_phone_number"],
                customers_tra["cust_income_level"],
                customers_tra["cust_credit_limit"],
                customers_tra["cust_email"],
            ):
                customers_col_dict["cust_id"].append(cus_id)
                customers_col_dict["cust_name"].append(
                    name
                )
                customers_col_dict["cust_gender"].append(gender)
                customers_col_dict["cust_year_of_birth"].append(y_birth)
                customers_col_dict["cust_marital_status"].append(marital_status
                                                                 )
                customers_col_dict["cust_street_address"].append(street
                                                                 )
                customers_col_dict["cust_postal_code"].append(postal
                                                              )
                customers_col_dict["cust_city"].append(city)
                customers_col_dict["cust_state_province"].append(state_province
                                                                 )
                customers_col_dict["country_id"].append(country_id)
                customers_col_dict["cust_main_phone_number"].append(phone_number
                                                                    )
                customers_col_dict["cust_income_level"].append(income
                                                               )
                customers_col_dict["cust_credit_limit"].append(credit)
                customers_col_dict["cust_email"].append(email)
                customers_col_dict["cod_etl"].append(curr_cod_etl)

        if customers_col_dict["cust_id"]:
            # Creating Dataframe
            # Persisting into db
            df_customers = pd.DataFrame(customers_col_dict)
            df_customers.to_sql(
                "dim_customers", ses_db_sor, if_exists="append", index=False
            )
    except:
        traceback.print_exc()
    finally:
        pass
