import traceback

import pandas as pd

from transform.transformations import str_to_char, str_to_int, str_to_str_w_length, join_2_strings


# Db stays the same
def tran_customers(curr_cod_etl, ses_db_stg):
    try:

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
        customers_ext = pd.read_sql(
            "SELECT CUST_ID,CUST_FIRST_NAME, CUST_LAST_NAME, CUST_GENDER,CUST_YEAR_OF_BIRTH,CUST_MARITAL_STATUS,CUST_STREET_ADDRESS,CUST_POSTAL_CODE,CUST_CITY,CUST_STATE_PROVINCE,COUNTRY_ID,CUST_MAIN_PHONE_NUMBER,CUST_INCOME_LEVEL,CUST_CREDIT_LIMIT,CUST_EMAIL FROM customers_ext",
            ses_db_stg,
        )
        # Processing rows
        if not customers_ext.empty:
            for (
                    id,
                    f_name,
                    last_name,
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
                customers_ext["CUST_ID"],
                customers_ext["CUST_FIRST_NAME"],
                customers_ext["CUST_LAST_NAME"],
                customers_ext["CUST_GENDER"],
                customers_ext["CUST_YEAR_OF_BIRTH"],
                customers_ext["CUST_MARITAL_STATUS"],
                customers_ext["CUST_STREET_ADDRESS"],
                customers_ext["CUST_POSTAL_CODE"],
                customers_ext["CUST_CITY"],
                customers_ext["CUST_STATE_PROVINCE"],
                customers_ext["COUNTRY_ID"],
                customers_ext["CUST_MAIN_PHONE_NUMBER"],
                customers_ext["CUST_INCOME_LEVEL"],
                customers_ext["CUST_CREDIT_LIMIT"],
                customers_ext["CUST_EMAIL"],
            ):
                customers_col_dict["cust_id"].append(str_to_int(id))
                customers_col_dict["cust_name"].append(
                    join_2_strings(f_name, last_name)
                )
                customers_col_dict["cust_gender"].append(str_to_char(gender))
                customers_col_dict["cust_year_of_birth"].append(str_to_int(y_birth))
                customers_col_dict["cust_marital_status"].append(
                    str_to_str_w_length(marital_status, 20)
                )
                customers_col_dict["cust_street_address"].append(
                    str_to_str_w_length(street, 40)
                )
                customers_col_dict["cust_postal_code"].append(
                    str_to_str_w_length(postal, 10)
                )
                customers_col_dict["cust_city"].append(str_to_str_w_length(city, 30))
                customers_col_dict["cust_state_province"].append(
                    str_to_str_w_length(state_province, 40)
                )
                customers_col_dict["country_id"].append(str_to_int(country_id))
                customers_col_dict["cust_main_phone_number"].append(
                    str_to_str_w_length(phone_number, 25)
                )
                customers_col_dict["cust_income_level"].append(
                    str_to_str_w_length(income, 30)
                )
                customers_col_dict["cust_credit_limit"].append(str_to_int(credit))
                customers_col_dict["cust_email"].append(str_to_str_w_length(email, 30))
                customers_col_dict["cod_etl"].append(curr_cod_etl)

        if customers_col_dict["cust_id"]:
            # Creating Dataframe
            # Persisting into db
            df_customers = pd.DataFrame(customers_col_dict)
            df_customers.to_sql(
                "customers_tra", ses_db_stg, if_exists="append", index=False
            )
            # Dispose db connection
            ses_db_stg.dispose()
    except:
        traceback.print_exc()
    finally:
        pass
