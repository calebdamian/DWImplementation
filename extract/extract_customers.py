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


def ext_customers():
    try:
        # Connecting db
        conn = stg_conn.start()
        if conn == -1:
            raise Exception(f"The database type {stg_conn.type} is not valid")
        elif conn == -2:
            raise Exception("Error trying to connect to cdnastaging")

        # Dictionary of values

        customers_col_dict = {
            "cust_id": [],
            "cust_first_name": [],
            "cust_last_name": [],
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
        }

        # Read CSV
        customers_csv = pd.read_csv(config.get(cvsSectionName, "CUSTOMERS_PATH"))

        # Processing CSV content
        if not customers_csv.empty:
            for (
                cust_id,
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
                customers_csv["CUST_ID"],
                customers_csv["CUST_FIRST_NAME"],
                customers_csv["CUST_LAST_NAME"],
                customers_csv["CUST_GENDER"],
                customers_csv["CUST_YEAR_OF_BIRTH"],
                customers_csv["CUST_MARITAL_STATUS"],
                customers_csv["CUST_STREET_ADDRESS"],
                customers_csv["CUST_POSTAL_CODE"],
                customers_csv["CUST_CITY"],
                customers_csv["CUST_STATE_PROVINCE"],
                customers_csv["COUNTRY_ID"],
                customers_csv["CUST_MAIN_PHONE_NUMBER"],
                customers_csv["CUST_INCOME_LEVEL"],
                customers_csv["CUST_CREDIT_LIMIT"],
                customers_csv["CUST_EMAIL"],
            ):

                customers_col_dict["cust_id"].append(cust_id)
                customers_col_dict["cust_first_name"].append(f_name)
                customers_col_dict["cust_last_name"].append(last_name)
                customers_col_dict["cust_gender"].append(gender)
                customers_col_dict["cust_year_of_birth"].append(y_birth)
                customers_col_dict["cust_marital_status"].append(marital_status)
                customers_col_dict["cust_street_address"].append(street)
                customers_col_dict["cust_postal_code"].append(postal)
                customers_col_dict["cust_city"].append(city)
                customers_col_dict["cust_state_province"].append(state_province)
                customers_col_dict["country_id"].append(country_id)
                customers_col_dict["cust_main_phone_number"].append(phone_number)
                customers_col_dict["cust_income_level"].append(income)
                customers_col_dict["cust_credit_limit"].append(credit)
                customers_col_dict["cust_email"].append(email)

        if customers_col_dict["cust_id"]:
            conn.connect().execute("TRUNCATE TABLE customers_ext")
            # Creating Dataframe
            # Persisting into db
            df_countries = pd.DataFrame(customers_col_dict)
            df_countries.to_sql("customers_ext", conn, if_exists="append", index=False)
            # Dispose db connection
            conn.dispose()
    except:
        traceback.print_exc()
    finally:
        pass
