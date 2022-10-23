from configparser import ConfigParser
import configparser
from datetime import date
from transform.transform_chann import tran_chann
from transform.transform_products import tran_products
from transform.transform_promotions import tran_promotions
from transform.transform_countries import tran_countries
from transform.transform_customers import tran_customers
from transform.transform_sales import tran_sales
from transform.transform_times import tran_times
import pandas as pd

from util.db_connection import Db_Connection


config = configparser.ConfigParser()
config.read(".properties")
config.get("DatabaseSection", "DB_TYPE")
# Creating a new db conn object
sectionName = "DatabaseSection"
stg_conn = Db_Connection(
    config.get(sectionName, "DB_TYPE"),
    config.get(sectionName, "DB_HOST"),
    config.get(sectionName, "DB_PORT"),
    config.get(sectionName, "DB_USER"),
    config.get(sectionName, "DB_PWD"),
    config.get(sectionName, "STG_NAME"),
)


def tran_all_tables():
    # insertar en etl
    # sacar el cod
    # mandar
    ses_db_stg = stg_conn.start()

    # etl_code = pd.read_sql()

    colummns_dict = {
        "created_at": [],
    }
    colummns_dict["created_at"].append(date.today())
    etl_df = pd.DataFrame(colummns_dict)
    etl_df.to_sql("etl_proc", ses_db_stg, if_exists="append", index=False)

    etl_curr_code = pd.read_sql(
        "SELECT cod_etl FROM etl_proc ORDER BY cod_etl DESC LIMIT 1 ",
        ses_db_stg,
    )

    curr_etl_code = etl_curr_code["cod_etl"][0]

    print(type(curr_etl_code))

    tran_chann(curr_etl_code)
    tran_countries(curr_etl_code)
    tran_customers(curr_etl_code)
    tran_products(curr_etl_code)
    tran_promotions(curr_etl_code)
    tran_sales(curr_etl_code)
    tran_times(curr_etl_code)
