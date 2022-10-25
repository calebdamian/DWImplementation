from transform.transform_chann import tran_chann
from transform.transform_countries import tran_countries
from transform.transform_customers import tran_customers
from transform.transform_products import tran_products
from transform.transform_promotions import tran_promotions
from transform.transform_sales import tran_sales
from transform.transform_times import tran_times


def transform_all_tables(curr_etl_code, ses_db_stg):
    print("Transforming channels...")
    tran_chann(curr_etl_code, ses_db_stg=ses_db_stg)
    print("Transforming countries...")
    tran_countries(curr_etl_code, ses_db_stg=ses_db_stg)
    print("Transforming customers...")
    tran_customers(curr_etl_code, ses_db_stg=ses_db_stg)
    print("Transforming products...")
    tran_products(curr_etl_code, ses_db_stg=ses_db_stg)
    print("Transforming promotions...")
    tran_promotions(curr_etl_code, ses_db_stg=ses_db_stg)
    print("Transforming times...")
    tran_times(curr_etl_code, ses_db_stg=ses_db_stg)
    print("Transforming sales...")
    tran_sales(curr_etl_code, ses_db_stg=ses_db_stg)
