from extract.extract_channels import ext_channels
from extract.extract_countries import ext_countries
from extract.extract_customers import ext_customers
from extract.extract_products import ext_products
from extract.extract_promotions import ext_promotions
from extract.extract_sales import ext_sales
from extract.extract_times import ext_times


def extract_all_tables(ses_db_stg):
    print("Extracting channels...")
    ext_channels(ses_db_stg)
    print("Extracting countries...")
    ext_countries(ses_db_stg)
    print("Extracting customers...")
    ext_customers(ses_db_stg)
    print("Extracting products...")
    ext_products(ses_db_stg)
    print("Extracting promotions...")
    ext_promotions(ses_db_stg)
    print("Extracting sales...")
    ext_sales(ses_db_stg)
    print("Extracting times...")
    ext_times(ses_db_stg)
