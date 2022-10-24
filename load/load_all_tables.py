from datetime import date

import pandas as pd

from load.load_channels import load_channels
from load.load_countries import load_countries
from load.load_customers import load_customers
from load.load_products import load_products
from load.load_promotions import load_promotions
from load.load_sales import load_sales
from load.load_times import load_times


def load_all_tables(ses_db_stg, ses_db_sor):
    etl_dict = {
        "created_at": [],
    }
    etl_dict["created_at"].append(date.today())
    etl_df = pd.DataFrame(etl_dict)
    etl_df.to_sql("etl_proc", ses_db_stg, if_exists="append", index=False)

    etl_curr_code = pd.read_sql(
        "SELECT cod_etl FROM etl_proc ORDER BY cod_etl DESC LIMIT 1 ",
        ses_db_stg,
    )

    curr_etl_code = etl_curr_code["cod_etl"][0]

    print("Loading channels...")
    load_channels(curr_etl_code, ses_db_stg, ses_db_sor)
    print("Loading countries...")
    load_countries(curr_etl_code, ses_db_stg, ses_db_sor)
    print("Loading customers...")
    load_customers(curr_etl_code, ses_db_stg, ses_db_sor)
    print("Loading products...")
    load_products(curr_etl_code, ses_db_stg, ses_db_sor)
    print("Loading promotions...")
    load_promotions(curr_etl_code, ses_db_stg, ses_db_sor)
    print("Loading sales...")
    load_sales(curr_etl_code, ses_db_stg, ses_db_sor)
    print("Loading times...")
    load_times(curr_etl_code, ses_db_stg, ses_db_sor)
