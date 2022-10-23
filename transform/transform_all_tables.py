from transform.transform_chann import tran_chann
from transform.transform_products import tran_products
from transform.transform_promotions import tran_promotions
from transform.transform_countries import tran_countries
from transform.transform_customers import tran_customers
from transform.transform_sales import tran_sales
from transform.transform_times import tran_times


def tran_all_tables():
    tran_chann()
    tran_countries()
    tran_customers()
    tran_products()
    tran_promotions()
    tran_sales()
    tran_times()
