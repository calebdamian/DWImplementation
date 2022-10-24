from datetime import date

import pandas as pd


def get_curr_etl_code(ses_db_stg):
    etl_dict = {
        "created_at": [],
    }
    etl_dict["created_at"].append(date.today())
    etl_df = pd.DataFrame(etl_dict)
    etl_df.to_sql("etl_proc", ses_db_stg, if_exists="append", index=False)

    # get last etl_code available
    etl_curr_code = pd.read_sql(
        "SELECT cod_etl FROM etl_proc ORDER BY cod_etl DESC LIMIT 1 ",
        ses_db_stg,
    )

    curr_etl_code = etl_curr_code["cod_etl"][0]

    return curr_etl_code
