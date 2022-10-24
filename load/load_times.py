import configparser
import traceback

import pandas as pd

from util import db_connection

config = configparser.ConfigParser()
config.read(".properties")
# Creating a new db ses_db_sor object
sectionName = "DatabaseSection"
sor_conn = db_connection.Db_Connection(
    config.get(sectionName, "DB_TYPE"),
    config.get(sectionName, "DB_HOST"),
    config.get(sectionName, "DB_PORT"),
    config.get(sectionName, "DB_USER"),
    config.get(sectionName, "DB_PWD"),
    config.get(sectionName, "SOR_NAME"),
)
stg_conn = db_connection.Db_Connection(
    config.get(sectionName, "DB_TYPE"),
    config.get(sectionName, "DB_HOST"),
    config.get(sectionName, "DB_PORT"),
    config.get(sectionName, "DB_USER"),
    config.get(sectionName, "DB_PWD"),
    config.get(sectionName, "STG_NAME"),
)
cvsSectionName = "CSVSection"


# Db stays the same
def load_times(curr_cod_etl, ses_db_stg, ses_db_sor):
    try:

        # Dictionary of values

        times_col_dict = {
            "time_id": [],
            "day_name": [],
            "day_number_in_week": [],
            "day_number_in_month": [],
            "calendar_week_number": [],
            "calendar_month_number": [],
            "calendar_month_desc": [],
            "end_of_cal_month": [],
            "calendar_month_name": [],
            "calendar_quarter_desc": [],
            "calendar_year": [],
            'cod_etl': [],
        }

        # Read extraction table
        times_tra = pd.read_sql(
            "SELECT time_id,day_name, day_number_in_week, day_number_in_month,calendar_week_number,calendar_month_number,calendar_month_desc,end_of_cal_month,calendar_month_name,calendar_quarter_desc,calendar_year FROM times_tra",
            ses_db_stg,
        )
        # Processing rows
        if not times_tra.empty:
            for (
                    t_id,
                    t_day_name,
                    t_day_number_wk,
                    t_day_number_mth,
                    t_cal_wk_number,
                    t_cal_mth_number,
                    t_cal_mth_desc,
                    t_eocal_mth,
                    t_cal_mth_name,
                    t_cal_qua_desc,
                    t_cal_yr,
            ) in zip(
                times_tra["time_id"],
                times_tra["day_name"],
                times_tra["day_number_in_week"],
                times_tra["day_number_in_month"],
                times_tra["calendar_week_number"],
                times_tra["calendar_month_number"],
                times_tra["calendar_month_desc"],
                times_tra["end_of_cal_month"],
                times_tra["calendar_month_name"],
                times_tra["calendar_quarter_desc"],
                times_tra["calendar_year"],
            ):
                times_col_dict["time_id"].append(t_id)
                times_col_dict["day_name"].append(t_day_name)
                times_col_dict["day_number_in_week"].append(t_day_number_wk)
                times_col_dict["day_number_in_month"].append(t_day_number_mth
                                                             )
                times_col_dict["calendar_week_number"].append(t_cal_wk_number
                                                              )
                times_col_dict["calendar_month_number"].append(t_cal_mth_number
                                                               )
                times_col_dict["calendar_month_desc"].append(t_cal_mth_desc
                                                             )
                times_col_dict["end_of_cal_month"].append(t_eocal_mth)
                times_col_dict["calendar_month_name"].append(t_cal_mth_name
                                                             )
                times_col_dict["calendar_quarter_desc"].append(t_cal_qua_desc
                                                               )
                times_col_dict["calendar_year"].append(t_cal_yr)
                times_col_dict['cod_etl'].append(curr_cod_etl)

        if times_col_dict["time_id"]:
            # Creating Dataframe
            # Persisting into db
            df_times = pd.DataFrame(times_col_dict)
            df_times.to_sql("dim_times", ses_db_sor, if_exists="append", index=False)

    except:
        traceback.print_exc()
    finally:
        pass
