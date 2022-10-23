import traceback
from transform.transformations import (
    get_month_name,
    str_to_date,
    str_to_int,
    str_to_str_w_length,
)
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

# Db stays the same
def tran_times(curr_cod_etl):
    try:

        # Connecting db
        ses_db_stg = stg_conn.start()
        if ses_db_stg == -1:
            raise Exception(f"The database type {stg_conn.type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to cdnastaging")

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
        times_ext = pd.read_sql(
            "SELECT TIME_ID,DAY_NAME, DAY_NUMBER_IN_WEEK, DAY_NUMBER_IN_MONTH,CALENDAR_WEEK_NUMBER,CALENDAR_MONTH_NUMBER,CALENDAR_MONTH_DESC,END_OF_CAL_MONTH,CALENDAR_QUARTER_DESC,CALENDAR_YEAR FROM times_ext",
            ses_db_stg,
        )
        # Processing rows
        if not times_ext.empty:
            for (
                t_id,
                t_day_name,
                t_day_number_wk,
                t_day_number_mth,
                t_cal_wk_number,
                t_cal_mth_number,
                t_cal_mth_desc,
                t_eocal_mth,
                t_cal_qua_desc,
                t_cal_yr,
            ) in zip(
                times_ext["TIME_ID"],
                times_ext["DAY_NAME"],
                times_ext["DAY_NUMBER_IN_WEEK"],
                times_ext["DAY_NUMBER_IN_MONTH"],
                times_ext["CALENDAR_WEEK_NUMBER"],
                times_ext["CALENDAR_MONTH_NUMBER"],
                times_ext["CALENDAR_MONTH_DESC"],
                times_ext["END_OF_CAL_MONTH"],
                times_ext["CALENDAR_QUARTER_DESC"],
                times_ext["CALENDAR_YEAR"],
            ):

                times_col_dict["time_id"].append(str_to_date(t_id))
                times_col_dict["day_name"].append(str_to_str_w_length(t_day_name, 9))
                times_col_dict["day_number_in_week"].append(str_to_int(t_day_number_wk))
                times_col_dict["day_number_in_month"].append(
                    str_to_int(t_day_number_mth)
                )
                times_col_dict["calendar_week_number"].append(
                    str_to_int(t_cal_wk_number)
                )
                times_col_dict["calendar_month_number"].append(
                    str_to_int(t_cal_mth_number)
                )
                times_col_dict["calendar_month_desc"].append(
                    str_to_str_w_length(t_cal_mth_desc, 8)
                )
                times_col_dict["end_of_cal_month"].append(str_to_date(t_eocal_mth))
                times_col_dict["calendar_month_name"].append(
                    get_month_name(t_cal_mth_number)
                )
                times_col_dict["calendar_quarter_desc"].append(
                    str_to_str_w_length(t_cal_qua_desc, 7)
                )
                times_col_dict["calendar_year"].append(str_to_int(t_cal_yr))
                times_col_dict['cod_etl'].append(curr_cod_etl)

        if times_col_dict["time_id"]:
            # Creating Dataframe
            # Persisting into db
            df_times = pd.DataFrame(times_col_dict)
            df_times.to_sql("times_tra", ses_db_stg, if_exists="append", index=False)
            # Dispose db connection
            ses_db_stg.dispose()
    except:
        traceback.print_exc()
    finally:
        pass
