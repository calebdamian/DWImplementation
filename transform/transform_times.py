import traceback
from transform.transformations import str_to_date, str_to_int, str_to_str_w_length
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
def tran_times():
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
            "calendar_quarter_desc": [],
            "calendar_year": [],
        }

        # Read extraction table
        times_ext = pd.read_sql(
            "SELECT TIME_ID,DAY_NAME, DAY_NUMBER_IN_WEEK, DAY_NUMBER_IN_MONTH,CALENDAR_WEEK_NUMBER,CALENDAR_MONTH_NUMBER,CALENDAR_MONTH_DESC,END_OF_CAL_MONTH,CALENDAR_QUARTER_DESC,CALENDAR_YEAR FROM times_ext",
            ses_db_stg,
        )
        # Processing rows
        if not times_ext.empty:
            for (
                id,
                t_day_n,
                t_day_nbr_w,
                t_day_nbr_m,
                t_c_w_n,
                t_c_m_nbr,
                t_c_m_desc,
                t_end,
                t_qua_desc,
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

                times_col_dict["time_id"].append(str_to_int(id))
                times_col_dict["day_name"].append(str_to_str_w_length(t_day_n, 9))
                times_col_dict["day_number_in_week"].append(str_to_int(t_day_nbr_w))
                times_col_dict["day_number_in_month"].append(str_to_int(t_day_nbr_m))
                times_col_dict["calendar_week_number"].append(str_to_int(t_c_w_n))
                times_col_dict["calendar_month_number"].append(str_to_int(t_c_m_nbr))
                times_col_dict["calendar_month_desc"].append(
                    str_to_str_w_length(t_c_m_desc, 8)
                )
                times_col_dict["end_of_cal_month"].append(str_to_date(t_end))
                times_col_dict["calendar_quarter_desc"].append(
                    str_to_str_w_length(t_qua_desc, 7)
                )
                times_col_dict["calendar_year"].append(str_to_int(t_cal_yr))

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
