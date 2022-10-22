from time import time
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


def ext_times():
    try:
        # Connecting db
        conn = stg_conn.start()
        if conn == -1:
            raise Exception(f"The database type {stg_conn.type} is not valid")
        elif conn == -2:
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

        # Read CSV
        times_csv = pd.read_csv(config.get(cvsSectionName, "TIMES_PATH"))

        # Processing CSV content
        if not times_csv.empty:
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
                times_csv["TIME_ID"],
                times_csv["DAY_NAME"],
                times_csv["DAY_NUMBER_IN_WEEK"],
                times_csv["DAY_NUMBER_IN_MONTH"],
                times_csv["CALENDAR_WEEK_NUMBER"],
                times_csv["CALENDAR_MONTH_NUMBER"],
                times_csv["CALENDAR_MONTH_DESC"],
                times_csv["END_OF_CAL_MONTH"],
                times_csv["CALENDAR_QUARTER_DESC"],
                times_csv["CALENDAR_YEAR"],
            ):

                times_col_dict["time_id"].append(id)
                times_col_dict["day_name"].append(t_day_n)
                times_col_dict["day_number_in_week"].append(t_day_nbr_w)
                times_col_dict["day_number_in_month"].append(t_day_nbr_m)
                times_col_dict["calendar_week_number"].append(t_c_w_n)
                times_col_dict["calendar_month_number"].append(t_c_m_nbr)
                times_col_dict["calendar_month_desc"].append(t_c_m_desc)
                times_col_dict["end_of_cal_month"].append(t_end)
                times_col_dict["calendar_quarter_desc"].append(t_qua_desc)
                times_col_dict["calendar_year"].append(t_cal_yr)

        if times_col_dict["time_id"]:
            conn.connect().execute("TRUNCATE TABLE times_ext")
            # Creating Dataframe
            # Persisting into db
            df_countries = pd.DataFrame(times_col_dict)
            df_countries.to_sql("times_ext", conn, if_exists="append", index=False)
            # Dispose db connection
            conn.dispose()
    except:
        traceback.print_exc()
    finally:
        pass
