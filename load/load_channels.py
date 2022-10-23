import traceback
from transform.transformations import str_to_int, str_to_str_w_length
from util import db_ses_db_sorection
import pandas as pd
import configparser

from util.db_connection import Db_Connection


config = configparser.ConfigParser()
config.read(".properties")
# Creating a new db ses_db_sor object
sectionName = "DatabaseSection"
sor_conn = Db_Connection(
    config.get(sectionName, "DB_TYPE"),
    config.get(sectionName, "DB_HOST"),
    config.get(sectionName, "DB_PORT"),
    config.get(sectionName, "DB_USER"),
    config.get(sectionName, "DB_PWD"),
    config.get(sectionName, "SOR_NAME"),
)
stg_conn = Db_Connection(
    config.get(sectionName, "DB_TYPE"),
    config.get(sectionName, "DB_HOST"),
    config.get(sectionName, "DB_PORT"),
    config.get(sectionName, "DB_USER"),
    config.get(sectionName, "DB_PWD"),
    config.get(sectionName, "STG_NAME"),
)
cvsSectionName = "CSVSection"

# Db changes to be SOR
def load_channels():
    try:

        # Using both dbs
        ses_db_sor = sor_conn.start()
        ses_db_stg = stg_conn.start()
        if ses_db_sor == -1:
            raise Exception(f"The database type {ses_db_sor.type} is not valid")
        elif ses_db_sor == -2:
            raise Exception("Error trying to ses_db_sorect to cdnastaging")

        if ses_db_stg == -1:
            raise Exception(f"The database type {ses_db_stg.type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to ses_db_sorect to cdnastaging")

        # Dictionary of values

        colummns_dict = {
            "channel_id": [],
            "channel_desc": [],
            "channel_class": [],
            "channel_class_id": [],
        }

        # Read extraction table
        channel_tra = pd.read_sql(
            "SELECT CHANNEL_ID,CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID FROM channels_tra",
            ses_db_stg,
        )

        # Processing rows
        if not channel_tra.empty:
            for ch_id, ch_desc, ch_class, ch_class_id in zip(
                channel_tra["CHANNEL_ID"],
                channel_tra["CHANNEL_DESC"],
                channel_tra["CHANNEL_CLASS"],
                channel_tra["CHANNEL_CLASS_ID"],
            ):
                colummns_dict["channel_id"].append(ch_id)
                colummns_dict["channel_desc"].append(ch_desc)
                colummns_dict["channel_class"].append(ch_class)
                colummns_dict["channel_class_id"].append(ch_class_id)
        if colummns_dict["channel_id"]:
            # Creating Dataframe
            # Persisting into db
            # Loading
            df_ch_tra = pd.DataFrame(colummns_dict)
            df_ch_tra.to_sql(
                "dim_channels", ses_db_sor, if_exists="append", index=False
            )
            # Dispose dbs conn
            ses_db_stg.dispose()
            ses_db_sor.dispose()
    except:
        traceback.print_exc()
    finally:
        pass
