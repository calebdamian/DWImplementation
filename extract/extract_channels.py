import configparser
import traceback

import pandas as pd

config = configparser.ConfigParser()
config.read(".properties")
config.get("DatabaseSection", "DB_TYPE")
cvsSectionName = "CSVSection"


def ext_channels(ses_db_stg):
    try:
        # Dictionary of values

        colummns_dict = {
            "channel_id": [],
            "channel_desc": [],
            "channel_class": [],
            "channel_class_id": [],
        }

        # Read CSV
        channel_csv = pd.read_csv(config.get(cvsSectionName, "CHANNELS_PATH"))

        # Processing CSV content
        if not channel_csv.empty:
            for id, desc, ch_class, ch_class_id in zip(
                    channel_csv["CHANNEL_ID"],
                    channel_csv["CHANNEL_DESC"],
                    channel_csv["CHANNEL_CLASS"],
                    channel_csv["CHANNEL_CLASS_ID"],
            ):
                colummns_dict["channel_id"].append(id)
                colummns_dict["channel_desc"].append(desc)
                colummns_dict["channel_class"].append(ch_class)
                colummns_dict["channel_class_id"].append(ch_class_id)
        if colummns_dict["channel_id"]:
            ses_db_stg.connect().execute("TRUNCATE TABLE channels_ext")
            # Creating Dataframe
            # Persisting into db
            df_channels = pd.DataFrame(colummns_dict)
            df_channels.to_sql("channels_ext", ses_db_stg, if_exists="append", index=False)
            # Dispose db connection

    except:
        traceback.print_exc()
    finally:
        pass
