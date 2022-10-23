import traceback
from transform.transformations import str_to_int, str_to_str_w_length
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
def tran_chann():
    try:

        # Connecting db
        ses_db_stg = stg_conn.start()
        if ses_db_stg == -1:
            raise Exception(f"The database type {stg_conn.type} is not valid")
        elif ses_db_stg == -2:
            raise Exception("Error trying to connect to cdnastaging")

        # Dictionary of values

        colummns_dict = {
            "channel_id": [],
            "channel_desc": [],
            "channel_class": [],
            "channel_class_id": [],
            "cod_etl": [],
        }

        # Read extraction table
        channel_ext = pd.read_sql(
            "SELECT CHANNEL_ID,CHANNEL_DESC, CHANNEL_CLASS, CHANNEL_CLASS_ID FROM channels_ext",
            ses_db_stg,
        )
        """ etl_code_df = pd.read_sql(
            "SELECT * FROM proc_etl ORDER BY cod_etl DESC LIMIT 1", ses_db_stg
        )"""
        """curr_etl_code = ses_db_stg.execute(
            "SELECT * FROM proc_etl ORDER BY cod_etl DESC LIMIT 1"
        )
        if curr_etl_code is None:
            # etl_code["cod_etl"].append(1)
            # etl_code.to_sql("proc_etl", ses_db_stg, if_exists="append")
            ses_db_stg.execute("INSERT INTO proc_etl(cod_etl) VALUES (1)")
            curr_etl_code = ses_db_stg.execute(
                "SELECT * FROM proc_etl ORDER BY cod_etl DESC LIMIT 1"
            )
        else:
            for Data in curr_etl_code.fetchall():
                item = Data[0]
            val = int(item)
            print(val + 1)
            ses_db_stg.execute("insert into proc_etl(cod_etl) values (%d)", (int(val)))
        # etl_code.drop(columns="cod_etl", axis=1, inplace=True)
        # etl_code["cod_etl"].append(int_curr_etl_code)
        # etl_code.to_sql("proc_etl", ses_db_stg, if_exists="append")
        print(curr_etl_code)
        """
        # Processing rows
        if not channel_ext.empty:
            for ch_id, desc, ch_class, ch_class_id in zip(
                channel_ext["CHANNEL_ID"],
                channel_ext["CHANNEL_DESC"],
                channel_ext["CHANNEL_CLASS"],
                channel_ext["CHANNEL_CLASS_ID"],
            ):
                colummns_dict["channel_id"].append(str_to_int(ch_id))
                colummns_dict["channel_desc"].append(str_to_str_w_length(desc, 20))
                colummns_dict["channel_class"].append(str_to_str_w_length(ch_class, 20))
                colummns_dict["channel_class_id"].append(str_to_int(ch_class_id))
                # colummns_dict["cod_etl"].append(curr_etl_code)

        if colummns_dict["channel_id"]:
            # Creating Dataframe
            # Persisting into db
            # colummns_dict["cod_etl"].append(curr_etl_code)
            df_ch_tra = pd.DataFrame(colummns_dict)
            df_ch_tra.to_sql(
                "channels_tra", ses_db_stg, if_exists="append", index=False
            )

            # Dispose db connection
            ses_db_stg.dispose()
    except:
        traceback.print_exc()
    finally:
        pass
