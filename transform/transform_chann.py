import traceback

import pandas as pd

from transform.transformations import str_to_int, str_to_str_w_length


# Db stays the same
def tran_chann(curr_cod_etl, ses_db_stg):
    try:

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
                colummns_dict["cod_etl"].append(curr_cod_etl)

        if colummns_dict["channel_id"]:
            # Creating Dataframe
            # Persisting into db
            # colummns_dict["cod_etl"].append(curr_etl_code)
            df_ch_tra = pd.DataFrame(colummns_dict)
            df_ch_tra.to_sql(
                "channels_tra", ses_db_stg, if_exists="append", index=False
            )

    except:
        traceback.print_exc()
    finally:
        pass
