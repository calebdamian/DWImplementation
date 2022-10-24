import traceback

import pandas as pd


# Db changes to be SOR
def load_channels(curr_cod_etl, ses_db_stg, ses_db_sor):
    try:
        # Dictionary of values

        chann_dict = {
            "channel_id": [],
            "channel_desc": [],
            "channel_class": [],
            "channel_class_id": [],
            "cod_etl": [],
        }

        # Read extraction table
        channel_tra = pd.read_sql(
            "SELECT channel_id,channel_desc, channel_class, channel_class_id FROM channels_tra",
            ses_db_stg,
        )

        # Processing rows
        if not channel_tra.empty:

            for ch_id, ch_desc, ch_class, ch_class_id in zip(
                    channel_tra["channel_id"],
                    channel_tra["channel_desc"],
                    channel_tra["channel_class"],
                    channel_tra["channel_class_id"],
            ):
                chann_dict["channel_id"].append(ch_id)
                chann_dict["channel_desc"].append(ch_desc)
                chann_dict["channel_class"].append(ch_class)
                chann_dict["channel_class_id"].append(ch_class_id)
                chann_dict["cod_etl"].append(curr_cod_etl)

        if chann_dict["channel_id"]:
            # Creating Dataframe
            # Persisting into db
            # Loading
            df_ch_tra = pd.DataFrame(chann_dict)
            df_ch_tra.to_sql(
                "dim_channels", ses_db_sor, if_exists="append", index=False
            )

    except:
        traceback.print_exc()
    finally:
        pass
