import pandas as pd


def get_surrogate_keys(table_name, business_key_col, db_context):
    natural_keys_str = ','.join(business_key_col)
    return pd.read_sql_query(f'SELECT surr_id, {natural_keys_str} FROM {table_name}', db_context).set_index(
        business_key_col).to_dict()['surr_id']


def merge_dfs_tables(table_name, business_key_col, dataframe, db_context):
    existing_table_key_pairs = get_surrogate_keys(table_name=table_name, business_key_col=business_key_col,
                                                  db_context=db_context)
    columns = dataframe.columns.tolist()
    if len(columns) > 0:
        for natural_key in business_key_col:
            columns.remove(natural_key)

    if len(business_key_col) == 1:
        dataframe['surr_id'] = dataframe.apply(
            lambda row: existing_table_key_pairs.get(*tuple(row[business_key_col].values), None), axis=1)
    else:
        dataframe['surr_id'] = dataframe.apply(
            lambda row: existing_table_key_pairs.get(tuple(row[business_key_col].values), None), axis=1)

    elements_to_update = dataframe[dataframe['surr_id'].notnull()]

    if not elements_to_update.empty:
        existing_elements = pd.read_sql_query(
            'SELECT * FROM {table_name} WHERE surr_id IN ({ids})'.format(table_name=table_name,
                                                                         ids=','.join(
                                                                             elements_to_update['surr_id'].astype(
                                                                                 str))),
            db_context)
        elements_to_update = elements_to_update.merge(existing_elements, how='outer', indicator=True).query(
            '_merge == "left_only"').drop('_merge', axis=1)
        update_query = f'UPDATE {table_name} SET {",".join([f"{col} = %s" for col in columns])} WHERE surr_id = %s'
        for index, row in elements_to_update.iterrows():
            db_context.execute(update_query, tuple(row[columns].values) + (row['surr_id'],))

    elements_to_insert = dataframe[dataframe['surr_id'].isnull()]
    if not elements_to_insert.empty:
        elements_to_insert.to_sql(table_name, db_context, if_exists='append', index=False)
