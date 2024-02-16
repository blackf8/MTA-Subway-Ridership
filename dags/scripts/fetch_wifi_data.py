import pandas as pd
import logging
from scripts.data_utils import (connect_to_db, table_exists, build_table_wifi, upload_df_to_db, api_call)
from airflow.models import Variable


def fetch_wifi_locations():
    """
    Description: Makes api call and fetches wifi_locations dataset.
    :return: data pd.DataFrame: The wifi_locations dataset as a pandas df.
    """
    wifi_location_endpoint = 'https://data.ny.gov/resource/pwa9-tmie.json'
    data = api_call(wifi_location_endpoint)
    data = pd.DataFrame(data)
    if data is None or data.shape[0] == 0:
        raise ValueError(f"Dataset is null or empty.")
    data.rename(columns={':@computed_region_kjdx_g34t': 'computed_region_kjdx_g34t',
                         ':@computed_region_wbg7_3whc': 'computed_region_wbg7_3whc',
                         ':@computed_region_yamh_8v7k': 'computed_region_yamh_8v7k'}, inplace=True)
    data = data.drop(['location', 'georeference'], axis=1)
    return data


def fetch_wifi_data(**kwargs):
    """
    Description: Fetches wifi_locations dataset and uploads to db.
    """
    wifi_df = fetch_wifi_locations()
    conn = connect_to_db()
    conn_string = f'postgresql://{Variable.get("user")}:{Variable.get("password")}@host.docker.internal:{Variable.get("port")}/{Variable.get("database")}'
    table_name = kwargs['wifi']
    if not table_exists(conn, table_name)[0]:
        build_table_wifi(conn, table_name)
        df_shape = upload_df_to_db(conn_string, table_name, wifi_df)
        logging.info(f"Uploaded df with shape: {df_shape}")
    conn.close()
