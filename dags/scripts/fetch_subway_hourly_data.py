import pandas as pd
import logging
from scripts.data_utils import (api_call, connect_to_db, table_exists, date_exists, build_table_subway, upload_df_to_db)
from airflow.models import Variable


def backfill_subway_hourly(start_date, end_date):
    """
    Description: Makes api call and fetches a portion of subway hourly dataset.
    @:param start_date str: The start of the interval which we will use to fetch data.
    @:param end_date str: The end of the interval which we will use to fetch data.
    :return: data pd.DataFrame: The portion of the whole subway hourly dataset as a pandas df.
    """
    subway_hourly_endpoint = ('https://data.ny.gov/resource/wujg-7c2s.json?'
                              f"$where=transit_timestamp between '{start_date}T00:00:00' and '{end_date}T23:59:59' &"
                              '$order="transit_timestamp DESC"&'
                              '$limit=10000000&'
                              f'$$app_token={Variable.get("app_token")}')
    data = api_call(subway_hourly_endpoint)
    data = pd.DataFrame(data)
    if data is None or data.shape[0] == 0:
        raise ValueError(f"Dataset is null or empty.")
    data = data.drop(['georeference'], axis=1)
    return data


def fetch_subway_hourly_data(**kwargs):
    """
    Description: Fetches subway_hourly dataset and uploads to db.
    :param kwargs: Utilizes airflow data_interval_start/end variables.
    """
    start_date = kwargs['data_interval_start'].to_date_string()
    end_date = kwargs['data_interval_end'].subtract(days=1).to_date_string()
    subway_df = backfill_subway_hourly(start_date, end_date)
    conn = connect_to_db()
    conn_string = (f'postgresql://{Variable.get("user")}:'
                   f'{Variable.get("password")}'
                   f'@host.docker.internal:{Variable.get("port")}/'
                   f'{Variable.get("database")}')
    table_name = kwargs['subway']
    if not table_exists(conn, table_name)[0]:
        build_table_subway(conn, table_name)
    data_in_db = date_exists(conn, table_name, 'transit_timestamp', start_date)[0]
    if not data_in_db:
        df_shape = upload_df_to_db(conn_string, table_name, subway_df)
        logging.info(f"Uploaded df with shape: {df_shape}")
    else:
        logging.info(f"Date already exists in db, no need to upload: {data_in_db}")
    conn.close()
