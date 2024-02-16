import logging
from scripts.data_utils import (connect_to_db, table_exists, date_exists, build_table_aggregate, upload_aggregation)


def aggregate(**kwargs):
    """
    Description: Aggregates the ingested wifi_locations and subway_hourly datasets.
    :param kwargs: Utilizes airflow data_interval_start to get appropriate portion of data.
    """
    start_date = kwargs['data_interval_start'].to_date_string()
    conn = connect_to_db()
    table_name, wifi_table_name, subway_table_name = kwargs['aggregation'], kwargs['wifi'], kwargs['subway']
    if not table_exists(conn, table_name)[0]:
        build_table_aggregate(conn, table_name)
    data_in_db = date_exists(conn, table_name, 'transit_timestamp', start_date)[0]
    if not data_in_db:
        upload_aggregation(conn, start_date, table_name, wifi_table_name, subway_table_name)
    else:
        logging.info(f"Date already exists in db, no need to upload: {data_in_db}")
    conn.close()

