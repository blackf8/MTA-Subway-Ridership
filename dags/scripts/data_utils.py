import psycopg2
import traceback
import logging
import requests
from sqlalchemy import create_engine
from string import Template
from airflow.models import Variable


def connect_to_db():
    """
    Description: Connects to local PostgreSQL database using Airflow Variables.
    :return: conn psycopg2 connection obj: Returns a connection object used to interact with db.
    """
    try:
        conn = psycopg2.connect(
            host='host.docker.internal',
            database=Variable.get("database"),
            user=Variable.get("user"),
            password=Variable.get("password"),
            port=str(Variable.get("port"))
        )
        logging.info('Postgres server connection is successful')
        return conn
    except TypeError:
        traceback.print_exc()
        raise TypeError("Couldn't create the Postgres connection")


def table_exists(conn, table_name):
    """
    Description: Runs a sql query to check if the given table exists in db.
    :param conn: A connection object used to interact with db.
    :param table_name: A string that represents the table's name.
    :return: The result of the query containing True/False if table in db.
    """
    sql_path = 'dags/sql_queries/table_exists.sql'
    with open(sql_path, 'r') as fp:
        sql = fp.read()
    query = Template(sql).substitute(
        table_name=table_name,
    )
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchone()
    return result


def date_exists(conn, table_name, col, date):
    """
    Description: Checks if given data of a specific date exists in table.
    :param conn: A connection object used to interact with db.
    :param table_name: A string that represents the table's name.
    :param col: The date column we are checking.
    :param date: The date we are interested in.
    :return: Query results dictating if date exists in col.
    """
    sql_path = 'dags/sql_queries/date_exists.sql'
    with open(sql_path, 'r') as fp:
        sql = fp.read()
    query = Template(sql).substitute(
        table_name=table_name,
        col=col,
        date=date,
    )
    cur = conn.cursor()
    cur.execute(query)
    result = cur.fetchone()
    return result


def upload_aggregation(conn, start_date, table_name, wifi_table_name, subway_table_name):
    """
    Description: Utilizes params to fetch data from wifi_locations and subway_hourly tables.
                 Aggregates results and inserts it within the aggregation table having table_name.
    :param conn: A connection object used to interact with db.
    :param start_date: The date for which data we will be aggregating.
    :param table_name: A string that represents the aggregation table's name.
    :param wifi_table_name: The wifi_location table's name.
    :param subway_table_name: The subway_hourly table's name.
    """
    sql_path = 'dags/sql_queries/insert_aggregation.sql'
    with open(sql_path, 'r') as fp:
        sql = fp.read()
    query = Template(sql).substitute(
        table_name=table_name,
        start_date=start_date,
        wifi_table=wifi_table_name,
        subway_table=subway_table_name,
    )
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    cursor.close()


def build_table_wifi(conn, table_name):
    """
    Description: Creates the wifi_locations table.
    :param conn: A connection object used to interact with db.
    :param table_name: The wifi_location table's name.
    """
    sql_path = 'dags/sql_queries/create_wifi_location.sql'
    with open(sql_path, 'r') as fp:
        sql = fp.read()
    query = Template(sql).substitute(
        table_name=table_name,
    )
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    cursor.close()


def build_table_subway(conn, table_name):
    """
    Description: Creates the subway_hourly table.
    :param conn: A connection object used to interact with db.
    :param table_name: The subway_hourly table's name.
    :return:
    """
    sql_path = 'dags/sql_queries/create_subway_hourly.sql'
    with open(sql_path, 'r') as fp:
        sql = fp.read()
    query = Template(sql).substitute(
        table_name=table_name,
    )
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    cursor.close()


def build_table_aggregate(conn, table_name):
    """
    Description: Creates the aggregation table.
    :param conn: A connection object used to interact with db.
    :param table_name: The aggregation table's name.
    :return:
    """
    sql_path = 'dags/sql_queries/create_aggregation.sql'
    with open(sql_path, 'r') as fp:
        sql = fp.read()
    query = Template(sql).substitute(
        table_name=table_name,
    )
    cursor = conn.cursor()
    cursor.execute(query)
    conn.commit()
    cursor.close()


def upload_df_to_db(conn_string, table_name, df):
    """
    Descripition: Uploads a given df with data to our db.
    :param conn_string: A connection object used to interact with db.
    :param table_name: The table we are uploading to.
    :param df: The df with data we will be uploading.
    :return: The resulting shape of the dataframe after uploading.
    """
    db = create_engine(conn_string)
    conn = db.connect()
    df.to_sql(table_name, con=conn, if_exists='append', method='multi', index=False)
    return df.shape


def api_call(url):
    """
    Description: Helper method for making api calls.
    :param url: The url we will use to make a request to the api.
    :return: A response json object holding response data.
    """
    response = requests.get(url)
    response_json = response.json()
    response.close()
    return response_json
