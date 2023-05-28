# -------------------------------------- #
# Imports
# -------------------------------------- #

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import datetime

import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, ForeignKey, MetaData, create_engine, text, inspect
from sqlalchemy_utils import database_exists, create_database
import pandas as pd

from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook

# -------------------------------------- #
# DAG
# -------------------------------------- #

my_dag = DAG(
    dag_id='dag_test_conn_alch_01',
    description='test MySQL connection',
    tags=['MovieReco', 'test'],
    schedule_interval=datetime.timedelta(seconds=60),
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, minute=1),
    },
    catchup=False
)

# -------------------------------------- #
# Global variables
# -------------------------------------- #


mysql_url = 'container_mysql:3306'
mysql_user = 'root'
mysql_password = 'my-secret-pw'
database_name = 'db_movie'


# -------------------------------------- #
# FUNCTIONS
# -------------------------------------- #


def test_mysql_connection():

    # Creating the URL connection
    connection_url = 'mysql://{user}:{password}@{url}/{database}'.format(
        user=mysql_user,
        password=mysql_password,
        url=mysql_url,
        database = database_name
    )

    # creating the connection
    engine = create_engine(connection_url)
    conn = engine.connect()
    inspector = inspect(engine)

    print('database_exists : ', database_exists(engine.url))
    print('table names : ', inspector.get_table_names())

    conn.close()

    return 0



# -------------------------------------- #
# TASKS
# -------------------------------------- #

task1 = PythonOperator(
    task_id='test_conn_alch',
    python_callable=test_mysql_connection,
    dag=my_dag
)



# -------------------------------------- #
# DEPENDANCIES
# -------------------------------------- #

