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
    dag_id='dag_test_conn_hook_v01',
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




# -------------------------------------- #
# FUNCTIONS
# -------------------------------------- #



def fetch_records():
    
    mysql_hook = MySqlHook(mysql_conn_id = 'MySQL_connection', schema = 'db_movie')
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()

    request = "SELECT * FROM db_movie.table_movies LIMIT 2"
    cursor.execute(request)
    sources = cursor.fetchall()
    print(sources)

    return 0


# -------------------------------------- #
# TASKS
# -------------------------------------- #


task1 = PythonOperator(
    task_id='test_conn_hook',
    python_callable=fetch_records,
    dag=my_dag
)

# -------------------------------------- #
# DEPENDANCIES
# -------------------------------------- #

