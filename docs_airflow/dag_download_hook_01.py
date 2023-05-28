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
    dag_id='download_bulk_data_hook_06',
    description='download_bulk_data',
    tags=['MovieReco', 'download'],
    schedule_interval=datetime.timedelta(hours=24),
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, minute=1),
    },
    catchup=False
)

# -------------------------------------- #
# Global variables
# -------------------------------------- #


imdb_base_url =  'https://datasets.imdbws.com/'
imdb_files_names = ['title.basics.tsv.gz', 
                    'name.basics.tsv.gz', 
                    'title.akas.tsv.gz', 
                    'title.crew.tsv.gz', 
                    'title.basics.tsv.gz', 
                    'title.episode.tsv.gz', 
                    'title.principals.tsv.gz', 
                    'title.ratings.tsv.gz']

# -------------------------------------- #
# FUNCTIONS
# -------------------------------------- #

def create_table():

    mysql_hook = MySqlHook(mysql_conn_id = 'MySQL_connection', schema = 'db_movie')
    connection = mysql_hook.get_conn()
    cursor = connection.cursor()

    request = "CREATE TABLE IF NOT EXISTS title_basics ( \
        tconst VARCHAR(15) PRIMARY KEY NOT NULL, \
        titleType VARCHAR(100), \
        primaryTitle  VARCHAR(100), \
        originalTitle VARCHAR(100), \
        isAdult BINARY, \
        startYear VARCHAR(15), \
        endYear  VARCHAR(15), \
        runtimeMinutes VARCHAR(15), \
        genres SET \
        )"
    
    cursor.execute(request)
    sources = cursor.fetchall()
    print(sources)

    return 0



def bulk_load():
    

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
    task_id='create_table',
    python_callable=create_table,
    dag=my_dag
)

# -------------------------------------- #
# DEPENDANCIES
# -------------------------------------- #

