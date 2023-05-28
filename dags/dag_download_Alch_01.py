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
import requests

from airflow.operators.mysql_operator import MySqlOperator
from airflow.hooks.mysql_hook import MySqlHook

# -------------------------------------- #
# DAG
# -------------------------------------- #

my_dag = DAG(
    dag_id='download_bulk_data_Alch_03',
    description='download_bulk_data',
    tags=['MovieReco', 'download'],
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

imdb_base_url =  'https://datasets.imdbws.com/'
imdb_files_names = ['title.basics.tsv.gz', 
                    'name.basics.tsv.gz', 
                    'title.akas.tsv.gz', 
                    'title.crew.tsv.gz', 
                    'title.basics.tsv.gz', 
                    'title.episode.tsv.gz', 
                    'title.principals.tsv.gz', 
                    'title.ratings.tsv.gz']

processed_filenames = ['title.basics.zip', 
                        'imdb_content.csv.zip']

path_raw_data = '/app/raw_data/'
path_processed_data = '/app/processed_data/'
path_reco_data = '/app/reco_data/'

# -------------------------------------- #
# FUNCTIONS
# -------------------------------------- #

def create_table():

    # Creating the URL connection
    connection_url = 'mysql://{user}:{password}@{url}/{database}'.format(
        user=mysql_user,
        password=mysql_password,
        url=mysql_url,
        database = database_name
    )

    engine = create_engine(connection_url)
    conn = engine.connect()
    inspector = inspect(engine)

    # Table creation
    if not 'title_basics' in inspector.get_table_names():
        meta = MetaData()

        title_basics = Table(
        'title_basics', meta, 
        Column('tconst', String(15), primary_key=True), 
        Column('titleType', String(100)), 
        Column('primaryTitle', String(100)),
        Column('originalTitle', String(100)),
        Column('isAdult', String(1)),
        Column('startYear', Integer),
        Column('endYear', Integer),
        Column('runtimeMinutes', Integer),
        Column('genres',  String(300))
        ) 

        meta.create_all(engine)

    return 0


def download(filepath, url):
    r = requests.get(url, allow_redirects=True)
    open(filepath, 'wb').write(r.content)

    return 0


def bulk_load_mysql(filepath):
    
    # Creating the URL connection
    connection_url = 'mysql://{user}:{password}@{url}/{database}'.format(
        user=mysql_user,
        password=mysql_password,
        url=mysql_url,
        database = database_name
    )

    engine = create_engine(connection_url)
    conn = engine.connect()
    inspector = inspect(engine)


    # Load data
    df = pd.read_csv(filepath , compression = 'gzip', sep = ',')

    df.to_sql('title_basics', conn, if_exists='replace')

    inspector = inspect(engine)
    print('table names : ', inspector.get_table_names())

    conn.close()
    print('done')

    return 0


# -------------------------------------- #
# TASKS
# -------------------------------------- #


task1 = PythonOperator(
    task_id='create_table',
    python_callable=create_table,
    dag=my_dag
)

task2 = PythonOperator(
    task_id='download',
    python_callable=download,
    op_kwargs={'filepath':path_raw_data + imdb_files_names[0], 'url':imdb_base_url + imdb_files_names[0]},
    dag=my_dag
)

task3 = PythonOperator(
    task_id='bulk_load_mysql',
    python_callable=download,
    op_kwargs={'filepath':path_raw_data + imdb_files_names[0]},
    dag=my_dag
)

# -------------------------------------- #
# DEPENDANCIES
# -------------------------------------- #

task1 >> task2
task2 >> task3