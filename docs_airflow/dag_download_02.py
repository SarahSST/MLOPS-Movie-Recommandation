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


# -------------------------------------- #
# DAG
# -------------------------------------- #

my_dag = DAG(
    dag_id='dag_download_v02',
    description='download new data',
    tags=['MovieReco', 'Download'],
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


mysql_url = '0.0.0.0:3306'
mysql_user = 'root'
mysql_password = 'my-secret-pw'
database_name = 'db_movie'


# -------------------------------------- #
# FUNCTIONS
# -------------------------------------- #


def get_data():

    connection_url = 'mysql://{user}:{password}@{url}/{database}'.format(
        user=mysql_user,
        password=mysql_password,
        url=mysql_url,
        database = database_name
    )

    # Connection to MySQL Database
    engine = create_engine(connection_url)
    conn = engine.connect()
    inspector = inspect(engine)


    # Retrieve existing tcont in MySQL db

    stmt = text('SELECT tconst FROM table_movies')
    existing_movie_id = pd.read_sql(stmt, conn)

    controlA = existing_movie_id.shape[0]

    list_existing_movie_id = existing_movie_id['tconst'].tolist()

    # Load new movie_id only
    data_path = '../data/imdb_content.csv.zip'
    load_cols = ['tconst','primaryTitle', 'titleType', 'genres', 'runtimeCategory', 'yearCategory', 'combined_features']
    dict_type = {'tconst':object, 'titleType':object, 'genres':object, 'runtimeCategory':object, 'yearCategory':object, 'combined_features':object}

    df = pd.read_csv(data_path,
        compression = 'zip', 
        sep = ',',
        usecols = load_cols,
        dtype = dict_type)

    df = df[~df['tconst'].isin(list_existing_movie_id)] # '~' sign allows to reverse the logic of isin()
    controlB = df.shape[0]

    if controlB > 0 :
        df.to_sql('table_movies', conn, if_exists='append', index=False)


    # Check integration

    stmt = text('SELECT tconst FROM table_movies')
    existing_movie_id = pd.read_sql(stmt, conn)
    controlC = existing_movie_id.shape[0]

    conn.close()

    print('Existing movies before update : ', controlA)
    print('Added movies : ', controlB)
    print('Theorical new size : ', controlA + controlB)
    print('Existing movies after update : ', controlC)

    print('done')


    return 0



# -------------------------------------- #
# TASKS
# -------------------------------------- #

task1 = PythonOperator(
    task_id='test_mysql_connection',
    python_callable=test_mysql_connection,
    dag=my_dag
)


# -------------------------------------- #
# DEPENDANCIES
# -------------------------------------- #


