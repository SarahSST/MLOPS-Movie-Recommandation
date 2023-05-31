# -------------------------------------- #
# Imports
# -------------------------------------- #

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import datetime

import pandas as pd
import numpy as np
import requests
import string

import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, ForeignKey, MetaData, create_engine, text, inspect
from sqlalchemy_utils import database_exists, create_database

# -------------------------------------- #
# DAG
# -------------------------------------- #

my_dag = DAG(
    dag_id='Process_Data',
    description='Process_Data',
    tags=['MovieReco', 'Process'],
    schedule_interval=datetime.timedelta(minutes=30),
    default_args={
        'owner': 'airflow',
        'start_date': days_ago(0, minute=1),
    },
    catchup=False
)

# -------------------------------------- #
# Global variables
# -------------------------------------- #


processed_filenames = ['title.basics_reduced.zip', 
                        'title_basics_processed.zip',
                        'imdb_content.csv.zip',
                        'api_data.zip']

path_raw_data = '/app/raw_data/'
path_processed_data = '/app/processed_data/'

mysql_url = 'container_mysql:3306'
mysql_user = 'root'
mysql_password = 'my-secret-pw'
database_name = 'db_movie'


# -------------------------------------- #
# FUNCTIONS
# -------------------------------------- #

def process_title_basics(source_path, destination_path):
        """
        This function clean the file title_basics, discretise some fields and reduce the size of the dataset
        """
        print('title basics started')

        # Load
        column_list = ['tconst', 'titleType', 'primaryTitle', 'startYear', 'runtimeMinutes', 'genres', 'isAdult']
        dict_types = {'tconst':object, 'titleType':object, 'primaryTitle':object, 'startYear':object, 'runtimeMinutes':object, 'genres':object, 'isAdult':object}

        df = pd.read_csv(source_path,
                                compression='zip', 
                                sep=',', 
                                usecols= column_list,
                                na_values=['\\N' , 'NA', 'nan',  ' nan','  nan', '   nan'],
                                dtype=dict_types
                                )

        print('title basics loaded')

        # Drop of rows containing NANs
        df = df.dropna(how='any', axis=0, subset=['startYear', 'runtimeMinutes', 'genres','isAdult'])

        # Drop of rows containing errors
        runtime_errors = ['Reality-TV','Talk-Show','Documentary','Game-Show','Animation,Comedy,Family','Game-Show,Reality-TV']
        df = df[~df['runtimeMinutes'].isin(runtime_errors)] # '~' sign allows to reverse the logic of isin()

        # Format change
        df['startYear']      = df['startYear'].astype('float')
        df['runtimeMinutes'] = df['runtimeMinutes'].astype('float')
        df['isAdult']        = df['isAdult'].astype('float')

        df['startYear']      = df['startYear'].apply(np.int64)
        df['runtimeMinutes'] = df['runtimeMinutes'].apply(np.int64)
        df['isAdult']        = df['isAdult'].apply(np.int64)

        df['startYear']      = df['startYear'].astype('int')
        df['runtimeMinutes'] = df['runtimeMinutes'].astype('int')
        df['isAdult']        = df['isAdult'].astype('int')


        # Limitation of the data set size
        df = df[df['startYear']>2010.0]
        df = df[df['titleType']=='movie']
        df = df[df['isAdult']==0]

        df = df.drop(columns=['isAdult'], axis=1)

        print('title basics cleaned')

        # Discretisation of runtime

        generic_labels = list(string.ascii_uppercase)

        bins_runtime = [0, 10, 20, 30, 45, 60, 120, 150, 180, 9999]
        df['runtimeCategory'] = pd.cut(x = df['runtimeMinutes'],
                                                bins = bins_runtime,
                                                labels = generic_labels[:len(bins_runtime)-1],
                                                include_lowest=True)

        df['runtimeCategory'] = df['runtimeCategory'].astype(str)


        # Discretisation of startYear
        df['startYear'] = df['startYear'].astype(int)

        bins_years = [1850, 1900, 1930, 1950, 1960, 1970, 1980, 1990, 2000, 2010, 2020, 2030]
        df['yearCategory'] = pd.cut(x = df['startYear'],
                                                bins = bins_years,
                                                labels = generic_labels[:len(bins_years)-1],
                                                include_lowest=True)

        df['yearCategory'] = df['yearCategory'].astype(str)

        print('title basics descretised')

        # Save
        df.to_csv(destination_path, index=False, compression="zip")

        print('title basics done')
        return 0


def merge_content(source_path, destination_path):
        """
        Merge of processed tables
        """
        print('merge started')

        # Load
        column_list = ['tconst', 'titleType', 'primaryTitle', 'startYear', 'runtimeMinutes', 'genres', 'runtimeCategory', 'yearCategory']
        dict_types = {'tconst':object, 'titleType':object, 'primaryTitle':object, 'startYear':int, 'runtimeMinutes':int, 'genres':object, 'runtimeCategory':object, 'yearCategory':object}

        title_basics = pd.read_csv(source_path,
            usecols= column_list,
            compression='zip',
            sep= ',',
            dtype=dict_types)

        # Merge
        imdb_content = title_basics


        # Temporary : NANs clean-up
        imdb_content = imdb_content.dropna(how='any', axis=0)

        # Save
        imdb_content.to_csv(destination_path, index=False, compression="zip")

        print('merge done')
        return 0


def feature_build(source_path, destination_path):
        """
        This function build the combined feature that will be used for cosine similarity
        """

        print('combined features started')

        # Load
        column_list = [
            'tconst', 
            'titleType', 
            'primaryTitle', 
            'startYear', 
            'runtimeMinutes', 
            'genres', 
            'runtimeCategory', 
            'yearCategory']
        dict_types = {
            'tconst':object, 
            'titleType':object, 
            'primaryTitle':object, 
            'startYear':int, 
            'runtimeMinutes':int, 
            'genres':object, 
            'runtimeCategory':object, 
            'yearCategory':object
            }

        df = pd.read_csv(source_path,
            usecols= column_list,
            compression='zip',
            sep= ',',
            dtype=dict_types)


        # Feature build
        list_cols = ['primaryTitle','titleType', 'genres', 'runtimeCategory', 'yearCategory']
        df['combined_features'] = df[list_cols].apply(lambda x: ' '.join(x), axis=1)

        # Save
        df.to_csv(destination_path, index=False, compression="zip")

        print('combined features done')

        return 0



# -------------------------------------- #
# TASKS
# -------------------------------------- #


task1 = PythonOperator(
    task_id='process_title_basics',
    python_callable=process_title_basics,
    op_kwargs={'source_path':path_processed_data + processed_filenames[0], 'destination_path':path_processed_data + processed_filenames[1]},
    dag=my_dag
)

task2 = PythonOperator(
    task_id='merge_content',
    python_callable=merge_content,
    op_kwargs={'source_path':path_processed_data + processed_filenames[1], 'destination_path':path_processed_data + processed_filenames[2]},
    dag=my_dag
)

task3 = PythonOperator(
    task_id='feature_build',
    python_callable=feature_build,
    op_kwargs={'source_path':path_processed_data + processed_filenames[2], 'destination_path':path_processed_data + processed_filenames[3]},
    dag=my_dag
)


# -------------------------------------- #
# TASKS DEPENDANCIES
# -------------------------------------- #

task1 >> task2
task2 >> task3

