# -------------------------------------- #
# Imports
# -------------------------------------- #

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import datetime

import requests
import pandas as pd
import numpy as np
import string


#import json
#import os
#import zipfile as zipfile


# -------------------------------------- #
# DAG
# -------------------------------------- #

my_dag = DAG(
    dag_id='dag_PreProcess_v01',
    description='MovieReco_py',
    tags=['MovieReco', 'Pre-Process'],
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

path_raw_data = '/app/raw_data/'
path_processed_data = '/app/processed_data/'
path_reco_data = '/app/reco_data/'

imdb_base_url =  'https://datasets.imdbws.com/'
imdb_files_names = ['title.basics.tsv.gz', 'name.basics.tsv.gz', 'title.akas.tsv.gz', 'title.crew.tsv.gz', 'title.basics.tsv.gz', 'title.episode.tsv.gz', 'title.principals.tsv.gz', 'title.ratings.tsv.gz']
processed_filenames = ['title.basics_reduced.zip', 'imdb_content.csv.zip']
api_data = ['imdb_content.csv.zip']


# -------------------------------------- #
# FUNCTIONS
# -------------------------------------- #



def process_title_basics(source_path, destination_path):
    """
    description to add
    """

    # Load
    column_list = ['tconst', 'titleType', 'primaryTitle', 'startYear', 'runtimeMinutes', 'genres']
    dict_types = {'tconst':object, 'titleType':object, 'primaryTitle':object, 'startYear':int, 'runtimeMinutes':int, 'genres':object}

    df = pd.read_csv(source_path,
                            compression='zip', 
                            sep=',', 
                            usecols= column_list,
                            na_values=['\\N', 'nan', 'NA']
                            ) #, dtype=dict_types


    # Drop of rows containing errors
    runtime_errors = ['Reality-TV','Talk-Show','Documentary','Game-Show','Animation,Comedy,Family','Game-Show,Reality-TV']
    df = df[~df['runtimeMinutes'].isin(runtime_errors)] # '~' sign allows to reverse the logic of isin()


    # NANs handling
    df['runtimeMinutes'] = df['runtimeMinutes'].fillna(0)
    df = df.dropna(how='any', axis=0)


    # Discretisation of runtime
    df['runtimeMinutes'] = df['runtimeMinutes'].astype(float)

    generic_labels = list(string.ascii_uppercase)

    bins_runtime = [0.0, 10.0, 20.0, 30.0, 45.0, 60.0, 120.0, 150.0, 180.0, 9999.0]
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


    # Save
    df.to_csv(destination_path, index=False, compression="zip")

    print('title basics done')
    return 0



#----------------------------------------#

def merge_content(source_path, destination_path):
    """
    description to add
    """
    print('merge started')

    # Load
    dict_type = {'tconst':object, 'titleType':object, 'genres':object, 'runtimeCategory':object, 'yearCategory':object}

    title_basics = pd.read_csv(source_path,
        compression='zip',
        sep= ',',
        dtype=dict_type)

    # Merge
    imdb_content = title_basics

    # Temporary : NANs clean-up
    imdb_content = imdb_content.dropna(how='any', axis=0)

    # Save
    imdb_content.to_csv(destination_path, index=False, compression="zip")

    print('merge done')
        
    return 0


#----------------------------------------#

def feature_build(source_path, destination_path):
        """
        This function build the combined feature that will be used for cosine similarity
        """

        print('combined features started')

        # Load
        dict_type = {'tconst':object, 'titleType':object, 'genres':object, 'runtimeCategory':object, 'yearCategory':object}

        df = pd.read_csv(source_path,
            compression='zip',
            sep= ',',
            dtype=dict_type)
        
        # Feature build
        list_cols = ['titleType', 'genres', 'runtimeCategory', 'yearCategory']
        df['combined_features'] = df[list_cols].apply(lambda x: ' '.join(x), axis=1)

        # Save
        df.to_csv(destination_path, index=False, compression="zip")

        print('combined features done')

        return 0


# -------------------------------------- #
# TASKS
# -------------------------------------- #



task2b = PythonOperator(
    task_id='Pre-Process_titlebasics',
    op_kwargs={'source_path':path_raw_data + processed_filenames[0], 'destination_path':path_processed_data + processed_filenames[0]},
    python_callable=process_title_basics,
    dag = my_dag
)

task3 = PythonOperator(
    task_id ='merge_content',
    python_callable = merge_content,
    op_kwargs={'source_path':path_processed_data + processed_filenames[0], 'destination_path':path_processed_data + processed_filenames[-1]},
    dag = my_dag
)

task4 = PythonOperator(
    task_id='feature_build',
    python_callable=feature_build,
    op_kwargs={'source_path':path_processed_data + processed_filenames[1], 'destination_path':path_reco_data + api_data[0]},
    dag=my_dag
)


# -------------------------------------- #
# DEPENDANCIES
# -------------------------------------- #


task2b >> task3
task3 >> task4