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
    dag_id='dag_ReduceSize_v01',
    description='ReduceSize',
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


def reduce_title_basics(source_path, destination_path):
    """
     description to add
    """

    # Load
    column_list = ['tconst', 'titleType', 'primaryTitle', 'startYear', 'runtimeMinutes', 'genres']

    df = pd.read_csv(source_path,
                     compression='gzip', 
                     sep='\t', 
                     usecols= column_list,
                     na_values=['\\N', 'nan', 'NA']
                        ) 

    # Dropna
    df['startYear'] = df['startYear'].dropna(how='any', axis=0)


    # Format correction
    df['startYear'] = df['startYear'].astype('float')

    # Limitation of the data set size
    df = df[(df['startYear'] > 2019) & (df['startYear'] < 2022)]
    df = df[df['titleType']=='movie']

	# Save
    df.to_csv(destination_path, index=False, compression="zip")

    return 0
    

# -------------------------------------- #
# TASKS
# -------------------------------------- #



task2a = PythonOperator(
    task_id='Reduce_titlebasics',
    op_kwargs={'source_path' : path_raw_data + imdb_files_names[0], 'destination_path' : path_raw_data + processed_filenames[0]},
    python_callable=reduce_title_basics,
    dag = my_dag
)


# -------------------------------------- #
# DEPENDANCIES
# -------------------------------------- #
