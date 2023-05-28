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
    dag_id='dag_download_v01',
    description='download_py',
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


def download_data_imdb(filepath, url):
    r = requests.get(url, allow_redirects=True)
    open(filepath, 'wb').write(r.content)

    return



# -------------------------------------- #
# TASKS
# -------------------------------------- #

task1 = PythonOperator(
    task_id='download_data',
    python_callable=download_data_imdb,
    op_kwargs={'filepath':path_raw_data + imdb_files_names[0], 'url':imdb_base_url + processed_filenames[0]},
    dag=my_dag
)



# -------------------------------------- #
# DEPENDANCIES
# -------------------------------------- #

