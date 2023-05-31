# -------------------------------------- #
# Imports
# -------------------------------------- #

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import datetime

import pandas as pd
import requests

# -------------------------------------- #
# DAG
# -------------------------------------- #

my_dag = DAG(
    dag_id='download_reduce',
    description='download_reduce',
    tags=['MovieReco', 'download'],
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


imdb_base_url =  'https://datasets.imdbws.com/'
imdb_files_names = ['title.basics.tsv.gz', 
                    'name.basics.tsv.gz', 
                    'title.akas.tsv.gz', 
                    'title.crew.tsv.gz', 
                    'title.basics.tsv.gz', 
                    'title.episode.tsv.gz', 
                    'title.principals.tsv.gz', 
                    'title.ratings.tsv.gz']


processed_filenames = ['title.basics_reduced.zip', 
                        'title_basics_processed.zip',
                        'imdb_content.csv.zip',
                        'api_data.zip']

path_raw_data = '/app/raw_data/'
path_processed_data = '/app/processed_data/'
path_reco_data = '/app/reco_data/'


# -------------------------------------- #
# FUNCTIONS
# -------------------------------------- #


def download(destination_path, url):
    r = requests.get(url, allow_redirects=True)
    open(destination_path, 'wb').write(r.content)

    return 0


def reduce_titlebasics(source_path, destination_path):
    
    # Load data
    df = pd.read_csv(source_path,
                 compression='gzip', 
                 sep='\t', 
                 na_values=['\\N', 'nan', 'NA', ' nan','  nan', '   nan']
                 ) 

    # Limitation of the data set size
    df = df[df['titleType']=='movie']
    
    # Save
    df.to_csv(destination_path, index=False, compression="zip")

    print('done')

    return 0


# -------------------------------------- #
# TASKS
# -------------------------------------- #


task1 = PythonOperator(
    task_id='download',
    python_callable=download,
    op_kwargs={'destination_path':path_raw_data + imdb_files_names[0], 'url':imdb_base_url + imdb_files_names[0]},
    dag=my_dag
)

task2 = PythonOperator(
    task_id='reduce_titlebasics',
    python_callable=reduce_titlebasics,
    op_kwargs={'source_path':path_raw_data + imdb_files_names[0], 'destination_path':path_processed_data + processed_filenames[0]},
    dag=my_dag
)

# -------------------------------------- #
# DEPENDANCIES
# -------------------------------------- #

task1 >> task2
