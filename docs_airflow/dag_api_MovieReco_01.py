# -------------------------------------- #
# Imports
# -------------------------------------- #

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import datetime

import requests
import json
import os
import pandas as pd
import numpy as np
import string
import zipfile as zipfile
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity


# -------------------------------------- #
# DAG
# -------------------------------------- #

my_dag = DAG(
    dag_id='dag_API_MovieReco_v01',
    description='API_MovieReco',
    tags=['MovieReco', 'API'],
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

api_data = ['imdb_content.csv.zip']


# -------------------------------------- #
# FUNCTIONS
# -------------------------------------- #

def make_moviereco(movie_user_title):

    # Load
    load_cols = ['tconst','primaryTitle', 'titleType', 'genres', 'runtimeCategory', 'yearCategory', 'combined_features']
    dict_type = {'tconst':object, 'titleType':object, 'genres':object, 'runtimeCategory':object, 'yearCategory':object, 'combined_features':object}

    df = pd.read_csv(path_reco_data+api_data[0],
        compression = 'zip', 
        sep = ',',
        usecols = load_cols,
        dtype = dict_type)


    # Find movie index
    movie_user_index = df[df.primaryTitle == movie_user_title].index.values[0]


    # Tokenization
    cv = CountVectorizer()
    count_matrix = cv.fit_transform(df["combined_features"])
    count_matrix_target = count_matrix[movie_user_index]

    # Cosine Similarity computation
    cosine_sim = cosine_similarity(count_matrix, count_matrix_target)

    # Movie Recommandation
    similar_movies = list(enumerate(cosine_sim))
    sorted_similar_movies = sorted(similar_movies, key=lambda x:x[1], reverse=True)[:10]

    print('sorted_similar_movies : ', sorted_similar_movies)

    list_index = []
    for e in range(len(sorted_similar_movies)):
        list_index.append(sorted_similar_movies[e][0])

    print('list_index  : ', list_index)

    # Retrieve info on recommended movies
    
    movie_reco = df.iloc[list_index]

    list_titles = movie_reco['tconst'].tolist()

    print(list_titles)
    
    return list_titles

# -------------------------------------- #
# TASKS
# -------------------------------------- #


task1 = PythonOperator(
    task_id='MakeRecommendation',
    python_callable=make_moviereco,
    op_kwargs={'movie_user_title' : "Chronicles of Her"},
    dag=my_dag
)


# -------------------------------------- #
# DEPENDANCIES
# -------------------------------------- #
