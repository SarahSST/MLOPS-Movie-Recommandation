
# ---------- Imports ---------- #

from fastapi import FastAPI, status, Header, Response, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from fastapi.responses import JSONResponse
import os
import time

from fastapi import Depends, Security
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from passlib.context import CryptContext
from fastapi.security.api_key import APIKeyHeader, APIKey

import pandas as pd
import zipfile as zipfile
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, ForeignKey, MetaData, create_engine, text, inspect, select
from sqlalchemy_utils import database_exists, create_database


# ---------- MySQL Connection ---------- #


mysql_url = 'container_mysql:3306'
mysql_user = os.environ.get('MYSQL_USER')
mysql_password = os.environ.get('MYSQL_ROOT_PASSWORD')
database_name = os.environ.get('MYSQL_DATABASE')
table_users = os.environ.get('MYSQL_TABLE_USERS')
table_movies =  os.environ.get('MYSQL_TABLE_MOVIES')


# Creating the URL connection
connection_url = 'mysql://{user}:{password}@{url}/{database}'.format(
    user=mysql_user,
    password=mysql_password,
    url=mysql_url,
    database = database_name
)

# Creating the connection
mysql_engine = create_engine(connection_url, echo=True)
conn = mysql_engine.connect()
inspector = inspect(mysql_engine)


# ---------- Function definition ---------- #


def convert_df_to_json(df):
    return Response(df.to_json(orient="records"), media_type="application/json")


# ---------- Load data for recommandation ---------- #


# Load data from MySQL
stmt = 'SELECT tconst, combined_features FROM {table};'.format(table=table_movies)
df = pd.read_sql(sql=text(stmt), con=conn)


# Load users
stmt = 'SELECT * FROM {table};'.format(table=table_users)
df_users = pd.read_sql(sql=text(stmt), con=conn)


# ---------- Pydantic class ---------- #


class User(BaseModel):
    user_id: str
    username: str
    role:str
    password: str
    email: str

class Movie(BaseModel):
    tconst: str
    titleType: str
    primaryTitle: str
    startYear:int
    runtimeMinutes:int
    genres: str
    runtimeCategory: str
    yearCategory: str
    combined_features: str


# ---------- API INITIALISATION ---------- #


api = FastAPI(
    title="Movie recommendation",
    description="Content based Movie recommendation",
    version="1.5.9",
    openapi_tags=[
              {'name':'Info', 'description':'Info'},
              {'name':'MovieReco','description':'Get recommendation'}, 
              {'name':'Admin', 'description':'Staff only'} 
             ]
)


# ---------- SECURITY : ADMIN ---------- #


API_KEY = os.environ.get('API_KEY')
API_KEY_NAME = os.environ.get('API_KEY_NAME')


api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

async def get_api_key(api_key_header: str = Security(api_key_header)):
    if api_key_header == API_KEY:
        return api_key_header
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Could not validate credentials"
    )


# ---------- SECURITY : USERS ---------- #


security = HTTPBasic()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

users_b = {
    "Admin": {
        "username": "admin",
        "name": "Admin",
        'role' : ['admin'],
        "hashed_password": pwd_context.hash('Admin'),
    },
    "Daniel" : {
        "username" :  "Daniel",
        "name" : "Daniel",
        'role' : ['user'],
        "hashed_password" : pwd_context.hash('Daniel'),
    },
    "Dominique": {
        "username": "Dominique",
        "name": "Dominique",
        'role' : ['user'],
        "hashed_password": pwd_context.hash('Dominique'),
    },
    "Diane": {
        "username": "Diane",
        "name": "Diane",
        'role' : ['user'],
        "hashed_password": pwd_context.hash('Diane'),
    }
}

def get_current_user(credentials: HTTPBasicCredentials = Depends(security)):
    username = credentials.username
    if not(users_b.get(username)) or not(pwd_context.verify(credentials.password, users_b[username]['hashed_password'])):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect ID or password",
            headers={"WWW-Authenticate": "Basic"},
        )
  

# ---------- API Routes ---------- #


@api.get('/status', tags=['Info']) 
async def get_status(): 
    return 200


@api.get('/welcome',  name="Return a list of users", response_model=User, tags=['Info'])
async def get_users(username: str = Depends(get_current_user)):
    """ 
    Return the list of users
    """

    stmt = 'SELECT * FROM {table};'.format(table=table_users)
    #stmt = 'SELECT * FROM {table} WHERE user_id = {value};'.format(table=table_users, value='Diane')

    with mysql_engine.connect() as connection:
        results = connection.execute(text(stmt))

    results = [
        User(
            user_id=i[0],
            username=i[1],
            role=i[2],
            password=i[3],
            email=i[4]
            ) for i in results.fetchall()]
    
    return results[1]



@api.get('/get-film-info/{tconst:str}', name="Return information on a film" , response_model=Movie, tags=['Info'])
async def list_genres(tconst, username: str = Depends(get_current_user)):
    """ 
    Return information on a film
    """

    stmt = 'SELECT * FROM {table} WHERE tconst = {tconst};'.format(table=table_movies, tconst=tconst)

    with mysql_engine.connect() as connection:
        results = connection.execute(text(stmt))

    results = [
        Movie(
            tconst=i[0],
            titleType=i[1],
            primaryTitle=i[2],
            startYear=i[3],
            runtimeMinutes=i[4],
            genres=i[5],
            runtimeCategory=i[6],
            yearCategory=i[7],
            combined_features=i[8]
            ) for i in results.fetchall()]

    if len(results) == 0:
        raise HTTPException(
            status_code=404,
            detail='Unknown movie')
    else:
        return results[0]


@api.get('/get_recommendation/{movie_user_title:str}', name="Return a list of similar movies" , tags=['MovieReco'])
async def get_recommendation(movie_user_title:str, username: str = Depends(get_current_user)):
    """ 
    Return a list of similar movies
    """

    start_time = time.time()

    # Find movie index
    movie_user_index = df[df['tconst'] == movie_user_title].index.item()

    # Tokenization
    cv = CountVectorizer()
    count_matrix = cv.fit_transform(df["combined_features"])
    count_matrix_target = count_matrix[movie_user_index]

    # Cosine Similarity computation
    cosine_sim = cosine_similarity(count_matrix, count_matrix_target)

    # Movie Recommandation
    similar_movies = list(enumerate(cosine_sim))
    sorted_similar_movies = sorted(similar_movies, key=lambda x:x[1], reverse=True)[:10]

    list_index = []
    for e in range(len(sorted_similar_movies)):
        movie_url = sorted_similar_movies[e][0]
        #movie_url = 'https://www.imdb.com/title/'+sorted_similar_movies[e][0]
        list_index.append(movie_url)

    # Retrieve info on recommended movies
    movie_reco = df.iloc[list_index]

    list_titles = movie_reco['tconst'].tolist()
    
    end_time = time.time()
    

    # Log
    duration = end_time - start_time

    output = '''
    ============================
        Recommandation log
    ============================

    request done at "/get_recommendation"
    | username={username}

    Target movie = https://www.imdb.com/title/{movie_id}/
    Duration = {duration}s
    Recommandation=https://www.imdb.com/title/{reco}/

    '''

    output.format(movie_id=movie_user_title, duration=duration, reco=list_titles, username=username)

    if os.environ.get('LOG') == '1':
        with open('log_api.log', 'a') as file:
            file.write(output)

    return list_titles



@api.get('/get-films-list/{number:int}', name="get-films-list" , tags=['Info'])
async def list_films(number:int, username: str = Depends(get_current_user)):
    """ 
    get a list of films
    """

    stmt = 'SELECT * FROM {table} LIMIT {number};'.format(table=table_movies, number=number)

    with mysql_engine.connect() as connection:
        results = connection.execute(text(stmt))

    results = [
        Movie(
            tconst=i[0],
            titleType=i[1],
            primaryTitle=i[2],
            startYear=i[3],
            runtimeMinutes=i[4],
            genres=i[5],
            runtimeCategory=i[6],
            yearCategory=i[7],
            combined_features=i[8]
            ) for i in results.fetchall()]

    return results



@api.get('/get-tables', name="Send a list of existing tables" , tags=['Admin'])
async def get_tables(api_key_header: APIKey = Depends(get_api_key) ):
    """ 
    Send a list of existing tables in MySQL
    """

    return inspector.get_table_names()


@api.get('/get-columns-info/{TableName:str}', name="Send a list of existing columns" , tags=['Admin'])
async def get_columns(TableName:str, api_key_header: APIKey = Depends(get_api_key)) :
    """ 
    Send a list of existing columns in a given table name
    """

    return inspector.get_columns(table_name=TableName)


@api.get('/get-users-alchemy',  name="Return a list of users", response_model=User, tags=['Admin'])
async def get_users(api_key_header: APIKey = Depends(get_api_key)):
    """ 
    Return the list of users
    """

    stmt = 'SELECT * FROM {table};'.format(table=table_users)

    with mysql_engine.connect() as connection:
        results = connection.execute(text(stmt))

    results = [
        User(
            user_id=i[0],
            username=i[1],
            role=i[2],
            password=i[3],
            email=i[4]
            ) for i in results.fetchall()]
    
    return results

@api.get('/get-users-pandas',  name="Return a list of users", response_model=User, tags=['Admin'])
async def get_users(api_key_header: APIKey = Depends(get_api_key)):
    """ 
    Return a list of similar movies
    """

    return convert_df_to_json(df_users)