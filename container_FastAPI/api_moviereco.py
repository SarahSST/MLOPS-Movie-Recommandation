
# ---------- Imports ---------- #

from fastapi import FastAPI 
from pydantic import BaseModel
from typing import List, Optional
from fastapi.responses import JSONResponse
import os

import pandas as pd
import zipfile as zipfile
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity

import sqlalchemy
from sqlalchemy import Table, Column, Integer, String, ForeignKey, MetaData, create_engine, text, inspect, select
from sqlalchemy_utils import database_exists, create_database


# ---------- Variables ---------- #


mysql_url = 'container_mysql:3306'
mysql_user = os.environ.get('MYSQL_USER')
mysql_password = os.environ.get('MYSQL_ROOT_PASSWORD')
database_name = os.environ.get('MYSQL_DATABASE')
table_users = os.environ.get('MYSQL_TABLE_USERS')
table_movies =  os.environ.get('MYSQL_TABLE_MOVIES')


# ---------- Function definition ---------- #

# def convert_df_to_json(df):
#      return Response(df.to_json(orient="records"), media_type="application/json")


# ---------- MySQL Connection ---------- #

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


# ---------- Load data for recommendation ---------- #

# Load data from MySQL
stmt = 'SELECT tconst, combined_features FROM {table};'.format(table=table_movies)
df = pd.read_sql(text(stmt), conn)

# ---------- Pydantic class ---------- #

class User(BaseModel):
    user_id: str
    username: str
    password: str
    email: str

class Movie(BaseModel):
    index:str
    tconst: str
    titleType: str
    primaryTitle: str
    startYear:int
    runtimeMinutes:int
    genres: str
    runtimeCategory: str
    yearCategory: str
    combined_features: str


# ---------- API initialisation ---------- #

api = FastAPI(
    title="Movie recommendation",
    description="Content based Movie recommendation",
    version="1.4.1",
    openapi_tags=[
              {'name':'Info', 'description':'Info'},
              {'name':'MovieReco','description':'Get recommendation'}, 
              {'name':'Admin', 'description':'Staff only'} 
             ]
)

# ---------- SECURITY : ADMIN ---------- #


API_KEY = "admin"
API_KEY_NAME = "admin"

api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

async def get_api_key(api_key_header: str = Security(api_key_header)):
    if api_key_header == API_KEY:
        return api_key_header
    raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Could not validate credentials"
    )

# ---------- SECURITY : USERS ---------- #




# ---------- API Routes ---------- #

@api.get('/status', tags=['Info']) 
async def get_status(): 
    return 200


@api.get('/users',  name="Return a list of users", response_model=User, tags=['Info'])
async def get_users():
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
            password=i[2],
            email=i[3]
            ) for i in results.fetchall()]
    
    return results



@api.get('/get-film-info/{tconst:str}', name="Return information on a film" , response_model=Movie, tags=['Info'])
async def list_genres(tconst):
    """ 
    Return information on a film
    """

    stmt = 'SELECT * FROM {table} WHERE tconst = {tconst};'.format(table=table_movies, tconst=tconst)

    with mysql_engine.connect() as connection:
        results = connection.execute(text(stmt))

    results = [
        Movie(
            index=i[0],
            tconst=i[1],
            titleType=i[2],
            primaryTitle=i[3],
            startYear=i[4],
            runtimeMinutes=i[5],
            genres=i[6],
            runtimeCategory=i[7],
            yearCategory=i[8],
            combined_features=i[9]
            ) for i in results.fetchall()]

    if len(results) == 0:
        raise HTTPException(
            status_code=404,
            detail='Unknown movie')
    else:
        return results[0]


@api.get('/get_recommendation/{movie_user_title:str}', name="Return a list of similar movies" , tags=['MovieReco'])
async def get_recommendation(movie_user_title:str):
    """ 
    Return a list of similar movies
    """


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
    
    return list_titles



@api.get('/get-films-list/{number:int}', name="get-films-list" , tags=['Info'])
async def list_films(number:int):
    """ 
    get a list of films
    """

    stmt = 'SELECT * FROM {table} LIMIT {number};'.format(table=table_movies, number=number)

    with mysql_engine.connect() as connection:
        results = connection.execute(text(stmt))

    results = [
        Movie(
            index=i[0],
            tconst=i[1],
            titleType=i[2],
            primaryTitle=i[3],
            startYear=i[4],
            runtimeMinutes=i[5],
            genres=i[6],
            runtimeCategory=i[7],
            yearCategory=i[8],
            combined_features=i[9]
            ) for i in results.fetchall()]

    return results



@api.get('/get-tables', name="Send a list of existing tables" , tags=['Admin'])
async def get_tables(api_key_header: APIKey = Depends(get_api_key) ):
    """ 
    Send a list of existing tables in MySQL
    """

    return inspector.get_table_names()


@api.get('/get-columns-info/{TableName:str}', name="Send a list of existing columns" , tags=['Admin'])
async def get_columns(TableName:str), api_key_header: APIKey = Depends(get_api_key) :
    """ 
    Send a list of existing columns in a given table name
    """

    return inspector.get_columns(table_name=TableName)