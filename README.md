# MLOPS-Movie-Recommandation

## What is this Movie-Recommandation project ?

This project aims to provide a list of 10 recommanded movies, based on one movie.
Approach choosen by the team is content-based, so recommanded movies are choosen on similarites on a defined intrisec characteristic.

Similarity between movies are computed using a cosine similarity


## Context

This project is realized in team during a MLOps training provided by [DataScientest](https://datascientest.com/).


## Data

- [https://www.imdb.com/interfaces/](https://www.imdb.com/interfaces/)

Characteritics used are :
- primaryTitle
- YearCategory 
- genres
- RuntimeCategory


## How to install ?

#### Repository clone

``` 
git clone git@github.com:SarahSST/MLOPS-Movie-Recommandation.git
```

#### Additionnal Linux packages

SQLAlchemy python package requires  mysqlclient and  mysql-connector-python packages, but both packages requires to be built, and requires some additionnales packages to be installed on linux

```
sudo apt update && apt upgrade
sudo apt install build-essential libssl-dev
sudo apt install python3-dev

sudo apt install default-libmysqlclient-dev
# if last package is not found, try this one : 
sudo apt install libmysqlclient-dev

# At last, a package to be interact direclty with MySQL
sudo apt install mysql-client-core-8.0
```

#### Additionnal Python Packages
``` bash
pip install -r requirements.txt
```


#### Permission change on logs dir

```
sudo chmod -R 777 logs/
```

#### Airflow initialisation

Airflow requires an initialisation phase.

```
docker-compose up airflow-init
```

If everything went well, last line should display "mlops-movie-recommandation_airflow-init_1 exited with code 0"

#### Airflow start

```
docker-compose up -d
```

#### SSH Tunnel initialisation

Airflow interface will be available on port 8080
FastAPI is available on port 8000

Tunnel SSH initialisation :
``` bash
ssh -i "data_enginering_machine.pem" -L 8000:localhost:8000 -L 8080:localhost:8080 ubuntu@54.73.108.184

```


#### Airflow Connection

login: airflow
password: airflow

#### MySQL connection setup

![MySQL connection setup](./images/mysql_connection_creation.png)

