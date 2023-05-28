# Projet Movie-Recommandation

## What is Movie-Recommandation project ?

This project is realized in team during a MLOps training provided by [DataScientest](https://datascientest.com/).
The main goal is to deploy a solution using MLOps techniques we learn during the training.

The solution consist on one API sending a list of 10 recommanded movies, based on one movie provided by the user.
Approach choosen by the team is content-based, so recommanded movies are choosen on similarites on a defined intrisec characteristic.
Similarity between movies are computed using a cosine similarity

Data are hosted in a MySQL Database, and data-processing is managed with Airflow



## Data

- [https://www.imdb.com/interfaces/](https://www.imdb.com/interfaces/)

For this project, and due to limited computer ressources, we used the table contained in the file title.basics.tsv.gz
After a pre-process, we end-up

Characteritics used are :
- tconst
- titleType
- primaryTitle
- genres
- RuntimeCategory
- YearCategory 
- combined-features : this field concatenante several fields, and after a tokenization step, will become the vector for cosine-similarity computation



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


#### Permission changes

```
sudo chmod -R 777 logs/ raw_data/ data_processed/
```

#### Airflow initialisation

Airflow requires an initialisation phase.

```
docker-compose up airflow-init
```

If everything went well, last line should display "mlops-movie-recommandation_airflow-init_1 exited with code 0"

#### Airflow start

Start of container in a detached mode
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

Within the Airflow interface, a connection has to be created for MySQL.
In Admin menu, select Connection and fill fields as shown in the screen capture below.
MySQL password is : my-secret-pw

![MySQL connection setup](./images/mysql_connection_creation.png)

