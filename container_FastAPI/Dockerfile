FROM debian:latest

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    default-libmysqlclient-dev \
    libssl-dev

RUN pip3 install mysqlclient \
    mysql-connector-python \
    sqlalchemy \
    SQLAlchemy-Utils

RUN pip3 install requests \
    pandas \
    scikit-learn \
    fastapi \
    uvicorn \
    passlib

EXPOSE 8000
WORKDIR /home
RUN mkdir log data
ADD api_moviereco.py ./
CMD uvicorn api_moviereco:api --host 0.0.0.0