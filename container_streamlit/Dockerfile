FROM debian:latest

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip

RUN pip install streamlit requests

RUN mkdir app
RUN cd app
WORKDIR /app

RUN mkdir asset pages utils

EXPOSE 8501
ADD streamlit_app.py ./
ADD 1_Welcome.py ./pages
ADD 2_Movie-Reco.py ./pages

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

CMD streamlit run streamlit_app.py
