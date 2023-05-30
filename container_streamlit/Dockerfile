FROM debian:latest

RUN apt update && apt install -y /
    apt install pip /

RUN pip install streamlit

WORKDIR /home
RUN mkdir
EXPOSE 9000
ADD streamlit_app.py ./
CMD python3 streamlit_app.py