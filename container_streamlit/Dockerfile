FROM debian:latest

RUN apt update && apt install -y /
    apt install pip /

WORKDIR /home
RUN mkdir streamlit
RUN cd streamlit
RUN mkdir asset pages utils

RUN python -m venv .venv
RUN source .venv/bin/activate

RUN pip install streamlit requests

EXPOSE 9000
ADD streamlit_app.py ./streamlit
ADD page01.py ./streamlit/pages
ADD page02.py ./streamlit/pages
CMD python3 streamlit run streamlit_app.py