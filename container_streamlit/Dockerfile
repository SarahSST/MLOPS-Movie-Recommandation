FROM debian:latest

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip

RUN pip install streamlit requests

WORKDIR /home
RUN mkdir streamlit
RUN cd streamlit
RUN mkdir asset pages utils

EXPOSE 9000
ADD streamlit_app.py ./streamlit
ADD page01.py ./streamlit/pages
ADD page02.py ./streamlit/pages
CMD python3 streamlit run streamlit_app.py