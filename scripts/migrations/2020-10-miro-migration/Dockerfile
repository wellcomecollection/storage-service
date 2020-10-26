FROM python:3.8.3

ADD . /migration
WORKDIR /migration
VOLUME /migration

RUN pip install -r requirements.txt

ENTRYPOINT ["/migration/main.py"]